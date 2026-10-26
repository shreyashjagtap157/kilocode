// Kilo Swarm M3 — execution bridge to the existing Kilo SessionExecution service.
// Swarm owns admission/fencing; Kilo owns provider execution.
import { Effect, Schema } from "effect"
import { sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SwarmDispatcher } from "./dispatcher"
import { SwarmState } from "./state"
import { SwarmSupervisor } from "./supervisor"
import { SwarmWorkerBound } from "./events"

export namespace SwarmWorker {
  export class ExecutionConflict extends Schema.TaggedErrorClass<ExecutionConflict>()("SwarmWorker.ExecutionConflict", { message: Schema.String }) {}

  export const bindSession = Effect.fn("SwarmWorker.bindSession")(function* (input: {
    taskId: string
    sessionId: string
    leaseId: string
    fence: number
    corrId?: string
  }) {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const task = yield* SwarmState.getTask(input.taskId)
    yield* SwarmDispatcher.beforeExecution({ taskId: input.taskId, leaseId: input.leaseId, fence: input.fence })
    const claim = yield* db.get<{ owner_id: string }>(sql`SELECT owner_id FROM kilo_swarm_claim WHERE task_id = ${input.taskId} AND lease_id = ${input.leaseId} AND fence = ${input.fence} AND state = 'active'`).pipe(Effect.orDie)
    if (!claim) return yield* Effect.fail(new ExecutionConflict({ message: `active claim disappeared for ${input.taskId}` }))
    const existing = yield* db.get<{ session_id: string; lease_id: string; fence: number }>(sql`SELECT session_id, lease_id, fence FROM kilo_swarm_worker_binding WHERE task_id = ${input.taskId}`).pipe(Effect.orDie)
    if (existing && (existing.session_id !== input.sessionId || existing.lease_id !== input.leaseId || existing.fence !== input.fence)) return yield* Effect.fail(new ExecutionConflict({ message: `worker binding already belongs to another execution for ${input.taskId}` }))
    const now = Date.now()
    yield* events.publish(SwarmWorkerBound, {
      taskId: input.taskId as any, swarmId: task.swarm_id, sessionId: input.sessionId, ownerId: claim.owner_id,
      leaseId: input.leaseId, fence: input.fence, corrId: (input.corrId ?? "cor_swarm_bind") as any,
    }, { commit: () => Effect.gen(function* () {
      const current = yield* db.get<{ fence: number; state: string; cancel_requested: number }>(sql`SELECT fence, state, cancel_requested FROM kilo_swarm_task WHERE id = ${input.taskId}`)
      if (!current || current.fence !== input.fence || current.state !== task.state || current.cancel_requested !== 0) return yield* Effect.die(new ExecutionConflict({ message: `task changed before worker binding commit: ${input.taskId}` }))
      yield* db.run(sql`INSERT INTO kilo_swarm_worker_binding (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated) VALUES (${input.taskId}, ${task.swarm_id}, ${input.sessionId}, ${claim.owner_id}, ${input.leaseId}, ${input.fence}, 'bound', ${now}, ${now}) ON CONFLICT(task_id) DO UPDATE SET session_id = excluded.session_id, owner_id = excluded.owner_id, lease_id = excluded.lease_id, fence = excluded.fence, state = 'bound', time_updated = excluded.time_updated`)
    }).pipe(Effect.orDie) })
    return { taskId: input.taskId, sessionId: input.sessionId, leaseId: input.leaseId, fence: input.fence }
  })

  export const getBinding = Effect.fn("SwarmWorker.getBinding")(function* (taskId: string) {
    const { db } = yield* Database.Service
    return yield* db.get<{ task_id: string; swarm_id: string; session_id: string; owner_id: string; lease_id: string; fence: number; state: string }>(sql`SELECT task_id, swarm_id, session_id, owner_id, lease_id, fence, state FROM kilo_swarm_worker_binding WHERE task_id = ${taskId}`).pipe(Effect.orDie)
  })

  /** Resume an admitted session while a lease supervisor renews ownership. */
  export const resumeSession = Effect.fn("SwarmWorker.resumeSession")(function* (input: {
    taskId: string
    sessionId: string
    leaseId: string
    fence: number
    actor: string
    corrId: string
    leaseMs?: number
    renewalIntervalMs?: number
    /** Optional work executed after durable admission and before the normal session resume. */
    prepare?: Effect.Effect<void>
    /** Optional real session-driving work. When supplied, this must include the provider-run drain/wait. */
    work?: Effect.Effect<void>
  }) {
    yield* SwarmDispatcher.beforeExecution({ taskId: input.taskId, leaseId: input.leaseId, fence: input.fence })
    const existing = yield* getBinding(input.taskId)
    if (existing && (existing.session_id !== input.sessionId || existing.lease_id !== input.leaseId || existing.fence !== input.fence)) return yield* Effect.fail(new ExecutionConflict({ message: `worker binding does not match admitted execution for ${input.taskId}` }))
    if (!existing) yield* bindSession(input)
    yield* SwarmDispatcher.startExecution({ taskId: input.taskId, leaseId: input.leaseId, fence: input.fence, actor: input.actor, corrId: input.corrId })

    const execution = yield* SessionExecution.Service
    const leaseMs = input.leaseMs ?? 30_000
    const renewalIntervalMs = input.renewalIntervalMs ?? Math.max(1_000, Math.floor(leaseMs / 3))
    const work = input.work ?? Effect.gen(function* () {
      if (input.prepare) yield* input.prepare
      yield* execution.resume(input.sessionId as any)
    })
    return yield* SwarmSupervisor.supervise(
      { taskId: input.taskId, leaseId: input.leaseId, fence: input.fence, leaseMs, intervalMs: renewalIntervalMs },
      work,
    ).pipe(
      Effect.catchAll((error) => Effect.gen(function* () {
        yield* SwarmDispatcher.assertCompletionOwnership({ taskId: input.taskId, leaseId: input.leaseId, fence: input.fence }).pipe(Effect.catchAll(() => Effect.void))
        const task = yield* SwarmState.getTask(input.taskId)
        if (task.state === "running" && task.cancel_requested === 0) {
          yield* SwarmState.transition({ taskId: input.taskId, to: "failed", expectedVersion: task.version, actor: input.actor, corrId: input.corrId }).pipe(Effect.catchAll(() => Effect.void))
        }
        return yield* Effect.fail(error)
      })),
    ).pipe(
      Effect.tap(() => Effect.gen(function* () {
        yield* SwarmDispatcher.assertCompletionOwnership({ taskId: input.taskId, leaseId: input.leaseId, fence: input.fence })
        const task = yield* SwarmState.getTask(input.taskId)
        if (task.state !== "running") return yield* Effect.fail(new ExecutionConflict({ message: `task ${input.taskId} changed during execution (${task.state})` }))
        yield* SwarmState.transition({ taskId: input.taskId, to: "reviewing", expectedVersion: task.version, actor: input.actor, corrId: input.corrId })
      })),
    )
  })
}
