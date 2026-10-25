// Kilo Swarm M3 — execution bridge to the existing Kilo SessionExecution service.
//
// This is deliberately a bridge, not a second model runner. Session creation,
// provider selection, permissions, tool execution, and turn handling remain in
// Kilo's existing session stack. Swarm owns admission, fencing, lifecycle, and
// stale-worker protection around that execution.
import { Effect, Schema } from "effect"
import { sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SwarmDispatcher } from "./dispatcher"
import { SwarmState } from "./state"

export namespace SwarmWorker {
  export class ExecutionConflict extends Schema.TaggedErrorClass<ExecutionConflict>()("SwarmWorker.ExecutionConflict", {
    message: Schema.String,
  }) {}

  /** Persist the task/session relationship under the current lease fence. */
  export const bindSession = Effect.fn("SwarmWorker.bindSession")(function* (input: {
    taskId: string
    sessionId: string
    leaseId: string
    fence: number
  }) {
    const { db } = yield* Database.Service
    const task = yield* SwarmState.getTask(input.taskId)
    yield* SwarmDispatcher.beforeExecution({
      taskId: input.taskId,
      leaseId: input.leaseId,
      fence: input.fence,
    })
    const now = Date.now()
    yield* db.run(sql`
      INSERT INTO kilo_swarm_worker_binding
        (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
      SELECT ${input.taskId}, swarm_id, ${input.sessionId},
             (SELECT owner_id FROM kilo_swarm_claim WHERE task_id = ${input.taskId}),
             ${input.leaseId}, ${input.fence}, 'bound', ${now}, ${now}
      FROM kilo_swarm_task
      WHERE id = ${input.taskId} AND fence = ${input.fence} AND state = ${task.state}
      ON CONFLICT(task_id) DO UPDATE SET
        session_id = excluded.session_id,
        owner_id = excluded.owner_id,
        lease_id = excluded.lease_id,
        fence = excluded.fence,
        state = 'bound',
        time_updated = excluded.time_updated
    `)
    return { taskId: input.taskId, sessionId: input.sessionId, leaseId: input.leaseId, fence: input.fence }
  })

  export const getBinding = Effect.fn("SwarmWorker.getBinding")(function* (taskId: string) {
    const { db } = yield* Database.Service
    return yield* db.get<{
      task_id: string
      swarm_id: string
      session_id: string
      owner_id: string
      lease_id: string
      fence: number
      state: string
    }>(sql`
      SELECT task_id, swarm_id, session_id, owner_id, lease_id, fence, state
      FROM kilo_swarm_worker_binding WHERE task_id = ${taskId}
    `).pipe(Effect.orDie)
  })

  /**
   * Resume an already-created Kilo session under a previously admitted swarm
   * lease. Session execution remains in Kilo's existing stack.
   */
  export const resumeSession = Effect.fn("SwarmWorker.resumeSession")(function* (input: {
    taskId: string
    sessionId: string
    leaseId: string
    fence: number
    actor: string
    corrId: string
  }) {
    yield* SwarmDispatcher.beforeExecution({ taskId: input.taskId, leaseId: input.leaseId, fence: input.fence })
    const existing = yield* getBinding(input.taskId)
    if (existing && (existing.session_id !== input.sessionId || existing.lease_id !== input.leaseId || existing.fence !== input.fence)) {
      return yield* Effect.fail(new ExecutionConflict({ message: `worker binding does not match the admitted execution for ${input.taskId}` }))
    }
    if (!existing) yield* bindSession(input)

    yield* SwarmDispatcher.startExecution({
      taskId: input.taskId,
      leaseId: input.leaseId,
      fence: input.fence,
      actor: input.actor,
      corrId: input.corrId,
    })

    const execution = yield* SessionExecution.Service
    yield* execution.resume(input.sessionId as any).pipe(
      Effect.catchAll((error) =>
        Effect.gen(function* () {
          yield* SwarmDispatcher.assertCompletionOwnership({
            taskId: input.taskId,
            leaseId: input.leaseId,
            fence: input.fence,
          })
          const task = yield* SwarmState.getTask(input.taskId)
          yield* SwarmState.transition({
            taskId: input.taskId,
            to: "failed",
            expectedVersion: task.version,
            actor: input.actor,
            corrId: input.corrId,
          })
          return yield* Effect.fail(error)
        }),
      ),
    )

    yield* SwarmDispatcher.assertCompletionOwnership({
      taskId: input.taskId,
      leaseId: input.leaseId,
      fence: input.fence,
    })
    const task = yield* SwarmState.getTask(input.taskId)
    if (task.state !== "running") {
      return yield* Effect.fail(new ExecutionConflict({ message: `task ${input.taskId} changed during execution (${task.state})` }))
    }
    return yield* SwarmState.transition({
      taskId: input.taskId,
      to: "reviewing",
      expectedVersion: task.version,
      actor: input.actor,
      corrId: input.corrId,
    })
  })
}
