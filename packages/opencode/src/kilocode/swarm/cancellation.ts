// Kilo Swarm M3 — cancellation bridge.
// Durable cancellation is authoritative; this module additionally interrupts the
// already-running Kilo SessionExecution when the session belongs to the active
// worker process. A later stale worker cannot regain authority because its fence
// remains invalid.
import { Effect, Schema } from "effect"
import { sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SwarmState } from "./state"
import { SwarmClaims } from "./claims"

export namespace SwarmCancellation {
  export class NoBinding extends Schema.TaggedErrorClass<NoBinding>()("SwarmCancellation.NoBinding", { message: Schema.String }) {}

  export const request = Effect.fn("SwarmCancellation.request")(function* (input: {
    taskId: string
    actor: string
    corrId: string
  }) {
    const result = yield* SwarmState.requestCancel(input)
    const { db } = yield* Database.Service
    const binding = yield* db.get<{ session_id: string; lease_id: string; fence: number; state: string }>(sql`
      SELECT session_id, lease_id, fence, state FROM kilo_swarm_worker_binding WHERE task_id = ${input.taskId}
    `).pipe(Effect.orDie)

    if (!binding) {
      // Nothing is running. Finalize cancellation through the authoritative
      // lifecycle CAS so queued/reserved/dispatched work cannot remain stranded
      // with only a cancellation marker.
      const task = yield* SwarmState.getTask(input.taskId)
      if (task.state !== "cancelled" && !["integrated", "failed", "quarantined"].includes(task.state)) {
        yield* SwarmState.transition({ taskId: input.taskId, to: "cancelled", expectedVersion: task.version, actor: input.actor, corrId: input.corrId })
      }
      return { ...result, interrupted: false as const, cancelled: true as const }
    }

    // Cancellation may race with worker completion. Only interrupt when this
    // binding still owns the current fence; otherwise it is merely recovery data.
    const owned = yield* SwarmClaims.assertOwnership({
      taskId: input.taskId,
      leaseId: binding.lease_id,
      fence: binding.fence,
    }).pipe(Effect.as(true), Effect.catchAll(() => Effect.succeed(false)))
    if (!owned) return { ...result, interrupted: false as const, cancelled: false as const }

    const execution = yield* SessionExecution.Service
    yield* execution.interrupt(binding.session_id as any)

    // The interrupt request is process-local; the durable cancelled transition
    // is the authoritative result and also prevents stale completion from being
    // accepted by the fenced worker.
    const task = yield* SwarmState.getTask(input.taskId)
    if (task.state !== "cancelled" && task.cancel_requested !== 0) {
      yield* SwarmState.transition({ taskId: input.taskId, to: "cancelled", expectedVersion: task.version, actor: input.actor, corrId: input.corrId })
    }
    return { ...result, interrupted: true as const, cancelled: true as const, sessionId: binding.session_id }
  })
}
