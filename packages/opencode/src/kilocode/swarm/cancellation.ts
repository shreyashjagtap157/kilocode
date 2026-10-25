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
      // lifecycle CAS so queued/reserved/dispatched work cannot remain stranded.
      const task = yield* SwarmState.getTask(input.taskId)
      if (task.state !== "cancelled" && !["integrated", "failed", "quarantined"].includes(task.state)) {
        yield* SwarmState.transition({ taskId: input.taskId, to: "cancelled", expectedVersion: task.version, actor: input.actor, corrId: input.corrId })
      }
      return { ...result, interrupted: false as const, cancelled: true as const }
    }

    // Cancellation deliberately revokes normal ownership, so assertOwnership()
    // cannot be used here. Verify the binding against the durable claim/fence
    // directly, then interrupt the process-local execution if it is still the
    // admitted generation.
    const task = yield* SwarmState.getTask(input.taskId)
    const claim = yield* db.get<{ lease_id: string; fence: number; state: string }>(sql`
      SELECT lease_id, fence, state FROM kilo_swarm_claim WHERE task_id = ${input.taskId}
    `).pipe(Effect.orDie)
    const ownedBinding = !!claim && claim.lease_id === binding.lease_id && claim.fence === binding.fence && claim.state === "active" && task.fence === binding.fence
    if (!ownedBinding) return { ...result, interrupted: false as const, cancelled: false as const }

    const execution = yield* SessionExecution.Service
    yield* execution.interrupt(binding.session_id as any)

    // The interrupt request is process-local; the durable cancelled transition
    // is the authoritative result and prevents stale completion from succeeding.
    const current = yield* SwarmState.getTask(input.taskId)
    if (current.state !== "cancelled" && current.cancel_requested !== 0) {
      yield* SwarmState.transition({ taskId: input.taskId, to: "cancelled", expectedVersion: current.version, actor: input.actor, corrId: input.corrId })
    }
    return { ...result, interrupted: true as const, cancelled: true as const, sessionId: binding.session_id }
  })
}
