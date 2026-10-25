// Kilo Swarm M3 — orchestration runtime.
//
// This is the first cohesive runtime entry point: it admits a queued task,
// binds a Kilo session, and runs the existing SessionExecution service under
// durable swarm fencing/lease supervision. It intentionally does not create a
// second provider runtime.
import { Effect, Schema } from "effect"
import { SwarmDispatcher } from "./dispatcher"
import { SwarmWorker } from "./worker"

export namespace SwarmRuntime {
  export class InvalidInput extends Schema.TaggedErrorClass<InvalidInput>()("SwarmRuntime.InvalidInput", {
    message: Schema.String,
  }) {}

  export type Input = {
    taskId: string
    ownerId: string
    sessionId: string
    actor: string
    corrId: string
    leaseMs?: number
    renewalIntervalMs?: number
  }

  /**
   * Run one already-created Kilo session as a swarm worker. Session creation is
   * deliberately kept outside this primitive until the caller can supply the
   * real Kilo session-creation context (project/workspace/model/permission).
   */
  export const runSession = Effect.fn("SwarmRuntime.runSession")(function* (input: Input) {
    if (!input.taskId.startsWith("tsk_")) return yield* Effect.fail(new InvalidInput({ message: "invalid swarm task id" }))
    if (!input.sessionId) return yield* Effect.fail(new InvalidInput({ message: "sessionId is required" }))
    const admission = yield* SwarmDispatcher.admit({
      taskId: input.taskId,
      ownerId: input.ownerId,
      leaseMs: input.leaseMs ?? 30_000,
      actor: input.actor,
      corrId: input.corrId,
    })
    return yield* SwarmWorker.resumeSession({
      taskId: admission.taskId,
      sessionId: input.sessionId,
      leaseId: admission.leaseId,
      fence: admission.fence,
      actor: input.actor,
      corrId: input.corrId,
      leaseMs: input.leaseMs,
      renewalIntervalMs: input.renewalIntervalMs,
    })
  })
}
