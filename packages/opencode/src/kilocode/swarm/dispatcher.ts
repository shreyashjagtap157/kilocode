// Kilo Swarm M3 — durable dispatch admission boundary.
//
// This module deliberately separates durable admission from provider execution.
// The caller owns the actual worker invocation and must keep the returned fence
// and lease identity attached to every completion/update. A worker that loses
// ownership must never commit a result.
import { Effect, Schema } from "effect"
import { SwarmClaims } from "./claims"
import { SwarmState } from "./state"

export namespace SwarmDispatcher {
  export class DispatchConflict extends Schema.TaggedErrorClass<DispatchConflict>()("SwarmDispatcher.DispatchConflict", {
    message: Schema.String,
  }) {}

  export type Admission = {
    taskId: string
    swarmId: string
    ownerId: string
    leaseId: string
    fence: number
    expiresAt: number
    corrId: string
  }

  /**
   * Admit exactly one queued task to one worker.
   *
   * Ordering is intentional:
   *   1. durable claim/fence admission;
   *   2. queued -> reserved;
   *   3. reserved -> dispatched.
   *
   * If the process dies between these operations, no provider invocation has
   * been started by this module. The durable claim can expire and recovery can
   * reconcile it without inventing a result. Execution must only begin after
   * this method returns successfully.
   */
  export const admit = Effect.fn("SwarmDispatcher.admit")(function* (input: {
    taskId: string
    ownerId: string
    leaseMs: number
    actor: string
    corrId: string
  }) {
    const task = yield* SwarmState.getTask(input.taskId)
    if (task.state !== "queued") {
      return yield* Effect.fail(new DispatchConflict({ message: `task ${input.taskId} is not queued (${task.state})` }))
    }

    const claim = yield* SwarmClaims.claim(input)

    try {
      const reserved = yield* SwarmState.transition({
        taskId: input.taskId,
        to: "reserved",
        expectedVersion: task.version,
        actor: input.actor,
        corrId: input.corrId,
      })

      const dispatched = yield* SwarmState.transition({
        taskId: input.taskId,
        to: "dispatched",
        expectedVersion: reserved.version,
        actor: input.actor,
        corrId: input.corrId,
      })

      return {
        taskId: input.taskId,
        swarmId: task.swarm_id,
        ownerId: claim.ownerId,
        leaseId: claim.leaseId,
        fence: claim.fence,
        expiresAt: claim.expiresAt,
        corrId: input.corrId,
        version: dispatched.version,
      }
    } catch (error) {
      // No provider execution has started. Release the durable claim so a retry
      // can safely re-admit the task. The release itself is fenced.
      yield* SwarmClaims.release({
        taskId: input.taskId,
        leaseId: claim.leaseId,
        fence: claim.fence,
        corrId: input.corrId,
      }).pipe(Effect.catchAll(() => Effect.void))
      return yield* Effect.fail(error)
    }
  })

  /** Check durable ownership immediately before provider execution begins. */
  export const beforeExecution = Effect.fn("SwarmDispatcher.beforeExecution")(function* (input: {
    taskId: string
    leaseId: string
    fence: number
  }) {
    const task = yield* SwarmState.getTask(input.taskId)
    if (task.state !== "dispatched") {
      return yield* Effect.fail(new DispatchConflict({ message: `task ${input.taskId} is not dispatched (${task.state})` }))
    }
    yield* SwarmClaims.assertOwnership(input)
    return task
  })

  /**
   * Fenced completion gate. Callers must invoke this before persisting any
   * worker result. Provider output is never treated as ownership authority.
   */
  export const assertCompletionOwnership = Effect.fn("SwarmDispatcher.assertCompletionOwnership")(function* (input: {
    taskId: string
    leaseId: string
    fence: number
  }) {
    yield* SwarmClaims.assertOwnership(input)
    const task = yield* SwarmState.getTask(input.taskId)
    if (task.state === "cancelled" || task.cancel_requested !== 0) {
      return yield* Effect.fail(new DispatchConflict({ message: `task ${input.taskId} was cancelled` }))
    }
    return task
  })
}
