// Kilo Swarm M3 — durable dispatch admission boundary.
//
// This module separates durable admission from provider execution. The caller
// owns the actual worker invocation and must keep the returned lease identity
// and fence attached to every lifecycle update and completion.
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

  export const admit = Effect.fn("SwarmDispatcher.admit")(function* (input: {
    taskId: string
    ownerId: string
    leaseMs: number
    actor: string
    corrId: string
  }) {
    const initial = yield* SwarmState.getTask(input.taskId)
    if (initial.state !== "queued") {
      return yield* Effect.fail(new DispatchConflict({ message: `task ${input.taskId} is not queued (${initial.state})` }))
    }

    const claim = yield* SwarmClaims.claim(input)
    try {
      // Claim admission advances the task fence without advancing its lifecycle
      // version. Refresh before the transition so the CAS binds the transition
      // to the newly admitted fence generation rather than the pre-claim fence.
      const admitted = yield* SwarmState.getTask(input.taskId)
      if (admitted.state !== "queued" || admitted.version !== initial.version || admitted.fence !== claim.fence) {
        return yield* Effect.fail(new DispatchConflict({ message: `task ${input.taskId} changed during claim admission` }))
      }

      const reserved = yield* SwarmState.transition({
        taskId: input.taskId,
        to: "reserved",
        expectedVersion: admitted.version,
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
        swarmId: admitted.swarm_id,
        ownerId: claim.ownerId,
        leaseId: claim.leaseId,
        fence: claim.fence,
        expiresAt: claim.expiresAt,
        corrId: input.corrId,
        version: dispatched.version,
      }
    } catch (error) {
      yield* SwarmClaims.release({
        taskId: input.taskId,
        leaseId: claim.leaseId,
        fence: claim.fence,
        corrId: input.corrId,
      }).pipe(Effect.catchAll(() => Effect.void))
      return yield* Effect.fail(error)
    }
  })

  /** Revalidate ownership and atomically advance dispatched -> running. */
  export const startExecution = Effect.fn("SwarmDispatcher.startExecution")(function* (input: {
    taskId: string
    leaseId: string
    fence: number
    actor: string
    corrId: string
  }) {
    yield* SwarmClaims.assertOwnership(input)
    const task = yield* SwarmState.getTask(input.taskId)
    if (task.state !== "dispatched") {
      return yield* Effect.fail(new DispatchConflict({ message: `task ${input.taskId} is not dispatched (${task.state})` }))
    }
    return yield* SwarmState.transition({
      taskId: input.taskId,
      to: "running",
      expectedVersion: task.version,
      actor: input.actor,
      corrId: input.corrId,
    })
  })

  /** Check durable ownership immediately before provider execution begins. */
  export const beforeExecution = Effect.fn("SwarmDispatcher.beforeExecution")(function* (input: {
    taskId: string
    leaseId: string
    fence: number
  }) {
    const task = yield* SwarmState.getTask(input.taskId)
    if (task.state !== "dispatched" && task.state !== "running") {
      return yield* Effect.fail(new DispatchConflict({ message: `task ${input.taskId} is not executable (${task.state})` }))
    }
    yield* SwarmClaims.assertOwnership(input)
    return task
  })

  /** Fenced completion gate. Provider output never grants ownership. */
  export const assertCompletionOwnership = Effect.fn("SwarmDispatcher.assertCompletionOwnership")(function* (input: {
    taskId: string
    leaseId: string
    fence: number
  }) {
    yield* SwarmClaims.assertOwnership(input)
    const task = yield* SwarmState.getTask(input.taskId)
    if (task.state !== "running" || task.cancel_requested !== 0) {
      return yield* Effect.fail(new DispatchConflict({ message: `task ${input.taskId} is not completable (${task.state})` }))
    }
    return task
  })
}
