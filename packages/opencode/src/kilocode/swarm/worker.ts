// Kilo Swarm M3 — execution bridge to the existing Kilo SessionExecution service.
//
// This is deliberately a bridge, not a second model runner. Session creation,
// provider selection, permissions, tool execution, and turn handling remain in
// Kilo's existing session stack. Swarm owns admission, fencing, lifecycle, and
// stale-worker protection around that execution.
import { Effect, Schema } from "effect"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SwarmDispatcher } from "./dispatcher"
import { SwarmState } from "./state"

export namespace SwarmWorker {
  export class ExecutionConflict extends Schema.TaggedErrorClass<ExecutionConflict>()("SwarmWorker.ExecutionConflict", {
    message: Schema.String,
  }) {}

  /**
   * Resume an already-created Kilo session under a previously admitted swarm
   * lease. Session creation, provider selection, permissions, and tool
   * execution remain in Kilo's existing session stack.
   *
   * A session ID is required from the caller because M3 does not yet invent a
   * durable task-payload/session mapping. This keeps the bridge honest instead
   * of guessing which session a task should execute.
   */
  export const resumeSession = Effect.fn("SwarmWorker.resumeSession")(function* (input: {
    taskId: string
    sessionId: string
    leaseId: string
    fence: number
    actor: string
    corrId: string
  }) {
    yield* SwarmDispatcher.beforeExecution({
      taskId: input.taskId,
      leaseId: input.leaseId,
      fence: input.fence,
    })

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
          // Failure is committed only while this worker still owns the fence.
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

    // A successful SessionExecution is only allowed to publish the next swarm
    // lifecycle state while the same lease/fence remains authoritative.
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
