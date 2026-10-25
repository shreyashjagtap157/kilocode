// Kilo Swarm M3 — orchestration runtime.
//
// Swarm owns durable admission/fencing; Kilo owns the actual agent/provider/tool
// execution. The runtime also provides the high-level planned-worker path so a
// caller does not have to manufacture a child session manually.
import { Effect, Schema } from "effect"
import { randomUUID } from "node:crypto"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import type { Agent } from "@/agent/agent"
import type { Config } from "@/config/config"
import { SwarmDispatcher } from "./dispatcher"
import { SwarmState } from "./state"
import { SwarmWorker } from "./worker"
import { DynamicAgentFactory } from "./factory"

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

  /** Resume an already-bound Kilo session under a new durable swarm admission. */
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

  export type PlannedInput = {
    swarmId: string
    parentSessionId: SessionID
    description: string
    caller: Agent.Info
    subagent: Agent.Info
    actor: string
    corrId: string
    mcp?: Config.Info["mcp"]
    depth?: number
    maxDepth?: number
    leaseMs?: number
    renewalIntervalMs?: number
    ownerId?: string
    idempotencyKey?: string
  }

  /**
   * Create and execute a real Kilo child session from the declarative worker
   * plan. Permission derivation is performed by DynamicAgentFactory, while
   * Session.Service remains the single authority for child-session creation.
   */
  export const runPlanned = Effect.fn("SwarmRuntime.runPlanned")(function* (input: PlannedInput) {
    const sessions = yield* Session.Service
    const parent = yield* sessions.get(input.parentSessionId)
    const plan = DynamicAgentFactory.plan({
      parentID: parent.id,
      description: input.description,
      caller: input.caller,
      subagent: input.subagent,
      session: parent,
      mcp: input.mcp,
      depth: input.depth,
      maxDepth: input.maxDepth,
    })

    const taskId = yield* SwarmState.createTask({
      swarmId: input.swarmId,
      parentId: parent.id,
      actor: input.actor,
      corrId: input.corrId,
      idemKey: input.idempotencyKey,
    })

    const child = yield* sessions.create({
      parentID: parent.id,
      title: plan.title,
      agent: plan.agent,
      model: input.subagent.model
        ? {
            id: input.subagent.model.modelID,
            providerID: input.subagent.model.providerID,
            variant: input.subagent.variant,
          }
        : undefined,
      permission: plan.permission,
    }).pipe(
      Effect.tapError(() =>
        SwarmState.requestCancel({ taskId, actor: input.actor, corrId: input.corrId }).pipe(Effect.ignore),
      ),
    )

    const ownerId = input.ownerId ?? `wrk_${randomUUID()}`
    const admission = yield* SwarmDispatcher.admit({
      taskId,
      ownerId,
      leaseMs: input.leaseMs ?? 30_000,
      actor: input.actor,
      corrId: input.corrId,
    })

    return yield* SwarmWorker.resumeSession({
      taskId,
      sessionId: child.id,
      leaseId: admission.leaseId,
      fence: admission.fence,
      actor: input.actor,
      corrId: input.corrId,
      leaseMs: input.leaseMs,
      renewalIntervalMs: input.renewalIntervalMs,
    })
  })
}
