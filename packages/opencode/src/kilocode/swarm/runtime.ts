// Kilo Swarm M3 — orchestration runtime.
// Swarm owns durable admission/fencing; Kilo owns the actual agent/provider/tool execution.
import { Effect, Schema } from "effect"
import { randomUUID } from "node:crypto"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionID, MessageID } from "@/session/schema"
import { SessionDrain } from "@/kilocode/session/drain"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { Config } from "@/config/config"
import { Provider } from "@/provider/provider"
import { KiloTask } from "@/kilocode/tool/task"
import type { Agent } from "@/agent/agent"
import type { Config as ConfigType } from "@/config/config"
import { SwarmDispatcher } from "./dispatcher"
import { SwarmState } from "./state"
import { SwarmWorker } from "./worker"
import { DynamicAgentFactory } from "./factory"

export namespace SwarmRuntime {
  export class InvalidInput extends Schema.TaggedErrorClass<InvalidInput>()("SwarmRuntime.InvalidInput", { message: Schema.String }) {}

  export type Input = { taskId: string; ownerId: string; sessionId: string; actor: string; corrId: string; leaseMs?: number; renewalIntervalMs?: number }

  export const runSession = Effect.fn("SwarmRuntime.runSession")(function* (input: Input) {
    if (!input.taskId.startsWith("tsk_")) return yield* Effect.fail(new InvalidInput({ message: "invalid swarm task id" }))
    if (!input.sessionId) return yield* Effect.fail(new InvalidInput({ message: "sessionId is required" }))
    const admission = yield* SwarmDispatcher.admit({ taskId: input.taskId, ownerId: input.ownerId, leaseMs: input.leaseMs ?? 30_000, actor: input.actor, corrId: input.corrId })
    yield* SwarmWorker.resumeSession({ taskId: admission.taskId, sessionId: input.sessionId, leaseId: admission.leaseId, fence: admission.fence, actor: input.actor, corrId: input.corrId, leaseMs: input.leaseMs, renewalIntervalMs: input.renewalIntervalMs })
    return { taskId: admission.taskId, sessionId: input.sessionId }
  })

  /** Cancel a swarm task and interrupt its actual Kilo execution when one is bound. */
  export const cancel = Effect.fn("SwarmRuntime.cancel")(function* (input: { taskId: string; actor: string; corrId: string; idemKey?: string }) {
    const task = yield* SwarmState.getTask(input.taskId)
    if (["accepted", "rejected", "integrated", "failed", "cancelled", "stale", "quarantined"].includes(task.state)) return { taskId: input.taskId, sessionId: undefined, alreadyTerminal: true }
    const result = yield* SwarmState.requestCancel(input)
    const binding = yield* SwarmWorker.getBinding(input.taskId)
    if (binding && binding.state === "bound") {
      const execution = yield* SessionExecution.Service
      yield* execution.interrupt(binding.session_id as SessionID)
    }
    const current = yield* SwarmState.getTask(input.taskId)
    if (!current.cancel_requested || current.state === "cancelled") return { taskId: input.taskId, sessionId: binding?.session_id, alreadyTerminal: result.alreadyTerminal }
    yield* SwarmState.transition({ taskId: input.taskId, to: "cancelled", expectedVersion: current.version, actor: input.actor, corrId: input.corrId })
    return { taskId: input.taskId, sessionId: binding?.session_id, alreadyTerminal: false }
  })

  export type PlannedInput = {
    swarmId: string; parentSessionId: SessionID; description: string; prompt: string; caller: Agent.Info; subagent: Agent.Info; actor: string; corrId: string
    mcp?: ConfigType.Info["mcp"]; depth?: number; maxDepth?: number; leaseMs?: number; renewalIntervalMs?: number; ownerId?: string
  }
  export type PlannedResult = { taskId: string; sessionId: string; agent: string }

  export const runPlanned = Effect.fn("SwarmRuntime.runPlanned")(function* (input: PlannedInput) {
    const sessions = yield* Session.Service
    const prompt = yield* SessionPrompt.Service
    const drain = yield* SessionDrain.Service
    const config = yield* Config.Service
    const provider = yield* Provider.Service
    const parent = yield* sessions.get(input.parentSessionId)
    if (!parent.model) return yield* Effect.fail(new InvalidInput({ message: `parent session ${parent.id} has no model` }))
    const plan = DynamicAgentFactory.plan({ parentID: parent.id, description: input.description, caller: input.caller, subagent: input.subagent, session: parent, mcp: input.mcp, depth: input.depth, maxDepth: input.maxDepth })
    const cfg = yield* config.get()
    const resolved = yield* KiloTask.resolveModel({ name: plan.agent, agent: input.subagent, config: cfg, parent: { modelID: parent.model.id, providerID: parent.model.providerID }, variant: input.subagent.variant, provider, selection: {} })
    const taskId = yield* SwarmState.createTask({ swarmId: input.swarmId, parentId: parent.id, actor: input.actor, corrId: input.corrId })

    const execute = Effect.gen(function* () {
      const child = yield* sessions.create({ parentID: parent.id, title: plan.title, agent: plan.agent, permission: plan.permission })
      yield* drain.link(child.id, parent.id)
      const ownerId = input.ownerId ?? `wrk_${randomUUID()}`
      const admission = yield* SwarmDispatcher.admit({ taskId, ownerId, leaseMs: input.leaseMs ?? 30_000, actor: input.actor, corrId: input.corrId })
      const work = Effect.gen(function* () {
        yield* prompt.prompt({ messageID: MessageID.ascending(), sessionID: child.id, model: { modelID: resolved.model.modelID, providerID: resolved.model.providerID }, variant: resolved.variant, agent: plan.agent, tools: { question: false, ...(plan.canTodo ? {} : { todowrite: false }), ...(plan.canTask ? {} : { task: false }) }, parts: [{ type: "text", text: input.prompt }] })
        yield* drain.wait(child.id)
      }).pipe(Effect.asVoid)
      yield* SwarmWorker.resumeSession({ taskId, sessionId: child.id, leaseId: admission.leaseId, fence: admission.fence, actor: input.actor, corrId: input.corrId, leaseMs: input.leaseMs, renewalIntervalMs: input.renewalIntervalMs, work })
      return { taskId, sessionId: child.id, agent: plan.agent }
    })
    return yield* execute.pipe(Effect.catchAll((error) => SwarmState.requestCancel({ taskId, actor: input.actor, corrId: input.corrId }).pipe(Effect.ignore, Effect.andThen(Effect.fail(error)))))
  })

  export const runPlannedBatch = Effect.fn("SwarmRuntime.runPlannedBatch")(function* (input: { workers: readonly PlannedInput[]; concurrency?: number }) {
    if (input.workers.length === 0) return []
    const concurrency = input.concurrency ?? input.workers.length
    if (!Number.isSafeInteger(concurrency) || concurrency < 1) return yield* Effect.fail(new InvalidInput({ message: "batch concurrency must be a positive safe integer" }))
    return yield* Effect.all(input.workers.map((worker) => runPlanned(worker)), { concurrency })
  })

  /** Run all workers independently, preserving successful workers when another worker fails. */
  export const runPlannedBatchSettled = Effect.fn("SwarmRuntime.runPlannedBatchSettled")(function* (input: { workers: readonly PlannedInput[]; concurrency?: number }) {
    if (input.workers.length === 0) return { completed: [], failed: [] as Array<{ index: number; error: unknown }> }
    const concurrency = input.concurrency ?? input.workers.length
    if (!Number.isSafeInteger(concurrency) || concurrency < 1) return yield* Effect.fail(new InvalidInput({ message: "batch concurrency must be a positive safe integer" }))
    const outcomes = yield* Effect.forEach(input.workers, (worker) => Effect.either(runPlanned(worker)), { concurrency })
    const completed: Array<{ index: number; result: PlannedResult }> = []
    const failed: Array<{ index: number; error: unknown }> = []
    outcomes.forEach((outcome, index) => outcome._tag === "Right" ? completed.push({ index, result: outcome.right }) : failed.push({ index, error: outcome.left }))
    return { completed, failed }
  })
}
