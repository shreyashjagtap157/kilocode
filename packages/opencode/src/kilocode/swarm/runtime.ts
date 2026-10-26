// Kilo Swarm M3 — orchestration runtime.
// Swarm owns durable admission/fencing; Kilo owns the actual agent/provider/tool execution.
import { Effect, Schema } from "effect"
import { randomUUID } from "node:crypto"
import { Session } from "@/session/session"
import { SessionPrompt } from "@/session/prompt"
import { SessionID, MessageID } from "@/session/schema"
import { SessionDrain } from "@/kilocode/session/drain"
import { Config } from "@/config/config"
import { Provider } from "@/provider/provider"
import { KiloTask } from "@/kilocode/tool/task"
import { SwarmCancellation } from "./cancellation"
import type { Agent } from "@/agent/agent"
import type { Config as ConfigType } from "@/config/config"
import { SwarmDispatcher } from "./dispatcher"
import { SwarmState } from "./state"
import { SwarmWorker } from "./worker"
import { DynamicAgentFactory } from "./factory"

export namespace SwarmRuntime {
  export const MAX_WORKERS = 12
  export class InvalidInput extends Schema.TaggedErrorClass<InvalidInput>()("SwarmRuntime.InvalidInput", { message: Schema.String }) {}
  export type Input = { taskId: string; ownerId: string; sessionId: string; actor: string; corrId: string; leaseMs?: number; renewalIntervalMs?: number }

  export const runSession = Effect.fn("SwarmRuntime.runSession")(function* (input: Input) {
    if (!input.taskId.startsWith("tsk_")) return yield* Effect.fail(new InvalidInput({ message: "invalid swarm task id" }))
    if (!input.sessionId) return yield* Effect.fail(new InvalidInput({ message: "sessionId is required" }))
    const admission = yield* SwarmDispatcher.admit({ taskId: input.taskId, ownerId: input.ownerId, leaseMs: input.leaseMs ?? 30_000, actor: input.actor, corrId: input.corrId })
    yield* SwarmWorker.resumeSession({ taskId: admission.taskId, sessionId: input.sessionId, leaseId: admission.leaseId, fence: admission.fence, actor: input.actor, corrId: input.corrId, leaseMs: input.leaseMs, renewalIntervalMs: input.renewalIntervalMs })
    return { taskId: admission.taskId, sessionId: input.sessionId }
  })

  export const cancel = Effect.fn("SwarmRuntime.cancel")(function* (input: { taskId: string; actor: string; corrId: string; idemKey?: string }) {
    return yield* SwarmCancellation.request(input)
  })

  export type PlannedInput = {
    swarmId: string; parentSessionId: SessionID; description: string; prompt: string; caller: Agent.Info; subagent: Agent.Info; actor: string; corrId: string
    mcp?: ConfigType.Info["mcp"]; depth?: number; maxDepth?: number; leaseMs?: number; renewalIntervalMs?: number; ownerId?: string
    readOnly?: boolean
  }
  export type PlannedResult = { taskId: string; sessionId: string; agent: string; durationMs: number }

  export const runPlanned = Effect.fn("SwarmRuntime.runPlanned")(function* (input: PlannedInput) {
    const startedAt = Date.now()
    const sessions = yield* Session.Service
    const prompt = yield* SessionPrompt.Service
    const drain = yield* SessionDrain.Service
    const config = yield* Config.Service
    const provider = yield* Provider.Service
    const parent = yield* sessions.get(input.parentSessionId)
    if (!parent.model) return yield* Effect.fail(new InvalidInput({ message: `parent session ${parent.id} has no model` }))
    // Root sessions create root tasks. A currently executing swarm worker creates
    // true child tasks tied to its active fenced task, never to the session ID.
    const parentTaskId = yield* SwarmWorker.parentTaskForSession(parent.id)
    const plan = DynamicAgentFactory.plan({ parentID: parent.id, description: input.description, caller: input.caller, subagent: input.subagent, session: parent, mcp: input.mcp, depth: input.depth, maxDepth: input.maxDepth })
    const executionPlan = input.readOnly
      ? {
          ...plan,
          permission: DynamicAgentFactory.intersect(plan.permission, DynamicAgentFactory.readOnlyPermissions),
          canTask: false,
          canTodo: false,
        }
      : plan
    const cfg = yield* config.get()
    const resolved = yield* KiloTask.resolveModel({ name: executionPlan.agent, agent: input.subagent, config: cfg, parent: { modelID: parent.model.id, providerID: parent.model.providerID }, variant: input.subagent.variant, provider, selection: {} })
    const taskId = yield* SwarmState.createTask({ swarmId: input.swarmId, parentId: parentTaskId, actor: input.actor, corrId: input.corrId })

    const execute = Effect.gen(function* () {
      const child = yield* sessions.create({
        parentID: parent.id,
        title: executionPlan.title,
        agent: executionPlan.agent,
        model: { id: resolved.model.modelID, providerID: resolved.model.providerID, variant: resolved.variant },
        permission: executionPlan.permission,
      })
      yield* drain.link(child.id, parent.id)
      const ownerId = input.ownerId ?? `wrk_${randomUUID()}`
      const admission = yield* SwarmDispatcher.admit({ taskId, ownerId, leaseMs: input.leaseMs ?? 30_000, actor: input.actor, corrId: input.corrId })
      const work = Effect.gen(function* () {
        yield* prompt.prompt({ messageID: MessageID.ascending(), sessionID: child.id, model: { modelID: resolved.model.modelID, providerID: resolved.model.providerID }, variant: resolved.variant, agent: executionPlan.agent, tools: { question: false, ...(executionPlan.canTodo ? {} : { todowrite: false }), ...(executionPlan.canTask ? {} : { task: false }) }, parts: [{ type: "text", text: input.prompt }] })
        yield* drain.wait(child.id)
      }).pipe(Effect.asVoid)
      yield* SwarmWorker.resumeSession({ taskId, sessionId: child.id, leaseId: admission.leaseId, fence: admission.fence, actor: input.actor, corrId: input.corrId, leaseMs: input.leaseMs, renewalIntervalMs: input.renewalIntervalMs, work })
      return { taskId, sessionId: child.id, agent: executionPlan.agent, durationMs: Math.max(0, Date.now() - startedAt) }
    })
    return yield* execute.pipe(Effect.catchAll((error) => SwarmState.requestCancel({ taskId, actor: input.actor, corrId: input.corrId }).pipe(Effect.ignore, Effect.andThen(Effect.fail(error)))))
  })

  export const runPlannedBatch = Effect.fn("SwarmRuntime.runPlannedBatch")(function* (input: { workers: readonly PlannedInput[]; concurrency?: number }) {
    if (input.workers.length === 0) return []
    if (input.workers.length > MAX_WORKERS) return yield* Effect.fail(new InvalidInput({ message: `worker count cannot exceed ${MAX_WORKERS}` }))
    const concurrency = input.concurrency ?? input.workers.length
    if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > MAX_WORKERS) return yield* Effect.fail(new InvalidInput({ message: `batch concurrency must be between 1 and ${MAX_WORKERS}` }))
    return yield* Effect.all(input.workers.map((worker) => runPlanned(worker)), { concurrency })
  })

  export const runPlannedBatchSettled = Effect.fn("SwarmRuntime.runPlannedBatchSettled")(function* (input: { workers: readonly PlannedInput[]; concurrency?: number }) {
    if (input.workers.length === 0) return { completed: [], failed: [] as Array<{ index: number; error: unknown }> }
    if (input.workers.length > MAX_WORKERS) return yield* Effect.fail(new InvalidInput({ message: `worker count cannot exceed ${MAX_WORKERS}` }))
    const concurrency = input.concurrency ?? input.workers.length
    if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > MAX_WORKERS) return yield* Effect.fail(new InvalidInput({ message: `batch concurrency must be between 1 and ${MAX_WORKERS}` }))
    const outcomes = yield* Effect.forEach(input.workers, (worker) => Effect.either(runPlanned(worker)), { concurrency })
    const completed: Array<{ index: number; result: PlannedResult }> = []
    const failed: Array<{ index: number; error: unknown }> = []
    outcomes.forEach((outcome, index) => outcome._tag === "Right" ? completed.push({ index, result: outcome.right }) : failed.push({ index, error: outcome.left }))
    return { completed, failed }
  })
}
