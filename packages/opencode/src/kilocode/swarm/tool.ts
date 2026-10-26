import { Effect, Schema } from "effect"
import * as Tool from "@/tool/tool"
import { Agent } from "@/agent/agent"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import * as MessageV2 from "@/session/message-v2"
import { BoardStore } from "@/kilocode/board/store"
import { SwarmRuntime } from "./runtime"
import { SwarmState } from "./state"
import { DynamicAgentFactory } from "./factory"

const Worker = Schema.Struct({ agent: Schema.String, role: Schema.String, prompt: Schema.String })
const Parameters = Schema.Struct({
  workers: Schema.Array(Worker),
  maxWorkers: Schema.optional(Schema.Int.pipe(Schema.greaterThanOrEqualTo(1), Schema.lessThanOrEqualTo(12))),
  objective: Schema.String,
})

function finding(messages: MessageV2.WithParts[]) {
  const latest = MessageV2.latest(messages).finished ?? MessageV2.latest(messages).assistant
  if (!latest) return "No assistant finding was persisted."
  const message = messages.find((item) => item.info.id === latest.id)
  if (!message) return "No assistant finding was persisted."
  return message.parts.filter((part) => part.type === "text").map((part) => part.text).join("\n").trim() || "No textual assistant finding was persisted."
}

function errorText(error: unknown) {
  return error instanceof Error ? error.message : typeof error === "string" ? error : String(error)
}

export const SwarmTool: Tool.Info<typeof Parameters> = {
  id: "swarm",
  init: () => Effect.succeed({
    description: "Run multiple configured Kilo specialist agents concurrently as a durable team swarm. Workers share Kilo's persistent session board so they can read team context, post findings, ask questions, and coordinate with peers while the parent synthesizes the result.",
    parameters: Parameters,
    execute: (args, ctx) => Effect.gen(function* () {
      if (args.workers.length === 0) return yield* Effect.fail(new Error("swarm requires at least one worker"))
      if (args.workers.length > 12) return yield* Effect.fail(new Error("swarm supports at most 12 workers per invocation"))
      const caller = yield* Agent.get(ctx.agent)
      const sessions = yield* Session.Service
      const parent = yield* sessions.get(ctx.sessionID as SessionID)
      const resolved = yield* Effect.forEach(args.workers, (worker) => Agent.get(worker.agent), { concurrency: "unbounded" })
      for (const [index, subagent] of resolved.entries()) {
        DynamicAgentFactory.plan({ parentID: parent.id, description: args.workers[index]?.role ?? subagent.name, caller, subagent, session: parent })
      }

      // Kilo's shared agent board is enabled by default and follows the parent
      // session's ancestry. Initialize it before creating workers so every child
      // session sees the same objective and can communicate with its peers.
      yield* BoardStore.scope(parent.id as SessionID)
      const base = ctx.callID ? `cor_swarm_${ctx.callID}` : `cor_swarm_${Date.now()}`
      yield* BoardStore.post({
        sessionID: parent.id as SessionID,
        messageID: `${base}_objective`,
        callID: base,
        to: "ALL",
        type: "INFO",
        body: `SWARM OBJECTIVE: ${args.objective}\nWorkers: ${args.workers.map((worker) => `${worker.role} (${worker.agent})`).join(", ")}\nTeam protocol: read shared board context before acting; post useful discoveries, blockers, questions, and verified findings for peers; avoid duplicating work already reported by another worker.`,
      })

      const swarmId = yield* SwarmState.createSwarm({ maxWorkers: args.maxWorkers ?? args.workers.length })
      const planned = args.workers.map((worker, index) => ({
        swarmId,
        parentSessionId: parent.id,
        description: worker.role,
        prompt: [
          `Shared objective: ${args.objective}`,
          `Role: ${worker.role}`,
          `Task: ${worker.prompt}`,
          "",
          "TEAM COORDINATION PROTOCOL:",
          "You are a member of a persistent Kilo agent team. Before doing substantive work, inspect the shared agent board for the objective and peer progress when the board tools are available.",
          "During work, post concise discoveries, blockers, questions, and verified findings to the shared board so other workers can build on them. Do not merely wait for the parent; communicate useful intermediate results to teammates.",
          "If another worker has already solved part of your task, build on its finding instead of duplicating the same investigation. If you disagree, post the evidence and explain the contradiction.",
          "Before finishing, post your most important verified finding and any remaining uncertainty to the shared board. Your final response should still contain your complete role-specific result for the parent.",
        ].join("\n"),
        caller,
        subagent: resolved[index],
        actor: ctx.agent,
        corrId: `${base}_${index}` as any,
      }))
      const batch = yield* SwarmRuntime.runPlannedBatchSettled({ workers: planned, concurrency: args.maxWorkers ?? args.workers.length })
      const reports = yield* Effect.forEach(batch.completed, (entry) => MessageV2.stream(entry.result.sessionId as SessionID), { concurrency: "unbounded" })
      const entries = batch.completed.map((entry, reportIndex) => ({
        role: args.workers[entry.index]?.role ?? entry.result.agent,
        taskId: entry.result.taskId,
        sessionId: entry.result.sessionId,
        durationMs: entry.result.durationMs,
        finding: finding(reports[reportIndex] ?? []),
      }))
      const failures = batch.failed.map((entry) => ({ role: args.workers[entry.index]?.role ?? "worker", error: errorText(entry.error) }))
      const board = yield* BoardStore.read({ sessionID: parent.id as SessionID, limit: 50 })
      const communications = board.messages
        .filter((message) => message.from !== "main")
        .map((message) => ({ from: message.fromLabel ?? message.from, to: message.toLabel ?? message.to, type: message.type, body: message.body, id: message.id }))
      return {
        title: `Swarm completed ${entries.length}/${args.workers.length} workers`,
        output: [
          `Swarm objective: ${args.objective}`,
          `Workers completed: ${entries.length}/${args.workers.length}`,
          `Team communications: ${communications.length}`,
          ...entries.flatMap((entry, index) => [`\n## ${index + 1}. ${entry.role}`, entry.finding, `(task=${entry.taskId}, session=${entry.sessionId}, duration=${entry.durationMs}ms)`]),
          ...(communications.length > 0 ? ["\n## Team communication", ...communications.map((message) => `- ${message.from} → ${message.to} [${message.type}]: ${message.body}`)] : []),
          ...failures.flatMap((entry) => [`\n## FAILED: ${entry.role}`, entry.error]),
        ].join("\n"),
        metadata: { swarmId, workerCount: entries.length, failedWorkerCount: failures.length, workers: entries, failures, communications },
      }
    }),
  }),
}
