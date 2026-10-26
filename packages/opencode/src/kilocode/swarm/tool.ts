import { Effect, Schema } from "effect"
import * as Tool from "@/tool/tool"
import { Agent } from "@/agent/agent"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import * as MessageV2 from "@/session/message-v2"
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
    description: "Run multiple configured Kilo specialist agents concurrently as a durable swarm. Use this for independent research, implementation, verification, or review work that benefits from parallel specialist perspectives; synthesize the returned findings in the parent agent.",
    parameters: Parameters,
    execute: (args, ctx) => Effect.gen(function* () {
      if (args.workers.length === 0) return yield* Effect.fail(new Error("swarm requires at least one worker"))
      const caller = yield* Agent.get(ctx.agent)
      const sessions = yield* Session.Service
      const parent = yield* sessions.get(ctx.sessionID as SessionID)
      const resolved = yield* Effect.forEach(args.workers, (worker) => Agent.get(worker.agent), { concurrency: "unbounded" })
      for (const [index, subagent] of resolved.entries()) {
        DynamicAgentFactory.plan({ parentID: parent.id, description: args.workers[index]?.role ?? subagent.name, caller, subagent, session: parent })
      }
      const swarmId = yield* SwarmState.createSwarm({ maxWorkers: args.maxWorkers ?? args.workers.length })
      const base = ctx.callID ? `cor_swarm_${ctx.callID}` : `cor_swarm_${Date.now()}`
      const planned = args.workers.map((worker, index) => ({
        swarmId,
        parentSessionId: parent.id,
        description: worker.role,
        prompt: `Shared objective: ${args.objective}\n\nRole: ${worker.role}\n\nTask: ${worker.prompt}`,
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
        finding: finding(reports[reportIndex] ?? []),
      }))
      const failures = batch.failed.map((entry) => ({ role: args.workers[entry.index]?.role ?? "worker", error: errorText(entry.error) }))
      return {
        title: `Swarm completed ${entries.length}/${args.workers.length} workers`,
        output: [
          `Swarm objective: ${args.objective}`,
          `Workers completed: ${entries.length}/${args.workers.length}`,
          ...entries.flatMap((entry, index) => [`\n## ${index + 1}. ${entry.role}`, entry.finding, `(task=${entry.taskId}, session=${entry.sessionId})`]),
          ...failures.flatMap((entry) => [`\n## FAILED: ${entry.role}`, entry.error]),
        ].join("\n"),
        metadata: { swarmId, workerCount: entries.length, failedWorkerCount: failures.length, workers: entries, failures },
      }
    }),
  }),
}
