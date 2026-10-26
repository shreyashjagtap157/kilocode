import { Effect, Schema } from "effect"
import * as Tool from "@/tool/tool"
import { Agent } from "@/agent/agent"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import * as MessageV2 from "@/session/message-v2"
import { SwarmRuntime } from "./runtime"
import { SwarmState } from "./state"

const Worker = Schema.Struct({
  agent: Schema.String,
  role: Schema.String,
  prompt: Schema.String,
})
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

export const SwarmTool: Tool.Info<typeof Parameters> = {
  id: "swarm",
  init: () => Effect.succeed({
    description: "Run multiple configured Kilo specialist agents concurrently as a durable swarm and return each worker's latest persisted finding.",
    parameters: Parameters,
    execute: (args, ctx) => Effect.gen(function* () {
      if (args.workers.length === 0) return yield* Effect.fail(new Error("swarm requires at least one worker"))
      const caller = yield* Agent.get(ctx.agent)
      const sessions = yield* Session.Service
      const parent = yield* sessions.get(ctx.sessionID as SessionID)
      const swarmId = yield* SwarmState.createSwarm({ maxWorkers: args.maxWorkers ?? args.workers.length })
      const base = ctx.callID ? `cor_swarm_${ctx.callID}` : `cor_swarm_${Date.now()}`
      const resolved = yield* Effect.forEach(args.workers, (worker) => Agent.get(worker.agent), { concurrency: "unbounded" })
      const planned = args.workers.map((worker, index) => ({ swarmId, parentSessionId: parent.id, description: worker.role, prompt: `Shared objective: ${args.objective}\n\nRole: ${worker.role}\n\nTask: ${worker.prompt}`, caller, subagent: resolved[index], actor: ctx.agent, corrId: `${base}_${index}` }))
      const results = yield* SwarmRuntime.runPlannedBatch({ workers: planned, concurrency: args.maxWorkers ?? args.workers.length })
      const reports = yield* Effect.forEach(results, (result) => MessageV2.stream(result.sessionId as SessionID), { concurrency: "unbounded" })
      const entries = results.map((result, index) => ({ role: args.workers[index]?.role ?? result.agent, taskId: result.taskId, sessionId: result.sessionId, finding: finding(reports[index] ?? []) }))
      return {
        title: `Swarm completed ${entries.length} workers`,
        output: [`Swarm objective: ${args.objective}`, `Workers completed: ${entries.length}/${args.workers.length}`, ...entries.flatMap((entry, index) => [`\n## ${index + 1}. ${entry.role}`, entry.finding, `(task=${entry.taskId}, session=${entry.sessionId})`])].join("\n"),
        metadata: { swarmId, workerCount: entries.length, workers: entries },
      }
    }),
  }),
}
