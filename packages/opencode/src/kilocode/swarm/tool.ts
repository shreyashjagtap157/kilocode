import { Effect, Schema } from "effect"
import * as Tool from "@/tool/tool"
import { Agent } from "@/agent/agent"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import { SwarmRuntime } from "./runtime"
import { SwarmState } from "./state"

const Worker = Schema.Struct({
  agent: Schema.String.annotate({ description: "Configured Kilo subagent name to run as this specialist." }),
  role: Schema.String.annotate({ description: "The specialist role and responsibility." }),
  prompt: Schema.String.annotate({ description: "The concrete work this specialist must perform." }),
})

const Parameters = Schema.Struct({
  workers: Schema.Array(Worker).annotate({ description: "Specialist workers to execute concurrently under one durable swarm." }),
  maxWorkers: Schema.optional(Schema.Int.pipe(Schema.greaterThanOrEqualTo(1), Schema.lessThanOrEqualTo(12))).annotate({ description: "Maximum concurrent workers." }),
  objective: Schema.String.annotate({ description: "Shared swarm objective used to frame the specialist work." }),
})

export const SwarmTool: Tool.Info<typeof Parameters> = {
  id: "swarm",
  init: () => Effect.succeed({
    description: "Run multiple configured Kilo specialist agents concurrently as a durable swarm. Use for genuinely parallelizable research, implementation, verification, or review work. Each worker receives its own child session and permission ceiling. Do not use for trivial single-agent tasks.",
    parameters: Parameters,
    execute: (args, ctx) => Effect.gen(function* () {
      if (args.workers.length === 0) return yield* Effect.fail(new Error("swarm requires at least one worker"))
      const caller = yield* Agent.get(ctx.agent)
      const sessions = yield* Session.Service
      const parent = yield* sessions.get(ctx.sessionID as SessionID)
      const swarmId = yield* SwarmState.createSwarm({ maxWorkers: args.maxWorkers ?? args.workers.length })
      const corrBase = ctx.callID ? `cor_swarm_${ctx.callID}` : `cor_swarm_${Date.now()}`
      const resolved = yield* Effect.forEach(args.workers, (worker) => Agent.get(worker.agent), { concurrency: "unbounded" })
      const planned = args.workers.map((worker, index) => ({
        swarmId,
        parentSessionId: parent.id,
        description: worker.role,
        prompt: `Shared objective: ${args.objective}\n\nSpecialist role: ${worker.role}\n\nTask: ${worker.prompt}`,
        caller,
        subagent: resolved[index],
        actor: ctx.agent,
        corrId: `${corrBase}_${index}`,
      }))
      const results = yield* SwarmRuntime.runPlannedBatch({
        workers: planned,
        concurrency: args.maxWorkers ?? args.workers.length,
      })
      return {
        title: `Swarm completed ${results.length} worker${results.length === 1 ? "" : "s"}`,
        output: [
          `Swarm objective: ${args.objective}`,
          `Workers completed: ${results.length}/${args.workers.length}`,
          ...results.map((result, index) => `${index + 1}. ${args.workers[index]?.role ?? result.agent}: task=${result.taskId}, session=${result.sessionId}`),
          "Each worker reached durable reviewing state; inspect the child sessions for detailed findings.",
        ].join("\n"),
        metadata: { swarmId, workerCount: results.length, workers: results },
      }
    }),
  }),
}
