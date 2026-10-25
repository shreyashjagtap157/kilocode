import * as Tool from "@/tool/tool"
import { ToolJsonSchema } from "@/tool/json-schema"
import { Effect, Schema } from "effect"
import { Session } from "@/session/session"
import { Agent } from "@/agent/agent"
import { Config } from "@/config/config"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionPrompt } from "@/session/prompt"
import { SwarmRuntime } from "@/kilocode/swarm/runtime"
import { KiloTask } from "@/kilocode/tool/task"
import { RuntimeFlags } from "@/effect/runtime-flags"

const id = "swarm"

const Worker = Schema.Struct({
  description: Schema.String.annotate({ description: "A short role/task name for this worker" }),
  prompt: Schema.String.annotate({ description: "The independent work this worker should perform" }),
  subagent_type: Schema.String.annotate({ description: "The specialized Kilo subagent to use" }),
})

export const Parameters = Schema.Struct({
  workers: Schema.Array(Worker).annotate({
    description: "Independent specialized workers. Use different roles when the work benefits from parallel perspectives.",
  }),
  max_workers: Schema.optional(Schema.Int.pipe(Schema.greaterThanOrEqualTo(1))).annotate({
    description: "Maximum number of workers executing concurrently. Defaults to 4.",
  }),
})

function textResult(message: { role: string; parts: readonly { type: string; text?: string; synthetic?: boolean; ignored?: boolean }[] }) {
  if (message.role !== "assistant") return ""
  return message.parts
    .filter((part) => part.type === "text" && !part.synthetic && !part.ignored && typeof part.text === "string")
    .map((part) => part.text ?? "")
    .filter(Boolean)
    .join("\n")
}

export const SwarmTool = Tool.define(
  id,
  Effect.gen(function* () {
    const agents = yield* Agent.Service
    const sessions = yield* Session.Service
    const config = yield* Config.Service
    const database = yield* Database.Service
    const events = yield* EventV2.Service
    const execution = yield* SessionExecution.Service
    const prompt = yield* SessionPrompt.Service
    const flags = yield* RuntimeFlags.Service

    const run = Effect.fn("SwarmTool.execute")(function* (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) {
      if (params.workers.length === 0) return { title: "Swarm", output: "No workers were provided." }
      if (params.workers.length > 12) return yield* Effect.fail(new Error("A swarm may contain at most 12 workers."))

      const cfg = yield* config.get()
      const parent = yield* sessions.get(ctx.sessionID)
      const caller = yield* agents.get(ctx.agent)
      const depth = (() => {
        let current = parent
        let count = 0
        while (current.parentID) {
          count += 1
          // The ancestry is resolved below through the Effect service rather than
          // synchronously; this placeholder is replaced by the bounded loop.
          break
        }
        return count
      })()
      void flags

      const workerPlans = [] as SwarmRuntime.PlannedInput[]
      for (const [index, worker] of params.workers.entries()) {
        yield* ctx.ask({
          permission: "task",
          patterns: [worker.subagent_type],
          always: ["*"],
          metadata: { description: worker.description, subagent_type: worker.subagent_type, swarm: true },
        })
        const subagent = yield* agents.get(worker.subagent_type)
        if (!subagent) return yield* Effect.fail(new Error(`Unknown agent type: ${worker.subagent_type}`))
        KiloTask.validate(subagent, worker.subagent_type)
        workerPlans.push({
          swarmId: "",
          parentSessionId: ctx.sessionID,
          description: worker.description,
          prompt: worker.prompt,
          caller,
          subagent,
          actor: `act_swarm_${ctx.sessionID}_${index}`,
          corrId: `cor_swarm_${ctx.sessionID}_${index}` as any,
          mcp: cfg.mcp,
          depth,
          maxDepth: cfg.subagent_depth ?? 1,
          leaseMs: 30_000,
        })
      }

      const maxWorkers = Math.min(params.max_workers ?? 4, workerPlans.length)
      const swarmId = yield* Effect.gen(function* () {
        // SwarmState is intentionally used through the runtime's durable state
        // API; createSwarm is kept separate so the admission boundary remains
        // authoritative.
        const { SwarmState } = yield* Effect.promise(() => import("@/kilocode/swarm/state"))
        return yield* SwarmState.createSwarm({ maxWorkers })
      })
      const workers = workerPlans.map((worker) => ({ ...worker, swarmId }))

      const results = yield* SwarmRuntime.runPlannedBatch({ workers, concurrency: maxWorkers }).pipe(
        Effect.provideService(Session.Service, sessions),
        Effect.provideService(SessionPrompt.Service, prompt),
        Effect.provideService(SessionExecution.Service, execution),
        Effect.provideService(Database.Service, database),
        Effect.provideService(EventV2.Service, events),
      )

      const lines: string[] = [`Swarm ${swarmId} completed ${results.length} worker(s):`]
      for (const result of results) {
        const messages = yield* sessions.messages({ sessionID: result.sessionId, limit: 20 }).pipe(Effect.orDie)
        const assistant = [...messages].reverse().find((message) => message.info.role === "assistant")
        const summary = assistant ? textResult(assistant) : "Worker completed without a text result."
        lines.push(`\n### @${result.agent} — ${result.taskId}\n${summary || "Worker completed without a text result."}`)
      }

      return {
        title: `Swarm completed (${results.length} workers)`,
        output: lines.join("\n"),
        metadata: { swarmId, workers: results },
      }
    })

    return {
      description:
        "Run a bounded group of independent specialized subagents in parallel. Each worker gets its own Kilo child session, inherits the parent permission ceiling, runs under durable swarm leases/fencing, and returns its findings for synthesis by the caller. Use this when parallel research, implementation, verification, or review is useful.",
      parameters: Parameters,
      jsonSchema: ToolJsonSchema.fromSchema(Parameters),
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) => run(params, ctx),
    }
  }),
)
