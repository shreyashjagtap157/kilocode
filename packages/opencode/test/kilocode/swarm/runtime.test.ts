import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionPrompt } from "@/session/prompt"
import { SessionDrain } from "@/kilocode/session/drain"
import { sql } from "drizzle-orm"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import type { Agent } from "@/agent/agent"
import { Permission } from "@/permission"
import { SwarmRuntime } from "@/kilocode/swarm/runtime"
import { SwarmState } from "@/kilocode/swarm/state"
import { tmpdir } from "../../fixture/fixture"

const parentID = SessionID.make("ses_swarm_parent")

const agent = (name: string, permission: Agent.Info["permission"], mode: Agent.Info["mode"] = "subagent"): Agent.Info => ({
  name,
  mode,
  permission,
  options: {},
})

const sessionInfo = (id: SessionID, permission: Session.Info["permission"]): Session.Info =>
  ({
    id,
    slug: "swarm-test",
    projectID: "prj_swarm_test",
    directory: "/tmp/swarm-test",
    title: "swarm test",
    version: "test",
    time: { created: Date.now(), updated: Date.now() },
    permission,
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  }) as Session.Info

const use = (effect: any, file: string, run?: () => Effect.Effect<void>) => {
  const db = Database.layerFromPath(file)
  const ev = EventV2.layerWith().pipe(Layer.provide(db))
  let childNumber = 0
  const sessions = Layer.succeed(Session.Service, {
    get: () => Effect.succeed(sessionInfo(parentID, Permission.fromConfig({ bash: "deny" }))),
    create: () => {
      childNumber += 1
      return Effect.succeed(sessionInfo(SessionID.make(`ses_swarm_child_${childNumber}`), Permission.fromConfig({ bash: "deny" })))
    },
  } as unknown as Session.Interface)
  const prompt = Layer.succeed(SessionPrompt.Service, {
    cancel: () => Effect.void,
    paused: () => Effect.succeed(false),
    prompt: () => run ? run().pipe(Effect.as(undefined as never)) : Effect.succeed(undefined as never),
    loop: () => Effect.succeed(undefined as never),
    shell: () => Effect.succeed(undefined as never),
    command: () => Effect.succeed(undefined as never),
    resolvePromptParts: () => Effect.succeed([]),
  } as unknown as SessionPrompt.Interface)
  const drain = Layer.succeed(SessionDrain.Service, {
    hold: () => Effect.succeed(() => undefined),
    link: () => Effect.void,
    wait: () => Effect.void,
    track: (_id, work) => work,
  } as unknown as SessionDrain.Interface)
  const execution = SessionExecution.noopLayer
  return Effect.runPromise(Effect.provide(Layer.mergeAll(db, ev, prompt, drain, execution, sessions))(Effect.scoped(effect)))
}

describe("SwarmRuntime planned worker path (M3)", () => {
  test("creates a child-session request and executes it through durable swarm admission", async () => {
    await using tmp = await tmpdir()
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const caller = agent("main", Permission.fromConfig({ read: "allow" }), "primary")
        const subagent = agent("worker", Permission.fromConfig({ read: "allow" }))

        yield* SwarmRuntime.runPlanned({
          swarmId,
          parentSessionId: parentID,
          description: "inspect the repository",
          prompt: "Inspect the repository and report the most important findings.",
          caller,
          subagent,
          actor: "act_runtime_test",
          corrId: "cor_runtime_test" as any,
        })

        const { db } = yield* Database.Service
        const task = yield* db
          .get<{ state: string; parent_id: string | null }>(sql`SELECT state, parent_id FROM kilo_swarm_task WHERE swarm_id = ${swarmId}`)
          .pipe(Effect.orDie)
        expect(task?.state).toBe("reviewing")
        expect(task?.parent_id).toBe(parentID)
      }),
      (tmp as any).path + "/swarm.db",
    )
  })

  test("runs specialized workers concurrently with an explicit bounded concurrency", async () => {
    await using tmp = await tmpdir()
    let active = 0
    let maxActive = 0
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 2 })
        const caller = agent("main", Permission.fromConfig({ read: "allow" }), "primary")
        const worker = agent("worker", Permission.fromConfig({ read: "allow" }))
        const workers = ["research", "implementation", "verification", "review"].map((description, index) => ({
          swarmId,
          parentSessionId: parentID,
          description,
          prompt: `Perform the ${description} role and return concise findings.`,
          caller,
          subagent: worker,
          actor: `act_runtime_${index}`,
          corrId: `cor_runtime_${index}` as any,
          leaseMs: 5_000,
          renewalIntervalMs: 1_000,
        }))

        yield* SwarmRuntime.runPlannedBatch({ workers, concurrency: 2 })

        const { db } = yield* Database.Service
        const rows = yield* db
          .all<{ state: string }>(sql`SELECT state FROM kilo_swarm_task WHERE swarm_id = ${swarmId} ORDER BY time_created`)
          .pipe(Effect.orDie)
        expect(rows).toHaveLength(4)
        expect(rows.every((row) => row.state === "reviewing")).toBe(true)
        expect(maxActive).toBeLessThanOrEqual(2)
        expect(maxActive).toBeGreaterThan(1)
      }),
      (tmp as any).path + "/swarm.db",
      () =>
        Effect.gen(function* () {
          active += 1
          maxActive = Math.max(maxActive, active)
          yield* Effect.sleep("50 millis")
          active -= 1
        }).pipe(Effect.asVoid),
    )
  })
})
