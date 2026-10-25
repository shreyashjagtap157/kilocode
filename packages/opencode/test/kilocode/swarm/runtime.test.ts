import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { sql } from "drizzle-orm"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import type { Agent } from "@/agent/agent"
import { Permission } from "@/permission"
import { SwarmRuntime } from "@/kilocode/swarm/runtime"
import { SwarmState } from "@/kilocode/swarm/state"
import { tmpdir } from "../../fixture/fixture"

const parentID = SessionID.make("ses_swarm_parent")
const childID = SessionID.make("ses_swarm_child")

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

const use = (effect: any, file: string) => {
  const db = Database.layerFromPath(file)
  const ev = EventV2.layerWith().pipe(Layer.provide(db))
  const sessions = Layer.succeed(Session.Service, {
    get: () => Effect.succeed(sessionInfo(parentID, Permission.fromConfig({ bash: "deny" }))),
    create: () => Effect.succeed(sessionInfo(childID, Permission.fromConfig({ bash: "deny" }))),
  } as unknown as Session.Interface)
  return Effect.runPromise(
    Effect.provide(Layer.mergeAll(db, ev, SessionExecution.noopLayer, sessions))(Effect.scoped(effect)),
  )
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
})
