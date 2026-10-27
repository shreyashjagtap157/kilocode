import { describe, expect, test } from "bun:test"
import { Cause, Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionPrompt } from "@/session/prompt"
import { SessionDrain } from "@/kilocode/session/drain"
import { Config } from "@/config/config"
import { Provider } from "@/provider/provider"
import { sql } from "drizzle-orm"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import type { Agent } from "@/agent/agent"
import { Permission } from "@/permission"
import { SwarmRuntime } from "@/kilocode/swarm/runtime"
import { SwarmState } from "@/kilocode/swarm/state"
import { SwarmDispatcher } from "@/kilocode/swarm/dispatcher"
import { SwarmWorker } from "@/kilocode/swarm/worker"
import { tmpdir } from "../../fixture/fixture"

const parentID = SessionID.make("ses_swarm_parent")

const agent = (name: string, permission: Agent.Info["permission"], mode: Agent.Info["mode"] = "subagent"): Agent.Info => ({
  name,
  mode,
  permission,
  model: { providerID: "test-provider" as any, modelID: "test-model" as any },
  options: {},
})

const sessionInfo = (id: SessionID, permission: Session.Info["permission"], model = { providerID: "test-provider" as any, id: "test-model" as any }): Session.Info =>
  ({
    id,
    slug: "swarm-test",
    projectID: "prj_swarm_test",
    directory: "/tmp/swarm-test",
    title: "swarm test",
    version: "test",
    model,
    time: { created: Date.now(), updated: Date.now() },
    permission,
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  }) as Session.Info

const use = (
  effect: any,
  file: string,
  run?: () => Effect.Effect<void>,
  onCreate?: (input: unknown) => void,
  parentPermission: Session.Info["permission"] = Permission.fromConfig({ bash: "deny" }),
  onPrompt?: (input: unknown) => void,
) => {
  const db = Database.layerFromPath(file)
  const ev = EventV2.layerWith().pipe(Layer.provide(db))
  let childNumber = 0
  const sessions = Layer.succeed(Session.Service, {
    get: () => Effect.succeed(sessionInfo(parentID, parentPermission)),
    create: (input: unknown) => {
      onCreate?.(input)
      childNumber += 1
      return Effect.succeed(sessionInfo(SessionID.make(`ses_swarm_child_${childNumber}`), Permission.fromConfig({ bash: "deny" })))
    },
  } as unknown as Session.Interface)
  const prompt = Layer.succeed(SessionPrompt.Service, {
    cancel: () => Effect.void,
    paused: () => Effect.succeed(false),
    prompt: (input: unknown) => {
      onPrompt?.(input)
      return run ? run().pipe(Effect.as(undefined as never)) : Effect.succeed(undefined as never)
    },
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
  const config = Layer.succeed(Config.Service, { get: () => Effect.succeed({ subagent_depth: 3, subagent_model: undefined, subagent_variant: undefined, subagent_variant_overrides: {}, mcp: {} }) } as unknown as Config.Interface)
  const provider = Layer.succeed(Provider.Service, {} as unknown as Provider.Interface)
  const execution = SessionExecution.noopLayer
  return Effect.runPromise(Effect.provide(Layer.mergeAll(db, ev, prompt, drain, config, provider, execution, sessions))(Effect.scoped(effect)))
}

describe("SwarmRuntime planned worker path (M3)", () => {
  test("disables shared-board tools in child prompts when advisory coordination is unavailable", () => {
    const unavailable = SwarmRuntime.promptTools({ canTask: true, canTodo: true, boardEnabled: false })
    expect(unavailable.board_read).toBe(false)
    expect(unavailable.board_post).toBe(false)
    expect(unavailable.question).toBe(false)
    expect(unavailable.task).toBeUndefined()
    expect(unavailable.todowrite).toBeUndefined()

    const enabled = SwarmRuntime.promptTools({ canTask: false, canTodo: false, boardEnabled: true })
    expect(enabled.board_read).toBeUndefined()
    expect(enabled.board_post).toBeUndefined()
    expect(enabled.task).toBe(false)
    expect(enabled.todowrite).toBe(false)

    // An omitted flag preserves legacy/non-swarm planned execution behavior.
    const inherited = SwarmRuntime.promptTools({ canTask: true, canTodo: true })
    expect(inherited.board_read).toBeUndefined()
    expect(inherited.board_post).toBeUndefined()
  })

  test("disables board tools in the actual child prompt when coordination is unavailable", async () => {
    await using tmp = await tmpdir()
    let prompted: any
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const caller = agent("main", Permission.fromConfig({ read: "allow" }), "primary")
        const subagent = agent("worker", Permission.fromConfig({ read: "allow" }))
        yield* SwarmRuntime.runPlanned({
          swarmId,
          parentSessionId: parentID,
          description: "review without shared board",
          prompt: "Inspect the target and report evidence.",
          caller,
          subagent,
          actor: "act_runtime_board_disabled",
          corrId: "cor_runtime_board_disabled" as any,
          boardEnabled: false,
        })
      }),
      dbFile(tmp),
      undefined,
      undefined,
      undefined,
      (input) => { prompted = input },
    )

    expect(prompted?.tools?.board_read).toBe(false)
    expect(prompted?.tools?.board_post).toBe(false)
    expect(prompted?.tools?.question).toBe(false)
  })

  test("creates a child session with the resolved worker model and executes it through durable swarm admission", async () => {
    await using tmp = await tmpdir()
    let created: any
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const caller = agent("main", Permission.fromConfig({ read: "allow" }), "primary")
        const subagent = agent("worker", Permission.fromConfig({ read: "allow" }))

        const result = yield* SwarmRuntime.runPlanned({
          swarmId,
          parentSessionId: parentID,
          description: "inspect the repository",
          prompt: "Inspect the repository and report the most important findings.",
          caller,
          subagent,
          actor: "act_runtime_test",
          corrId: "cor_runtime_test" as any,
        })

        expect(result.sessionId).toBe("ses_swarm_child_1")
        expect(result.taskId).toMatch(/^tsk_/)
        expect(result.durationMs).toBeGreaterThanOrEqual(0)
        expect(result.agent).toBe("worker")
        expect(created?.model).toEqual({ id: "test-model", providerID: "test-provider", variant: undefined })
        const { db } = yield* Database.Service
        const task = yield* db
          .get<{ state: string; parent_id: string | null }>(sql`SELECT state, parent_id FROM kilo_swarm_task WHERE swarm_id = ${swarmId}`)
          .pipe(Effect.orDie)
        expect(task?.state).toBe("reviewing")
        expect(task?.parent_id).toBeNull()
      }),
      (tmp as any).path + "/swarm.db",
      undefined,
      (input) => { created = input },
    )
  })

  test("links nested swarm tasks to the active parent task, not its session ID", async () => {
    await using tmp = await tmpdir()
    await use(
      Effect.gen(function* () {
        const parentSwarmId = yield* SwarmState.createSwarm({ maxWorkers: 2 })
        const parentTaskId = yield* SwarmState.createTask({
          swarmId: parentSwarmId,
          actor: "act_nested_parent",
          corrId: "cor_nested_parent" as any,
        })
        const admission = yield* SwarmDispatcher.admit({
          taskId: parentTaskId,
          ownerId: "wrk_nested_parent",
          leaseMs: 60_000,
          actor: "act_nested_parent",
          corrId: "cor_nested_parent_admit" as any,
        })
        yield* SwarmWorker.bindSession({
          taskId: parentTaskId,
          sessionId: parentID,
          leaseId: admission.leaseId,
          fence: admission.fence,
          corrId: "cor_nested_parent_bind",
        })
        yield* SwarmDispatcher.startExecution({
          taskId: parentTaskId,
          leaseId: admission.leaseId,
          fence: admission.fence,
          actor: "act_nested_parent",
          corrId: "cor_nested_parent_start" as any,
        })

        const nestedSwarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const result = yield* SwarmRuntime.runPlanned({
          swarmId: nestedSwarmId,
          parentSessionId: parentID,
          description: "nested specialist work",
          prompt: "Perform a delegated subtask and report evidence.",
          caller: agent("worker", Permission.fromConfig({ read: "allow" })),
          subagent: agent("nested-worker", Permission.fromConfig({ read: "allow" })),
          actor: "act_nested_child",
          corrId: "cor_nested_child" as any,
        })

        const { db } = yield* Database.Service
        const child = yield* db.get<{ parent_id: string | null; state: string; swarm_id: string }>(sql`
          SELECT parent_id, state, swarm_id FROM kilo_swarm_task WHERE id = ${result.taskId}
        `).pipe(Effect.orDie)
        expect(child?.swarm_id).toBe(nestedSwarmId)
        expect(child?.parent_id).toBe(parentTaskId)
        expect(child?.parent_id).not.toBe(parentID)
        expect(child?.state).toBe("reviewing")

        const parent = yield* SwarmState.getTask(parentTaskId)
        expect(parent.state).toBe("running")
        const parentAcceptance = yield* Effect.exit(SwarmState.transition({
          taskId: parentTaskId,
          to: "reviewing",
          expectedVersion: parent.version,
          actor: "act_nested_parent",
          corrId: "cor_nested_parent_review" as any,
        }))
        expect(parentAcceptance._tag).toBe("Success")
        const reviewTask = yield* SwarmState.getTask(parentTaskId)
        const acceptance = yield* Effect.exit(SwarmState.transition({
          taskId: parentTaskId,
          to: "accepted",
          expectedVersion: reviewTask.version,
          actor: "act_nested_parent",
          corrId: "cor_nested_parent_accept" as any,
        }))
        expect(acceptance._tag).toBe("Failure")
        if (acceptance._tag === "Failure") expect(Cause.pretty(acceptance.cause)).toContain("child task")
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

        const results = yield* SwarmRuntime.runPlannedBatch({ workers, concurrency: 2 })

        const { db } = yield* Database.Service
        const rows = yield* db
          .all<{ state: string }>(sql`SELECT state FROM kilo_swarm_task WHERE swarm_id = ${swarmId} ORDER BY time_created`)
          .pipe(Effect.orDie)
        expect(rows).toHaveLength(4)
        expect(rows.every((row) => row.state === "reviewing")).toBe(true)
        expect(results.every((result) => result.durationMs >= 0)).toBe(true)
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

  test("durably cancels a queued task without creating or interrupting a worker session", async () => {
    await using tmp = await tmpdir()
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_cancel_test", corrId: "cor_cancel_test" as any })
        const result = yield* SwarmRuntime.cancel({ taskId, actor: "act_cancel_test", corrId: "cor_cancel_test_2" as any })
        expect(result.taskId).toBe(taskId)
        expect(result.sessionId).toBeUndefined()
        expect(result.alreadyTerminal).toBe(false)
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("cancelled")
        expect(task.cancel_requested).toBe(1)
      }),
      (tmp as any).path + "/swarm.db",
    )
  })


  test("settled batches preserve successful workers when a sibling defects", async () => {
    await using tmp = await tmpdir()
    const file = (tmp as any).path + "/swarm.db"
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 2 })
        const caller = agent("main", Permission.fromConfig({ read: "allow" }), "primary")
        const worker = agent("worker", Permission.fromConfig({ read: "allow" }))
        const workers = [
          {
            swarmId,
            parentSessionId: parentID,
            description: "valid worker",
            prompt: "Return a short result.",
            caller,
            subagent: worker,
            actor: "act_settled_valid",
            corrId: "cor_settled_valid" as any,
          },
          {
            swarmId,
            parentSessionId: parentID,
            description: "defective worker",
            prompt: "This plan should defect before execution.",
            caller,
            subagent: undefined as unknown as Agent.Info,
            actor: "act_settled_defect",
            corrId: "cor_settled_defect" as any,
          },
        ]

        const result = yield* SwarmRuntime.runPlannedBatchSettled({ workers, concurrency: 2 })
        expect(result.completed).toHaveLength(1)
        expect(result.completed[0]?.index).toBe(0)
        expect(result.completed[0]?.result.agent).toBe("worker")
        expect(result.failed).toHaveLength(1)
        expect(result.failed[0]?.index).toBe(1)
        expect(String(result.failed[0]?.error)).toMatch(/undefined|name/i)

        const tasks = yield* SwarmState.listTasks(swarmId)
        expect(tasks).toHaveLength(1)
        expect(tasks[0]?.state).toBe("reviewing")
      }),
      file,
    )
  })

  test("read-only reviewer session enforces its permission ceiling", async () => {
    await using tmp = await tmpdir()
    let created: any
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const caller = agent("main", Permission.fromConfig({ "*": "allow" }), "primary")
        const reviewer = agent("reviewer", Permission.fromConfig({ "*": "allow" }))
        const result = yield* SwarmRuntime.runPlanned({
          swarmId,
          parentSessionId: parentID,
          description: "independent review",
          prompt: "Review an implementation without modifying it.",
          caller,
          subagent: reviewer,
          actor: "act_readonly_review",
          corrId: "cor_readonly_review" as any,
          readOnly: true,
        })
        expect(result.agent).toBe("reviewer")
        expect(created).toBeDefined()
        const permission = created.permission as Permission.Ruleset
        // Evaluate the original agent's permissions followed by the session's
        // restrictive override, matching the order used by Kilo's permission gate.
        expect(Permission.evaluate("read", "src/example.ts", reviewer.permission, permission).action).toBe("allow")
        expect(Permission.evaluate("bash", "bun test test/example.test.ts", reviewer.permission, permission).action).toBe("allow")
        expect(Permission.evaluate("bash", "git status", reviewer.permission, permission).action).toBe("allow")
        expect(Permission.evaluate("bash", "rm -rf src", reviewer.permission, permission).action).toBe("deny")
        expect(Permission.evaluate("bash", "bun test test/example.test.ts > test.log", reviewer.permission, permission).action).toBe("deny")
        expect(Permission.evaluate("edit", "src/example.ts", reviewer.permission, permission).action).toBe("deny")
        expect(Permission.evaluate("write", "src/example.ts", reviewer.permission, permission).action).toBe("deny")
        expect(Permission.evaluate("apply_patch", "*", reviewer.permission, permission).action).toBe("deny")
        expect(Permission.evaluate("task", "*", reviewer.permission, permission).action).toBe("deny")
        expect(Permission.evaluate("todowrite", "*", reviewer.permission, permission).action).toBe("deny")
      }),
      (tmp as any).path + "/swarm.db",
      undefined,
      (input) => { created = input },
      Permission.fromConfig({ "*": "allow" }),
    )
  })

})
