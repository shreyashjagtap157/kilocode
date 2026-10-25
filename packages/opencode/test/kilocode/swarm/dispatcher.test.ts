import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { tmpdir } from "../../fixture/fixture"
import { SwarmDispatcher } from "@/kilocode/swarm/dispatcher"
import { SwarmState } from "@/kilocode/swarm/state"

const dbLayer = (file: string) => Database.layerFromPath(file)
const fullLayer = (file: string) => {
  const db = dbLayer(file)
  const ev = EventV2.layerWith().pipe(Layer.provide(db))
  return Layer.mergeAll(db, ev)
}
const use = (effect: any, file: string) =>
  Effect.runPromise(Effect.provide(fullLayer(file))(Effect.scoped(effect)))
const dbFile = (tmp: any) => (tmp as any).path + "/swarm.db"

const input = (taskId: string) => ({
  taskId,
  ownerId: "wrk_dispatcher_test",
  leaseMs: 30_000,
  actor: "act_dispatcher_test",
  corrId: "cor_dispatcher_test" as any,
})

describe("swarm dispatcher (M3 admission boundary)", () => {
  test("admits queued task through reserved to dispatched with a durable fence", async () => {
    await using tmp = await tmpdir()
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const admission = yield* SwarmDispatcher.admit(input(taskId))

        expect(admission.taskId).toBe(taskId)
        expect(admission.ownerId).toBe("wrk_dispatcher_test")
        expect(admission.leaseId).toMatch(/^lease_/)
        expect(admission.fence).toBe(1)

        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("dispatched")
        expect(task.version).toBe(2)
        expect(task.fence).toBe(1)
      }),
      dbFile(tmp),
    )
  })

  test("requires durable ownership before execution and advances to running", async () => {
    await using tmp = await tmpdir()
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const admission = yield* SwarmDispatcher.admit(input(taskId))
        const started = yield* SwarmDispatcher.startExecution({
          taskId,
          leaseId: admission.leaseId,
          fence: admission.fence,
          actor: "act_dispatcher_test",
          corrId: "cor_dispatcher_test" as any,
        })

        expect(started.version).toBe(3)
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("running")
      }),
      dbFile(tmp),
    )
  })

  test("does not allow a cancelled task to pass the execution gate", async () => {
    await using tmp = await tmpdir()
    let rejected = false
    try {
      await use(
        Effect.gen(function* () {
          const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
          const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
          const admission = yield* SwarmDispatcher.admit(input(taskId))
          yield* SwarmState.requestCancel({ taskId, actor: "act_test", corrId: "cor_cancel" as any })
          yield* SwarmDispatcher.beforeExecution({ taskId, leaseId: admission.leaseId, fence: admission.fence })
        }),
        dbFile(tmp),
      )
    } catch {
      rejected = true
    }
    expect(rejected).toBe(true)
  })
})
