import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { tmpdir } from "../../fixture/fixture"
import { SwarmDispatcher } from "@/kilocode/swarm/dispatcher"
import { SwarmState } from "@/kilocode/swarm/state"
import { SwarmWorker } from "@/kilocode/swarm/worker"

const dbLayer = (file: string) => Database.layerFromPath(file)
const fullLayer = (file: string) => {
  const db = dbLayer(file)
  const ev = EventV2.layerWith().pipe(Layer.provide(db))
  return Layer.mergeAll(db, ev, SessionExecution.noopLayer)
}
const use = (effect: any, file: string) => Effect.runPromise(Effect.provide(fullLayer(file))(Effect.scoped(effect)))
const dbFile = (tmp: any) => (tmp as any).path + "/swarm.db"

const admit = (taskId: string) =>
  SwarmDispatcher.admit({
    taskId,
    ownerId: "wrk_worker_test",
    leaseMs: 30_000,
    actor: "act_test",
    corrId: "cor_test" as any,
  })

describe("swarm worker execution bridge (M3)", () => {
  test("resumes an existing session through SessionExecution and enters reviewing", async () => {
    await using tmp = await tmpdir()
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const admission = yield* admit(taskId)
        yield* SwarmWorker.resumeSession({
          taskId,
          sessionId: "ses_swarm_worker_test",
          leaseId: admission.leaseId,
          fence: admission.fence,
          actor: "act_worker_test",
          corrId: "cor_worker_test" as any,
        })
        const task = yield* SwarmState.getTask(taskId)
        const binding = yield* SwarmWorker.getBinding(taskId)
        expect(task.state).toBe("reviewing")
        expect(task.fence).toBe(admission.fence)
        expect(binding?.session_id).toBe("ses_swarm_worker_test")
      }),
      dbFile(tmp),
    )
  })

  test("rejects a second session from replacing an existing admitted binding", async () => {
    await using tmp = await tmpdir()
    let rejected = false
    try {
      await use(
        Effect.gen(function* () {
          const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
          const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
          const admission = yield* admit(taskId)
          yield* SwarmWorker.bindSession({
            taskId,
            sessionId: "ses_first",
            leaseId: admission.leaseId,
            fence: admission.fence,
            corrId: "cor_bind_1" as any,
          })
          yield* SwarmWorker.bindSession({
            taskId,
            sessionId: "ses_second",
            leaseId: admission.leaseId,
            fence: admission.fence,
            corrId: "cor_bind_2" as any,
          })
        }),
        dbFile(tmp),
      )
    } catch {
      rejected = true
    }
    expect(rejected).toBe(true)
  })
})
