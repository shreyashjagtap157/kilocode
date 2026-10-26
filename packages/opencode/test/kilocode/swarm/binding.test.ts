import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { tmpdir } from "../../fixture/fixture"
import { SwarmDispatcher } from "@/kilocode/swarm/dispatcher"
import { SwarmState } from "@/kilocode/swarm/state"
import { SwarmWorker } from "@/kilocode/swarm/worker"

const fullLayer = (file: string) => {
  const db = Database.layerFromPath(file)
  return Layer.mergeAll(db, EventV2.layerWith().pipe(Layer.provide(db)))
}
const use = (effect: any, file: string) => Effect.runPromise(Effect.provide(fullLayer(file))(Effect.scoped(effect)))
const dbFile = (tmp: any) => (tmp as any).path + "/swarm.db"

describe("swarm worker binding", () => {
  test("persists the exact session, lease and fence generation", async () => {
    await using tmp = await tmpdir()
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const admission = yield* SwarmDispatcher.admit({
          taskId,
          ownerId: "wrk_binding_test",
          leaseMs: 30_000,
          actor: "act_test",
          corrId: "cor_test" as any,
        })
        const binding = yield* SwarmWorker.bindSession({
          taskId,
          sessionId: "ses_binding_test",
          leaseId: admission.leaseId,
          fence: admission.fence,
        })
        expect(binding.sessionId).toBe("ses_binding_test")
        expect((yield* SwarmWorker.getBinding(taskId))?.fence).toBe(admission.fence)
      }),
      dbFile(tmp),
    )
  })

  test("never allows a concurrent binder to replace the winning session", async () => {
    await using tmp = await tmpdir()
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_race", corrId: "cor_race" as any })
        const admission = yield* SwarmDispatcher.admit({
          taskId,
          ownerId: "wrk_binding_race",
          leaseMs: 30_000,
          actor: "act_race",
          corrId: "cor_race" as any,
        })
        const outcomes = yield* Effect.all(
          [
            Effect.either(SwarmWorker.bindSession({ taskId, sessionId: "ses_binding_left", leaseId: admission.leaseId, fence: admission.fence })),
            Effect.either(SwarmWorker.bindSession({ taskId, sessionId: "ses_binding_right", leaseId: admission.leaseId, fence: admission.fence })),
          ],
          { concurrency: 2, discard: false },
        )
        const successful = outcomes.filter((outcome) => outcome._tag === "Right")
        const failed = outcomes.filter((outcome) => outcome._tag === "Left")
        expect(successful).toHaveLength(1)
        expect(failed).toHaveLength(1)
        const binding = yield* SwarmWorker.getBinding(taskId)
        expect(["ses_binding_left", "ses_binding_right"]).toContain(binding?.session_id)
        expect(binding?.lease_id).toBe(admission.leaseId)
        expect(binding?.fence).toBe(admission.fence)
      }),
      dbFile(tmp),
    )
  })
})
