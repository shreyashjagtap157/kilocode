import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { tmpdir } from "../../fixture/fixture"
import { SwarmClaims } from "@/kilocode/swarm/claims"
import { SwarmState } from "@/kilocode/swarm/state"
import { SwarmSupervisor } from "@/kilocode/swarm/supervisor"

const use = (effect: any, file: string) => {
  const db = Database.layerFromPath(file)
  const ev = EventV2.layerWith().pipe(Layer.provide(db))
  return Effect.runPromise(Effect.provide(Layer.mergeAll(db, ev))(Effect.scoped(effect)))
}

describe("swarm lease supervisor", () => {
  test("renews an admitted lease", async () => {
    await using tmp = await tmpdir()
    await use(Effect.gen(function* () {
      const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
      const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
      const claim = yield* SwarmClaims.claim({ taskId, ownerId: "wrk_test", leaseMs: 5000, actor: "act_test", corrId: "cor_test" as any })
      const renewed = yield* SwarmSupervisor.renewOnce({ taskId, leaseId: claim.leaseId, fence: claim.fence, leaseMs: 5000, intervalMs: 1000 })
      expect(renewed.expiresAt).toBeGreaterThan(claim.expiresAt)
    }), tmp.path + "/swarm.db")
  })

  test("rejects a renewal after cancellation", async () => {
    await using tmp = await tmpdir()
    await use(Effect.gen(function* () {
      const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
      const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
      const claim = yield* SwarmClaims.claim({ taskId, ownerId: "wrk_test", leaseMs: 5000, actor: "act_test", corrId: "cor_test" as any })
      yield* SwarmState.requestCancel({ taskId, actor: "act_test", corrId: "cor_cancel" as any })
      const result = yield* Effect.exit(SwarmSupervisor.renewOnce({ taskId, leaseId: claim.leaseId, fence: claim.fence, leaseMs: 5000, intervalMs: 1000 }))
      expect(result._tag).toBe("Failure")
    }), tmp.path + "/swarm.db")
  })
})
