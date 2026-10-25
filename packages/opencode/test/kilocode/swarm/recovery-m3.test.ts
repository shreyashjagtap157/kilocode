import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { sql } from "drizzle-orm"
import { tmpdir } from "../../fixture/fixture"
import { SwarmClaims } from "@/kilocode/swarm/claims"
import { SwarmRecovery } from "@/kilocode/swarm/recovery"
import { SwarmState } from "@/kilocode/swarm/state"
import { SwarmWorker } from "@/kilocode/swarm/worker"

const fullLayer = (file: string) => {
  const db = Database.layerFromPath(file)
  return Layer.mergeAll(db, EventV2.layerWith().pipe(Layer.provide(db)))
}
const use = (effect: any, file: string) => Effect.runPromise(Effect.provide(fullLayer(file))(Effect.scoped(effect)))
const dbFile = (tmp: any) => (tmp as any).path + "/swarm.db"

describe("swarm worker binding recovery", () => {
  test("expires a binding when its exact claim generation is no longer active", async () => {
    await using tmp = await tmpdir()
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const claim = yield* SwarmClaims.claim({
          taskId,
          ownerId: "wrk_recovery_test",
          leaseMs: 30_000,
          actor: "act_test",
          corrId: "cor_test" as any,
        })
        yield* SwarmWorker.bindSession({
          taskId,
          sessionId: "ses_recovery_test",
          leaseId: claim.leaseId,
          fence: claim.fence,
        })
        const { db } = yield* Database.Service
        yield* db.run(sql`UPDATE kilo_swarm_claim SET state = 'expired', expires_at = 0 WHERE task_id = ${taskId}`)
        yield* SwarmRecovery.reconcile()
        const binding = yield* SwarmWorker.getBinding(taskId)
        expect(binding?.state).toBe("expired")
      }),
      dbFile(tmp),
    )
  })
})
