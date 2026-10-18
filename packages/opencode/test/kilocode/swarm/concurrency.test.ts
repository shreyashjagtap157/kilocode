import { describe, expect, test } from "bun:test"
import { Effect, Exit, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { tmpdir } from "../../fixture/fixture"
import { sql } from "drizzle-orm"
import { SwarmState } from "@/kilocode/swarm/state"
import { SwarmClaims } from "@/kilocode/swarm/claims"

const dbLayer = (file: string) => Database.layerFromPath(file)
const fullLayer = (file: string) => {
  const db = dbLayer(file)
  const ev = EventV2.layerWith().pipe(Layer.provide(db))
  return Layer.mergeAll(db, ev)
}
const use = (effect: any, file: string) =>
  Effect.runPromise(Effect.provide(fullLayer(file))(Effect.scoped(effect)))
const dbFile = (tmp: any) => (tmp as any).path + "/swarm.db"

describe("swarm concurrency (M2)", () => {
  // AC16.A — simultaneous claim acquisition: exactly one contender wins
  test("concurrent claim acquisition allows exactly one owner", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })

        // Two concurrent claim attempts for the same task — actual overlap via Effect concurrency.
        const results = yield* Effect.all(
          [
            Effect.exit(
              SwarmClaims.claim({ taskId, ownerId: "w1", leaseMs: 60000, actor: "act_test", corrId: "cor_a" as any }),
            ),
            Effect.exit(
              SwarmClaims.claim({ taskId, ownerId: "w2", leaseMs: 60000, actor: "act_test", corrId: "cor_b" as any }),
            ),
          ],
          { concurrency: "unbounded" },
        )

        const successes = results.filter((r) => r._tag === "Success")
        const failures = results.filter((r) => r._tag === "Failure")

        // Exactly one contender succeeds.
        expect(successes.length).toBe(1)
        // The loser receives an authoritative conflict.
        expect(failures.length).toBe(1)

        // Exactly one active claim row.
        const claims = yield* (Effect.gen(function* () {
          const { db } = yield* Database.Service
          return yield* db.all<{ state: string }>(sql`SELECT state FROM kilo_swarm_claim WHERE task_id = ${taskId}`)
        }) as any)
        expect(claims.length).toBe(1)
        expect(claims[0].state).toBe("active")

        // Task fence advanced exactly once (from 0 to 1).
        const task = yield* SwarmState.getTask(taskId)
        expect(task.fence).toBe(1)
        const winner = successes[0] as unknown as Exit.Success<{ fence: number }>
        expect(winner.value.fence).toBe(1)
      }),
      file,
    )
  })

  // AC16.B — concurrent CAS writers: at most one stale-version writer succeeds
  test("concurrent CAS writers serialize on version", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })

        // Two concurrent writers both expect version 0; only one can transition.
        const results = yield* Effect.all(
          [
            Effect.exit(
              SwarmState.transition({ taskId, to: "reserved", expectedVersion: 0, actor: "act_test", corrId: "cor_a" as any }),
            ),
            Effect.exit(
              SwarmState.transition({ taskId, to: "dispatched", expectedVersion: 0, actor: "act_test", corrId: "cor_b" as any }),
            ),
          ],
          { concurrency: "unbounded" },
        )

        const successes = results.filter((r) => r._tag === "Success")
        const failures = results.filter((r) => r._tag === "Failure")

        expect(successes.length).toBe(1)
        expect(failures.length).toBe(1)

        const task = yield* SwarmState.getTask(taskId)
        expect(task.version).toBe(1)
        expect(task.state).toBe("reserved")

        // History must contain exactly one committed transition.
        const transitions = yield* (Effect.gen(function* () {
          const { db } = yield* Database.Service
          return yield* db.all<{ from_state: string; to_state: string }>(sql`SELECT from_state, to_state FROM kilo_swarm_task_transition WHERE task_id = ${taskId} ORDER BY seq`)
        }) as any)
        expect(transitions.length).toBe(1)
        expect(transitions[0].from_state).toBe("queued")
        expect(transitions[0].to_state).toBe("reserved")
      }),
      file,
    )
  })

  // AC16.C — renewal/expiry race: expired lease cannot regain authority via renewal
  test("renewal of expired lease fails, fresh claim succeeds", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const claim = yield* SwarmClaims.claim({ taskId, ownerId: "w1", leaseMs: 20, actor: "act_test", corrId: "cor_test" as any })

        // Let the lease expire.
        yield* Effect.sleep("50 millis")

        // Attempt renewal of the now-expired lease must fail.
        const renewed = yield* SwarmClaims.renew({
          taskId,
          leaseId: claim.leaseId,
          fence: claim.fence,
          leaseMs: 60000,
        }).pipe(Effect.map(() => "ok"), Effect.catchCause(() => Effect.succeed("failed")))
        expect(renewed).toBe("failed")

        // A fresh claim after expiry must succeed with incremented fence.
        const reclaimed = yield* SwarmClaims.claim({ taskId, ownerId: "w2", leaseMs: 60000, actor: "act_test", corrId: "cor_test" as any })
        expect(reclaimed.fence).toBe(claim.fence + 1)
        const task = yield* SwarmState.getTask(taskId)
        expect(task.fence).toBe(claim.fence + 1)
      }),
      file,
    )
  })

  // AC16.D — release/renewal race: released claim cannot be renewed
  test("release then renewal fails", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const claim = yield* SwarmClaims.claim({ taskId, ownerId: "w1", leaseMs: 60000, actor: "act_test", corrId: "cor_test" as any })

        yield* SwarmClaims.release({ taskId, leaseId: claim.leaseId, fence: claim.fence, corrId: "cor_test" as any })

        const renewed = yield* SwarmClaims.renew({
          taskId,
          leaseId: claim.leaseId,
          fence: claim.fence,
          leaseMs: 60000,
        }).pipe(Effect.map(() => "ok"), Effect.catchCause(() => Effect.succeed("failed")))
        expect(renewed).toBe("failed")
      }),
      file,
    )
  })
})
