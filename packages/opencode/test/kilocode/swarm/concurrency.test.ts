import { describe, expect, test } from "bun:test"
import { Effect, Exit, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { tmpdir } from "../../fixture/fixture"
import { sql } from "drizzle-orm"
import path from "path"
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
        const claim = yield* SwarmClaims.claim({ taskId, ownerId: "w1", leaseMs: 100, actor: "act_test", corrId: "cor_test" as any })

        // Let the lease expire.
        yield* Effect.sleep("150 millis")

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
  test("multiple tasks may share a state within one swarm", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const tasks = yield* Effect.forEach([1, 2, 3], () => SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any }))
        expect(new Set(tasks).size).toBe(3)
        const { db } = yield* Database.Service
        const rows = yield* db.all<{ c: number }>(sql`SELECT COUNT(*) AS c FROM kilo_swarm_task WHERE swarm_id = ${swarmId} AND state = 'queued'`)
        expect(rows[0].c).toBe(3)
      }),
      file,
    )
  })

  test("worker capacity atomically admits no more than configured slots", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 2 })
        const taskIds = yield* Effect.forEach(Array.from({ length: 12 }, () => 0), () => SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any }))
        const results = yield* Effect.all(taskIds.map((taskId, i) => Effect.exit(SwarmClaims.claim({ taskId, ownerId: `worker-${i}`, leaseMs: 60000, actor: "act_test", corrId: `cor_${i}` as any }))), { concurrency: "unbounded" })
        expect(
          results.filter((result) => result._tag === "Success").length,
          JSON.stringify(results.map((result) => result._tag === "Success" ? { tag: "Success" } : { tag: "Failure", cause: result.cause })),
        ).toBe(2)
        expect(results.filter((result) => result._tag === "Failure").length).toBe(10)
        const { db } = yield* Database.Service
        const reservations = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_worker_reservation WHERE swarm_id = ${swarmId} AND state = 'active' AND expires_at > ${Date.now()}`)
        const claims = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_claim c JOIN kilo_swarm_task t ON t.id = c.task_id WHERE t.swarm_id = ${swarmId} AND c.state = 'active' AND c.expires_at > ${Date.now()}`)
        expect(reservations?.count).toBe(2)
        expect(claims?.count).toBe(2)
      }),
      file,
    )
  })

  test("capacity exhaustion rolls back claim fence and durable event", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 0 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const result = yield* Effect.exit(SwarmClaims.claim({ taskId, ownerId: "worker-0", leaseMs: 60000, actor: "act_test", corrId: "cor_test" as any }))
        expect(result._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.fence).toBe(0)
        const { db } = yield* Database.Service
        const claimCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_claim WHERE task_id = ${taskId}`)
        const reservationCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_worker_reservation WHERE task_id = ${taskId}`)
        const eventCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.claimed%'`)
        expect(claimCount?.count).toBe(0)
        expect(reservationCount?.count).toBe(0)
        expect(eventCount?.count).toBe(0)
      }),
      file,
    )
  })

  test("releasing a worker frees its reservation for another task", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const [task1, task2] = yield* Effect.forEach([1, 2], () => SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any }))
        const first = yield* SwarmClaims.claim({ taskId: task1, ownerId: "worker-1", leaseMs: 60000, actor: "act_test", corrId: "cor_test" as any })
        yield* SwarmClaims.release({ taskId: task1, leaseId: first.leaseId, fence: first.fence, corrId: "cor_test" as any })
        const second = yield* SwarmClaims.claim({ taskId: task2, ownerId: "worker-2", leaseMs: 60000, actor: "act_test", corrId: "cor_test" as any })
        const { db } = yield* Database.Service
        const released = yield* db.get<{ state: string }>(sql`SELECT state FROM kilo_swarm_worker_reservation WHERE task_id = ${task1}`)
        const active = yield* db.get<{ state: string; lease_id: string }>(sql`SELECT state, lease_id FROM kilo_swarm_worker_reservation WHERE task_id = ${task2}`)
        expect(released?.state).toBe("released")
        expect(active?.state).toBe("active")
        expect(active?.lease_id).toBe(second.leaseId)
      }),
      file,
    )
  })

  test("stale lease cannot release a replacement claim or reservation", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const old = yield* SwarmClaims.claim({ taskId, ownerId: "old-worker", leaseMs: 200, actor: "act_test", corrId: "cor_old" as any })
        yield* Effect.sleep("250 millis")
        const fresh = yield* SwarmClaims.claim({ taskId, ownerId: "new-worker", leaseMs: 60000, actor: "act_test", corrId: "cor_new" as any })
        const staleRelease = yield* Effect.exit(SwarmClaims.release({ taskId, leaseId: old.leaseId, fence: old.fence, corrId: "cor_stale" as any }))
        expect(staleRelease._tag).toBe("Failure")
        const { db } = yield* Database.Service
        const claim = yield* db.get<{ lease_id: string; fence: number; state: string }>(sql`SELECT lease_id, fence, state FROM kilo_swarm_claim WHERE task_id = ${taskId}`)
        const reservation = yield* db.get<{ lease_id: string; fence: number; state: string }>(sql`SELECT lease_id, fence, state FROM kilo_swarm_worker_reservation WHERE task_id = ${taskId}`)
        expect(claim?.lease_id).toBe(fresh.leaseId)
        expect(claim?.fence).toBe(fresh.fence)
        expect(claim?.state).toBe("active")
        expect(reservation?.lease_id).toBe(fresh.leaseId)
        expect(reservation?.fence).toBe(fresh.fence)
        expect(reservation?.state).toBe("active")
      }),
      file,
    )
  })
  test("cross-process worker admission respects configured capacity", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    const setup = (await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 2 })
        const taskIds = yield* Effect.forEach(Array.from({ length: 8 }, () => 0), () =>
          SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any }),
        )
        return { swarmId, taskIds }
      }),
      file,
    )) as { swarmId: string; taskIds: string[] }

    const workerPath = path.join(import.meta.dir, "..", "fixture/swarm-admission-worker.ts")
    const children = setup.taskIds.map((taskId, i) =>
      Bun.spawn([process.execPath, workerPath, file, taskId, `process-${i}`], { stdout: "pipe", stderr: "pipe" }),
    )
    const outcomes = await Promise.all(children.map(async (child) => {
      const [exitCode, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ])
      expect(exitCode, `worker process failed: ${stderr}`).toBe(0)
      const line = stdout.trim().split(/\r?\n/).filter(Boolean).at(-1)
      expect(line, `worker process emitted no result: ${stderr}`).toBeTruthy()
      return JSON.parse(line!) as { ok: boolean; ownerId: string; tag?: string; fence?: number }
    }))

    const admitted = outcomes.filter((outcome) => outcome.ok)
    const rejected = outcomes.filter((outcome) => !outcome.ok)
    expect(admitted.length).toBe(2)
    expect(rejected.length).toBe(6)
    expect(rejected.every((outcome) => outcome.tag === "SwarmClaims.CapacityConflict")).toBe(true)
    expect(new Set(admitted.map((outcome) => outcome.fence)).size).toBe(1)

    await use(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        const active = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM kilo_swarm_worker_reservation
          WHERE swarm_id = ${setup.swarmId} AND state = 'active' AND expires_at > ${Date.now()}
        `)
        expect(active?.count).toBe(2)
      }),
      file,
    )
  })
  test("cross-process claim race advances a task fence exactly once", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    const setup = (await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 4 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        return { swarmId, taskId }
      }),
      file,
    )) as { swarmId: string; taskId: string }
    const workerPath = path.join(import.meta.dir, "..", "fixture/swarm-admission-worker.ts")
    const children = Array.from({ length: 4 }, (_, i) =>
      Bun.spawn([process.execPath, workerPath, file, setup.taskId, `same-task-${i}`], { stdout: "pipe", stderr: "pipe" }),
    )
    const outcomes = await Promise.all(children.map(async (child) => {
      const [exitCode, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()])
      expect(exitCode, `claim worker process failed: ${stderr}`).toBe(0)
      const line = stdout.trim().split(/\r?\n/).filter(Boolean).at(-1)
      expect(line, `claim worker emitted no result: ${stderr}`).toBeTruthy()
      return JSON.parse(line!) as { ok: boolean; tag?: string; fence?: number }
    }))
    expect(outcomes.filter((outcome) => outcome.ok).length).toBe(1)
    const rejected = outcomes.filter((outcome) => !outcome.ok)
    expect(rejected.length).toBe(3)
    expect(rejected.every((outcome) => outcome.tag === "SwarmClaims.ClaimConflict")).toBe(true)
    await use(
      Effect.gen(function* () {
        const task = yield* SwarmState.getTask(setup.taskId)
        expect(task.fence).toBe(1)
        const { db } = yield* Database.Service
        const claim = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_claim WHERE task_id = ${setup.taskId} AND state = 'active'`)
        const events = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${setup.taskId} AND type LIKE 'swarm.task.claimed%'`)
        expect(claim?.count).toBe(1)
        expect(events?.count).toBe(1)
      }),
      file,
    )
  })

  test("active lease renewal extends its capacity reservation atomically", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const claim = yield* SwarmClaims.claim({ taskId, ownerId: "renew-worker", leaseMs: 60000, actor: "act_test", corrId: "cor_test" as any })
        const renewed = yield* SwarmClaims.renew({ taskId, leaseId: claim.leaseId, fence: claim.fence, leaseMs: 120000 })
        expect(renewed.expiresAt).toBeGreaterThan(claim.expiresAt)
        const { db } = yield* Database.Service
        const rows = yield* db.get<{ claim_expiry: number; reservation_expiry: number; reservation_state: string }>(sql`
          SELECT c.expires_at AS claim_expiry, r.expires_at AS reservation_expiry, r.state AS reservation_state
          FROM kilo_swarm_claim c JOIN kilo_swarm_worker_reservation r ON r.task_id = c.task_id
          WHERE c.task_id = ${taskId}
        `)
        expect(rows?.claim_expiry).toBe(renewed.expiresAt)
        expect(rows?.reservation_expiry).toBe(renewed.expiresAt)
        expect(rows?.reservation_state).toBe("active")
      }),
      file,
    )
  })
})
