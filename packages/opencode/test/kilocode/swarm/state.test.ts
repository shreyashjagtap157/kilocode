import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { tmpdir } from "../../fixture/fixture"
import { sql } from "drizzle-orm"
import { Layer } from "effect"
import { SwarmState } from "@/kilocode/swarm/state"
import { SwarmTaskClaimed } from "@/kilocode/swarm/events"
import { SwarmClaims } from "@/kilocode/swarm/claims"
import { SwarmRecovery } from "@/kilocode/swarm/recovery"

const dbLayer = (file: string) => Database.layerFromPath(file)
const fullLayer = (file: string) => {
  const db = dbLayer(file)
  const ev = EventV2.layerWith().pipe(Layer.provide(db))
  return Layer.mergeAll(db, ev)
}
const use = (effect: any, file: string) =>
  Effect.runPromise(Effect.provide(fullLayer(file))(Effect.scoped(effect)))
const dbFile = (tmp: any) => (tmp as any).path + "/swarm.db"

describe("swarm state (M2)", () => {
  test("creates a swarm and task", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    const result: any = await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("queued")
        expect(task.version).toBe(0)
        expect(task.fence).toBe(0)
        return { swarmId, taskId }
      }),
      file,
    )
    expect(result.taskId).toMatch(/^tsk_/)
  })

  test("transitions task state with CAS", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const r = yield* SwarmState.transition({ taskId, to: "reserved", expectedVersion: 0, actor: "act_test", corrId: "cor_test" as any })
        expect(r.version).toBe(1)
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("reserved")
        expect(task.version).toBe(1)
      }),
      file,
    )
  })

  test("rejects illegal transition", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    let threw = false
    try {
      await use(
        Effect.gen(function* () {
          const swarmId = yield* SwarmState.createSwarm()
          const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
          yield* SwarmState.transition({ taskId, to: "integrated", expectedVersion: 0, actor: "act_test", corrId: "cor_test" as any })
        }),
        file,
      )
    } catch (e) {
      threw = true
    }
    expect(threw).toBe(true)
  })

  test("rejects stale version (CAS)", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    let threw = false
    try {
      await use(
        Effect.gen(function* () {
          const swarmId = yield* SwarmState.createSwarm()
          const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
          yield* SwarmState.transition({ taskId, to: "reserved", expectedVersion: 0, actor: "act_test", corrId: "cor_test" as any })
          yield* SwarmState.transition({ taskId, to: "dispatched", expectedVersion: 0, actor: "act_test", corrId: "cor_test" as any })
        }),
        file,
      )
    } catch (e) {
      threw = true
    }
    expect(threw).toBe(true)
  })

  test("claim + fence + stale rejection", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const c1 = yield* SwarmClaims.claim({ taskId, ownerId: "w1", leaseMs: 60000, actor: "act_test", corrId: "cor_test" as any })
        expect(c1.fence).toBe(1)
        // second claimant conflicts
        const r1 = yield* SwarmClaims.claim({ taskId, ownerId: "w2", leaseMs: 60000, actor: "act_test", corrId: "cor_test" as any }).pipe(
          Effect.map(() => "ok"),
          Effect.catchCause(() => Effect.succeed("failed")),
        )
        expect(r1).toBe("failed")
        // stale fence rejected
        const r2 = yield* SwarmClaims.assertOwnership({ taskId, leaseId: c1.leaseId, fence: 0 }).pipe(
          Effect.map(() => "ok"),
          Effect.catchCause(() => Effect.succeed("failed")),
        )
        expect(r2).toBe("failed")
        const ok = yield* SwarmClaims.assertOwnership({ taskId, leaseId: c1.leaseId, fence: c1.fence })
        expect(ok.ok).toBe(true)
      }),
      file,
    )
  })

  test("idempotency: duplicate create returns same id", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    const t1 = await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        return yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any, idemKey: "k1" })
      }),
      file,
    )
    const t2 = await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        return yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any, idemKey: "k1" })
      }),
      file,
    )
    expect(t1).toBe(t2)
  })

  test("survives close/reopen (persistence)", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    const swarmId = await use(
      Effect.gen(function* () {
        const sid = yield* SwarmState.createSwarm()
        const tid = yield* SwarmState.createTask({ swarmId: sid, actor: "act_test", corrId: "cor_test" as any })
        yield* SwarmState.transition({ taskId: tid, to: "reserved", expectedVersion: 0, actor: "act_test", corrId: "cor_test" as any })
        return sid
      }),
      file,
    )
    const tasks: any = await use(SwarmState.listTasks(swarmId as string) as any, file)
    expect(tasks.length).toBe(1)
    expect(tasks[0].state).toBe("reserved")
    expect(tasks[0].version).toBe(1)
  })

  test("claim rejects a task after cancellation is durably requested", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 2 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const canceled = yield* SwarmState.requestCancel({ taskId, actor: "act_test", corrId: "cor_cancel" as any })
        expect(canceled.cancelRequested).toBe(true)
        expect(canceled.alreadyTerminal).toBe(false)

        const attempt = yield* Effect.exit(SwarmClaims.claim({ taskId, ownerId: "late-worker", leaseMs: 60000, actor: "act_test", corrId: "cor_claim" as any }))
        expect(attempt._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.cancel_requested).toBe(1)
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

  test("claim rechecks cancellation inside the durable event transaction", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 2 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const { db } = yield* Database.Service
        const events = yield* EventV2.Service

        // Inject a cancellation marker after EventV2 has acquired its immediate
        // transaction but before the claim commit hook runs. The claim must
        // reject and rollback the marker, event, fence, claim, and reservation.
        yield* events.project(SwarmTaskClaimed, () =>
          db.run(sql`UPDATE kilo_swarm_task SET cancel_requested = 1 WHERE id = ${taskId}`).pipe(Effect.asVoid, Effect.orDie),
        )
        const attempt = yield* Effect.exit(
          SwarmClaims.claim({ taskId, ownerId: "racing-worker", leaseMs: 60000, actor: "act_test", corrId: "cor_race" as any }),
        )
        expect(attempt._tag).toBe("Failure")

        const task = yield* SwarmState.getTask(taskId)
        expect(task.cancel_requested).toBe(0)
        expect(task.fence).toBe(0)
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
  test("claim rejects a task that has already reached a terminal state", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 2 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        yield* SwarmState.transition({ taskId, to: "cancelled", expectedVersion: 0, actor: "act_test", corrId: "cor_cancelled" as any })

        const attempt = yield* Effect.exit(SwarmClaims.claim({ taskId, ownerId: "late-worker", leaseMs: 60000, actor: "act_test", corrId: "cor_claim" as any }))
        expect(attempt._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("cancelled")
        expect(task.fence).toBe(0)

        const { db } = yield* Database.Service
        const claimCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_claim WHERE task_id = ${taskId}`)
        const reservationCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_worker_reservation WHERE task_id = ${taskId}`)
        const claimedEventCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.claimed%'`)
        expect(claimCount?.count).toBe(0)
        expect(reservationCount?.count).toBe(0)
        expect(claimedEventCount?.count).toBe(0)
      }),
      file,
    )
  })
  test("reconciliation marks expired claims", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        yield* SwarmClaims.claim({ taskId, ownerId: "w1", leaseMs: 100, actor: "act_test", corrId: "cor_test" as any })
        yield* Effect.sleep("150 millis")
        yield* SwarmRecovery.reconcile()
      }),
      file,
    )
    const layer2 = dbLayer(file)
    const ev2 = EventV2.layerWith().pipe(Layer.provide(layer2))
    const full2 = Layer.mergeAll(layer2, ev2)
    const states = await Effect.runPromise(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        return yield* db.get<{ claim_state: string; reservation_state: string }>(sql`
          SELECT c.state AS claim_state, r.state AS reservation_state
          FROM kilo_swarm_claim c
          JOIN kilo_swarm_worker_reservation r ON r.task_id = c.task_id
          WHERE c.task_id IN (SELECT id FROM kilo_swarm_task)
        `)
      }).pipe(Effect.provide(full2)),
    )
    expect(states?.claim_state).toBe("expired")
    expect(states?.reservation_state).toBe("expired")
  })
})