import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { tmpdir } from "../../fixture/fixture"
import { sql } from "drizzle-orm"
import { Layer } from "effect"
import { SwarmState } from "@/kilocode/swarm/state"
import { SwarmTaskCancelRequested, SwarmTaskClaimed, SwarmTaskReviewPassed, SwarmTaskTransitioned } from "@/kilocode/swarm/events"
import { SwarmVerification } from "@/kilocode/swarm/verification"
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
const verifiedCommand = (command: string, sourceSessionId: string) => ({
  command,
  exitCode: 0,
  sourceSessionId,
  messageId: "msg_test",
  partId: "part_test",
  callId: "call_test",
  outputSha256: "a".repeat(64),
})

const seedHostCommandEvents = (
  db: Database.Interface["db"],
  input: { sessionId: string; command: string },
) => Effect.gen(function* () {
  const messageId = "msg_test"
  const callId = "call_test"
  const timestamp = "2026-10-26T00:00:00.000Z"
  const called = {
    sessionID: input.sessionId,
    timestamp,
    assistantMessageID: messageId,
    callID: callId,
    tool: "bash",
    input: { command: input.command },
    provider: { executed: true },
  }
  const succeeded = {
    sessionID: input.sessionId,
    timestamp,
    assistantMessageID: messageId,
    callID: callId,
    structured: { exit: 0 },
    content: [],
    provider: { executed: true },
  }
  yield* db.run(sql`
    INSERT INTO event_sequence (aggregate_id, seq, owner_id)
    VALUES (${input.sessionId}, 2, NULL)
    ON CONFLICT(aggregate_id) DO UPDATE SET seq = excluded.seq
  `).pipe(Effect.orDie)
  yield* db.run(sql`
    INSERT INTO event (id, aggregate_id, seq, type, data)
    VALUES (${"evt_" + input.sessionId + "_called"}, ${input.sessionId}, 1, 'session.next.tool.called', ${JSON.stringify(called)})
  `).pipe(Effect.orDie)
  yield* db.run(sql`
    INSERT INTO event (id, aggregate_id, seq, type, data)
    VALUES (${"evt_" + input.sessionId + "_success"}, ${input.sessionId}, 2, 'session.next.tool.success', ${JSON.stringify(succeeded)})
  `).pipe(Effect.orDie)
})

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

  test("lease renewal refuses cancellation-requested and terminal tasks", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()

        const cancellingTask = yield* SwarmState.createTask({
          swarmId, actor: "act_test", corrId: "cor_renew_cancel" as any,
        })
        const cancellingClaim = yield* SwarmClaims.claim({
          taskId: cancellingTask, ownerId: "w_cancel", leaseMs: 60_000,
          actor: "act_test", corrId: "cor_renew_cancel" as any,
        })
        yield* SwarmState.requestCancel({
          taskId: cancellingTask, actor: "act_test", corrId: "cor_renew_cancel" as any,
        })
        const cancelledRenewal = yield* Effect.exit(SwarmClaims.renew({
          taskId: cancellingTask, leaseId: cancellingClaim.leaseId,
          fence: cancellingClaim.fence, leaseMs: 120_000,
        }))
        expect(cancelledRenewal._tag).toBe("Failure")

        const terminalTask = yield* SwarmState.createTask({
          swarmId, actor: "act_test", corrId: "cor_renew_terminal" as any,
        })
        const terminalClaim = yield* SwarmClaims.claim({
          taskId: terminalTask, ownerId: "w_terminal", leaseMs: 60_000,
          actor: "act_test", corrId: "cor_renew_terminal" as any,
        })
        yield* SwarmState.transition({
          taskId: terminalTask, to: "cancelled", expectedVersion: 0,
          actor: "act_test", corrId: "cor_renew_terminal" as any,
        })
        const terminalRenewal = yield* Effect.exit(SwarmClaims.renew({
          taskId: terminalTask, leaseId: terminalClaim.leaseId,
          fence: terminalClaim.fence, leaseMs: 120_000,
        }))
        expect(terminalRenewal._tag).toBe("Failure")

        const { db } = yield* Database.Service
        const cancelClaim = yield* db.get<{ expires_at: number }>(sql`
          SELECT expires_at FROM kilo_swarm_claim WHERE task_id = ${cancellingTask}
        `)
        const cancelReservation = yield* db.get<{ expires_at: number }>(sql`
          SELECT expires_at FROM kilo_swarm_worker_reservation WHERE task_id = ${cancellingTask}
        `)
        const terminalClaimRow = yield* db.get<{ expires_at: number }>(sql`
          SELECT expires_at FROM kilo_swarm_claim WHERE task_id = ${terminalTask}
        `)
        const terminalReservation = yield* db.get<{ expires_at: number }>(sql`
          SELECT expires_at FROM kilo_swarm_worker_reservation WHERE task_id = ${terminalTask}
        `)
        expect(cancelClaim?.expires_at).toBe(cancellingClaim.expiresAt)
        expect(cancelReservation?.expires_at).toBe(cancellingClaim.expiresAt)
        expect(terminalClaimRow?.expires_at).toBe(terminalClaim.expiresAt)
        expect(terminalReservation?.expires_at).toBe(terminalClaim.expiresAt)
      }),
      file,
    )
  })

  test("idempotency: duplicate create returns same id for the same payload", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    const swarmId = await use(SwarmState.createSwarm(), file)
    const request = { swarmId, actor: "act_test", corrId: "cor_test" as any, idemKey: "k1" }
    const t1 = await use(SwarmState.createTask(request), file)
    const t2 = await use(SwarmState.createTask(request), file)
    expect(t1).toBe(t2)
  })

  test("idempotency: malformed task-create replay data fails closed", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_idempotency (scope, key, result, time_created)
          VALUES ('task.create', 'broken-create', '123', ${Date.now()})
        `)
        const attempt = yield* Effect.exit(SwarmState.createTask({
          swarmId, actor: "act_test", corrId: "cor_create" as any, idemKey: "broken-create",
        }))
        expect(attempt._tag).toBe("Failure")
        const taskCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_task`)
        expect(taskCount?.count).toBe(0)
      }),
      file,
    )
  })

  test("idempotency: duplicate create with a different payload replays the first result", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    const [swarmA, swarmB] = await use(
      Effect.gen(function* () {
        return [yield* SwarmState.createSwarm(), yield* SwarmState.createSwarm()] as const
      }),
      file,
    )
    const first = await use(SwarmState.createTask({
      swarmId: swarmA, actor: "act_test", corrId: "cor_first" as any, idemKey: "create-conflict",
    }), file)
    const duplicate = await use(SwarmState.createTask({
      swarmId: swarmB, actor: "act_test", corrId: "cor_second" as any, idemKey: "create-conflict",
    }), file)
    expect(duplicate).toBe(first)
    const tasksA = await use(SwarmState.listTasks(swarmA), file)
    const tasksB = await use(SwarmState.listTasks(swarmB), file)
    expect(tasksA.map((task: any) => task.id)).toContain(first)
    expect(tasksA.length).toBe(1)
    expect(tasksB.length).toBe(0)
  })

  test("idempotency: malformed cancellation replay data fails closed", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const { db } = yield* Database.Service
        const scope = `task.cancel:${taskId}`
        yield* db.run(sql`
          INSERT INTO kilo_swarm_idempotency (scope, key, result, time_created)
          VALUES (${scope}, 'broken-cancel', '{"cancelRequested":"yes","alreadyTerminal":false}', ${Date.now()})
        `)
        const attempt = yield* Effect.exit(SwarmState.requestCancel({
          taskId, actor: "act_test", corrId: "cor_cancel" as any, idemKey: "broken-cancel",
        }))
        expect(attempt._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.cancel_requested).toBe(0)
        const eventCount = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM event
          WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.cancel_requested%'
        `)
        expect(eventCount?.count).toBe(0)
      }),
      file,
    )
  })

  test("idempotency: malformed transition replay data fails closed", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const { db } = yield* Database.Service
        const scope = `task.transition:${taskId}`
        yield* db.run(sql`
          INSERT INTO kilo_swarm_idempotency (scope, key, result, time_created)
          VALUES (${scope}, 'broken-transition', 'not-json', ${Date.now()})
        `)

        const attempt = yield* Effect.exit(SwarmState.transition({
          taskId, to: "reserved", expectedVersion: 0, actor: "act_test",
          corrId: "cor_transition" as any, idemKey: "broken-transition",
        }))
        expect(attempt._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("queued")
        const historyCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_task_transition WHERE task_id = ${taskId}`)
        const eventCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.transitioned%'`)
        expect(historyCount?.count).toBe(0)
        expect(eventCount?.count).toBe(0)
      }),
      file,
    )
  })

  test("idempotency: duplicate transition with a different payload replays the first result", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const first = yield* SwarmState.transition({
          taskId, to: "reserved", expectedVersion: 0, actor: "act_test", corrId: "cor_first" as any, idemKey: "transition-conflict",
        })
        const replay = yield* SwarmState.transition({
          taskId, to: "reserved", expectedVersion: 0, actor: "another-actor", corrId: "cor_replay" as any, idemKey: "transition-conflict",
        })
        expect(replay).toEqual(first)
        const conflictingReplay = yield* SwarmState.transition({
          taskId, to: "cancelled", expectedVersion: 1, actor: "act_test", corrId: "cor_conflict" as any, idemKey: "transition-conflict",
        })
        expect(conflictingReplay).toEqual(first)
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("reserved")
        const { db } = yield* Database.Service
        const historyCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_task_transition WHERE task_id = ${taskId}`)
        const eventCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.transitioned%'`)
        expect(historyCount?.count).toBe(1)
        expect(eventCount?.count).toBe(1)
      }),
      file,
    )
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

  test("cancel request idempotency replays the original result and emits one event", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const first = yield* SwarmState.requestCancel({
          taskId, actor: "act_test", corrId: "cor_cancel" as any, idemKey: "cancel-once",
        })
        const second = yield* SwarmState.requestCancel({
          taskId, actor: "different-actor", corrId: "cor_retry" as any, idemKey: "cancel-once",
        })
        expect(first).toEqual({ cancelRequested: true, alreadyTerminal: false })
        expect(second).toEqual(first)

        const { db } = yield* Database.Service
        const eventCount = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM event
          WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.cancel_requested%'
        `)
        const scope = "task.cancel:" + taskId
        const idem = yield* db.get<{ result: string }>(sql`
          SELECT result FROM kilo_swarm_idempotency
          WHERE scope = ${scope} AND key = 'cancel-once'
        `)
        expect(eventCount?.count).toBe(1)
        expect(idem?.result).toBe(JSON.stringify(first))
      }),
      file,
    )
  })

  test("cancel request revalidates terminal state inside the durable event transaction", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const { db } = yield* Database.Service
        const events = yield* EventV2.Service

        // Simulate a terminal transition winning inside the same immediate
        // transaction before the cancellation commit hook validates its CAS.
        yield* events.project(SwarmTaskCancelRequested, () =>
          db.run(sql`
            UPDATE kilo_swarm_task SET state = 'cancelled', version = version + 1
            WHERE id = ${taskId}
          `).pipe(Effect.asVoid, Effect.orDie),
        )
        const attempt = yield* Effect.exit(SwarmState.requestCancel({
          taskId, actor: "act_test", corrId: "cor_race" as any,
        }))
        expect(attempt._tag).toBe("Failure")

        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("queued")
        expect(task.cancel_requested).toBe(0)
        const eventCount = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM event
          WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.cancel_requested%'
        `)
        expect(eventCount?.count).toBe(0)
      }),
      file,
    )
  })

  test("cancel request after a terminal transition is a no-op", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        yield* SwarmState.transition({
          taskId, to: "cancelled", expectedVersion: 0, actor: "act_test", corrId: "cor_transition" as any,
        })
        const result = yield* SwarmState.requestCancel({
          taskId, actor: "act_test", corrId: "cor_cancel" as any, idemKey: "cancel-terminal",
        })
        expect(result).toEqual({ cancelRequested: true, alreadyTerminal: true })
        const { db } = yield* Database.Service
        const eventCount = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM event
          WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.cancel_requested%'
        `)
        expect(eventCount?.count).toBe(0)
      }),
      file,
    )
  })

  test("transition refuses non-cancel state changes after cancellation is requested", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        yield* SwarmState.requestCancel({ taskId, actor: "act_test", corrId: "cor_cancel" as any })

        const attempt = yield* Effect.exit(SwarmState.transition({
          taskId, to: "reserved", expectedVersion: 0, actor: "act_test", corrId: "cor_transition" as any,
        }))
        expect(attempt._tag).toBe("Failure")

        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("queued")
        expect(task.cancel_requested).toBe(1)
        const { db } = yield* Database.Service
        const historyCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_task_transition WHERE task_id = ${taskId}`)
        const eventCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.transitioned%'`)
        expect(historyCount?.count).toBe(0)
        expect(eventCount?.count).toBe(0)
      }),
      file,
    )
  })

  test("transition to cancelled remains allowed after a cancellation request", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        yield* SwarmState.requestCancel({ taskId, actor: "act_test", corrId: "cor_cancel" as any })
        const result = yield* SwarmState.transition({
          taskId, to: "cancelled", expectedVersion: 0, actor: "act_test", corrId: "cor_transition" as any,
        })
        expect(result.version).toBe(1)
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("cancelled")
        expect(task.cancel_requested).toBe(1)
      }),
      file,
    )
  })

  test("transition rechecks cancellation marker inside the durable event transaction", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const { db } = yield* Database.Service
        const events = yield* EventV2.Service

        // Inject the cancellation marker inside EventV2's immediate transaction,
        // after the preflight snapshot but before the transition CAS.
        yield* events.project(SwarmTaskTransitioned, () =>
          db.run(sql`UPDATE kilo_swarm_task SET cancel_requested = 1 WHERE id = ${taskId}`).pipe(Effect.asVoid, Effect.orDie),
        )
        const attempt = yield* Effect.exit(SwarmState.transition({
          taskId, to: "reserved", expectedVersion: 0, actor: "act_test", corrId: "cor_race" as any,
        }))
        expect(attempt._tag).toBe("Failure")

        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("queued")
        expect(task.cancel_requested).toBe(0)
        const historyCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM kilo_swarm_task_transition WHERE task_id = ${taskId}`)
        const eventCount = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.transitioned%'`)
        expect(historyCount?.count).toBe(0)
        expect(eventCount?.count).toBe(0)
      }),
      file,
    )
  })

  test("acceptance gate refuses a task without an independent review", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_acceptance" as any })
        let version = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: version, actor: "act_test", corrId: "cor_acceptance" as any })
          version += 1
        }

        const attempt = yield* Effect.exit(SwarmState.transition({
          taskId, to: "accepted", expectedVersion: version, actor: "act_test", corrId: "cor_acceptance" as any,
        }))
        expect(attempt._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("reviewing")
        expect(task.version).toBe(version)
        const { db } = yield* Database.Service
        const acceptedTransitions = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM kilo_swarm_task_transition WHERE task_id = ${taskId} AND to_state = 'accepted'
        `)
        expect(acceptedTransitions?.count).toBe(0)
      }),
      file,
    )
  })

  test("acceptance gate permits a task only after a structured independent passing review", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_worker" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_reviewer" as any })
        let workerVersion = 0
        let reviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: workerVersion, actor: "act_test", corrId: "cor_worker" as any })
          workerVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_reviewer" as any })
          reviewerVersion += 1
        }

        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_independent_reviewer"}, ${"owner_test"}, ${"lease_test"}, 1, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
                yield* seedHostCommandEvents(db, {
          sessionId: "ses_independent_reviewer",
          command: "bun test test/kilocode/swarm/state.test.ts",
        })
        const review = yield* SwarmVerification.recordReview({
          taskId,
          reviewerTaskId,
          reviewerSessionId: "ses_independent_reviewer",
          reviewerAgent: "reviewer",
          corrId: "cor_review_record" as any,
          executedCommands: [verifiedCommand("bun test test/kilocode/swarm/state.test.ts", "ses_independent_reviewer")],
          report: {
            verdict: "pass",
            summary: "The worker output satisfies the declared test criterion.",
            checks: [{ criterion: "targeted tests pass", verdict: "pass", evidenceIds: ["e1"] }],
            evidence: [{
              id: "e1", kind: "command", reference: "bun test test/kilocode/swarm/state.test.ts",
              detail: "Targeted test command completed successfully.", result: "pass", exitCode: 0,
            }],
            blockingFindings: [],
            uncertainties: [],
          },
        })
        expect(review.accepted).toBe(true)
        const result = yield* SwarmState.transition({
          taskId, to: "accepted", expectedVersion: workerVersion, actor: "act_test", corrId: "cor_acceptance" as any,
        })
        expect(result.version).toBe(workerVersion + 1)
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("accepted")
      }),
      file,
    )
  })

  test("acceptance gate rechecks independent review inside the durable transition transaction", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_worker_race" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_reviewer_race" as any })
        let workerVersion = 0
        let reviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: workerVersion, actor: "act_test", corrId: "cor_worker_race" as any })
          workerVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_reviewer_race" as any })
          reviewerVersion += 1
        }
        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_racing_reviewer"}, ${"owner_test"}, ${"lease_test"}, 1, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* seedHostCommandEvents(db, {
          sessionId: "ses_racing_reviewer",
          command: "bun test test/kilocode/swarm/state.test.ts",
        })
        yield* SwarmVerification.recordReview({
          taskId,
          reviewerTaskId,
          reviewerSessionId: "ses_racing_reviewer",
          reviewerAgent: "reviewer",
          corrId: "cor_review_race" as any,
          executedCommands: [verifiedCommand("bun test test/kilocode/swarm/state.test.ts", "ses_racing_reviewer")],
          report: {
            verdict: "pass",
            summary: "Initial review passes but is removed in the transition transaction.",
            checks: [{ criterion: "tests pass", verdict: "pass", evidenceIds: ["e1"] }],
            evidence: [{ id: "e1", kind: "test", reference: "bun test test/kilocode/swarm/state.test.ts", detail: "Passed initially.", result: "pass", exitCode: 0 }],
            blockingFindings: [],
            uncertainties: [],
          },
        })
        const events = yield* EventV2.Service
        // Simulate a concurrent review invalidation after preflight but inside
        // EventV2's transaction, so the CAS commit-time qualification check must reject.
        yield* events.project(SwarmTaskTransitioned, () =>
          db.run(sql`DELETE FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.review_passed%'`).pipe(Effect.asVoid, Effect.orDie),
        )
        const attempt = yield* Effect.exit(SwarmState.transition({
          taskId, to: "accepted", expectedVersion: workerVersion, actor: "act_test", corrId: "cor_acceptance_race" as any,
        }))
        expect(attempt._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("reviewing")
        expect(task.version).toBe(workerVersion)
        const reviewEvent = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.review_passed%'
        `)
        expect(reviewEvent?.count).toBe(1)
      }),
      file,
    )
  })


  test("review event commit revalidates reviewer state", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_review_target" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_review_reviewer" as any })
        let targetVersion = 0
        let reviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: targetVersion, actor: "act_test", corrId: "cor_review_target" as any })
          targetVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_review_reviewer" as any })
          reviewerVersion += 1
        }

        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_racing_reviewer"}, ${"owner_test"}, ${"lease_test"}, 1, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* seedHostCommandEvents(db, {
          sessionId: "ses_racing_reviewer",
          command: "bun test test/kilocode/swarm/state.test.ts",
        })
        const events = yield* EventV2.Service
        // Change the reviewer after preflight but in the review event transaction.
        // The commit hook must reject this review and roll back the injected change.
        yield* events.project(SwarmTaskReviewPassed, () =>
          db.run(sql`UPDATE kilo_swarm_task SET state = 'failed' WHERE id = ${reviewerTaskId}`).pipe(Effect.asVoid, Effect.orDie),
        )
        const attempt = yield* Effect.exit(SwarmVerification.recordReview({
          taskId,
          reviewerTaskId,
          reviewerSessionId: "ses_racing_reviewer",
          reviewerAgent: "reviewer",
          corrId: "cor_review_commit_race" as any,
          executedCommands: [verifiedCommand("bun test test/kilocode/swarm/state.test.ts", "ses_racing_reviewer")],
          report: {
            verdict: "pass",
            summary: "Review should not be committed after reviewer state changes.",
            checks: [{ criterion: "tests pass", verdict: "pass", evidenceIds: ["e1"] }],
            evidence: [{ id: "e1", kind: "test", reference: "bun test test/kilocode/swarm/state.test.ts", detail: "Passed.", result: "pass", exitCode: 0 }],
            blockingFindings: [],
            uncertainties: [],
          },
        }))
        expect(attempt._tag).toBe("Failure")
        const reviewer = yield* SwarmState.getTask(reviewerTaskId)
        expect(reviewer.state).toBe("reviewing")
        const reviewEvent = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.review_passed%'
        `)
        expect(reviewEvent?.count).toBe(0)
      }),
      file,
    )
  })


  test("reviewer-asserted command success cannot qualify without host-recorded execution", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_evidence_target" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_evidence_reviewer" as any })
        let targetVersion = 0
        let reviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: targetVersion, actor: "act_test", corrId: "cor_evidence_target" as any })
          targetVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_evidence_reviewer" as any })
          reviewerVersion += 1
        }
        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_evidence_reviewer"}, ${"owner_test"}, ${"lease_test"}, 1, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)

        const recorded = yield* SwarmVerification.recordReview({
          taskId,
          reviewerTaskId,
          reviewerSessionId: "ses_evidence_reviewer",
          reviewerAgent: "reviewer",
          corrId: "cor_evidence_review" as any,
          executedCommands: [],
          report: {
            verdict: "pass",
            summary: "The reviewer claimed a test passed but no corresponding host tool execution exists.",
            checks: [{ criterion: "targeted tests pass", verdict: "pass", evidenceIds: ["e1"] }],
            evidence: [{
              id: "e1", kind: "command", reference: "bun test test/kilocode/swarm/state.test.ts",
              detail: "Reviewer-asserted exit code without a recorded invocation.", result: "pass", exitCode: 0,
            }],
            blockingFindings: [],
            uncertainties: [],
          },
        })
        expect(recorded.verdict).toBe("inconclusive")
        expect(recorded.accepted).toBe(false)
        const attempt = yield* Effect.exit(SwarmState.transition({
          taskId, to: "accepted", expectedVersion: targetVersion, actor: "act_test", corrId: "cor_evidence_accept" as any,
        }))
        expect(attempt._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("reviewing")
      }),
      file,
    )
  })

  test("review cannot be recorded against a session that is not bound to the reviewer task", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_binding_target" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_binding_reviewer" as any })
        let targetVersion = 0
        let reviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: targetVersion, actor: "act_test", corrId: "cor_binding_target" as any })
          targetVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_binding_reviewer" as any })
          reviewerVersion += 1
        }
        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_bound_reviewer"}, ${"owner_test"}, ${"lease_test"}, 1, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)

        const attempt = yield* Effect.exit(SwarmVerification.recordReview({
          taskId,
          reviewerTaskId,
          reviewerSessionId: "ses_forged_reviewer",
          reviewerAgent: "reviewer",
          corrId: "cor_binding_review" as any,
          executedCommands: [],
          report: {
            verdict: "pass",
            summary: "Structured review with execution evidence.",
            checks: [{ criterion: "tests pass", verdict: "pass", evidenceIds: ["e1"] }],
            evidence: [{ id: "e1", kind: "test", reference: "bun test", detail: "Command completed successfully.", result: "pass", exitCode: 0 }],
            blockingFindings: [],
            uncertainties: [],
          },
        }))
        expect(attempt._tag).toBe("Failure")
        const reviewEvents = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.review_passed%'
        `)
        expect(reviewEvents?.count).toBe(0)
      }),
      file,
    )
  })


  test("acceptance gate rejects a directly published review event without executable provenance", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_direct_target" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_direct_reviewer" as any })
        let targetVersion = 0
        let reviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: targetVersion, actor: "act_test", corrId: "cor_direct_target" as any })
          targetVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_direct_reviewer" as any })
          reviewerVersion += 1
        }
        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_direct_review"}, ${"owner_test"}, ${"lease_test"}, 1, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        const events = yield* EventV2.Service
        // A caller can technically publish a passing event, but the state gate must
        // reject it because no host-observed execution evidence was attached.
        yield* events.publish(SwarmTaskReviewPassed, {
          taskId: taskId as any,
          swarmId,
          reviewerTaskId: reviewerTaskId as any,
          reviewerSessionId: "ses_direct_review",
          reviewerAgent: "reviewer",
          verdict: "pass",
          summary: "Claimed to have reviewed the task.",
          evidenceJson: JSON.stringify([{ id: "e1", kind: "command", reference: "bun test", detail: "Claimed success.", result: "pass", exitCode: 0 }]),
          checksJson: JSON.stringify([{ criterion: "tests pass", verdict: "pass", evidenceIds: ["e1"] }]),
          blockingFindingsJson: "[]",
          uncertaintiesJson: "[]",
          corrId: "cor_direct_review" as any,
        })
        const attempt = yield* Effect.exit(SwarmState.transition({
          taskId, to: "accepted", expectedVersion: targetVersion, actor: "act_test", corrId: "cor_direct_accept" as any,
        }))
        expect(attempt._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("reviewing")
      }),
      file,
    )
  })

  test("a later blocked review overrides an earlier passing review", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_latest_target" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_latest_reviewer" as any })
        let targetVersion = 0
        let reviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: targetVersion, actor: "act_test", corrId: "cor_latest_target" as any })
          targetVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_latest_reviewer" as any })
          reviewerVersion += 1
        }
        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_latest_reviewer"}, ${"owner_test"}, ${"lease_test"}, 1, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
                yield* seedHostCommandEvents(db, {
          sessionId: "ses_latest_reviewer",
          command: "bun test test/kilocode/swarm/state.test.ts",
        })
        const passed = yield* SwarmVerification.recordReview({
          taskId,
          reviewerTaskId,
          reviewerSessionId: "ses_latest_reviewer",
          reviewerAgent: "reviewer",
          corrId: "cor_latest_pass" as any,
          report: {
            verdict: "pass",
            summary: "Initial review passed.",
            checks: [{ criterion: "targeted tests pass", verdict: "pass", evidenceIds: ["e1"] }],
            evidence: [{ id: "e1", kind: "test", reference: "bun test test/kilocode/swarm/state.test.ts", detail: "Tests passed.", result: "pass", exitCode: 0 }],
            blockingFindings: [],
            uncertainties: [],
          },
          executedCommands: [verifiedCommand("bun test test/kilocode/swarm/state.test.ts", "ses_latest_reviewer")],
        })
        expect(passed.accepted).toBe(true)
        const blocked = yield* SwarmVerification.recordReview({
          taskId,
          reviewerTaskId,
          reviewerSessionId: "ses_latest_reviewer",
          reviewerAgent: "reviewer",
          corrId: "cor_latest_blocked" as any,
          report: {
            verdict: "fail",
            summary: "A later review found a blocking regression.",
            checks: [{ criterion: "regression check", verdict: "fail", evidenceIds: ["e2"] }],
            evidence: [{ id: "e2", kind: "file", reference: "src/example.ts", detail: "The implementation contradicts the requirement.", result: "observed" }],
            blockingFindings: ["Regression remains."],
            uncertainties: [],
          },
          executedCommands: [],
        })
        expect(blocked.accepted).toBe(false)
        const attempt = yield* Effect.exit(SwarmState.transition({
          taskId, to: "accepted", expectedVersion: targetVersion, actor: "act_test", corrId: "cor_latest_accept" as any,
        }))
        expect(attempt._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("reviewing")
        const blockedEvents = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.review_blocked%'
        `)
        expect(blockedEvents?.count).toBe(1)
      }),
      file,
    )
  })

})
