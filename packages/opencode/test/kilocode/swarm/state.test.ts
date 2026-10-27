import { describe, expect, test } from "bun:test"
import { createHash } from "node:crypto"
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
const verifiedCommand = (
  command: string,
  sourceSessionId: string,
  exitCode = 0,
  output = "tests passed",
) => ({
  command,
  exitCode,
  sourceSessionId,
  messageId: "msg_" + sourceSessionId,
  partId: "prt_" + sourceSessionId,
  callId: "call_" + sourceSessionId,
  outputSha256: createHash("sha256").update(output).digest("hex"),
})

const seedHostCommandPart = (
  db: Database.Interface["db"],
  input: { sessionId: string; command: string; exitCode?: number; output?: string },
) => Effect.gen(function* () {
  const messageId = "msg_" + input.sessionId
  const partId = "prt_" + input.sessionId
  const callId = "call_" + input.sessionId
  const now = Date.now()
  // PartTable.message_id has a real foreign key. Seed the minimal parent chain
  // so these tests exercise canonical persisted tool parts rather than bypassing
  // the database's referential-integrity rules.
  yield* db.run(sql`
    INSERT OR IGNORE INTO project (id, worktree, name, time_created, time_updated, sandboxes)
    VALUES (${"prj_swarm_review_test"}, ${"/tmp/kilo-swarm-review-test"}, ${"swarm-review-test"}, ${now}, ${now}, ${JSON.stringify(["/tmp/kilo-swarm-review-test"])})
  `).pipe(Effect.orDie)
  yield* db.run(sql`
    INSERT OR IGNORE INTO session (id, project_id, slug, directory, title, version, time_created, time_updated)
    VALUES (${input.sessionId}, ${"prj_swarm_review_test"}, ${"swarm-review-test"}, ${"/tmp/kilo-swarm-review-test"}, ${"Swarm review test"}, ${"test"}, ${now}, ${now})
  `).pipe(Effect.orDie)
  yield* db.run(sql`
    INSERT OR IGNORE INTO message (id, session_id, time_created, time_updated, data)
    VALUES (${messageId}, ${input.sessionId}, ${now}, ${now}, ${JSON.stringify({ role: "assistant", time: { created: now, completed: now } })})
  `).pipe(Effect.orDie)
  const part = {
    type: "tool",
    callID: callId,
    tool: "bash",
    state: {
      status: "completed",
      input: { command: input.command },
      output: input.output ?? "tests passed",
      metadata: { exit: input.exitCode ?? 0 },
      title: "test validation command",
      time: { start: now, end: now },
    },
  }
  yield* db.run(sql`
    INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
    VALUES (${partId}, ${messageId}, ${input.sessionId}, ${now}, ${now}, ${JSON.stringify(part)})
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
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_independent_reviewer"}, ${"owner_test"}, ${"lease_test"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* seedHostCommandPart(db, {
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
        expect(review.executionEvidenceProblems).toEqual([])
        expect(review.executionEvidence).toHaveLength(1)
        expect(review.executionEvidence[0]).toMatchObject({
          command: "bun test test/kilocode/swarm/state.test.ts",
          exitCode: 0,
          sourceSessionId: "ses_independent_reviewer",
        })
        expect(review.executionEvidence[0]?.outputSha256).toBe(createHash("sha256").update("tests passed").digest("hex"))
        // Complete the review job first. The target may still be accepted because
        // its durable review event records the now-terminal independent reviewer.
        const reviewerCompletion = yield* SwarmState.transition({
          taskId: reviewerTaskId,
          to: "reviewed",
          expectedVersion: reviewerVersion,
          actor: "act_test",
          corrId: "cor_review_task_complete" as any,
        })
        expect(reviewerCompletion.version).toBe(reviewerVersion + 1)
        const reviewer = yield* SwarmState.getTask(reviewerTaskId)
        expect(reviewer.state).toBe("reviewed")

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


  test("a swarm configured for two independent reviews cannot accept after only one passing review", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    const command = "bun test test/kilocode/swarm/state.test.ts"
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ requiredReviews: 2 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_two_reviews_target" as any })
        const reviewerOneTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_two_reviews_reviewer_one" as any })
        const reviewerTwoTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_two_reviews_reviewer_two" as any })
        let targetVersion = 0
        let reviewerOneVersion = 0
        let reviewerTwoVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: targetVersion, actor: "act_test", corrId: "cor_two_reviews_target" as any })
          targetVersion += 1
          yield* SwarmState.transition({ taskId: reviewerOneTaskId, to, expectedVersion: reviewerOneVersion, actor: "act_test", corrId: "cor_two_reviews_reviewer_one" as any })
          reviewerOneVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTwoTaskId, to, expectedVersion: reviewerTwoVersion, actor: "act_test", corrId: "cor_two_reviews_reviewer_two" as any })
          reviewerTwoVersion += 1
        }
        const { db } = yield* Database.Service
        const reviewerOneSession = "ses_two_reviews_one"
        const reviewerTwoSession = "ses_two_reviews_two"
        for (const binding of [
          { taskId: reviewerOneTaskId, sessionId: reviewerOneSession, leaseId: "lease_two_reviews_one" },
          { taskId: reviewerTwoTaskId, sessionId: reviewerTwoSession, leaseId: "lease_two_reviews_two" },
        ]) {
          yield* db.run(sql`
            INSERT INTO kilo_swarm_worker_binding
              (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
            VALUES (${binding.taskId}, ${swarmId}, ${binding.sessionId}, ${"owner_test"}, ${binding.leaseId}, 0, 'bound', ${Date.now()}, ${Date.now()})
          `).pipe(Effect.orDie)
          yield* seedHostCommandPart(db, { sessionId: binding.sessionId, command })
        }

        const recordPassingReview = (reviewerTaskId: string, reviewerSessionId: string, corrId: string) =>
          SwarmVerification.recordReview({
            taskId,
            reviewerTaskId,
            reviewerSessionId,
            reviewerAgent: "independent-reviewer",
            corrId: corrId as any,
            executedCommands: [verifiedCommand(command, reviewerSessionId)],
            report: {
              verdict: "pass",
              summary: "The reviewer independently verified the required validation.",
              checks: [{ criterion: "targeted tests pass", verdict: "pass", evidenceIds: ["e1"] }],
              evidence: [{
                id: "e1", kind: "test", reference: command,
                detail: "The recorded validation command exited successfully.",
                result: "pass", exitCode: 0,
              }],
              blockingFindings: [],
              uncertainties: [],
            },
          })

        const firstReview = yield* recordPassingReview(reviewerOneTaskId, reviewerOneSession, "cor_two_reviews_record_one")
        expect(firstReview.accepted).toBe(true)
        yield* SwarmState.transition({
          taskId: reviewerOneTaskId,
          to: "reviewed",
          expectedVersion: reviewerOneVersion,
          actor: "act_test",
          corrId: "cor_two_reviews_close_one" as any,
        })
        const prematureAcceptance = yield* Effect.exit(SwarmState.transition({
          taskId,
          to: "accepted",
          expectedVersion: targetVersion,
          actor: "act_test",
          corrId: "cor_two_reviews_premature_accept" as any,
        }))
        expect(prematureAcceptance._tag).toBe("Failure")
        expect((yield* SwarmState.getTask(taskId)).state).toBe("reviewing")

        const secondReview = yield* recordPassingReview(reviewerTwoTaskId, reviewerTwoSession, "cor_two_reviews_record_two")
        expect(secondReview.accepted).toBe(true)
        yield* SwarmState.transition({
          taskId: reviewerTwoTaskId,
          to: "reviewed",
          expectedVersion: reviewerTwoVersion,
          actor: "act_test",
          corrId: "cor_two_reviews_close_two" as any,
        })
        const accepted = yield* SwarmState.transition({
          taskId,
          to: "accepted",
          expectedVersion: targetVersion,
          actor: "act_test",
          corrId: "cor_two_reviews_accept" as any,
        })
        expect(accepted.version).toBe(targetVersion + 1)
        expect((yield* SwarmState.getTask(taskId)).state).toBe("accepted")
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
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_racing_reviewer"}, ${"owner_test"}, ${"lease_test"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* seedHostCommandPart(db, {
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
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_racing_reviewer"}, ${"owner_test"}, ${"lease_test"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* seedHostCommandPart(db, {
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
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_evidence_reviewer"}, ${"owner_test"}, ${"lease_test"}, 0, 'bound', ${Date.now()}, ${Date.now()})
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
        expect(recorded.executionEvidence).toEqual([])
        expect(recorded.executionEvidenceProblems).toContain("review contains no host-recorded validation execution")
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

  test("review rejects mismatched sessions, expired bindings, and stale fences", async () => {
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
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_bound_reviewer"}, ${"owner_test"}, ${"lease_test"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        const report = {
          verdict: "pass",
          summary: "Structured review with execution evidence.",
          checks: [{ criterion: "tests pass", verdict: "pass", evidenceIds: ["e1"] }],
          evidence: [{ id: "e1", kind: "test", reference: "bun test test/kilocode/swarm/state.test.ts", detail: "Command completed successfully.", result: "pass", exitCode: 0 }],
          blockingFindings: [],
          uncertainties: [],
        }
        const forgedSession = yield* Effect.exit(SwarmVerification.recordReview({
          taskId, reviewerTaskId, reviewerSessionId: "ses_forged_reviewer", reviewerAgent: "reviewer",
          corrId: "cor_binding_review" as any, executedCommands: [], report,
        }))
        expect(forgedSession._tag).toBe("Failure")

        yield* db.run(sql`UPDATE kilo_swarm_worker_binding SET state = 'expired' WHERE task_id = ${reviewerTaskId}`).pipe(Effect.orDie)
        const expiredBinding = yield* Effect.exit(SwarmVerification.recordReview({
          taskId, reviewerTaskId, reviewerSessionId: "ses_bound_reviewer", reviewerAgent: "reviewer",
          corrId: "cor_expired_binding_review" as any, executedCommands: [], report,
        }))
        expect(expiredBinding._tag).toBe("Failure")

        yield* db.run(sql`UPDATE kilo_swarm_worker_binding SET state = 'bound', fence = 99 WHERE task_id = ${reviewerTaskId}`).pipe(Effect.orDie)
        const staleFence = yield* Effect.exit(SwarmVerification.recordReview({
          taskId, reviewerTaskId, reviewerSessionId: "ses_bound_reviewer", reviewerAgent: "reviewer",
          corrId: "cor_stale_fence_review" as any, executedCommands: [], report,
        }))
        expect(staleFence._tag).toBe("Failure")

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
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_direct_review"}, ${"owner_test"}, ${"lease_test"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        const events = yield* EventV2.Service
        // A caller can forge structurally valid evidence and a passing review event.
        // The state gate must reject it because there is no matching persisted shell
        // call/success in the claimed reviewer session.
        yield* events.publish(SwarmTaskReviewPassed, {
          taskId: taskId as any,
          swarmId,
          reviewerTaskId: reviewerTaskId as any,
          reviewerSessionId: "ses_direct_review",
          reviewerAgent: "reviewer",
          verdict: "pass",
          summary: "Claimed to have reviewed the task.",
          evidenceJson: JSON.stringify([{
            id: "e1", kind: "command", reference: "bun test test/kilocode/swarm/state.test.ts",
            detail: "Claimed success.", result: "pass", exitCode: 0,
          }]),
          executionEvidenceJson: JSON.stringify([verifiedCommand("bun test test/kilocode/swarm/state.test.ts", "ses_direct_review")]),
          checksJson: JSON.stringify([{ criterion: "tests pass", verdict: "pass", evidenceIds: ["e1"] }]),
          blockingFindingsJson: "[]",
          uncertaintiesJson: "[]",
          corrId: "cor_direct_review" as any,
        })
        const reviewerClosure = yield* Effect.exit(SwarmState.transition({
          taskId: reviewerTaskId,
          to: "reviewed",
          expectedVersion: reviewerVersion,
          actor: "act_test",
          corrId: "cor_direct_review_close" as any,
        }))
        expect(reviewerClosure._tag).toBe("Failure")
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
        const blockedReviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_latest_blocked_reviewer" as any })
        let targetVersion = 0
        let reviewerVersion = 0
        let blockedReviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: targetVersion, actor: "act_test", corrId: "cor_latest_target" as any })
          targetVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_latest_reviewer" as any })
          reviewerVersion += 1
          yield* SwarmState.transition({ taskId: blockedReviewerTaskId, to, expectedVersion: blockedReviewerVersion, actor: "act_test", corrId: "cor_latest_blocked_reviewer" as any })
          blockedReviewerVersion += 1
        }
        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${"ses_latest_reviewer"}, ${"owner_test"}, ${"lease_test"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${blockedReviewerTaskId}, ${swarmId}, ${"ses_latest_blocker"}, ${"owner_test"}, ${"lease_test_blocker"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* seedHostCommandPart(db, {
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
          reviewerTaskId: blockedReviewerTaskId,
          reviewerSessionId: "ses_latest_blocker",
          reviewerAgent: "reviewer-2",
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


  test("host execution evidence rejects failed exits and mismatched output hashes", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    const command = "bun test test/kilocode/swarm/state.test.ts"
    await use(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        const failedSession = "ses_host_exit_mismatch"
        yield* seedHostCommandPart(db, { sessionId: failedSession, command, exitCode: 1 })
        const failed = yield* SwarmVerification.hostExecutionProblems(db, {
          executionEvidenceJson: JSON.stringify([verifiedCommand(command, failedSession, 1)]),
          reviewerSessionId: failedSession,
        })
        // The failed execution is authentic evidence and should be preserved.
        // It still cannot satisfy a review criterion claiming exitCode 0.
        expect(failed).toEqual([])
        const qualification = SwarmVerification.qualificationProblems({
          summary: "Claimed success despite an observed failed exit.",
          evidenceJson: JSON.stringify([{
            id: "e1", kind: "command", reference: command, detail: "Claimed to pass.", result: "pass", exitCode: 0,
          }]),
          checksJson: JSON.stringify([{ criterion: "tests pass", verdict: "pass", evidenceIds: ["e1"] }]),
          blockingFindingsJson: "[]",
          uncertaintiesJson: "[]",
          executionEvidenceJson: JSON.stringify([verifiedCommand(command, failedSession, 1)]),
          reviewerSessionId: failedSession,
        })
        expect(qualification.some((item) => item.includes("no passing test/validation command"))).toBe(true)

        const hashSession = "ses_host_output_mismatch"
        yield* seedHostCommandPart(db, { sessionId: hashSession, command, output: "trusted output" })
        const mismatchedHash = yield* SwarmVerification.hostExecutionProblems(db, {
          executionEvidenceJson: JSON.stringify([verifiedCommand(command, hashSession, 0, "forged output")]),
          reviewerSessionId: hashSession,
        })
        expect(mismatchedHash.length).toBeGreaterThan(0)
        expect(mismatchedHash[0]).toContain("same output hash")
      }),
      file,
    )
  })


  test("acceptance gate blocks unqualified child tasks", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    const command = "bun test test/kilocode/swarm/state.test.ts"
    const reviewerSessionId = "ses_child_gate_reviewer"
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_child_parent" as any })
        const childTaskId = yield* SwarmState.createTask({ swarmId, parentId: taskId, actor: "act_test", corrId: "cor_child_pending" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_child_reviewer" as any })
        let targetVersion = 0
        let reviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: targetVersion, actor: "act_test", corrId: "cor_child_parent" as any })
          targetVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_child_reviewer" as any })
          reviewerVersion += 1
        }
        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${reviewerSessionId}, ${"owner_test"}, ${"lease_test"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* seedHostCommandPart(db, { sessionId: reviewerSessionId, command })
        const review = yield* SwarmVerification.recordReview({
          taskId,
          reviewerTaskId,
          reviewerSessionId,
          reviewerAgent: "reviewer",
          corrId: "cor_child_review" as any,
          executedCommands: [verifiedCommand(command, reviewerSessionId)],
          report: {
            verdict: "pass",
            summary: "The parent work passed independent review, but its child is unfinished.",
            checks: [{ criterion: "targeted tests pass", verdict: "pass", evidenceIds: ["e1"] }],
            evidence: [{ id: "e1", kind: "test", reference: command, detail: "Tests completed successfully.", result: "pass", exitCode: 0 }],
            blockingFindings: [],
            uncertainties: [],
          },
        })
        expect(review.accepted).toBe(true)
        const attempt = yield* Effect.exit(SwarmState.transition({
          taskId, to: "accepted", expectedVersion: targetVersion, actor: "act_test", corrId: "cor_child_accept" as any,
        }))
        expect(attempt._tag).toBe("Failure")
        const parent = yield* SwarmState.getTask(taskId)
        const child = yield* SwarmState.getTask(childTaskId)
        expect(parent.state).toBe("reviewing")
        expect(child.state).toBe("queued")
      }),
      file,
    )
  })


  test("reviewer cannot transition to reviewed without a durable review event", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_review_terminal" as any })
        let version = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: version, actor: "act_test", corrId: "cor_review_terminal" as any })
          version += 1
        }
        const attempt = yield* Effect.exit(SwarmState.transition({
          taskId, to: "reviewed", expectedVersion: version, actor: "act_test", corrId: "cor_review_terminal_no_event" as any,
        }))
        expect(attempt._tag).toBe("Failure")
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("reviewing")
        expect(task.version).toBe(version)
      }),
      file,
    )
  })


  test("a reviewer task cannot audit more than one target", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const firstTargetId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_single_use_target_1" as any })
        const secondTargetId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_single_use_target_2" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_single_use_reviewer" as any })
        let firstVersion = 0
        let secondVersion = 0
        let reviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId: firstTargetId, to, expectedVersion: firstVersion, actor: "act_test", corrId: "cor_single_use_target_1" as any })
          firstVersion += 1
          yield* SwarmState.transition({ taskId: secondTargetId, to, expectedVersion: secondVersion, actor: "act_test", corrId: "cor_single_use_target_2" as any })
          secondVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_single_use_reviewer" as any })
          reviewerVersion += 1
        }
        const { db } = yield* Database.Service
        const reviewerSessionId = "ses_single_use_reviewer"
        const command = "bun test test/kilocode/swarm/state.test.ts"
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${reviewerSessionId}, ${"owner_test"}, ${"lease_single_use"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* seedHostCommandPart(db, { sessionId: reviewerSessionId, command })
        const report = {
          verdict: "pass",
          summary: "A first independent review verified the target.",
          checks: [{ criterion: "target tests pass", verdict: "pass", evidenceIds: ["e1"] }],
          evidence: [{ id: "e1", kind: "test", reference: command, detail: "Persisted test command succeeded.", result: "pass", exitCode: 0 }],
          blockingFindings: [],
          uncertainties: [],
        }
        const first = yield* SwarmVerification.recordReview({
          taskId: firstTargetId,
          reviewerTaskId,
          reviewerSessionId,
          reviewerAgent: "reviewer",
          corrId: "cor_single_use_first" as any,
          report,
          executedCommands: [verifiedCommand(command, reviewerSessionId)],
        })
        expect(first.accepted).toBe(true)
        const reused = yield* Effect.exit(SwarmVerification.recordReview({
          taskId: secondTargetId,
          reviewerTaskId,
          reviewerSessionId,
          reviewerAgent: "reviewer",
          corrId: "cor_single_use_second" as any,
          report,
          executedCommands: [verifiedCommand(command, reviewerSessionId)],
        }))
        expect(reused._tag).toBe("Failure")
        const secondReviewEvents = yield* db.get<{ count: number }>(sql`
          SELECT COUNT(*) AS count FROM event
          WHERE aggregate_id = ${secondTargetId}
            AND (type LIKE 'swarm.task.review_passed%' OR type LIKE 'swarm.task.review_blocked%')
        `)
        expect(secondReviewEvents?.count).toBe(0)

        // A caller that bypasses recordReview must not reuse the already-consumed
        // reviewer task by publishing a second structurally plausible PASS directly.
        const events = yield* EventV2.Service
        yield* events.publish(SwarmTaskReviewPassed, {
          taskId: secondTargetId as any,
          swarmId,
          reviewerTaskId: reviewerTaskId as any,
          reviewerSessionId,
          reviewerAgent: "reviewer",
          verdict: "pass",
          summary: report.summary,
          evidenceJson: JSON.stringify(report.evidence),
          executionEvidenceJson: JSON.stringify([verifiedCommand(command, reviewerSessionId)]),
          checksJson: JSON.stringify(report.checks),
          blockingFindingsJson: JSON.stringify(report.blockingFindings),
          uncertaintiesJson: JSON.stringify(report.uncertainties),
          corrId: "cor_single_use_direct_publish" as any,
        })
        const reviewerClosure = yield* Effect.exit(SwarmState.transition({
          taskId: reviewerTaskId,
          to: "reviewed",
          expectedVersion: reviewerVersion,
          actor: "act_test",
          corrId: "cor_single_use_close_with_duplicate" as any,
        }))
        expect(reviewerClosure._tag).toBe("Failure")

        const forgedAcceptance = yield* Effect.exit(SwarmState.transition({
          taskId: secondTargetId,
          to: "accepted",
          expectedVersion: secondVersion,
          actor: "act_test",
          corrId: "cor_single_use_forged_accept" as any,
        }))
        expect(forgedAcceptance._tag).toBe("Failure")
        expect((yield* SwarmState.getTask(secondTargetId)).state).toBe("reviewing")
      }),
      file,
    )
  })


  test("acceptance gate uses durable evidence after the reviewer closes and its binding expires", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_review_lifecycle_worker" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_review_lifecycle_reviewer" as any })
        let workerVersion = 0
        let reviewerVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId, to, expectedVersion: workerVersion, actor: "act_test", corrId: "cor_review_lifecycle_worker" as any })
          workerVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_review_lifecycle_reviewer" as any })
          reviewerVersion += 1
        }
        const sessionId = "ses_review_lifecycle"
        const command = "bun test test/kilocode/swarm/state.test.ts"
        const { db } = yield* Database.Service
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${sessionId}, ${"owner_test"}, ${"lease_test"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* seedHostCommandPart(db, { sessionId, command })
        const review = yield* SwarmVerification.recordReview({
          taskId,
          reviewerTaskId,
          reviewerSessionId: sessionId,
          reviewerAgent: "reviewer",
          corrId: "cor_review_lifecycle_record" as any,
          report: {
            verdict: "pass",
            summary: "The independent review has valid host-recorded test evidence.",
            checks: [{ criterion: "targeted tests pass", verdict: "pass", evidenceIds: ["e1"] }],
            evidence: [{ id: "e1", kind: "test", reference: command, detail: "The recorded test command exited with code 0.", result: "pass", exitCode: 0 }],
            blockingFindings: [],
            uncertainties: [],
          },
          executedCommands: [verifiedCommand(command, sessionId)],
        })
        expect(review.accepted).toBe(true)

        const beforeClosure = yield* Effect.exit(SwarmState.transition({
          taskId,
          to: "accepted",
          expectedVersion: workerVersion,
          actor: "act_test",
          corrId: "cor_review_lifecycle_early_accept" as any,
        }))
        expect(beforeClosure._tag).toBe("Failure")
        const workerBeforeClosure = yield* SwarmState.getTask(taskId)
        expect(workerBeforeClosure.state).toBe("reviewing")
        expect(workerBeforeClosure.version).toBe(workerVersion)

        yield* SwarmState.transition({
          taskId: reviewerTaskId,
          to: "reviewed",
          expectedVersion: reviewerVersion,
          actor: "act_test",
          corrId: "cor_review_lifecycle_close" as any,
        })
        const reviewer = yield* SwarmState.getTask(reviewerTaskId)
        expect(reviewer.state).toBe("reviewed")

        // Simulate startup recovery after the reviewer completed and released
        // its lease. Recovery expires the execution binding, but the durable review
        // event and matching session/fence remain valid audit provenance.
        yield* SwarmRecovery.reconcile()
        const recoveredBinding = yield* db.get<{ state: string }>(sql`
          SELECT state FROM kilo_swarm_worker_binding WHERE task_id = ${reviewerTaskId}
        `).pipe(Effect.orDie)
        expect(recoveredBinding?.state).toBe("expired")

        const accepted = yield* SwarmState.transition({
          taskId,
          to: "accepted",
          expectedVersion: workerVersion,
          actor: "act_test",
          corrId: "cor_review_lifecycle_accept" as any,
        })
        expect(accepted.version).toBe(workerVersion + 1)
        expect((yield* SwarmState.getTask(taskId)).state).toBe("accepted")
      }),
      file,
    )
  })


  test("reviewer-task single-use is enforced across swarms", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const otherSwarmId = yield* SwarmState.createSwarm()
        const targetId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_cross_swarm_target" as any })
        const reviewerTaskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_cross_swarm_reviewer" as any })
        const forgedTargetId = yield* SwarmState.createTask({ swarmId: otherSwarmId, actor: "act_test", corrId: "cor_cross_swarm_forged_target" as any })
        let targetVersion = 0
        let reviewerVersion = 0
        let forgedTargetVersion = 0
        for (const to of ["reserved", "dispatched", "running", "reviewing"]) {
          yield* SwarmState.transition({ taskId: targetId, to, expectedVersion: targetVersion, actor: "act_test", corrId: "cor_cross_swarm_target" as any })
          targetVersion += 1
          yield* SwarmState.transition({ taskId: reviewerTaskId, to, expectedVersion: reviewerVersion, actor: "act_test", corrId: "cor_cross_swarm_reviewer" as any })
          reviewerVersion += 1
          yield* SwarmState.transition({ taskId: forgedTargetId, to, expectedVersion: forgedTargetVersion, actor: "act_test", corrId: "cor_cross_swarm_forged_target" as any })
          forgedTargetVersion += 1
        }

        const { db } = yield* Database.Service
        const reviewerSessionId = "ses_cross_swarm_reviewer"
        const command = "bun test test/kilocode/swarm/state.test.ts"
        yield* db.run(sql`
          INSERT INTO kilo_swarm_worker_binding
            (task_id, swarm_id, session_id, owner_id, lease_id, fence, state, time_created, time_updated)
          VALUES (${reviewerTaskId}, ${swarmId}, ${reviewerSessionId}, ${"owner_test"}, ${"lease_cross_swarm"}, 0, 'bound', ${Date.now()}, ${Date.now()})
        `).pipe(Effect.orDie)
        yield* seedHostCommandPart(db, { sessionId: reviewerSessionId, command })

        const report = {
          verdict: "pass",
          summary: "A valid review of the target in the reviewer's own swarm.",
          checks: [{ criterion: "targeted tests pass", verdict: "pass", evidenceIds: ["e1"] }],
          evidence: [{ id: "e1", kind: "test", reference: command, detail: "Persisted test command exited zero.", result: "pass", exitCode: 0 }],
          blockingFindings: [],
          uncertainties: [],
        }
        const recorded = yield* SwarmVerification.recordReview({
          taskId: targetId,
          reviewerTaskId,
          reviewerSessionId,
          reviewerAgent: "reviewer",
          corrId: "cor_cross_swarm_valid_review" as any,
          report,
          executedCommands: [verifiedCommand(command, reviewerSessionId)],
        })
        expect(recorded.accepted).toBe(true)

        // Bypass recordReview and publish a second review event in a different
        // swarm. Reviewer-task identity must remain globally single-use at closure.
        const events = yield* EventV2.Service
        yield* events.publish(SwarmTaskReviewPassed, {
          taskId: forgedTargetId as any,
          swarmId: otherSwarmId,
          reviewerTaskId: reviewerTaskId as any,
          reviewerSessionId,
          reviewerAgent: "reviewer",
          verdict: "pass",
          summary: report.summary,
          evidenceJson: JSON.stringify(report.evidence),
          executionEvidenceJson: JSON.stringify([verifiedCommand(command, reviewerSessionId)]),
          checksJson: JSON.stringify(report.checks),
          blockingFindingsJson: JSON.stringify(report.blockingFindings),
          uncertaintiesJson: JSON.stringify(report.uncertainties),
          corrId: "cor_cross_swarm_forged_review" as any,
        })

        const closure = yield* Effect.exit(SwarmState.transition({
          taskId: reviewerTaskId,
          to: "reviewed",
          expectedVersion: reviewerVersion,
          actor: "act_test",
          corrId: "cor_cross_swarm_closure" as any,
        }))
        expect(closure._tag).toBe("Failure")
        const reviewer = yield* SwarmState.getTask(reviewerTaskId)
        expect(reviewer.state).toBe("reviewing")
        expect(reviewer.version).toBe(reviewerVersion)
      }),
      file,
    )
  })

})
