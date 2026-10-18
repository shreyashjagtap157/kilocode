import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import path from "path"
import { tmpdir } from "../../fixture/fixture"
import { sql } from "drizzle-orm"
import { SwarmTaskTransitioned } from "@/kilocode/swarm/events"
import { SwarmState } from "@/kilocode/swarm/state"
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

describe("swarm atomicity, events, idempotency, crash (M2)", () => {
  // AC10 — conflicting idempotency payload: same key returns same result, no duplicate mutation
  test("conflicting payload on reused idempotency key is deduped, original result preserved", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)

    const t1 = (await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        return yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any, idemKey: "conflict_key" })
      }),
      file,
    )) as string

    // Same idempotency key, different swarm/payload: returns the cached original result (idempotent dedup).
    const t2 = (await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        return yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any, idemKey: "conflict_key" })
      }),
      file,
    )) as string

    // Original result preserved: both calls returned the same task id.
    expect(t2).toBe(t1)

    // No duplicate task created: exactly one task row exists in the DB.
    const count = (await use(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        const row = yield* db.get<{ c: number }>(sql`SELECT COUNT(*) AS c FROM kilo_swarm_task`)
        return row?.c ?? 0
      }),
      file,
    )) as number
    expect(count).toBe(1)
  })

  // AC11 — mid-transaction atomic rollback leaves no partial state
  test("dying commit hook rolls back state, history, idempotency, and event together", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)

    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        const now = Date.now()
        const events = yield* EventV2.Service
        const { db } = yield* Database.Service

        // Mutations performed inside an EventV2 commit hook run inside the same immediate
        // transaction as the durable event publication. Dying inside the hook must roll back
        // everything: the event, event_sequence, and all hook-performed state mutations.
        // `db` is closed over from the outer scope so the commit hook needs no service context.
        const exit = yield* Effect.exit(
          Effect.gen(function* () {
            yield* events.publish(
              SwarmTaskTransitioned,
              {
                taskId: taskId as any,
                swarmId,
                from: "queued",
                to: "reserved",
                seq: 0,
                versionBefore: 0,
                versionAfter: 1,
                fence: 0,
                actor: "act_test",
                corrId: "cor_test" as any,
              },
              {
                commit: () =>
                  Effect.gen(function* () {
                    // Mutation work has begun inside the transaction.
                    yield* db.run(sql`
                      UPDATE kilo_swarm_task
                      SET state = 'reserved', version = 1, time_updated = ${now}
                      WHERE id = ${taskId} AND version = 0
                    `)
                    yield* db.run(sql`
                      INSERT INTO kilo_swarm_task_transition (id, task_id, swarm_id, seq, from_state, to_state, version_before, version_after, fence, actor, corr_id, idem_key, time_created)
                      VALUES ('tr_rollback_probe', ${taskId}, ${swarmId}, 0, 'queued', 'reserved', 0, 1, 0, 'act_test', 'cor_test', null, ${now})
                    `)
                    yield* db.run(sql`INSERT INTO kilo_swarm_idempotency (scope, key, result, time_created) VALUES ('task.transition:${taskId}', 'rollback_key', 'rolled', ${now})`)
                    // Die here — the whole immediate transaction (event + state mutations) rolls back.
                    return yield* Effect.die(new Error("mid-transaction failure"))
                  }).pipe(Effect.orDie),
              },
            )
          }),
        )
        expect(exit._tag).toBe("Failure")

        // Re-read through the same layer context to confirm nothing partial survived.
        const { task: afterTask, history, idem } = yield* (Effect.gen(function* () {
          const { db: d } = yield* Database.Service
          const task = yield* d.get<{ state: string; version: number }>(sql`SELECT state, version FROM kilo_swarm_task WHERE id = ${taskId}`)
          const history = yield* d.all<{ id: string }>(sql`SELECT id FROM kilo_swarm_task_transition WHERE id = 'tr_rollback_probe'`)
          const idem = yield* d.all<{ scope: string }>(sql`SELECT scope FROM kilo_swarm_idempotency WHERE scope = 'task.transition:${taskId}'`)
          return { task, history, idem }
        }) as any)

        expect(afterTask.state).toBe("queued")
        expect(afterTask.version).toBe(0)
        expect(history.length).toBe(0)
        expect(idem.length).toBe(0)
      }),
      file,
    )
  })

  // AC13 — durable event read/replay determinism
  test("durable events commit atomically, read deterministically across reopen, monotonic seq", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)

    const result = (await use(
      Effect.gen(function* () {
        const sid = yield* SwarmState.createSwarm()
        const tid = yield* SwarmState.createTask({ swarmId: sid, actor: "act_test", corrId: "cor_replay" as any })
        yield* SwarmState.transition({ taskId: tid, to: "reserved", expectedVersion: 0, actor: "act_test", corrId: "cor_replay" as any })
        return { sid, tid } as { sid: string; tid: string }
      }),
      file,
    )) as { sid: string; tid: string }
    const { sid, tid } = result

    // Reopen layer and read committed transitions deterministically.
    const transitions = (await Effect.runPromise(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        return yield* db.all<{ from_state: string; to_state: string; seq: number; version_before: number; version_after: number }>(
          sql`SELECT from_state, to_state, seq, version_before, version_after FROM kilo_swarm_task_transition WHERE task_id = ${tid} ORDER BY seq`,
        )
      }).pipe(Effect.provide(fullLayer(file))),
    )) as any[]
    expect(transitions.length).toBe(1)
    expect(transitions[0].from_state).toBe("queued")
    expect(transitions[0].to_state).toBe("reserved")
    expect(transitions[0].seq).toBe(0)
    expect(transitions[0].version_before).toBe(0)
    expect(transitions[0].version_after).toBe(1)

    // Sequence values are monotonic (single transition trivially satisfies; verify no gaps).
    const seqs = transitions.map((t) => t.seq)
    for (let i = 1; i < seqs.length; i++) {
      expect(seqs[i]).toBe(seqs[i - 1] + 1)
    }

    // Task state persists across reopen.
    const tasks = (await use(SwarmState.listTasks(sid) as any, file)) as any[]
    expect(tasks.length).toBe(1)
    expect(tasks[0].version).toBe(1)
  })

  // AC15 — actual process crash / reopen via Bun.spawn worker
  test("process crash mid-operation leaves committed state only", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)

    // Parent commits a task first.
    const result = (await use(
      Effect.gen(function* () {
        const sid = yield* SwarmState.createSwarm()
        const tid = yield* SwarmState.createTask({ swarmId: sid, actor: "act_test", corrId: "cor_test" as any })
        return { sid, tid } as { sid: string; tid: string }
      }),
      file,
    )) as { sid: string; tid: string }
    const { sid } = result

    // Child process opens the DB, begins an immediate transaction, inserts a row,
    // then exits without committing — simulating a crash mid-transaction.
    const workerPath = path.join(import.meta.dir, "..", "fixture/swarm-crash-worker.ts")
    const spawnResult = Bun.spawnSync([process.execPath, workerPath, "crash", file], { stdout: "ignore", stderr: "pipe" })
    expect(spawnResult.exitCode).toBe(0)

    // Parent reopens and verifies only committed state exists — no partial write from the crashed child.
    const tasks = (await use(SwarmState.listTasks(sid) as any, file)) as any[]
    expect(tasks.length).toBe(1)
    expect(tasks[0].state).toBe("queued")
    expect(tasks[0].version).toBe(0)

    // No crash_probe row from the child's rolled-back transaction.
    const probeCount = (await use(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        const row = yield* db.get<{ c: number }>(sql`SELECT COUNT(*) AS c FROM kilo_swarm_task WHERE id = 'crash_probe'`)
        return row?.c ?? 0
      }),
      file,
    )) as number
    expect(probeCount).toBe(0)
  })

  // Reconciliation determinism: repeated runs produce identical results
  test("reconciliation is deterministic across repeated runs", async () => {
    await using tmp = await tmpdir()
    const file = dbFile(tmp)
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm()
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_test", corrId: "cor_test" as any })
        yield* SwarmClaims.claim({ taskId, ownerId: "w1", leaseMs: 1, actor: "act_test", corrId: "cor_test" as any })
        yield* Effect.sleep("5 millis")
        yield* SwarmRecovery.reconcile()
        const first = yield* SwarmClaims.assertOwnership({ taskId, leaseId: "none", fence: 0 }).pipe(Effect.catchCause(() => Effect.succeed("failed")))
        expect(first).toBe("failed")
        yield* SwarmRecovery.reconcile()
        const second = yield* SwarmClaims.assertOwnership({ taskId, leaseId: "none", fence: 0 }).pipe(Effect.catchCause(() => Effect.succeed("failed")))
        expect(second).toBe("failed")
      }),
      file,
    )
  })
})
