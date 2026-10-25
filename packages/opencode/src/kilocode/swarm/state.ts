// Kilo Swarm M2 — authoritative swarm/task state, lifecycle transitions, CAS.
//
// Kilo-owned. Uses EventV2 as the canonical durable event mechanism
// (commit hook runs inside the same immediate transaction as state writes).
import { Effect, Schema } from "effect"
import { randomUUID } from "node:crypto"
import { Database } from "@opencode-ai/core/database/database"
import * as Log from "@opencode-ai/core/util/log"
import { sql } from "drizzle-orm"
import { SwarmTaskID, TaskState, Lifecycle, TerminalStates, ActorID, CorrelationID } from "./schema"
import { SwarmTable, SwarmTaskTable, SwarmTaskTransitionTable, SwarmIdempotencyTable } from "@opencode-ai/core/kilocode/swarm/sql"
import { EventV2 } from "@opencode-ai/core/event"
import { SwarmTaskCreated, SwarmTaskTransitioned, SwarmTaskCancelRequested } from "./events"

const log = Log.create({ service: "swarm.state" })

type DB = Database.Interface["db"]
type TX = Parameters<Parameters<DB["transaction"]>[0]>[0]

export namespace SwarmState {
  // --- helpers ---------------------------------------------------------------

  const IDempotencyEnvelopeMarker = "__kiloSwarmIdempotencyV1" as const

  const encodeIdempotencyResult = <T>(request: unknown, result: T) =>
    JSON.stringify({
      [IDempotencyEnvelopeMarker]: true,
      request: JSON.stringify(request),
      result,
    })

  const decodeIdempotencyResult = <T>(
    stored: string,
    allowLegacyRawString = false,
  ): { result: T; malformed: boolean } => {
    let parsed: unknown
    try {
      parsed = JSON.parse(stored)
    } catch {
      // Only legacy task.create rows stored the task ID as an unquoted raw string.
      return allowLegacyRawString
        ? { result: stored as T, malformed: false }
        : { result: undefined as T, malformed: true }
    }

    if (
      typeof parsed === "object" &&
      parsed !== null &&
      (parsed as Record<string, unknown>)[IDempotencyEnvelopeMarker] === true
    ) {
      const envelope = parsed as { request?: unknown; result?: T }
      if (typeof envelope.request !== "string" || !Object.prototype.hasOwnProperty.call(parsed, "result")) {
        return { result: undefined as T, malformed: true }
      }
      // Existing M2 contract: the first committed result wins for an idempotency
      // key. Request payload is retained in the envelope for diagnostics, but a
      // different payload must not cause the already-committed operation to run again.
      return { result: envelope.result as T, malformed: false }
    }

    // Older rows stored the result directly, without the versioned envelope.
    return { result: parsed as T, malformed: false }
  }

  const decodeCancellationResult = (
    stored: string,
  ): { result: { cancelRequested: true; alreadyTerminal: boolean } | undefined; malformed: boolean } => {
    let parsed: unknown
    try {
      parsed = JSON.parse(stored)
    } catch {
      return { result: undefined, malformed: true }
    }

    if (
      typeof parsed !== "object" ||
      parsed === null ||
      (parsed as Record<string, unknown>).cancelRequested !== true ||
      typeof (parsed as Record<string, unknown>).alreadyTerminal !== "boolean"
    ) {
      return { result: undefined, malformed: true }
    }
    return { result: parsed as { cancelRequested: true; alreadyTerminal: boolean }, malformed: false }
  }

  export const newSwarmId = () => `swm_${randomUUID()}`
  export const newTaskId = () => `tsk_${randomUUID()}`

  export class NotFound extends Schema.TaggedErrorClass<NotFound>()("SwarmState.NotFound", {
    message: Schema.String,
  }) {}

  export class IllegalTransition extends Schema.TaggedErrorClass<IllegalTransition>()("SwarmState.IllegalTransition", {
    message: Schema.String,
  }) {}

  export class VersionConflict extends Schema.TaggedErrorClass<VersionConflict>()("SwarmState.VersionConflict", {
    message: Schema.String,
  }) {}

  export class IdempotencyConflict extends Schema.TaggedErrorClass<IdempotencyConflict>()("SwarmState.IdempotencyConflict", {
    message: Schema.String,
  }) {}

  export class InvalidCapacity extends Schema.TaggedErrorClass<InvalidCapacity>()("SwarmState.InvalidCapacity", {
    message: Schema.String,
  }) {}

  export class CancelConflict extends Schema.TaggedErrorClass<CancelConflict>()("SwarmState.CancelConflict", {
    message: Schema.String,
  }) {}

  // --- swarm -----------------------------------------------------------------

  export const createSwarm = Effect.fn("SwarmState.createSwarm")(function* (input: { maxWorkers?: number } = {}) {
    const { db } = yield* Database.Service
    const maxWorkers = input.maxWorkers ?? 4
    if (!Number.isSafeInteger(maxWorkers) || maxWorkers < 0) {
      return yield* Effect.fail(new InvalidCapacity({ message: "maxWorkers must be a non-negative safe integer" }))
    }
    const id = newSwarmId()
    const now = Date.now()
    yield* db.run(sql`INSERT INTO kilo_swarm (id, state, version, max_workers, time_created, time_updated) VALUES (${id}, 'active', 0, ${maxWorkers}, ${now}, ${now})`)
    return id
  })

  // --- tasks -----------------------------------------------------------------

  export const createTask = Effect.fn("SwarmState.createTask")(function* (input: {
    swarmId: string
    parentId?: string
    actor: string
    corrId: string
    idemKey?: string
  }) {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const idempotencyRequest = { swarmId: input.swarmId, parentId: input.parentId ?? null }

    // The first committed task.create result wins for a key; retries do not create another task.
    if (input.idemKey) {
      const existing = yield* db
        .get<{ result: string }>(sql`SELECT result FROM kilo_swarm_idempotency WHERE scope = 'task.create' AND key = ${input.idemKey}`)
        .pipe(Effect.orDie)
      if (existing) {
        const replay = decodeIdempotencyResult<string>(existing.result, true)
        if (replay.malformed || typeof replay.result !== "string" || !replay.result.startsWith("tsk_")) {
          return yield* Effect.fail(new IdempotencyConflict({ message: "stored task.create idempotency result is malformed" }))
        }
        return replay.result
      }
    }

    const taskId = newTaskId()
    const now = Date.now()

    yield* events
      .publish(
        SwarmTaskCreated,
        { taskId: taskId as any, swarmId: input.swarmId, state: "queued", parentId: (input.parentId ?? null) as any, corrId: input.corrId as any },
        {
          commit: (seq) =>
            Effect.gen(function* () {
              yield* db.run(sql`
                INSERT INTO kilo_swarm_task (id, swarm_id, state, version, fence, cancel_requested, parent_id, attempts, time_created, time_updated)
                VALUES (${taskId}, ${input.swarmId}, 'queued', 0, 0, 0, ${input.parentId ?? null}, 0, ${now}, ${now})
              `)
              if (input.idemKey) {
                yield* db.run(sql`
                  INSERT INTO kilo_swarm_idempotency (scope, key, result, time_created)
                  VALUES ('task.create', ${input.idemKey}, ${encodeIdempotencyResult(idempotencyRequest, taskId)}, ${now})
                `)
              }
            }).pipe(Effect.orDie),
        },
      )
      .pipe(
        Effect.catchDefect((defect) => {
          if (!input.idemKey) return Effect.die(defect)
          return Effect.gen(function* () {
            // A concurrent caller with the same key may have committed first.
            const existing = yield* db
              .get<{ result: string }>(sql`SELECT result FROM kilo_swarm_idempotency WHERE scope = 'task.create' AND key = ${input.idemKey}`)
              .pipe(Effect.orDie)
            if (!existing) return yield* Effect.die(defect)
            const replay = decodeIdempotencyResult<string>(existing.result, true)
            if (replay.malformed || typeof replay.result !== "string" || !replay.result.startsWith("tsk_")) {
              return yield* Effect.fail(new IdempotencyConflict({ message: "stored task.create idempotency result is malformed" }))
            }
            return replay.result
          })
        }),
      )

    return taskId
  })

  export const getTask = Effect.fn("SwarmState.getTask")(function* (taskId: string) {
    const { db } = yield* Database.Service
    const row = yield* db
      .get<{
        id: string; swarm_id: string; state: string; version: number; fence: number
        cancel_requested: number; parent_id: string | null; attempts: number
        time_created: number; time_updated: number
      }>(sql`SELECT id, swarm_id, state, version, fence, cancel_requested, parent_id, attempts, time_created, time_updated FROM kilo_swarm_task WHERE id = ${taskId}`)
      .pipe(Effect.orDie)
    if (!row) return yield* Effect.fail(new NotFound({ message: `task not found: ${taskId}` }))
    return row
  })

  export const transition = Effect.fn("SwarmState.transition")(function* (input: {
    taskId: string
    to: string
    expectedVersion: number
    actor: string
    corrId: string
    idemKey?: string
  }) {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const scope = input.idemKey ? `task.transition:${input.taskId}` : undefined
    const idempotencyRequest = { to: input.to, expectedVersion: input.expectedVersion }

    // The first committed transition result wins for a key; retries never apply a second transition.
    if (scope) {
      const existing = yield* db
        .get<{ result: string }>(sql`SELECT result FROM kilo_swarm_idempotency WHERE scope = ${scope} AND key = ${input.idemKey}`)
        .pipe(Effect.orDie)
      if (existing) {
        const replay = decodeIdempotencyResult<{ version: number }>(existing.result)
        if (replay.malformed || !replay.result || !Number.isSafeInteger(replay.result.version) || replay.result.version < 1) {
          return yield* Effect.fail(new IdempotencyConflict({ message: "stored task.transition idempotency result is malformed" }))
        }
        return replay.result
      }
    }

    const task = yield* getTask(input.taskId)
    const from = task.state
    // legal transition check
    const allowed = (Lifecycle as any)[from] as string[] | undefined
    if (!allowed || !allowed.includes(input.to)) {
      return yield* Effect.fail(new IllegalTransition({ message: `illegal transition ${from} -> ${input.to}` }))
    }
    if (input.expectedVersion !== task.version) {
      return yield* Effect.fail(new VersionConflict({ message: `expected version ${input.expectedVersion}, got ${task.version}` }))
    }
    if (task.cancel_requested !== 0 && input.to !== "cancelled") {
      return yield* Effect.fail(new CancelConflict({ message: `task ${input.taskId} has a cancellation request and may only transition to cancelled` }))
    }

    const now = Date.now()
    const nextVersion = task.version + 1
    const seq = (yield* db
      .get<{ c: number }>(sql`SELECT COUNT(*) AS c FROM kilo_swarm_task_transition WHERE task_id = ${input.taskId}`)
      .pipe(Effect.orDie))?.c ?? 0

    yield* events
      .publish(
        SwarmTaskTransitioned,
        {
          taskId: input.taskId as any,
          swarmId: task.swarm_id,
          from: from as any,
          to: input.to as any,
          seq,
          versionBefore: task.version,
          versionAfter: nextVersion,
          fence: task.fence,
          actor: input.actor,
          corrId: input.corrId as any,
          idemKey: input.idemKey,
        },
        {
          commit: (eventSeq) =>
            Effect.gen(function* () {
              // Revalidate the snapshot under EventV2's immediate transaction.
              // Claim acquisition also advances fence without changing task version.
              const current = yield* db.get<{ state: string; version: number; fence: number; cancel_requested: number }>(sql`
                SELECT state, version, fence, cancel_requested FROM kilo_swarm_task WHERE id = ${input.taskId}
              `)
              if (
                !current ||
                current.state !== from ||
                current.version !== task.version ||
                current.fence !== task.fence ||
                current.version !== input.expectedVersion ||
                current.cancel_requested !== task.cancel_requested ||
                (current.cancel_requested !== 0 && input.to !== "cancelled")
              ) {
                return yield* Effect.die(new VersionConflict({ message: "CAS failed: task state, version, or fence changed" }))
              }
              const currentAllowed = (Lifecycle as any)[current.state] as string[] | undefined
              if (!currentAllowed || !currentAllowed.includes(input.to)) {
                return yield* Effect.die(new IllegalTransition({ message: `illegal transition ${current.state} -> ${input.to}` }))
              }
              const updated = yield* db.get<{ id: string }>(sql`
                UPDATE kilo_swarm_task
                SET state = ${input.to}, version = ${nextVersion}, time_updated = ${now}
                WHERE id = ${input.taskId} AND version = ${task.version} AND state = ${from} AND fence = ${task.fence}
                  AND cancel_requested = ${task.cancel_requested}
                RETURNING id
              `)
              if (!updated) {
                return yield* Effect.die(new VersionConflict({ message: "CAS update changed no task row" }))
              }
              yield* db.run(sql`
                INSERT INTO kilo_swarm_task_transition (id, task_id, swarm_id, seq, from_state, to_state, version_before, version_after, fence, actor, corr_id, idem_key, time_created)
                VALUES (${`tr_${randomUUID()}`}, ${input.taskId}, ${task.swarm_id}, ${seq}, ${from}, ${input.to}, ${task.version}, ${nextVersion}, ${task.fence}, ${input.actor}, ${input.corrId}, ${input.idemKey ?? null}, ${now})
              `)
              if (input.idemKey && scope) {
                yield* db.run(sql`
                  INSERT INTO kilo_swarm_idempotency (scope, key, result, time_created)
                  VALUES (${scope}, ${input.idemKey}, ${encodeIdempotencyResult(idempotencyRequest, { version: nextVersion })}, ${now})
                `)
              }
              void eventSeq
            }).pipe(Effect.orDie),
        },
      )
      .pipe(
        Effect.catchDefect((defect) => {
          if (!scope) return Effect.die(defect)
          return Effect.gen(function* () {
            // If another request with the same key committed first, replay its
            // result after our losing transaction has rolled back.
            const existing = yield* db
              .get<{ result: string }>(sql`SELECT result FROM kilo_swarm_idempotency WHERE scope = ${scope} AND key = ${input.idemKey}`)
              .pipe(Effect.orDie)
            if (!existing) return yield* Effect.die(defect)
            const replay = decodeIdempotencyResult<{ version: number }>(existing.result)
            if (replay.malformed || !replay.result || !Number.isSafeInteger(replay.result.version) || replay.result.version < 1) {
              return yield* Effect.fail(new IdempotencyConflict({ message: "stored task.transition idempotency result is malformed" }))
            }
            return replay.result
          })
        }),
      )

    return { version: nextVersion }
  })

  export const requestCancel = Effect.fn("SwarmState.requestCancel")(function* (input: {
    taskId: string
    actor: string
    corrId: string
    idemKey?: string
  }) {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const scope = input.idemKey ? "task.cancel:" + input.taskId : undefined

    // Replays return the first committed result and must not publish another event.
    if (scope) {
      const existing = yield* db
        .get<{ result: string }>(sql`SELECT result FROM kilo_swarm_idempotency WHERE scope = ${scope} AND key = ${input.idemKey}`)
        .pipe(Effect.orDie)
      if (existing) {
        const replay = decodeCancellationResult(existing.result)
        if (replay.malformed || !replay.result) {
          return yield* Effect.fail(new IdempotencyConflict({ message: "stored task.cancel idempotency result is malformed" }))
        }
        return replay.result
      }
    }

    const task = yield* getTask(input.taskId)
    const completed = (alreadyTerminal: boolean) => ({ cancelRequested: true, alreadyTerminal })

    // A repeated cancellation request is a no-op even if the caller uses a new
    // key. Persist that key so a later retry replays the same logical result.
    if (TerminalStates.includes(task.state as any) || task.cancel_requested !== 0) {
      const result = completed(TerminalStates.includes(task.state as any))
      if (scope) {
        yield* db.run(sql`
          INSERT INTO kilo_swarm_idempotency (scope, key, result, time_created)
          VALUES (${scope}, ${input.idemKey}, ${JSON.stringify(result)}, ${Date.now()})
          ON CONFLICT(scope, key) DO NOTHING
        `).pipe(Effect.orDie)
        const existing = yield* db
          .get<{ result: string }>(sql`SELECT result FROM kilo_swarm_idempotency WHERE scope = ${scope} AND key = ${input.idemKey}`)
          .pipe(Effect.orDie)
        if (existing) {
          const replay = decodeCancellationResult(existing.result)
          if (replay.malformed || !replay.result) {
            return yield* Effect.fail(new IdempotencyConflict({ message: "stored task.cancel idempotency result is malformed" }))
          }
          return replay.result
        }
      }
      return result
    }

    const now = Date.now()
    const outcome = yield* events
      .publish(
        SwarmTaskCancelRequested,
        { taskId: input.taskId as any, swarmId: task.swarm_id, corrId: input.corrId as any },
        {
          commit: () =>
            Effect.gen(function* () {
              // The durable event and cancellation marker commit only if this
              // exact preflight snapshot still wins the immediate transaction.
              const current = yield* db.get<{
                swarm_id: string
                state: string
                version: number
                fence: number
                cancel_requested: number
              }>(sql`
                SELECT swarm_id, state, version, fence, cancel_requested
                FROM kilo_swarm_task WHERE id = ${input.taskId}
              `)
              if (
                !current ||
                current.swarm_id !== task.swarm_id ||
                current.state !== task.state ||
                current.version !== task.version ||
                current.fence !== task.fence ||
                current.cancel_requested !== task.cancel_requested ||
                current.cancel_requested !== 0 ||
                TerminalStates.includes(current.state as any)
              ) {
                return yield* Effect.die(new CancelConflict({
                  message: "task " + input.taskId + " changed before cancellation commit; retry",
                }))
              }

              const updated = yield* db.get<{ id: string }>(sql`
                UPDATE kilo_swarm_task SET cancel_requested = 1, time_updated = ${Date.now()}
                WHERE id = ${input.taskId} AND swarm_id = ${task.swarm_id}
                  AND state = ${task.state} AND version = ${task.version}
                  AND fence = ${task.fence} AND cancel_requested = 0
                RETURNING id
              `)
              if (!updated) {
                return yield* Effect.die(new CancelConflict({
                  message: "cancellation CAS failed for task " + input.taskId + "; retry",
                }))
              }
              if (scope) {
                const result = completed(false)
                yield* db.run(sql`
                  INSERT INTO kilo_swarm_idempotency (scope, key, result, time_created)
                  VALUES (${scope}, ${input.idemKey}, ${JSON.stringify(result)}, ${now})
                `)
              }
            }).pipe(Effect.orDie),
        },
      )
      .pipe(
        Effect.as(completed(false)),
        Effect.catchDefect((defect) => {
          if (!(defect instanceof CancelConflict)) return Effect.die(defect)
          // A competing committed cancellation/terminal transition may have
          // won after the preflight read. The failed EventV2 transaction has
          // rolled back before this handler runs, so it is safe to inspect and
          // return the winning result without emitting a duplicate event.
          return Effect.gen(function* () {
            if (scope) {
              const existing = yield* db
                .get<{ result: string }>(sql`SELECT result FROM kilo_swarm_idempotency WHERE scope = ${scope} AND key = ${input.idemKey}`)
                .pipe(Effect.orDie)
              if (existing) {
                const replay = decodeCancellationResult(existing.result)
                if (replay.malformed || !replay.result) {
                  return yield* Effect.fail(new IdempotencyConflict({ message: "stored task.cancel idempotency result is malformed" }))
                }
                return replay.result
              }
            }

            const current = yield* db.get<{ state: string; cancel_requested: number }>(sql`
              SELECT state, cancel_requested FROM kilo_swarm_task WHERE id = ${input.taskId}
            `).pipe(Effect.orDie)
            if (!current) return yield* Effect.die(defect)
            if (current.cancel_requested === 0 && !TerminalStates.includes(current.state as any)) {
              // The snapshot changed for another reason; do not mask a stale
              // transition/fence conflict as successful cancellation.
              return yield* Effect.die(defect)
            }

            const result = completed(TerminalStates.includes(current.state as any))
            if (scope) {
              yield* db.run(sql`
                INSERT INTO kilo_swarm_idempotency (scope, key, result, time_created)
                VALUES (${scope}, ${input.idemKey}, ${JSON.stringify(result)}, ${Date.now()})
                ON CONFLICT(scope, key) DO NOTHING
              `).pipe(Effect.orDie)
              const existing = yield* db
                .get<{ result: string }>(sql`SELECT result FROM kilo_swarm_idempotency WHERE scope = ${scope} AND key = ${input.idemKey}`)
                .pipe(Effect.orDie)
              if (existing) {
                const replay = decodeCancellationResult(existing.result)
                if (replay.malformed || !replay.result) {
                  return yield* Effect.fail(new IdempotencyConflict({ message: "stored task.cancel idempotency result is malformed" }))
                }
                return replay.result
              }
            }
            return result
          })
        }),
      )

    return outcome
  })
  export const listTasks = Effect.fn("SwarmState.listTasks")(function* (swarmId: string) {
    const { db } = yield* Database.Service
    const rows = yield* db
      .all<{ id: string; swarm_id: string; state: string; version: number; fence: number; cancel_requested: number; parent_id: string | null; attempts: number; time_created: number; time_updated: number }>(
        sql`SELECT id, swarm_id, state, version, fence, cancel_requested, parent_id, attempts, time_created, time_updated FROM kilo_swarm_task WHERE swarm_id = ${swarmId} ORDER BY time_created ASC`,
      )
      .pipe(Effect.orDie)
    return rows
  })
}