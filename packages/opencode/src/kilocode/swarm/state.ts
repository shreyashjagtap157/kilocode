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

    // idempotency pre-check
    if (input.idemKey) {
      const existing = yield* db
        .get<{ result: string }>(sql`SELECT result FROM kilo_swarm_idempotency WHERE scope = 'task.create' AND key = ${input.idemKey}`)
        .pipe(Effect.orDie)
      if (existing) return existing.result
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
                  VALUES ('task.create', ${input.idemKey}, ${taskId}, ${now})
                `)
              }
            }).pipe(Effect.orDie),
        },
      )
      .pipe(Effect.catchDefect((defect) => Effect.die(defect)))

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

    // idempotency pre-check
    if (input.idemKey) {
      const scope = `task.transition:${input.taskId}`
      const existing = yield* db
        .get<{ result: string }>(sql`SELECT result FROM kilo_swarm_idempotency WHERE scope = ${scope} AND key = ${input.idemKey}`)
        .pipe(Effect.orDie)
      if (existing) return JSON.parse(existing.result) as { version: number }
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

    const now = Date.now()
    const nextVersion = task.version + 1
    const seq = (yield* db
      .get<{ c: number }>(sql`SELECT COUNT(*) AS c FROM kilo_swarm_task_transition WHERE task_id = ${input.taskId}`)
      .pipe(Effect.orDie))?.c ?? 0

    const scope = input.idemKey ? `task.transition:${input.taskId}` : undefined

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
              const current = yield* db.get<{ state: string; version: number; fence: number }>(sql`
                SELECT state, version, fence FROM kilo_swarm_task WHERE id = ${input.taskId}
              `)
              if (!current || current.state !== from || current.version !== task.version || current.fence !== task.fence || current.version !== input.expectedVersion) {
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
                  VALUES (${scope}, ${input.idemKey}, ${JSON.stringify({ version: nextVersion })}, ${now})
                `)
              }
              void eventSeq
            }).pipe(Effect.orDie),
        },
      )
      .pipe(Effect.catchDefect((defect) => Effect.die(defect)))

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
    const task = yield* getTask(input.taskId)
    if (TerminalStates.includes(task.state as any)) {
      return { cancelRequested: true, alreadyTerminal: true }
    }
    const now = Date.now()
    yield* events
      .publish(
        SwarmTaskCancelRequested,
        { taskId: input.taskId as any, swarmId: task.swarm_id, corrId: input.corrId as any },
        {
          commit: () =>
            Effect.gen(function* () {
              yield* db.run(sql`UPDATE kilo_swarm_task SET cancel_requested = 1, time_updated = ${now} WHERE id = ${input.taskId}`)
            }).pipe(Effect.orDie),
        },
      )
      .pipe(Effect.catchDefect((defect) => Effect.die(defect)))
    return { cancelRequested: true, alreadyTerminal: false }
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