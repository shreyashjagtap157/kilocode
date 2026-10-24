// Kilo Swarm M2 — durable claims, leases, fencing.
import { Effect, Schema } from "effect"
import { randomUUID } from "node:crypto"
import { Database } from "@opencode-ai/core/database/database"
import { sql } from "drizzle-orm"
import { EventV2 } from "@opencode-ai/core/event"
import { SwarmTaskClaimed, SwarmTaskReleased } from "./events"
import { SwarmState } from "./state"

export namespace SwarmClaims {
  export class ClaimConflict extends Schema.TaggedErrorClass<ClaimConflict>()("SwarmClaims.ClaimConflict", {
    message: Schema.String,
  }) {}

  export class LeaseExpired extends Schema.TaggedErrorClass<LeaseExpired>()("SwarmClaims.LeaseExpired", {
    message: Schema.String,
  }) {}

  export class CapacityConflict extends Schema.TaggedErrorClass<CapacityConflict>()("SwarmClaims.CapacityConflict", {
    message: Schema.String,
  }) {}

  export class StaleFence extends Schema.TaggedErrorClass<StaleFence>()("SwarmClaims.StaleFence", {
    message: Schema.String,
  }) {}

  export const claim = Effect.fn("SwarmClaims.claim")(function* (input: {
    taskId: string
    ownerId: string
    leaseMs: number
    actor: string
    corrId: string
  }) {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    if (!Number.isSafeInteger(input.leaseMs) || input.leaseMs <= 0) {
      return yield* Effect.fail(new LeaseExpired({ message: `leaseMs must be a positive safe integer for ${input.taskId}` }))
    }
    const task = yield* SwarmState.getTask(input.taskId)
    const now = Date.now()
    const expiresAt = now + input.leaseMs

    // Fast rejection only. The same predicate is rechecked inside the
    // EventV2 BEGIN IMMEDIATE transaction, where it is authoritative.
    const existing = yield* db
      .get<{ owner_id: string; lease_id: string; fence: number; expires_at: number; state: string }>(
        sql`SELECT owner_id, lease_id, fence, expires_at, state FROM kilo_swarm_claim WHERE task_id = ${input.taskId}`
      )
      .pipe(Effect.orDie)
    if (existing && existing.state === "active" && existing.expires_at > now) {
      return yield* Effect.fail(new ClaimConflict({ message: `task ${input.taskId} already claimed by ${existing.owner_id}` }))
    }

    // The event payload is built before the commit hook. The snapshot must still
    // be current under the write lock or this event/state mutation rolls back.
    const newFence = task.fence + 1
    const leaseId = `lease_${randomUUID()}`
    const token = `lease_${randomUUID()}`

    yield* events.publish(
      SwarmTaskClaimed,
      { taskId: input.taskId as any, swarmId: task.swarm_id, ownerId: input.ownerId, leaseId, fence: newFence, expiresAt, corrId: input.corrId as any },
      {
        commit: () => Effect.gen(function* () {
          const nowInTransaction = Date.now()
          const currentTask = yield* db.get<{ swarm_id: string; state: string; version: number; fence: number }>(sql`
            SELECT swarm_id, state, version, fence FROM kilo_swarm_task WHERE id = ${input.taskId}
          `)
          if (!currentTask || currentTask.swarm_id !== task.swarm_id || currentTask.state !== task.state || currentTask.version !== task.version || currentTask.fence !== task.fence) {
            return yield* Effect.die(new ClaimConflict({ message: `task ${input.taskId} changed before claim commit; retry` }))
          }

          const currentClaim = yield* db.get<{ owner_id: string; lease_id: string; fence: number; expires_at: number; state: string }>(sql`
            SELECT owner_id, lease_id, fence, expires_at, state FROM kilo_swarm_claim WHERE task_id = ${input.taskId}
          `)
          if (currentClaim && currentClaim.state === "active" && currentClaim.expires_at > nowInTransaction) {
            return yield* Effect.die(new ClaimConflict({ message: `task ${input.taskId} already claimed by ${currentClaim.owner_id}` }))
          }

          const swarm = yield* db.get<{ max_workers: number }>(sql`SELECT max_workers FROM kilo_swarm WHERE id = ${task.swarm_id}`)
          if (!swarm || !Number.isSafeInteger(swarm.max_workers) || swarm.max_workers < 0) {
            return yield* Effect.die(new CapacityConflict({ message: `missing or invalid worker capacity for swarm ${task.swarm_id}` }))
          }
          const active = yield* db.get<{ count: number }>(sql`
            SELECT COUNT(*) AS count FROM kilo_swarm_worker_reservation
            WHERE swarm_id = ${task.swarm_id} AND state = 'active' AND expires_at > ${nowInTransaction}
          `)
          if (expiresAt <= nowInTransaction) {
            return yield* Effect.die(new LeaseExpired({ message: `lease expired before claim admission for ${input.taskId}` }))
          }
          if ((active?.count ?? 0) >= swarm.max_workers) {
            return yield* Effect.die(new CapacityConflict({ message: `swarm ${task.swarm_id} has reached its worker capacity of ${swarm.max_workers}` }))
          }

          const fenced = yield* db.get<{ id: string }>(sql`
            UPDATE kilo_swarm_task SET fence = ${newFence}, time_updated = ${nowInTransaction}
            WHERE id = ${input.taskId} AND fence = ${task.fence} AND version = ${task.version} AND state = ${task.state}
            RETURNING id
          `)
          if (!fenced) return yield* Effect.die(new ClaimConflict({ message: `task ${input.taskId} fence CAS failed; retry` }))
          yield* db.run(sql`
            INSERT INTO kilo_swarm_claim (task_id, owner_id, lease_id, fence, expires_at, state, time_created, time_updated)
            VALUES (${input.taskId}, ${input.ownerId}, ${leaseId}, ${newFence}, ${expiresAt}, 'active', ${nowInTransaction}, ${nowInTransaction})
            ON CONFLICT(task_id) DO UPDATE SET owner_id = ${input.ownerId}, lease_id = ${leaseId}, fence = ${newFence}, expires_at = ${expiresAt}, state = 'active', time_updated = ${nowInTransaction}
          `)
          yield* db.run(sql`
            INSERT INTO kilo_swarm_worker_reservation (task_id, swarm_id, owner_id, lease_id, fence, expires_at, state, time_created, time_updated)
            VALUES (${input.taskId}, ${task.swarm_id}, ${input.ownerId}, ${leaseId}, ${newFence}, ${expiresAt}, 'active', ${nowInTransaction}, ${nowInTransaction})
            ON CONFLICT(task_id) DO UPDATE SET swarm_id = ${task.swarm_id}, owner_id = ${input.ownerId}, lease_id = ${leaseId}, fence = ${newFence}, expires_at = ${expiresAt}, state = 'active', time_updated = ${nowInTransaction}
          `)
        }).pipe(Effect.orDie),
      },
    ).pipe(Effect.catchDefect((defect) => {
      if (defect instanceof ClaimConflict || defect instanceof CapacityConflict || defect instanceof LeaseExpired) return Effect.fail(defect)
      return Effect.die(defect)
    }))

    return { ownerId: input.ownerId, leaseId, fence: newFence, expiresAt, token }
  })

  export const renew = Effect.fn("SwarmClaims.renew")(function* (input: {
    taskId: string
    leaseId: string
    fence: number
    leaseMs: number
  }) {
    const { db } = yield* Database.Service
    return yield* db.transaction(
      (tx) => Effect.gen(function* () {
        const now = Date.now()
        const expiresAt = now + input.leaseMs
        const updated = yield* tx.get<{ expires_at: number }>(sql`
          UPDATE kilo_swarm_claim SET expires_at = ${expiresAt}, time_updated = ${now}
          WHERE task_id = ${input.taskId} AND lease_id = ${input.leaseId} AND fence = ${input.fence}
            AND state = 'active' AND expires_at > ${now}
          RETURNING expires_at
        `)
        if (!updated || updated.expires_at !== expiresAt) return yield* Effect.fail(new LeaseExpired({ message: `lease renewal failed for ${input.taskId}` }))
        const reservation = yield* tx.get<{ task_id: string }>(sql`
          UPDATE kilo_swarm_worker_reservation SET expires_at = ${expiresAt}, time_updated = ${now}
          WHERE task_id = ${input.taskId} AND lease_id = ${input.leaseId} AND fence = ${input.fence}
            AND state = 'active' AND expires_at > ${now}
          RETURNING task_id
        `)
        if (!reservation) return yield* Effect.fail(new LeaseExpired({ message: `worker reservation renewal failed for ${input.taskId}` }))
        return { expiresAt }
      }),
      { behavior: "immediate" },
    )
  })

  export const release = Effect.fn("SwarmClaims.release")(function* (input: {
    taskId: string
    leaseId: string
    fence: number
    corrId: string
  }) {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const task = yield* SwarmState.getTask(input.taskId)
    const now = Date.now()
    const claim = yield* db.get<{ lease_id: string; fence: number; state: string; expires_at: number }>(sql`
      SELECT lease_id, fence, state, expires_at FROM kilo_swarm_claim WHERE task_id = ${input.taskId}
    `).pipe(Effect.orDie)
    if (!claim || claim.lease_id !== input.leaseId || claim.fence !== input.fence || claim.state !== "active") {
      return yield* Effect.fail(new StaleFence({ message: `stale fence/lease for ${input.taskId}` }))
    }
    if (claim.expires_at <= now) return yield* Effect.fail(new LeaseExpired({ message: `expired lease cannot release ${input.taskId}` }))
    yield* events.publish(
      SwarmTaskReleased,
      { taskId: input.taskId as any, swarmId: task.swarm_id, leaseId: input.leaseId, fence: input.fence, corrId: input.corrId as any },
      {
        commit: () => Effect.gen(function* () {
          const nowInTransaction = Date.now()
          const current = yield* db.get<{ lease_id: string; fence: number; state: string; expires_at: number }>(sql`
            SELECT lease_id, fence, state, expires_at FROM kilo_swarm_claim WHERE task_id = ${input.taskId}
          `)
          if (!current || current.lease_id !== input.leaseId || current.fence !== input.fence || current.state !== "active") return yield* Effect.die(new StaleFence({ message: `stale fence/lease for ${input.taskId}` }))
          if (current.expires_at <= nowInTransaction) return yield* Effect.die(new LeaseExpired({ message: `expired lease cannot release ${input.taskId}` }))
          const updated = yield* db.get<{ task_id: string }>(sql`
            UPDATE kilo_swarm_claim SET state = 'released', time_updated = ${nowInTransaction}
            WHERE task_id = ${input.taskId} AND lease_id = ${input.leaseId} AND fence = ${input.fence} AND state = 'active' AND expires_at > ${nowInTransaction}
            RETURNING task_id
          `)
          if (!updated) return yield* Effect.die(new StaleFence({ message: `claim release CAS failed for ${input.taskId}` }))
          const reservation = yield* db.get<{ task_id: string }>(sql`
            UPDATE kilo_swarm_worker_reservation SET state = 'released', time_updated = ${nowInTransaction}
            WHERE task_id = ${input.taskId} AND lease_id = ${input.leaseId} AND fence = ${input.fence} AND state = 'active' AND expires_at > ${nowInTransaction}
            RETURNING task_id
          `)
          if (!reservation) return yield* Effect.die(new StaleFence({ message: `matching worker reservation missing for ${input.taskId}` }))
        }).pipe(Effect.orDie),
      },
    ).pipe(Effect.catchDefect((defect) => {
      if (defect instanceof StaleFence || defect instanceof LeaseExpired) return Effect.fail(defect)
      return Effect.die(defect)
    }))
    return { released: true }
  })

  export const assertOwnership = Effect.fn("SwarmClaims.assertOwnership")(function* (input: {
    taskId: string
    leaseId: string
    fence: number
  }) {
    const { db } = yield* Database.Service
    const now = Date.now()
    const claim = yield* db
      .get<{ lease_id: string; fence: number; state: string; expires_at: number }>(
        sql`SELECT lease_id, fence, state, expires_at FROM kilo_swarm_claim WHERE task_id = ${input.taskId}`,
      )
      .pipe(Effect.orDie)
    if (!claim) return yield* Effect.fail(new StaleFence({ message: `no claim for ${input.taskId}` }))
    if (claim.lease_id !== input.leaseId) return yield* Effect.fail(new StaleFence({ message: `lease mismatch for ${input.taskId}` }))
    if (claim.fence !== input.fence) return yield* Effect.fail(new StaleFence({ message: `stale fence for ${input.taskId}` }))
    if (claim.state !== "active") return yield* Effect.fail(new StaleFence({ message: `claim not active for ${input.taskId}` }))
    if (claim.expires_at <= now) return yield* Effect.fail(new LeaseExpired({ message: `lease expired for ${input.taskId}` }))
    // also verify task fence matches
    const task = yield* SwarmState.getTask(input.taskId)
    if (task.fence !== input.fence) return yield* Effect.fail(new StaleFence({ message: `task fence mismatch for ${input.taskId}` }))
    return { ok: true as const }
  })
}