// Kilo Swarm M2 — durable claims, leases, fencing.
import { Effect, Schema } from "effect"
import { randomUUID } from "node:crypto"
import { Database } from "@opencode-ai/core/database/database"
import * as Log from "@opencode-ai/core/util/log"
import { sql } from "drizzle-orm"
import { LeaseToken } from "./schema"
import { SwarmClaimTable, SwarmTaskTable } from "@opencode-ai/core/kilocode/swarm/sql"
import { EventV2 } from "@opencode-ai/core/event"
import { SwarmTaskClaimed, SwarmTaskReleased } from "./events"
import { SwarmState } from "./state"

const log = Log.create({ service: "swarm.claims" })

export namespace SwarmClaims {
  export class ClaimConflict extends Schema.TaggedErrorClass<ClaimConflict>()("SwarmClaims.ClaimConflict", {
    message: Schema.String,
  }) {}

  export class LeaseExpired extends Schema.TaggedErrorClass<LeaseExpired>()("SwarmClaims.LeaseExpired", {
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
    const task = yield* SwarmState.getTask(input.taskId)
    const now = Date.now()
    const expiresAt = now + input.leaseMs

    // check for active unexpired claim
    const existing = yield* db
      .get<{ owner_id: string; lease_id: string; fence: number; expires_at: number; state: string }>(
        sql`SELECT owner_id, lease_id, fence, expires_at, state FROM kilo_swarm_claim WHERE task_id = ${input.taskId}`,
      )
      .pipe(Effect.orDie)
    if (existing && existing.state === "active" && existing.expires_at > now) {
      return yield* Effect.fail(new ClaimConflict({ message: `task ${input.taskId} already claimed by ${existing.owner_id}` }))
    }

    const newFence = task.fence + 1
    const leaseId = `lease_${randomUUID()}`
    const token = `lease_${randomUUID()}`

    yield* events
      .publish(
        SwarmTaskClaimed,
        { taskId: input.taskId as any, swarmId: task.swarm_id, ownerId: input.ownerId, leaseId, fence: newFence, expiresAt, corrId: input.corrId as any },
        {
          commit: () =>
            Effect.gen(function* () {
              // bump fence on task
              yield* db.run(sql`UPDATE kilo_swarm_task SET fence = ${newFence}, time_updated = ${now} WHERE id = ${input.taskId}`)
              // upsert claim
              yield* db.run(sql`
                INSERT INTO kilo_swarm_claim (task_id, owner_id, lease_id, fence, expires_at, state, time_created, time_updated)
                VALUES (${input.taskId}, ${input.ownerId}, ${leaseId}, ${newFence}, ${expiresAt}, 'active', ${now}, ${now})
                ON CONFLICT(task_id) DO UPDATE SET
                  owner_id = ${input.ownerId}, lease_id = ${leaseId}, fence = ${newFence},
                  expires_at = ${expiresAt}, state = 'active', time_updated = ${now}
              `)
            }).pipe(Effect.orDie),
        },
      )
      .pipe(Effect.catchDefect((defect) => Effect.die(defect)))

    return { ownerId: input.ownerId, leaseId, fence: newFence, expiresAt, token }
  })

  export const renew = Effect.fn("SwarmClaims.renew")(function* (input: {
    taskId: string
    leaseId: string
    fence: number
    leaseMs: number
  }) {
    const { db } = yield* Database.Service
    const now = Date.now()
    const expiresAt = now + input.leaseMs
    const res = yield* db.run(sql`
      UPDATE kilo_swarm_claim
      SET expires_at = ${expiresAt}, time_updated = ${now}
      WHERE task_id = ${input.taskId} AND lease_id = ${input.leaseId} AND fence = ${input.fence}
        AND state = 'active' AND expires_at > ${now}
    `)
    // verify it applied
    const check = yield* db.get<{ expires_at: number }>(sql`SELECT expires_at FROM kilo_swarm_claim WHERE task_id = ${input.taskId} AND lease_id = ${input.leaseId}`)
    if (!check || check.expires_at !== expiresAt) {
      return yield* Effect.fail(new LeaseExpired({ message: `lease renewal failed for ${input.taskId}` }))
    }
    void res
    return { expiresAt }
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
    const claim = yield* db
      .get<{ lease_id: string; fence: number; state: string }>(sql`SELECT lease_id, fence, state FROM kilo_swarm_claim WHERE task_id = ${input.taskId}`)
      .pipe(Effect.orDie)
    if (!claim || claim.lease_id !== input.leaseId || claim.fence !== input.fence) {
      return yield* Effect.fail(new StaleFence({ message: `stale fence/lease for ${input.taskId}` }))
    }
    yield* events
      .publish(
        SwarmTaskReleased,
        { taskId: input.taskId as any, swarmId: task.swarm_id, leaseId: input.leaseId, fence: input.fence, corrId: input.corrId as any },
        {
          commit: () =>
            Effect.gen(function* () {
              yield* db.run(sql`UPDATE kilo_swarm_claim SET state = 'released', time_updated = ${now} WHERE task_id = ${input.taskId}`)
            }).pipe(Effect.orDie),
        },
      )
      .pipe(Effect.catchDefect((defect) => Effect.die(defect)))
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