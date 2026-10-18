// Kilo Swarm M2 — deterministic, non-executing reconciliation.
import { Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import * as Log from "@opencode-ai/core/util/log"
import { sql } from "drizzle-orm"

const log = Log.create({ service: "swarm.recovery" })

export namespace SwarmRecovery {
  export const reconcile = Effect.fn("SwarmRecovery.reconcile")(function* () {
    const { db } = yield* Database.Service
    const now = Date.now()
    // Mark expired claims (no transition, no reclaim, no execution).
    yield* db
      .run(sql`UPDATE kilo_swarm_claim SET state = 'expired', time_updated = ${now} WHERE state = 'active' AND expires_at <= ${now}`)
      .pipe(Effect.orDie)
    // Repair: tasks in non-terminal states with cancel_requested flag stay as-is (authoritative);
    // nothing else to repair in M2. Recovery never executes or resumes work.
    log.info("swarm reconciliation complete")
    return { reconciled: true as const }
  })
}