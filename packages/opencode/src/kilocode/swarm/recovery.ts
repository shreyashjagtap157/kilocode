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
    // Mark claims and their capacity reservations expired together. This is
    // metadata reconciliation only: no task transition, reclaim, dispatch, or
    // execution is introduced by this operation.
    yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          yield* tx
            .run(sql`UPDATE kilo_swarm_claim SET state = 'expired', time_updated = ${now} WHERE state = 'active' AND expires_at <= ${now}`)
            .pipe(Effect.orDie)
          yield* tx
            .run(sql`UPDATE kilo_swarm_worker_reservation SET state = 'expired', time_updated = ${now} WHERE state = 'active' AND expires_at <= ${now}`)
            .pipe(Effect.orDie)
        }),
      { behavior: "immediate" },
    )
    // Repair: tasks in non-terminal states with cancel_requested flag stay as-is (authoritative);
    // nothing else to repair in M2. Recovery never executes or resumes work.
    log.info("swarm reconciliation complete")
    return { reconciled: true as const }
  })
}