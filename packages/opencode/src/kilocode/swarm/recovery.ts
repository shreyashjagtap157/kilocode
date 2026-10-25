// Kilo Swarm M3 — deterministic, non-executing reconciliation.
import { Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import * as Log from "@opencode-ai/core/util/log"
import { sql } from "drizzle-orm"

const log = Log.create({ service: "swarm.recovery" })

export namespace SwarmRecovery {
  export const reconcile = Effect.fn("SwarmRecovery.reconcile")(function* () {
    const { db } = yield* Database.Service
    const now = Date.now()
    yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          yield* tx.run(sql`
            UPDATE kilo_swarm_claim SET state = 'expired', time_updated = ${now}
            WHERE state = 'active' AND expires_at <= ${now}
          `).pipe(Effect.orDie)
          yield* tx.run(sql`
            UPDATE kilo_swarm_worker_reservation SET state = 'expired', time_updated = ${now}
            WHERE state = 'active' AND expires_at <= ${now}
          `).pipe(Effect.orDie)
          // A binding belonging to an expired lease is retained as recovery
          // evidence, but is no longer executable by this worker generation.
          yield* tx.run(sql`
            UPDATE kilo_swarm_worker_binding SET state = 'expired', time_updated = ${now}
            WHERE state IN ('bound', 'running')
              AND NOT EXISTS (
                SELECT 1 FROM kilo_swarm_claim c
                WHERE c.task_id = kilo_swarm_worker_binding.task_id
                  AND c.lease_id = kilo_swarm_worker_binding.lease_id
                  AND c.fence = kilo_swarm_worker_binding.fence
                  AND c.state = 'active'
                  AND c.expires_at > ${now}
              )
          `).pipe(Effect.orDie)
        }),
      { behavior: "immediate" },
    )
    log.info("swarm reconciliation complete")
    return { reconciled: true as const }
  })
}
