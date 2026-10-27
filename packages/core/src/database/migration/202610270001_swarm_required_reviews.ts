import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "202610270001_swarm_required_reviews",
  up(tx) {
    return Effect.gen(function* () {
      // Existing invocations keep the original one-review contract unless
      // they explicitly opt into a stronger persisted qualification policy.
      yield* tx.run(`
        ALTER TABLE kilo_swarm
        ADD COLUMN required_reviews integer DEFAULT 1 NOT NULL
      `)
    })
  },
} satisfies DatabaseMigration.Migration
