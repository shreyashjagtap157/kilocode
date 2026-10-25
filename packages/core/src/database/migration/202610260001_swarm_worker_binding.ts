import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "202610260001_swarm_worker_binding",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS kilo_swarm_worker_binding (
          task_id TEXT NOT NULL PRIMARY KEY REFERENCES kilo_swarm_task(id) ON DELETE CASCADE,
          swarm_id TEXT NOT NULL REFERENCES kilo_swarm(id) ON DELETE CASCADE,
          session_id TEXT NOT NULL,
          owner_id TEXT NOT NULL,
          lease_id TEXT NOT NULL,
          fence INTEGER NOT NULL,
          state TEXT NOT NULL,
          time_created INTEGER NOT NULL,
          time_updated INTEGER NOT NULL
        )
      `)
      yield* tx.run(`
        CREATE UNIQUE INDEX IF NOT EXISTS kilo_swarm_worker_binding_session_idx
        ON kilo_swarm_worker_binding(session_id)
      `)
    })
  },
} satisfies DatabaseMigration.Migration
