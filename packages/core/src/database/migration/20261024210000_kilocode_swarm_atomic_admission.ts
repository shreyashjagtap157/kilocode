import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261024210000_kilocode_swarm_atomic_admission",
  up(tx) {
    return Effect.gen(function* () {
      // Existing swarms get a conservative default. Backfill reservations so
      // pre-migration claims keep consuming logical admission capacity.
      yield* tx.run(`
        ALTER TABLE \`kilo_swarm\`
        ADD COLUMN \`max_workers\` integer DEFAULT 4 NOT NULL CHECK (\`max_workers\` >= 0)
      `)
      yield* tx.run(`
        DROP INDEX IF EXISTS \`kilo_swarm_task_swarm_state_idx\`
      `)
      yield* tx.run(`
        CREATE INDEX \`kilo_swarm_task_swarm_state_idx\` ON \`kilo_swarm_task\` (\`swarm_id\`, \`state\`)
      `)
      yield* tx.run(`
        CREATE TABLE \`kilo_swarm_worker_reservation\` (
          \`task_id\` text PRIMARY KEY,
          \`swarm_id\` text NOT NULL,
          \`owner_id\` text NOT NULL,
          \`lease_id\` text NOT NULL,
          \`fence\` integer NOT NULL,
          \`expires_at\` integer NOT NULL,
          \`state\` text NOT NULL,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL,
          CONSTRAINT \`fk_kilo_swarm_worker_reservation_task_id_kilo_swarm_task_id_fk\` FOREIGN KEY (\`task_id\`) REFERENCES \`kilo_swarm_task\`(\`id\`) ON DELETE CASCADE,
          CONSTRAINT \`fk_kilo_swarm_worker_reservation_swarm_id_kilo_swarm_id_fk\` FOREIGN KEY (\`swarm_id\`) REFERENCES \`kilo_swarm\`(\`id\`) ON DELETE CASCADE
        )
      `)
      yield* tx.run(`
        CREATE INDEX \`kilo_swarm_worker_reservation_active_idx\` ON \`kilo_swarm_worker_reservation\` (\`swarm_id\`, \`state\`, \`expires_at\`)
      `)
      yield* tx.run(`
        INSERT INTO \`kilo_swarm_worker_reservation\` (\`task_id\`, \`swarm_id\`, \`owner_id\`, \`lease_id\`, \`fence\`, \`expires_at\`, \`state\`, \`time_created\`, \`time_updated\`)
        SELECT c.\`task_id\`, t.\`swarm_id\`, c.\`owner_id\`, c.\`lease_id\`, c.\`fence\`, c.\`expires_at\`,
          CASE WHEN c.\`state\` = 'active' THEN 'active' WHEN c.\`state\` = 'expired' THEN 'expired' ELSE 'released' END,
          c.\`time_created\`, c.\`time_updated\`
        FROM \`kilo_swarm_claim\` c JOIN \`kilo_swarm_task\` t ON t.\`id\` = c.\`task_id\`
      `)
    })
  },
} satisfies DatabaseMigration.Migration
