import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20261018105650_kilocode_swarm",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`kilo_swarm_claim\` (
          \`task_id\` text PRIMARY KEY,
          \`owner_id\` text NOT NULL,
          \`lease_id\` text NOT NULL,
          \`fence\` integer NOT NULL,
          \`expires_at\` integer NOT NULL,
          \`state\` text NOT NULL,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL,
          CONSTRAINT \`fk_kilo_swarm_claim_task_id_kilo_swarm_task_id_fk\` FOREIGN KEY (\`task_id\`) REFERENCES \`kilo_swarm_task\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`kilo_swarm_idempotency\` (
          \`scope\` text NOT NULL,
          \`key\` text NOT NULL,
          \`result\` text NOT NULL,
          \`time_created\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`kilo_swarm\` (
          \`id\` text PRIMARY KEY,
          \`state\` text NOT NULL,
          \`version\` integer DEFAULT 0 NOT NULL,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`kilo_swarm_task\` (
          \`id\` text PRIMARY KEY,
          \`swarm_id\` text NOT NULL,
          \`state\` text NOT NULL,
          \`version\` integer DEFAULT 0 NOT NULL,
          \`fence\` integer DEFAULT 0 NOT NULL,
          \`cancel_requested\` integer DEFAULT 0 NOT NULL,
          \`parent_id\` text,
          \`attempts\` integer DEFAULT 0 NOT NULL,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL,
          CONSTRAINT \`fk_kilo_swarm_task_swarm_id_kilo_swarm_id_fk\` FOREIGN KEY (\`swarm_id\`) REFERENCES \`kilo_swarm\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`kilo_swarm_task_transition\` (
          \`id\` text PRIMARY KEY,
          \`task_id\` text NOT NULL,
          \`swarm_id\` text NOT NULL,
          \`seq\` integer NOT NULL,
          \`from_state\` text NOT NULL,
          \`to_state\` text NOT NULL,
          \`version_before\` integer NOT NULL,
          \`version_after\` integer NOT NULL,
          \`fence\` integer NOT NULL,
          \`actor\` text NOT NULL,
          \`corr_id\` text,
          \`idem_key\` text,
          \`time_created\` integer NOT NULL,
          CONSTRAINT \`fk_kilo_swarm_task_transition_task_id_kilo_swarm_task_id_fk\` FOREIGN KEY (\`task_id\`) REFERENCES \`kilo_swarm_task\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(
        `CREATE UNIQUE INDEX \`kilo_swarm_idempotency_scope_key_idx\` ON \`kilo_swarm_idempotency\` (\`scope\`,\`key\`);`,
      )
      yield* tx.run(
        `CREATE UNIQUE INDEX \`kilo_swarm_task_swarm_state_idx\` ON \`kilo_swarm_task\` (\`swarm_id\`,\`state\`);`,
      )
      yield* tx.run(
        `CREATE UNIQUE INDEX \`kilo_swarm_task_transition_task_seq_idx\` ON \`kilo_swarm_task_transition\` (\`task_id\`,\`seq\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
