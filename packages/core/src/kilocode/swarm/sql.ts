import { index, integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core"

/**
 * Kilo Swarm M2/M3 — durable persistence (Kilo-owned).
 *
 * Events are canonical durable records in EventV2. These tables hold
 * authoritative swarm state plus the durable worker/session binding required
 * to recover an admitted worker after process restart.
 */
export const SwarmTable = sqliteTable("kilo_swarm", {
  id: text().notNull().primaryKey(),
  state: text().notNull(),
  version: integer().notNull().default(0),
  max_workers: integer().notNull().default(4),
  required_reviews: integer().notNull().default(1),
  time_created: integer().notNull(),
  time_updated: integer().notNull(),
})

export const SwarmTaskTable = sqliteTable(
  "kilo_swarm_task",
  {
    id: text().notNull().primaryKey(),
    swarm_id: text().notNull().references(() => SwarmTable.id, { onDelete: "cascade" }),
    state: text().notNull(),
    version: integer().notNull().default(0),
    fence: integer().notNull().default(0),
    cancel_requested: integer().notNull().default(0),
    parent_id: text(),
    attempts: integer().notNull().default(0),
    time_created: integer().notNull(),
    time_updated: integer().notNull(),
  },
  (table) => [index("kilo_swarm_task_swarm_state_idx").on(table.swarm_id, table.state)],
)

export const SwarmTaskTransitionTable = sqliteTable(
  "kilo_swarm_task_transition",
  {
    id: text().notNull().primaryKey(),
    task_id: text().notNull().references(() => SwarmTaskTable.id, { onDelete: "cascade" }),
    swarm_id: text().notNull(),
    seq: integer().notNull(),
    from_state: text().notNull(),
    to_state: text().notNull(),
    version_before: integer().notNull(),
    version_after: integer().notNull(),
    fence: integer().notNull(),
    actor: text().notNull(),
    corr_id: text(),
    idem_key: text(),
    time_created: integer().notNull(),
  },
  (table) => [uniqueIndex("kilo_swarm_task_transition_task_seq_idx").on(table.task_id, table.seq)],
)

export const SwarmClaimTable = sqliteTable("kilo_swarm_claim", {
  task_id: text().notNull().primaryKey().references(() => SwarmTaskTable.id, { onDelete: "cascade" }),
  owner_id: text().notNull(),
  lease_id: text().notNull(),
  fence: integer().notNull(),
  expires_at: integer().notNull(),
  state: text().notNull(),
  time_created: integer().notNull(),
  time_updated: integer().notNull(),
})

export const SwarmWorkerReservationTable = sqliteTable(
  "kilo_swarm_worker_reservation",
  {
    task_id: text().notNull().primaryKey().references(() => SwarmTaskTable.id, { onDelete: "cascade" }),
    swarm_id: text().notNull().references(() => SwarmTable.id, { onDelete: "cascade" }),
    owner_id: text().notNull(),
    lease_id: text().notNull(),
    fence: integer().notNull(),
    expires_at: integer().notNull(),
    state: text().notNull(),
    time_created: integer().notNull(),
    time_updated: integer().notNull(),
  },
  (table) => [index("kilo_swarm_worker_reservation_active_idx").on(table.swarm_id, table.state, table.expires_at)],
)

/** Durable task -> Kilo SessionExecution binding. */
export const SwarmWorkerBindingTable = sqliteTable(
  "kilo_swarm_worker_binding",
  {
    task_id: text().notNull().primaryKey().references(() => SwarmTaskTable.id, { onDelete: "cascade" }),
    swarm_id: text().notNull().references(() => SwarmTable.id, { onDelete: "cascade" }),
    session_id: text().notNull(),
    owner_id: text().notNull(),
    lease_id: text().notNull(),
    fence: integer().notNull(),
    state: text().notNull(),
    time_created: integer().notNull(),
    time_updated: integer().notNull(),
  },
  (table) => [uniqueIndex("kilo_swarm_worker_binding_session_idx").on(table.session_id)],
)

export const SwarmIdempotencyTable = sqliteTable(
  "kilo_swarm_idempotency",
  {
    scope: text().notNull(),
    key: text().notNull(),
    result: text().notNull(),
    time_created: integer().notNull(),
  },
  (table) => [uniqueIndex("kilo_swarm_idempotency_scope_key_idx").on(table.scope, table.key)],
)
