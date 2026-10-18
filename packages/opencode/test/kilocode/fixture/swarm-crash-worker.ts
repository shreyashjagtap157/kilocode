// Kilo Swarm M2 — test-only crash worker for AC15 process-crash/reopen testing.
//
// This worker opens a file-backed SQLite database directly via bun:sqlite,
// creates the M2 tables, begins an explicit transaction, inserts a probe row,
// then exits the process without committing — simulating a crash mid-transaction.
// SQLite WAL recovery rolls back the uncommitted transaction when the parent
// reopens the database, so no partial state survives.
//
// This is test-only infrastructure; no production code is affected.
import { Database as SQLite } from "bun:sqlite"

const mode = process.argv[2]
const file = process.argv[3]
if (!mode || !file) throw new Error("Expected mode and database file path")

if (mode === "crash") {
  const sqlite = new SQLite(file)
  sqlite.run("PRAGMA journal_mode = WAL")
  sqlite.run("PRAGMA busy_timeout = 5000")

  // Create M2 tables (mirrors migration 20261018105650_kilocode_swarm).
  sqlite.run(`
    CREATE TABLE IF NOT EXISTS kilo_swarm (
      id text PRIMARY KEY,
      state text NOT NULL,
      version integer DEFAULT 0 NOT NULL,
      time_created integer NOT NULL,
      time_updated integer NOT NULL
    )
  `)
  sqlite.run(`
    CREATE TABLE IF NOT EXISTS kilo_swarm_task (
      id text PRIMARY KEY,
      swarm_id text NOT NULL,
      state text NOT NULL,
      version integer DEFAULT 0 NOT NULL,
      fence integer DEFAULT 0 NOT NULL,
      cancel_requested integer DEFAULT 0 NOT NULL,
      parent_id text,
      attempts integer DEFAULT 0 NOT NULL,
      time_created integer NOT NULL,
      time_updated integer NOT NULL
    )
  `)
  sqlite.run(`
    CREATE TABLE IF NOT EXISTS kilo_swarm_claim (
      task_id text PRIMARY KEY,
      owner_id text NOT NULL,
      lease_id text NOT NULL,
      fence integer NOT NULL,
      expires_at integer NOT NULL,
      state text NOT NULL,
      time_created integer NOT NULL,
      time_updated integer NOT NULL
    )
  `)
  sqlite.run(`
    CREATE TABLE IF NOT EXISTS kilo_swarm_idempotency (
      scope text NOT NULL,
      key text NOT NULL,
      result text NOT NULL,
      time_created integer NOT NULL
    )
  `)
  sqlite.run(`
    CREATE TABLE IF NOT EXISTS kilo_swarm_task_transition (
      id text PRIMARY KEY,
      task_id text NOT NULL,
      swarm_id text NOT NULL,
      seq integer NOT NULL,
      from_state text NOT NULL,
      to_state text NOT NULL,
      version_before integer NOT NULL,
      version_after integer NOT NULL,
      fence integer NOT NULL,
      actor text NOT NULL,
      corr_id text,
      idem_key text,
      time_created integer NOT NULL
    )
  `)

  const now = Date.now()
  // Begin an explicit transaction, insert a probe row, then exit without committing.
  sqlite.transaction(() => {
    sqlite.run(`
      INSERT INTO kilo_swarm_task (id, swarm_id, state, version, fence, cancel_requested, parent_id, attempts, time_created, time_updated)
      VALUES ('crash_probe', 'swm_crash', 'queued', 0, 0, 0, null, 0, ${now}, ${now})
    `)
    // Exit the process without committing — uncommitted transaction remains in WAL.
    process.exit(0)
  })()

  // Never reached: process.exit(0) above terminates the process.
  await new Promise(() => {})
}
