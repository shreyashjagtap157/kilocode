/**
 * Kilo Swarm M1 — declarative core contracts (foundation only).
 *
 * This module defines the six M1 models — `SwarmTask`, `TaskTransition`,
 * `Claim`/`Lease`, `BudgetReservation`, `Artifact`, `SwarmMessage` — as Effect
 * `Schema` contracts. It carries fields, types, identifiers, references,
 * validation constraints, discriminated unions and structural invariants.
 *
 * It deliberately does NOT implement the runtime semantics these contracts will
 * eventually describe (frozen M1 scope, §2a):
 *   - no authoritative task persistence, lifecycle engine or event log;
 *   - no lease acquisition, renewal, expiration, fencing or locking;
 *   - no budget reserve/debit/reconcile/enforce;
 *   - no artifact storage, provenance tracking, review or integration;
 *   - no authoritative command or permission channel.
 *
 * Structural invariants from the re-audit (§8) are represented at the schema
 * level only:
 *   - (1) a task has exactly one `state` value and transitions carry an append
 *         only `seq` + `from`/`to`;
 *   - (2) `Lease` and `BudgetReservation` reference a `taskId` as the contracts a
 *         future dispatch gate would require (no gate is implemented here);
 *   - (3) `Lease.token` is a fencing token field;
 *   - (4) `SwarmMessage` carries no task-state and no permission fields, so it
 *         cannot change state or grant authority;
 *   - (5) `Artifact.hash` is a content-addressed SHA-256 hex digest;
 *   - (6) `BudgetReservation` amounts are non-negative and carry a reservation
 *         `state` value.
 */
import { Schema } from "effect"

// --- primitives -------------------------------------------------------------

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const NonNegativeFinite = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))

const prefixed = (prefix: string) => Schema.String.check(Schema.isStartsWith(prefix))

// --- identifiers ------------------------------------------------------------

export const SwarmTaskID = prefixed("tsk_").pipe(Schema.brand("SwarmTaskID"))
export type SwarmTaskID = Schema.Schema.Type<typeof SwarmTaskID>

export const SwarmMessageID = prefixed("smsg_").pipe(Schema.brand("SwarmMessageID"))
export type SwarmMessageID = Schema.Schema.Type<typeof SwarmMessageID>

export const BudgetReservationID = prefixed("bud_").pipe(Schema.brand("BudgetReservationID"))
export type BudgetReservationID = Schema.Schema.Type<typeof BudgetReservationID>

export const ArtifactID = prefixed("art_").pipe(Schema.brand("ArtifactID"))
export type ArtifactID = Schema.Schema.Type<typeof ArtifactID>

export const LeaseToken = prefixed("lease_").pipe(Schema.brand("LeaseToken"))
export type LeaseToken = Schema.Schema.Type<typeof LeaseToken>

export const WorkerID = prefixed("wrk_").pipe(Schema.brand("WorkerID"))
export type WorkerID = Schema.Schema.Type<typeof WorkerID>

export const ActorID = prefixed("act_").pipe(Schema.brand("ActorID"))
export type ActorID = Schema.Schema.Type<typeof ActorID>

export const CorrelationID = prefixed("cor_").pipe(Schema.brand("CorrelationID"))
export type CorrelationID = Schema.Schema.Type<typeof CorrelationID>

/** Content-addressed artifact identity: a lowercase SHA-256 hex digest. */
export const ContentHash = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))
export type ContentHash = Schema.Schema.Type<typeof ContentHash>

/** Additive-only swarm protocol version. */
export const ProtocolVersion = PositiveInt
export type ProtocolVersion = Schema.Schema.Type<typeof ProtocolVersion>

// --- value sets -------------------------------------------------------------

/** Authoritative task lifecycle states (declarative contract only). */
export const TaskState = Schema.Literals([
  "queued",
  "reserved",
  "dispatched",
  "running",
  "blocked",
  "awaiting_approval",
  "reviewing",
  // Terminal status for a reviewer task whose durable review event was recorded.
  // It is deliberately distinct from accepted/integrated implementation work.
  "reviewed",
  "accepted",
  "rejected",
  "integrated",
  "failed",
  "cancelled",
  "stale",
  "quarantined",
])
export type TaskState = Schema.Schema.Type<typeof TaskState>

/**
 * Declarative legality table for task states. This is inert data used to
 * describe the §8/§9 contract; M1 does not persist transitions, emit events, or
 * enforce this table at runtime (that is M2 work).
 */
export const Lifecycle: Readonly<Record<TaskState, ReadonlyArray<TaskState>>> = {
  queued: ["reserved", "cancelled"],
  reserved: ["dispatched", "stale", "cancelled"],
  dispatched: ["running", "failed", "stale", "cancelled"],
  running: ["blocked", "awaiting_approval", "reviewing", "failed", "cancelled", "stale"],
  blocked: ["running", "cancelled", "stale"],
  awaiting_approval: ["running", "cancelled"],
  reviewing: ["accepted", "rejected", "failed", "reviewed"],
  reviewed: [],
  accepted: ["integrated"],
  rejected: ["queued", "cancelled"],
  integrated: [],
  failed: [],
  cancelled: [],
  stale: ["queued"],
  quarantined: [],
}

/** States with no outgoing transitions. */
export const TerminalStates: ReadonlyArray<TaskState> = ["integrated", "reviewed", "failed", "cancelled", "quarantined"]

/** Reservation lifecycle values. Declarative only; no reconciliation runs here. */
export const BudgetState = Schema.Literals(["reserved", "reconciled", "released"])
export type BudgetState = Schema.Schema.Type<typeof BudgetState>

/** Artifact lifecycle values. Declarative only; no storage/review runs here. */
export const ArtifactState = Schema.Literals(["staged", "reviewed", "accepted", "integrated", "archived"])
export type ArtifactState = Schema.Schema.Type<typeof ArtifactState>

/** Advisory message kinds. None of these grant authority. */
export const SwarmMessageKind = Schema.Literals(["status", "question", "finding", "hold", "veto"])
export type SwarmMessageKind = Schema.Schema.Type<typeof SwarmMessageKind>

// --- the six M1 models ------------------------------------------------------

/** Authoritative task record. Invariant (1): exactly one `state` at a time. */
export const SwarmTask = Schema.Struct({
  id: SwarmTaskID,
  parentId: Schema.NullOr(SwarmTaskID),
  state: TaskState,
  ownerLease: Schema.NullOr(LeaseToken),
  attempts: NonNegativeInt,
  budgetId: Schema.NullOr(BudgetReservationID),
}).annotate({ identifier: "SwarmTask" })
export type SwarmTask = Schema.Schema.Type<typeof SwarmTask>

/** Append-only, sequenced state transition. Invariant (1). */
export const TaskTransition = Schema.Struct({
  taskId: SwarmTaskID,
  from: TaskState,
  to: TaskState,
  seq: NonNegativeInt,
  reason: Schema.optional(Schema.String),
  actor: ActorID,
}).annotate({ identifier: "TaskTransition" })
export type TaskTransition = Schema.Schema.Type<typeof TaskTransition>

/**
 * Fencing claim / lease contract. Invariants (2) and (3): carries a fencing
 * `token` and an `expiresAt`; M1 implements no acquisition, renewal, expiry or
 * fencing behavior.
 */
export const Lease = Schema.Struct({
  taskId: SwarmTaskID,
  workerId: WorkerID,
  token: LeaseToken,
  expiresAt: PositiveInt,
}).annotate({ identifier: "SwarmLease" })
export type Lease = Schema.Schema.Type<typeof Lease>

/** M1 models `Claim` and `Lease` as the same declarative contract. */
export const Claim = Lease
export type Claim = Lease

/** Reservation-based budget contract. Invariant (6); no reserve/debit/enforce. */
export const BudgetReservation = Schema.Struct({
  id: BudgetReservationID,
  taskId: SwarmTaskID,
  tokens: NonNegativeInt,
  cost: NonNegativeFinite,
  state: BudgetState,
}).annotate({ identifier: "BudgetReservation" })
export type BudgetReservation = Schema.Schema.Type<typeof BudgetReservation>

/** Provenance attached to an artifact. Declarative only; no tracking runs here. */
export const ArtifactProvenance = Schema.Struct({
  taskId: SwarmTaskID,
  workerId: WorkerID,
  tool: Schema.optional(Schema.String),
  model: Schema.optional(Schema.String),
  createdAt: PositiveInt,
}).annotate({ identifier: "ArtifactProvenance" })
export type ArtifactProvenance = Schema.Schema.Type<typeof ArtifactProvenance>

/** Content-addressed artifact contract. Invariant (5); no storage or review. */
export const Artifact = Schema.Struct({
  id: ArtifactID,
  taskId: SwarmTaskID,
  hash: ContentHash,
  uri: Schema.String,
  provenance: ArtifactProvenance,
}).annotate({ identifier: "SwarmArtifact" })
export type Artifact = Schema.Schema.Type<typeof Artifact>

const Envelope = {
  id: SwarmMessageID,
  corrId: CorrelationID,
  from: ActorID,
  to: Schema.Union([ActorID, Schema.Literal("ALL")]),
  version: ProtocolVersion,
} as const

/**
 * Advisory typed peer message. Invariant (4): it has no task-state and no
 * permission fields, so a message can never mutate task state or grant
 * authority. Implementation is a discriminated union keyed on `kind`.
 */
export const SwarmMessage = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("status"), detail: Schema.String, ...Envelope }),
  Schema.Struct({ kind: Schema.Literal("question"), prompt: Schema.String, ...Envelope }),
  Schema.Struct({ kind: Schema.Literal("finding"), detail: Schema.String, ...Envelope }),
  Schema.Struct({ kind: Schema.Literal("hold"), reason: Schema.String, ...Envelope }),
  Schema.Struct({ kind: Schema.Literal("veto"), reason: Schema.String, ...Envelope }),
]).annotate({ identifier: "SwarmMessage" })
export type SwarmMessage = Schema.Schema.Type<typeof SwarmMessage>

/** Named registry of the six M1 contracts, for schema-level qualification. */
export const Contracts = {
  SwarmTask,
  TaskTransition,
  Claim,
  Lease,
  BudgetReservation,
  Artifact,
  SwarmMessage,
} as const
