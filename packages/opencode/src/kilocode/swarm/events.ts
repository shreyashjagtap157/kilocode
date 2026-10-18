// Kilo Swarm M2 — durable EventV2 definitions + Kilo-owned manifest (Kilo-owned).
//
// EventV2 is the canonical durable event mechanism (M2 design addendum §3).
// Swarm durable events are defined here (Kilo-owned; no shared-manifest edit
// required for M2). Read/replay uses EventV2.readAggregate with the Kilo-owned
// SwarmDurable manifest. The shared @opencode-ai/schema manifest is NOT touched.
import { Effect, Schema } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { Event } from "@opencode-ai/schema/event"
import { SwarmTaskID, TaskState, CorrelationID } from "./schema"

// --- durable event definitions ---------------------------------------------

export const SwarmTaskCreated = EventV2.define({
  type: "swarm.task.created",
  durable: { version: 1, aggregate: "taskId" },
  schema: {
    taskId: SwarmTaskID,
    swarmId: Schema.String,
    state: TaskState,
    parentId: Schema.NullOr(SwarmTaskID),
    corrId: CorrelationID,
  },
})

export const SwarmTaskTransitioned = EventV2.define({
  type: "swarm.task.transitioned",
  durable: { version: 1, aggregate: "taskId" },
  schema: {
    taskId: SwarmTaskID,
    swarmId: Schema.String,
    from: TaskState,
    to: TaskState,
    seq: Schema.Int,
    versionBefore: Schema.Int,
    versionAfter: Schema.Int,
    fence: Schema.Int,
    actor: Schema.String,
    corrId: CorrelationID,
    idemKey: Schema.optional(Schema.String),
  },
})

export const SwarmTaskClaimed = EventV2.define({
  type: "swarm.task.claimed",
  durable: { version: 1, aggregate: "taskId" },
  schema: {
    taskId: SwarmTaskID,
    swarmId: Schema.String,
    ownerId: Schema.String,
    leaseId: Schema.String,
    fence: Schema.Int,
    expiresAt: Schema.Int,
    corrId: CorrelationID,
  },
})

export const SwarmTaskReleased = EventV2.define({
  type: "swarm.task.released",
  durable: { version: 1, aggregate: "taskId" },
  schema: {
    taskId: SwarmTaskID,
    swarmId: Schema.String,
    leaseId: Schema.String,
    fence: Schema.Int,
    corrId: CorrelationID,
  },
})

export const SwarmTaskCancelRequested = EventV2.define({
  type: "swarm.task.cancel_requested",
  durable: { version: 1, aggregate: "taskId" },
  schema: {
    taskId: SwarmTaskID,
    swarmId: Schema.String,
    corrId: CorrelationID,
  },
})

export const SwarmTaskDefinitions = [
  SwarmTaskCreated,
  SwarmTaskTransitioned,
  SwarmTaskClaimed,
  SwarmTaskReleased,
  SwarmTaskCancelRequested,
] as const

// --- Kilo-owned manifest (mirrors SessionDurable) --------------------------

const inventory = Event.inventory(...SwarmTaskDefinitions)
const unionSchema = Schema.Union(inventory, { mode: "oneOf" }).pipe(Schema.toTaggedUnion("type"))

export const SwarmDurable = {
  definitions: Event.durable(inventory),
  schema: unionSchema,
} as const

// --- read/replay helper (bounded, non-executing) ---------------------------

export const readSwarmEvents = (db: any, aggregateID: string, after?: number, limit: number = 50) =>
  EventV2.readAggregate(db, { aggregateID, after, limit, manifest: SwarmDurable })

// --- re-export for convenience ---------------------------------------------

export { EventV2 }