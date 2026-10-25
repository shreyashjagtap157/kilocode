// Kilo Swarm M2/M3 — durable EventV2 definitions + Kilo-owned manifest.
//
// EventV2 is the canonical durable event mechanism. Worker binding events are
// deliberately emitted only at successful persistence boundaries so replay does
// not imply that a provider invocation actually ran.
import { Schema } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { Event } from "@opencode-ai/schema/event"
import { SwarmTaskID, TaskState, CorrelationID } from "./schema"

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

export const SwarmWorkerBound = EventV2.define({
  type: "swarm.worker.bound",
  durable: { version: 1, aggregate: "taskId" },
  schema: {
    taskId: SwarmTaskID,
    swarmId: Schema.String,
    sessionId: Schema.String,
    ownerId: Schema.String,
    leaseId: Schema.String,
    fence: Schema.Int,
    corrId: CorrelationID,
  },
})

export const SwarmTaskDefinitions = [
  SwarmTaskCreated,
  SwarmTaskTransitioned,
  SwarmTaskClaimed,
  SwarmTaskReleased,
  SwarmTaskCancelRequested,
  SwarmWorkerBound,
] as const

const inventory = Event.inventory(...SwarmTaskDefinitions)
const unionSchema = Schema.Union(inventory, { mode: "oneOf" }).pipe(Schema.toTaggedUnion("type"))

export const SwarmDurable = {
  definitions: Event.durable(inventory),
  schema: unionSchema,
} as const

export const readSwarmEvents = (db: any, aggregateID: string, after?: number, limit: number = 50) =>
  EventV2.readAggregate(db, { aggregateID, after, limit, manifest: SwarmDurable })

export { EventV2 }
