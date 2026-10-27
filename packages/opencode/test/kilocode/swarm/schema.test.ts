import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import * as Swarm from "@/kilocode/swarm/schema"

const task = {
  id: "tsk_1",
  parentId: null,
  state: "queued",
  ownerLease: null,
  attempts: 0,
  budgetId: null,
} as const

const transition = { taskId: "tsk_1", from: "queued", to: "reserved", seq: 0, actor: "act_main" } as const
const lease = { taskId: "tsk_1", workerId: "wrk_1", token: "lease_1", expiresAt: 1_700_000_000_000 } as const
const budget = { id: "bud_1", taskId: "tsk_1", tokens: 0, cost: 0, state: "reserved" } as const
const artifact = {
  id: "art_1",
  taskId: "tsk_1",
  hash: "0".repeat(64),
  uri: "file:///tmp/out.txt",
  provenance: { taskId: "tsk_1", workerId: "wrk_1", createdAt: 1 },
} as const
const message = {
  kind: "finding",
  detail: "found",
  id: "smsg_1",
  corrId: "cor_1",
  from: "act_a",
  to: "ALL",
  version: 1,
} as const

describe("swarm core schemas", () => {
  test("AC1: all six contracts are registered and Claim aliases Lease", () => {
    expect(Object.keys(Swarm.Contracts).sort()).toEqual([
      "Artifact",
      "BudgetReservation",
      "Claim",
      "Lease",
      "SwarmMessage",
      "SwarmTask",
      "TaskTransition",
    ])
    expect(Swarm.Claim).toBe(Swarm.Lease)
  })

  test("AC1: each model decodes valid data and round-trips through encode", () => {
    expect(Schema.encodeUnknownSync(Swarm.SwarmTask)(Schema.decodeUnknownSync(Swarm.SwarmTask)(task))).toEqual(task)
    expect(Schema.encodeUnknownSync(Swarm.TaskTransition)(Schema.decodeUnknownSync(Swarm.TaskTransition)(transition))).toEqual(
      transition,
    )
    expect(Schema.encodeUnknownSync(Swarm.Lease)(Schema.decodeUnknownSync(Swarm.Lease)(lease))).toEqual(lease)
    expect(
      Schema.encodeUnknownSync(Swarm.BudgetReservation)(
        Schema.decodeUnknownSync(Swarm.BudgetReservation)(budget),
      ),
    ).toEqual(budget)
    expect(Schema.encodeUnknownSync(Swarm.Artifact)(Schema.decodeUnknownSync(Swarm.Artifact)(artifact))).toEqual(
      artifact,
    )
    expect(
      Schema.encodeUnknownSync(Swarm.SwarmMessage)(Schema.decodeUnknownSync(Swarm.SwarmMessage)(message)),
    ).toEqual(message)
  })

  test("AC1: identifiers and content hashes reject malformed values", () => {
    expect(Schema.is(Swarm.SwarmTask)({ ...task, id: "nope" })).toBe(false)
    expect(Schema.is(Swarm.SwarmTask)({ ...task, parentId: "nope" })).toBe(false)
    expect(Schema.is(Swarm.SwarmTask)({ ...task, attempts: -1 })).toBe(false)
    expect(Schema.is(Swarm.SwarmTask)({ ...task, state: "bogus" })).toBe(false)
    expect(Schema.is(Swarm.Artifact)({ ...artifact, hash: "0".repeat(63) })).toBe(false)
    expect(Schema.is(Swarm.Artifact)({ ...artifact, hash: "A".repeat(64) })).toBe(false)
  })

  test("AC2: numeric boundaries are structural (non-negative, positive where required)", () => {
    expect(Schema.is(Swarm.SwarmTask)({ ...task, attempts: 0 })).toBe(true)
    expect(Schema.is(Swarm.SwarmTask)({ ...task, attempts: 5 })).toBe(true)
    expect(Schema.is(Swarm.Lease)({ ...lease, expiresAt: 0 })).toBe(false)
    expect(Schema.is(Swarm.Lease)({ ...lease, expiresAt: 1 })).toBe(true)
    expect(Schema.is(Swarm.BudgetReservation)({ ...budget, tokens: -1 })).toBe(false)
    expect(Schema.is(Swarm.BudgetReservation)({ ...budget, cost: -0.5 })).toBe(false)
    expect(Schema.is(Swarm.TaskTransition)({ ...transition, seq: -1 })).toBe(false)
    expect(Schema.is(Swarm.TaskTransition)({ ...transition, seq: 0 })).toBe(true)
    expect(Schema.is(Swarm.SwarmMessage)({ ...message, version: 0 })).toBe(false)
    expect(Schema.is(Swarm.SwarmMessage)({ ...message, version: 1 })).toBe(true)
  })

  test("AC2: invariant (1) — one state field and a declarative lifecycle table", () => {
    expect(Object.keys(Swarm.Lifecycle).length).toBe(15)
    for (const [from, targets] of Object.entries(Swarm.Lifecycle)) {
      expect(Schema.is(Swarm.TaskState)(from)).toBe(true)
      for (const to of targets) expect(Schema.is(Swarm.TaskState)(to)).toBe(true)
    }
    for (const state of Swarm.TerminalStates) expect(Swarm.Lifecycle[state]).toEqual([])
  })

  test("AC2: invariant (4) — a message is advisory, with no state or permission fields", () => {
    const decoded = Schema.decodeUnknownSync(Swarm.SwarmMessage)(message)
    expect(Object.keys(decoded).sort()).toEqual(["corrId", "detail", "from", "id", "kind", "to", "version"])
    expect(Object.keys(decoded)).not.toContain("state")
    expect(Object.keys(decoded)).not.toContain("permission")
    expect(Schema.is(Swarm.SwarmMessage)({ ...message, kind: "command" })).toBe(false)
  })

  test("AC2: the message union decodes every advisory kind", () => {
    const kinds = {
      status: { detail: "s" },
      question: { prompt: "q" },
      finding: { detail: "f" },
      hold: { reason: "h" },
      veto: { reason: "v" },
    }
    for (const [kind, extra] of Object.entries(kinds)) {
      const value = { ...message, ...extra, kind }
      expect(Schema.is(Swarm.SwarmMessage)(value)).toBe(true)
    }
  })
})
