/**
 * Kilo Swarm M1 — star topology representation (foundation only).
 *
 * A coordinator + workers model only: one coordinator, zero or more workers,
 * inserted order preserved. There is no mesh and no peer-to-peer authority —
 * only the coordinator may hold the coordinator role, and nothing here grants
 * authority or schedules work. This module is pure/representational:
 * registration, ordered lookup and invariant checks. It never dispatches.
 */
import { Schema } from "effect"

export namespace SwarmTopology {
  export const Role = Schema.Literals(["coordinator", "worker"])
  export type Role = Schema.Schema.Type<typeof Role>

  export type Member = {
    readonly id: string
    readonly role: Role
    readonly label?: string
  }

  export type Topology = {
    readonly coordinator: Member
    readonly workers: readonly Member[]
  }

  /** Create a star topology around a single coordinator. */
  export function create(coordinator: Member): Topology {
    if (coordinator.role !== "coordinator")
      throw new Error(`Topology coordinator must have role "coordinator", received "${coordinator.role}"`)
    return { coordinator, workers: [] }
  }

  /** Register a worker, preserving insertion order. Rejects a duplicate id or a second coordinator. */
  export function register(topology: Topology, member: Member): Topology {
    if (member.role === "coordinator") throw new Error(`Topology already has a coordinator: "${topology.coordinator.id}"`)
    if (member.id === topology.coordinator.id) throw new Error(`Topology member id is already registered: "${member.id}"`)
    if (topology.workers.some((worker) => worker.id === member.id))
      throw new Error(`Topology member id is already registered: "${member.id}"`)
    return { coordinator: topology.coordinator, workers: [...topology.workers, member] }
  }

  export function registerAll(topology: Topology, members: readonly Member[]): Topology {
    return members.reduce(register, topology)
  }

  /** Deterministic order: the coordinator first, then workers in registration order. */
  export function members(topology: Topology): readonly Member[] {
    return [topology.coordinator, ...topology.workers]
  }

  export function get(topology: Topology, id: string): Member | undefined {
    return members(topology).find((member) => member.id === id)
  }

  export function coordinators(topology: Topology): readonly Member[] {
    return members(topology).filter((member) => member.role === "coordinator")
  }

  /** Return the structural invariant violations of a topology (empty when valid). */
  export function check(topology: Topology): readonly string[] {
    const problems: string[] = []
    if (topology.coordinator.role !== "coordinator") problems.push("coordinator has the wrong role")
    const ids = members(topology).map((member) => member.id)
    if (new Set(ids).size !== ids.length) problems.push("member ids are not unique")
    if (topology.workers.some((worker) => worker.role === "coordinator")) problems.push("a worker is marked as coordinator")
    if (coordinators(topology).length !== 1) problems.push("topology must have exactly one coordinator")
    return problems
  }

  export function validate(topology: Topology): void {
    const problems = check(topology)
    if (problems.length) throw new Error(`Invalid swarm topology: ${problems.join("; ")}`)
  }
}
