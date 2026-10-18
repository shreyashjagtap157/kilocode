import { describe, expect, test } from "bun:test"
import { SwarmTopology } from "@/kilocode/swarm/topology"

const coordinator = { id: "main", role: "coordinator" } as const
const worker = (id: string) => ({ id, role: "worker" }) as const

describe("SwarmTopology", () => {
  test("AC7: create builds a single-coordinator star with no workers", () => {
    const topology = SwarmTopology.create(coordinator)
    expect(topology.coordinator).toEqual(coordinator)
    expect(topology.workers).toEqual([])
    expect(SwarmTopology.check(topology)).toEqual([])
    expect(() => SwarmTopology.validate(topology)).not.toThrow()
  })

  test("AC7: create rejects a non-coordinator and register rejects a second coordinator", () => {
    expect(() => SwarmTopology.create(worker("w1"))).toThrow(/coordinator/)
    const topology = SwarmTopology.create(coordinator)
    expect(() => SwarmTopology.register(topology, { id: "other", role: "coordinator" })).toThrow(/already has a coordinator/)
  })

  test("AC7: workers keep deterministic registration order and ids stay unique", () => {
    const topology = SwarmTopology.registerAll(SwarmTopology.create(coordinator), [
      worker("w1"),
      worker("w2"),
      worker("w3"),
    ])
    expect(SwarmTopology.members(topology).map((member) => member.id)).toEqual(["main", "w1", "w2", "w3"])
    expect(SwarmTopology.members(topology).map((member) => member.id)).toEqual(["main", "w1", "w2", "w3"])
    expect(topology.workers.map((member) => member.id)).toEqual(["w1", "w2", "w3"])
    expect(() => SwarmTopology.register(topology, worker("w2"))).toThrow(/already registered/)
    expect(() => SwarmTopology.register(topology, { id: "main", role: "worker" })).toThrow(/already registered/)
  })

  test("AC7: ordered lookup returns the coordinator first", () => {
    const topology = SwarmTopology.registerAll(SwarmTopology.create(coordinator), [worker("w1"), worker("w2")])
    expect(SwarmTopology.get(topology, "main")).toEqual(coordinator)
    expect(SwarmTopology.get(topology, "w2")).toEqual(worker("w2"))
    expect(SwarmTopology.get(topology, "missing")).toBeUndefined()
    expect(SwarmTopology.coordinators(topology)).toEqual([coordinator])
  })

  test("AC7: check reports structural invariant violations", () => {
    const invalid = {
      coordinator: { id: "main", role: "worker" },
      workers: [
        { id: "main", role: "worker" },
        { id: "w1", role: "coordinator" },
      ],
    } as unknown as SwarmTopology.Topology
    const problems = SwarmTopology.check(invalid)
    expect(problems).toContain("coordinator has the wrong role")
    expect(problems).toContain("member ids are not unique")
    expect(problems).toContain("a worker is marked as coordinator")
    expect(() => SwarmTopology.validate(invalid)).toThrow(/Invalid swarm topology/)
  })

  test("AC10: topology exposes representation only, no scheduling or dispatch", () => {
    const api = Object.keys(SwarmTopology)
    expect(api.filter((key) => /dispatch|spawn|schedule|queue|enqueue|execute|assign/i.test(key))).toEqual([])
  })
})
