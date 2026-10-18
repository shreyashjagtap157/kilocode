import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import type { Agent } from "@/agent/agent"
import type { Session } from "@/session/session"
import { Permission } from "@/permission"
import { guarded } from "@/kilocode/agent"
import { DynamicAgentFactory } from "@/kilocode/swarm/factory"
import { SessionID } from "@/session/schema"

const parentID = SessionID.make("ses_parent")

const agent = (
  name: string,
  permission: Agent.Info["permission"],
  mode: Agent.Info["mode"] = "subagent",
): Agent.Info => ({
  name,
  mode,
  permission,
  options: {},
})

const session = (permission: Session.Info["permission"]): Pick<Session.Info, "permission"> => ({ permission })

const rank = { deny: 0, ask: 1, allow: 2 } as const
const permissions = [
  "*",
  "bash",
  "edit",
  "write",
  "read",
  "task",
  "notebook_edit",
  "notebook_execute",
  "agent_manager",
  "repo_clone",
  "webfetch",
]
const patterns = ["*", "src/a.ts", "secrets/x", "rm -rf /", "git status"]

describe("DynamicAgentFactory", () => {
  test("AC3/AC4: plan reuses the task derivation and never widens permissions", () => {
    const caller = agent(
      "main",
      Permission.fromConfig({ edit: { "secrets/**": "deny" }, write: "deny", task: "deny" }),
      "primary",
    )
    const subagent = agent(
      "worker",
      Permission.fromConfig({ "*": "deny", read: "allow", edit: { "src/**": "allow" } }),
    )
    const parent = session(Permission.fromConfig({ bash: "deny" }))

    const plan = DynamicAgentFactory.plan({ parentID, description: "build widget", caller, subagent, session: parent })

    expect(Schema.is(DynamicAgentFactory.Plan)(plan)).toBe(true)
    expect(plan.kind).toBe("swarm-worker-plan")
    expect(plan.agent).toBe("worker")
    expect(plan.title).toBe("build widget (@worker subagent)")
    expect(plan.parentID).toBe(parentID)

    // The ceiling introduces no allow rules of its own.
    expect(plan.permission.some((rule) => rule.action === "allow")).toBe(false)

    // Caller and parent-session denies are preserved.
    expect(Permission.evaluate("edit", "secrets/x", plan.permission).action).toBe("deny")
    expect(Permission.evaluate("bash", "git status", plan.permission).action).toBe("deny")

    // No probe is ever more permissive than the caller, the parent session, or the worker agent.
    for (const permission of permissions) {
      for (const pattern of patterns) {
        const worker = Permission.evaluate(permission, pattern, plan.permission).action
        const callerAction = Permission.evaluate(permission, pattern, caller.permission).action
        const sessionAction = Permission.evaluate(permission, pattern, parent.permission ?? []).action
        const subagentAction = Permission.evaluate(permission, pattern, subagent.permission).action
        expect(rank[worker]).toBeLessThanOrEqual(rank[callerAction])
        expect(rank[worker]).toBeLessThanOrEqual(rank[sessionAction])
        expect(rank[worker]).toBeLessThanOrEqual(rank[subagentAction])
      }
    }
  })

  test("AC3: parent-session denies form a hard ceiling over guarded tools", () => {
    const caller = agent("main", [], "primary")
    const subagent = agent("worker", Permission.fromConfig({ "*": "allow" }))
    const parent = session(Permission.fromConfig(Object.fromEntries(guarded.map((permission) => [permission, "deny"]))))

    const plan = DynamicAgentFactory.plan({ parentID, description: "guarded", caller, subagent, session: parent })

    for (const permission of guarded) {
      expect(Permission.evaluate(permission, "*", plan.permission).action).toBe("deny")
    }
  })

  test("AC3: intersect only allows a key every input allows", () => {
    const allow = [{ permission: "edit", pattern: "*", action: "allow" }] as const
    const deny = [{ permission: "edit", pattern: "secrets/**", action: "deny" }] as const
    const both = DynamicAgentFactory.intersect([...allow], [...allow])
    const mixed = DynamicAgentFactory.intersect([...allow], [...deny])

    // Allow survives only when every input explicitly allows that key.
    expect(Permission.evaluate("edit", "src/a.ts", both).action).toBe("allow")
    // A silent input is "ask", so an allow is never introduced from one side.
    expect(Permission.evaluate("edit", "src/a.ts", mixed).action).toBe("ask")
    expect(mixed.some((rule) => rule.action === "allow")).toBe(false)
    // A deny from any input wins.
    expect(Permission.evaluate("edit", "secrets/x", mixed).action).toBe("deny")
    expect(DynamicAgentFactory.intersect()).toEqual([])
  })

  test("AC5/AC6: construction is inert — a synchronous plan, no session, no execution handle", () => {
    const caller = agent("main", Permission.fromConfig({ read: "allow" }), "primary")
    const subagent = agent("worker", Permission.fromConfig({ read: "allow" }))
    const parent = session([])

    const plan = DynamicAgentFactory.plan({ parentID, description: "inert", caller, subagent, session: parent })

    // Pure/synchronous: not a promise, no effect, no live handle.
    expect(plan instanceof Promise).toBe(false)
    expect(typeof (plan as { then?: unknown }).then).toBe("undefined")
    expect(plan).not.toHaveProperty("sessionID")
    expect(plan).not.toHaveProperty("jobID")
    expect(plan).not.toHaveProperty("run")
    expect(plan).not.toHaveProperty("started")
    // Plain data: serializable with no live references.
    expect(() => JSON.stringify(plan)).not.toThrow()

    // Deterministic: repeated construction produces equal plans and no side effects.
    const again = DynamicAgentFactory.plan({ parentID, description: "inert", caller, subagent, session: parent })
    expect(again).toEqual(plan)

    // The factory exposes no dispatch/spawn/queue/execute surface.
    const api = Object.keys(DynamicAgentFactory)
    expect(api.filter((key) => /dispatch|spawn|schedule|queue|enqueue|execute|run/i.test(key))).toEqual([])
  })

  test("AC3: capability flags mirror the task depth rule and todowrite detection", () => {
    const caller = agent("main", [], "primary")
    const subagent = agent("worker", Permission.fromConfig({ todowrite: "allow" }))
    const parent = session([])

    const shallow = DynamicAgentFactory.plan({ parentID, description: "d", caller, subagent, session: parent })
    expect(shallow.canTask).toBe(false)
    expect(shallow.canTodo).toBe(true)

    const deep = DynamicAgentFactory.plan({
      parentID,
      description: "d",
      caller,
      subagent,
      session: parent,
      depth: 0,
      maxDepth: 2,
    })
    expect(deep.canTask).toBe(true)
  })
})
