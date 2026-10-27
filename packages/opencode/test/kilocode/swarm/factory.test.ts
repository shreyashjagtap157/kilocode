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

describe("DynamicAgentFactory", () => {
  test("AC3/AC4: plan persists restrictive session ceilings without replacing agent capabilities", () => {
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

    // Session permissions are restrictive overrides, not a replacement for the
    // selected agent's policy. Inherited denials survive, but the plan adds no allows.
    expect(plan.permission.every((rule) => rule.action === "deny")).toBe(true)
    expect(Permission.evaluate("read", "src/a.ts", plan.permission).action).not.toBe("allow")
    expect(Permission.evaluate("edit", "src/a.ts", plan.permission).action).not.toBe("allow")
    expect(Permission.evaluate("write", "src/a.ts", plan.permission).action).toBe("deny")
    expect(Permission.evaluate("task", "*", plan.permission).action).toBe("deny")
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
  test("read-only reviewer policy allows inspection and validation but blocks mutations", () => {
    const original = Permission.fromConfig({
      "*": "allow",
      board_read: "allow",
      board_post: "allow",
    })
    const policy = DynamicAgentFactory.readOnlyPermissions({ agentPermission: original, ceiling: [] })

    // The policy is a session override. It preserves the agent's existing safe
    // reads/board tools and allows only shell validations that the agent already permits.
    expect(Permission.evaluate("read", "src/a.ts", original, policy).action).toBe("allow")
    expect(Permission.evaluate("board_read", "*", original, policy).action).toBe("allow")
    expect(Permission.evaluate("board_post", "*", original, policy).action).toBe("allow")
    expect(Permission.evaluate("bash", "cat src/a.ts", original, policy).action).toBe("allow")
    expect(Permission.evaluate("bash", "git status", original, policy).action).toBe("allow")
    expect(Permission.evaluate("bash", "bun test test/example.test.ts", original, policy).action).toBe("allow")

    // The review ceiling cannot gain writes, arbitrary commands, or delegation.
    for (const permission of ["edit", "write", "apply_patch", "task", "todowrite"]) {
      expect(Permission.evaluate(permission, "*", original, policy).action).toBe("deny")
    }
    expect(Permission.evaluate("bash", "rm -rf src", original, policy).action).toBe("deny")
    expect(Permission.evaluate("bash", "rg --pre='touch /tmp/x' needle", original, policy).action).toBe("deny")
    expect(Permission.evaluate("bash", "git diff --ext-diff", original, policy).action).toBe("deny")
    // Git's output redirection flags bypass shell-operator suffix checks; deny
    // the write-capable Git flags explicitly as part of the reviewer ceiling.
    expect(Permission.evaluate("bash", "git diff --output=/tmp/review.diff", original, policy).action).toBe("deny")
    expect(Permission.evaluate("bash", "git show -o /tmp/review.txt HEAD", original, policy).action).toBe("deny")
    expect(Permission.evaluate("bash", "git log --output=/tmp/review.log", original, policy).action).toBe("deny")
    expect(Permission.evaluate("bash", "git grep --open-files-in-pager='touch /tmp/x' needle", original, policy).action).toBe("deny")
    expect(Permission.evaluate("bash", "bun test test/example.test.ts > test.log", original, policy).action).toBe("deny")
    expect(Permission.evaluate("bash", "bun test test/example.test.ts && rm -rf src", original, policy).action).toBe("deny")
  })

  test("read-only reviewer policy never grants a capability absent from the original plan", () => {
    const original = Permission.fromConfig({ read: "allow", bash: "deny", edit: "deny" })
    const policy = DynamicAgentFactory.readOnlyPermissions({ agentPermission: original, ceiling: [] })
    expect(Permission.evaluate("read", "src/a.ts", original, policy).action).toBe("allow")
    expect(Permission.evaluate("bash", "bun test test/example.test.ts", original, policy).action).toBe("deny")
    expect(Permission.evaluate("edit", "src/a.ts", original, policy).action).toBe("deny")
  })

  test("read-only reviewer policy preserves command-specific denials", () => {
    const original = Permission.fromConfig({
      "*": "allow",
      bash: { "cat secrets/**": "deny" },
    })
    const policy = DynamicAgentFactory.readOnlyPermissions({ agentPermission: original, ceiling: [] })

    // A broad safe-command pattern must not override a narrower user deny.
    expect(Permission.evaluate("bash", "cat secrets/private.txt", original, policy).action).toBe("deny")
    // Unrelated validation remains possible because it is already allowed by the agent.
    expect(Permission.evaluate("bash", "bun test test/example.test.ts", original, policy).action).toBe("allow")
  })

})
