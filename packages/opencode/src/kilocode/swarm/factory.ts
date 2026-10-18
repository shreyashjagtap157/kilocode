/**
 * Kilo Swarm M1 — thin, inert DynamicAgentFactory (foundation only).
 *
 * `plan` computes a declarative worker plan by reusing the exact permission
 * derivation the task tool uses when it delegates to a subagent
 * (`packages/opencode/src/tool/task.ts:196-231`):
 *   - `KiloTask.inherited`  (Kilo parent/session ceiling derivation)
 *   - `deriveSubagentSessionPermission` (upstream parent-session deny inheritance)
 *   - `KiloTask.permissions` / `KiloTask.merge` (subagent deny forcing)
 * It does not duplicate agent configuration, model resolution or
 * permission-inheritance logic. The plan's permission ceiling is the
 * intersection of the derived parent/session ceiling and the worker agent's own
 * ruleset, so it can never be wider than any of them.
 *
 * Construction is inert: `plan` is a pure synchronous function. It does not
 * create, start, enqueue or execute a worker task, does not create a child
 * session, does not resolve a model, and does not invoke a provider. Turning a
 * plan into a live session is M3 dispatch work and is intentionally absent.
 */
import { Schema } from "effect"
import type { Agent } from "@/agent/agent"
import type { Config } from "@/config/config"
import type { Session } from "@/session/session"
import { Permission } from "@/permission"
import { deriveSubagentSessionPermission } from "@/agent/subagent-permissions"
import { KiloTask } from "@/kilocode/tool/task"
import { SessionID } from "@/session/schema"

type Action = Permission.Rule["action"]

// deny is the strongest action; a rule placed later wins under Permission.evaluate.
const restrictiveness: Record<Action, number> = { allow: 0, ask: 1, deny: 2 }

const specificity = (pattern: string) => pattern.replaceAll("*", "").length

/**
 * Intersection of permission rulesets. The result never permits an action that
 * any input does not itself permit:
 *   - `deny` from any input wins for a key;
 *   - `allow` survives only when every input explicitly allows that key;
 *   - otherwise the key is `ask`.
 * Rules are ordered least-specific first (and least-restrictive last among equal
 * specificity) so `Permission.evaluate`'s last-match rule selects the narrowest,
 * most restrictive decision. This is the least-privilege ceiling for a worker.
 */
export function intersect(...rulesets: Permission.Ruleset[]): Permission.Rule[] {
  const keys = new Map<string, { permission: string; pattern: string }>()
  for (const rule of rulesets.flat())
    keys.set(`${rule.permission}\u0000${rule.pattern}`, { permission: rule.permission, pattern: rule.pattern })
  const out: Permission.Rule[] = []
  for (const { permission, pattern } of keys.values()) {
    const actions = rulesets.map((set) => Permission.evaluate(permission, pattern, set).action)
    const action: Action = actions.includes("deny") ? "deny" : actions.every((value) => value === "allow") ? "allow" : "ask"
    out.push({ permission, pattern, action })
  }
  return out.sort(
    (a, b) => specificity(a.pattern) - specificity(b.pattern) || restrictiveness[a.action] - restrictiveness[b.action],
  )
}

/**
 * Declarative worker plan. Mirrors the fields `sessions.create` receives from the
 * task tool (`parentID`, `title`, `agent`, `permission`) but is data only — it
 * carries no session id, job id or execution handle.
 */
export const Plan = Schema.Struct({
  kind: Schema.Literal("swarm-worker-plan"),
  parentID: SessionID,
  agent: Schema.String,
  title: Schema.String,
  permission: Permission.Ruleset,
  canTask: Schema.Boolean,
  canTodo: Schema.Boolean,
}).annotate({ identifier: "SwarmWorkerPlan" })
export type Plan = Schema.Schema.Type<typeof Plan>

export type Input = {
  parentID: SessionID
  description: string
  /** The delegating agent (the task tool's `ctx.agent`). */
  caller: Agent.Info
  /** The worker's agent definition; its own allowlist governs the worker at run time. */
  subagent: Agent.Info
  /** The parent session's accumulated permission ruleset. */
  session: Pick<Session.Info, "permission">
  mcp?: Config.Info["mcp"]
  /** Current subagent depth and the configured limit, matching task.ts. */
  depth?: number
  maxDepth?: number
}

export const DynamicAgentFactory = {
  Plan,
  intersect,
  plan(input: Input): Plan {
    const session = input.session.permission ?? []
    const rules = KiloTask.inherited({ caller: input.caller, session: input.session, mcp: input.mcp ?? {} })
    const canTask = (input.depth ?? 0) + 1 < (input.maxDepth ?? 1)
    const canTodo = input.subagent.permission.some((rule) => rule.permission === "todowrite")
    const derived = deriveSubagentSessionPermission({
      parentSessionPermission: session,
      subagent: input.subagent,
    })
    const ceiling = KiloTask.permissions(rules, canTask)
    const agent = input.subagent.name
    return {
      kind: "swarm-worker-plan",
      parentID: input.parentID,
      agent,
      title: `${input.description} (@${agent} subagent)`,
      permission: intersect(derived, ceiling, input.subagent.permission),
      canTask,
      canTodo,
    }
  },
} as const
