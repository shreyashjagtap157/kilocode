/**
 * Kilo Swarm M1/M3 worker planning. Permission and subagent validation are
 * delegated to the existing Kilo task policy so swarm cannot create a wider
 * execution authority than an ordinary Kilo subagent.
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
const restrictiveness: Record<Action, number> = { allow: 0, ask: 1, deny: 2 }
const specificity = (pattern: string) => pattern.replaceAll("*", "").length

export function intersect(...rulesets: Permission.Ruleset[]): Permission.Rule[] {
  const keys = new Map<string, { permission: string; pattern: string }>()
  for (const rule of rulesets.flat()) keys.set(`${rule.permission}\u0000${rule.pattern}`, { permission: rule.permission, pattern: rule.pattern })
  const out: Permission.Rule[] = []
  for (const { permission, pattern } of keys.values()) {
    const actions = rulesets.map((set) => Permission.evaluate(permission, pattern, set).action)
    const action: Action = actions.includes("deny") ? "deny" : actions.every((value) => value === "allow") ? "allow" : "ask"
    out.push({ permission, pattern, action })
  }
  return out.sort((a, b) => specificity(a.pattern) - specificity(b.pattern) || restrictiveness[a.action] - restrictiveness[b.action])
}

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
  caller: Agent.Info
  subagent: Agent.Info
  session: Pick<Session.Info, "permission">
  mcp?: Config.Info["mcp"]
  depth?: number
  maxDepth?: number
}

export const DynamicAgentFactory = {
  Plan,
  intersect,
  plan(input: Input): Plan {
    KiloTask.validate(input.subagent, input.subagent.name)
    const session = input.session.permission ?? []
    const rules = KiloTask.inherited({ caller: input.caller, session: input.session, mcp: input.mcp ?? {} })
    const canTask = (input.depth ?? 0) + 1 < (input.maxDepth ?? 1)
    const canTodo = input.subagent.permission.some((rule) => rule.permission === "todowrite")
    const derived = deriveSubagentSessionPermission({ parentSessionPermission: session, subagent: input.subagent })
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
