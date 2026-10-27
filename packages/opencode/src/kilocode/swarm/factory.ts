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

const readOnlyShellCommands = [
  "pwd*",
  "ls *",
  "cat *",
  "Get-Location*",
  "Get-ChildItem*",
  "Get-Content*",
  "Select-String*",
  "Measure-Object*",
  "Get-Item*",
  "Get-FileHash*",
  "Invoke-Pester*",
  "Resolve-Path*",
  "Test-Path*",

  "head *",
  "tail *",
  "rg *",
  "grep *",
  "wc *",
  "file *",
  "stat *",
  "git status*",
  "git diff*",
  "git log*",
  "git show*",
  "git grep*",
  "git rev-parse*",
  "git ls-files*",
  "git ls-tree*",
  "git cat-file*",
  "bun test*",
  "bun run test*",
  "bun typecheck*",
  "bun run typecheck*",
  "bun run lint*",
  "bun run check*",
  "bun run build*",
  "bun run format*",
  "npm test*",
  "npm run test*",
  "npm run typecheck*",
  "npm run lint*",
  "npm run build*",
  "pnpm test*",
  "pnpm run test*",
  "pnpm --filter * test*",
  "pnpm --filter * run test*",
  "pnpm --filter * run typecheck*",
  "pnpm --filter * run lint*",
  "pnpm --filter * run build*",
  "yarn test*",
  "yarn run test*",
  "yarn run build*",
  "cargo test*",
  "cargo check*",
  "cargo build*",
  "cargo clippy*",
  "cargo fmt --check*",
  "go test*",
  "pytest*",
  "python -m pytest*",
  "python3 -m pytest*",
  "tsc*",
  "eslint*",
  "deno test*",
  "deno check*",
  "gradle test*",
  "./gradlew test*",
  "mvn test*",
  "./mvnw test*",
  "dotnet test*",
  "make test*",
  "make check*",
  "make build*",
  "biome check*",
  "prettier --check*",
  "npx vitest*",
  "npx jest*",
] as const

const shellOperatorSuffixes = [">", ">>", "<", "&", ";", "\n", "|", "&&", "||", "$(", "`"] as const

const shellDeniedPrefixes = [
  // ripgrep can launch arbitrary preprocessors; that is not read-only inspection.
  "rg --pre*",
  "rg *--pre*",
  // Avoid user-configured external diff/textconv executables during review.
  "git diff --ext-diff*",
  "git diff *--ext-diff*",
  "git diff --textconv*",
  "git diff *--textconv*",
  "git show --ext-diff*",
  "git show *--ext-diff*",
  "git show --textconv*",
  "git show *--textconv*",
  // diff, show, and log can write their output to a file.
  // Broad read-only command patterns must not permit these write-capable flags.
  "git diff --output*",
  "git diff *--output*",
  "git diff -o *",
  "git diff -o*",
  "git diff * -o *",
  "git show --output*",
  "git show *--output*",
  "git show -o *",
  "git show -o*",
  "git show * -o *",
  "git log --output*",
  "git log *--output*",
  "git log -o *",
  "git log -o*",
  "git log * -o *",
  // git grep can open results in an arbitrary pager command.
  "git grep --open-files-in-pager*",
  "git grep *--open-files-in-pager*",
] as const

/**
 * Restrict reviewer capabilities to inspection and explicit validation commands.
 * Direct edit/write/patch tools, recursive delegation and general shell commands
 * are denied. Validation commands still execute project-controlled code, so this
 * is a tool-permission ceiling rather than an OS/filesystem sandbox. The policy is
 * intersected with the original plan and never grants a capability it lacked.
 */
export function readOnlyPermissions(input: {
  agentPermission: Permission.Ruleset
  ceiling: Permission.Ruleset
}): Permission.Rule[] {
  // The session stores overrides, not the complete effective agent policy.
  // Preserve the original agent/session policy and only allow commands already
  // allowed there. Reject broad command patterns that overlap a narrower deny/ask
  // rule, since appending "cat *: allow" would otherwise override "cat secrets/**: deny".
  const original = [...input.agentPermission, ...input.ceiling]
  const overlapsRestrictedPattern = (candidate: string, restricted: string) => {
    if (restricted === "*") return false
    const candidatePrefix = candidate.split("*", 1)[0]!
    const restrictedPrefix = restricted.split("*", 1)[0]!
    return candidatePrefix.startsWith(restrictedPrefix) || restrictedPrefix.startsWith(candidatePrefix)
  }
  const allowedShellCommands = readOnlyShellCommands.filter((pattern) =>
    Permission.evaluate("bash", pattern, input.agentPermission, input.ceiling).action === "allow" &&
    !original.some((rule) =>
      rule.action !== "allow" &&
      rule.pattern !== "*" &&
      (rule.permission === "bash" || rule.permission === "*") &&
      overlapsRestrictedPattern(pattern, rule.pattern),
    ),
  )
  return KiloTask.merge(
    input.ceiling,
    [
      { permission: "edit", pattern: "*", action: "deny" },
      { permission: "write", pattern: "*", action: "deny" },
      { permission: "apply_patch", pattern: "*", action: "deny" },
      { permission: "task", pattern: "*", action: "deny" },
      { permission: "todowrite", pattern: "*", action: "deny" },
      { permission: "bash", pattern: "*", action: "deny" },
      ...allowedShellCommands.map((pattern) => ({ permission: "bash", pattern, action: "allow" as const })),
      ...shellDeniedPrefixes.map((pattern) => ({ permission: "bash", pattern, action: "deny" as const })),
      ...allowedShellCommands.flatMap((pattern) =>
        shellOperatorSuffixes.map((operator) => ({
          permission: "bash",
          pattern: `${pattern}${operator}*`,
          action: "deny" as const,
        })),
      ),
    ],
  )
}
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
  primaryTools?: readonly string[]
}


export type DefaultTeamTask = {
  id: string
  role: "architect" | "implementer" | "tester" | "reviewer"
  title: string
  prompt: string
  dependsOn: string[]
  preferredAgents: string[]
  readOnly: boolean
}

export type DefaultTeamPlan = {
  objective: string
  tasks: DefaultTeamTask[]
  execution: { strategy: "sequential"; concurrency: 1; maxWorkers: 4 }
  verification: { independentReviewsPerTask: 2; requireCommandEvidence: true }
  acceptanceCriteria: string[]
  assumptions: string[]
}

/** Build a deterministic dependency-ordered plan without starting any agents. */
export function generateDefaultTeamPlanFromObjective(objective: string): DefaultTeamPlan {
  const normalizedObjective = objective.trim()
  if (!normalizedObjective) throw new Error("swarm objective must not be empty")
  return {
    objective: normalizedObjective,
    tasks: [
      {
        id: "tsk_1",
        role: "architect",
        title: "Architecture and acceptance plan",
        prompt: [
          "Objective: " + normalizedObjective,
          "Inspect the repository and produce an actionable implementation plan.",
          "Identify relevant files, APIs, constraints, security implications, acceptance criteria, and focused validation commands.",
          "Do not modify files. Post the plan and material risks to the shared board when available, then return a concise evidence-based handoff.",
        ].join("\n"),
        dependsOn: [],
        preferredAgents: ["architect", "general", "explore", "debug"],
        readOnly: true,
      },
      {
        id: "tsk_2",
        role: "implementer",
        title: "Implement the agreed plan",
        prompt: [
          "Objective: " + normalizedObjective,
          "Implement the objective using the architecture handoff and the repository's existing conventions.",
          "Read prerequisite outputs and shared-board context before editing. Make the smallest coherent change that meets the objective.",
          "Report exact files changed, design decisions, known risks, and any validation you actually performed. Never claim an unrun test passed.",
        ].join("\n"),
        dependsOn: ["tsk_1"],
        preferredAgents: ["implementer", "developer", "coder", "code", "general", "debug"],
        readOnly: false,
      },
      {
        id: "tsk_3",
        role: "tester",
        title: "Test and validate the implementation",
        prompt: [
          "Objective: " + normalizedObjective,
          "Validate the implementation against the objective and architecture handoff.",
          "Add or update focused tests where needed, run the narrowest useful test suite and available type/lint checks, and fix test-scope failures where appropriate.",
          "Record exact commands and outcomes. Clearly separate passed checks, failures, skipped checks, and environment blockers.",
        ].join("\n"),
        dependsOn: ["tsk_2"],
        preferredAgents: ["tester", "test", "qa", "general", "debug", "explore"],
        readOnly: false,
      },
      {
        id: "tsk_4",
        role: "reviewer",
        title: "Final independent review",
        prompt: [
          "Objective: " + normalizedObjective,
          "Review the completed implementation and testing handoff without editing files.",
          "Check objective coverage, correctness, security/permission boundaries, regressions, test quality, and whether every success claim has evidence.",
          "Run an appropriate read-only validation command if available and report concrete evidence, blocking findings, and remaining uncertainties.",
        ].join("\n"),
        dependsOn: ["tsk_3"],
        preferredAgents: ["reviewer", "review", "general", "explore", "debug"],
        readOnly: true,
      },
    ],
    execution: { strategy: "sequential", concurrency: 1, maxWorkers: 4 },
    verification: { independentReviewsPerTask: 2, requireCommandEvidence: true },
    acceptanceCriteria: [
      "The implementation satisfies the stated objective and repository conventions.",
      "Focused automated tests and available static checks are run, with exact outcomes reported.",
      "Two separate read-only reviews check the implementation and do not invent validation evidence.",
      "No task is treated as accepted solely because an agent reports success; the existing durable evidence gate remains authoritative.",
    ],
    assumptions: [
      "The objective is the source of intent; repository contracts and configured permissions remain authoritative.",
      "If a prerequisite fails, dependent stages must be reported as blocked rather than executed against missing output.",
    ],
  }
}

export const DynamicAgentFactory = {
  Plan,
  intersect,
  readOnlyPermissions,
  generateDefaultTeamPlanFromObjective,
  plan(input: Input): Plan {
    KiloTask.validate(input.subagent, input.subagent.name)
    const session = input.session.permission ?? []
    const rules = KiloTask.inherited({ caller: input.caller, session: input.session, mcp: input.mcp ?? {} })
    const canTask = (input.depth ?? 0) + 1 < (input.maxDepth ?? 1)
    const canTodo = input.subagent.permission.some((rule) => rule.permission === "todowrite")
    const derived = deriveSubagentSessionPermission({ parentSessionPermission: session, subagent: input.subagent })
    const ceiling = KiloTask.permissions(rules, canTask)
    const primaryTools = input.primaryTools?.map((permission) => ({ permission, pattern: "*", action: "deny" as const })) ?? []
    const agent = input.subagent.name
    return {
      kind: "swarm-worker-plan",
      parentID: input.parentID,
      agent,
      title: `${input.description} (@${agent} subagent)`,
      // Session permissions are restrictive overrides. The selected agent's own
      // rules remain authoritative for capabilities; only hard ceilings are stored.
      permission: KiloTask.merge(derived, primaryTools, ceiling),
      canTask,
      canTodo,
    }
  },
} as const
