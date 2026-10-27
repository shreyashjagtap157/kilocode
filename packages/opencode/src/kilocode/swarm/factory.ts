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
import { randomUUID } from "node:crypto"

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

/** Generate a default team plan from a high-level objective. Uses sensible
 *  specialist roles and task dependencies suitable for typical software projects.
 *  Returns a structured plan that can be passed to createTeamPlan or consumed
 *  directly by the swarm execution path. */
export function generateDefaultTeamPlanFromObjective(objective: string): {
  plan: string
  planId: string
  specialistRoles: string[]
  tasks: { id: string; title: string; description: string }[]
  dependencies: { taskId: string; dependsOn: string[] }[]
  executionStrategy: { concurrency: number; strategy: string }
  verificationStrategy: { reviewCount: number; independent: boolean }
  acceptanceCriteria: string[]
  integrationResponsibilities: string[]
  resourceLimits: { maxWorkers: number; maxDurationMs: number }
  assumptions: string[]
} {
  // Default specialist roles for a software project
  const specialistRoles = ["architect", "implementer", "tester", "reviewer"]

  // Default tasks based on common software development workflow
  const tasks = [
    { id: "tsk_1", title: "Architectural Design", description: "Design the overall architecture and patterns" },
    { id: "tsk_2", title: "Implementation", description: "Implement the core functionality" },
    { id: "tsk_3", title: "Testing", description: "Write and run tests to verify the implementation" },
    { id: "tsk_4", title: "Code Review", description: "Peer review of implemented code" }
  ]

  // Dependencies: tsk_2 depends on tsk_1, tsk_3 depends on tsk_2, tsk_4 depends on tsk_3
  const dependencies = [
    { taskId: "tsk_2", dependsOn: ["tsk_1"] },
    { taskId: "tsk_3", dependsOn: ["tsk_2"] },
    { taskId: "tsk_4", dependsOn: ["tsk_3"] }
  ]

  // Execution strategy: sequential with some parallelism (concurrency 2)
  const executionStrategy = { concurrency: 2, strategy: "sequential" }

  // Verification: 2 independent reviews per task
  const verificationStrategy = { reviewCount: 2, independent: true }

  // Acceptance criteria
  const acceptanceCriteria = [
    "All tasks complete with passing reviews",
    "Evidence supports all claimed results",
    "No blocking findings remain unresolved"
  ]

  // Integration responsibilities
  const integrationResponsibilities = ["Integrate accepted work, run final verification"]

  // Resource limits
  const resourceLimits = { maxWorkers: 4, maxDurationMs: 30_000 }

  // Assumptions
  const assumptions = [
    "All team members have access to the same codebase",
    "LLM providers are available and configured",
    "Tasks can be executed independently"
  ]

  return {
    plan: "",
    planId: `plan_${randomUUID()}`,
    specialistRoles,
    tasks,
    dependencies,
    executionStrategy,
    verificationStrategy,
    acceptanceCriteria,
    integrationResponsibilities,
    resourceLimits,
    assumptions
  }
}

/** Build a structured team plan from a high-level objective. */
export function createTeamPlan({
  objective,
  acceptanceCriteria,
  specialistRoles,
  tasks,
  dependencies,
  executionStrategy,
  verificationStrategy,
  integrationResponsibilities,
  resourceLimits,
  assumptions,
}: {
  objective: string
  acceptanceCriteria: string[]
  specialistRoles: string[]
  tasks: { id: string; title: string; description: string }[]
  dependencies: { taskId: string; dependsOn: string[] }[]
  executionStrategy: { concurrency: number; strategy: string }
  verificationStrategy: { reviewCount: number; independent: boolean }
  integrationResponsibilities: string[]
  resourceLimits: { maxWorkers: number; maxDurationMs: number }
  assumptions: string[]
}): { plan: string; planId: string } {
  const planId = `plan_${randomUUID()}`
  const plan = JSON.stringify({
    objective,
    acceptanceCriteria,
    specialistRoles,
    tasks,
    dependencies,
    executionStrategy,
    verificationStrategy,
    integrationResponsibilities,
    resourceLimits,
    assumptions,
  }, null, 2)
  return { plan, planId }
}

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

export const DynamicAgentFactory = {
  Plan,
  intersect,
  readOnlyPermissions,
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
  generateDefaultTeamPlanFromObjective(objective: string) {
    return generateDefaultTeamPlanFromObjective(objective)
  },
} as const
