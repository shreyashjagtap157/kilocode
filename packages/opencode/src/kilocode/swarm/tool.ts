import { Cause, Effect, Schema } from "effect"
import { createHash } from "node:crypto"
import * as Tool from "@/tool/tool"
import { Agent } from "@/agent/agent"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { BoardEnabled } from "@/kilocode/board/enabled"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import * as MessageV2 from "@/session/message-v2"
import { BoardStore } from "@/kilocode/board/store"
import { SwarmRuntime } from "./runtime"
import { SwarmState } from "./state"
import { SwarmVerification } from "./verification"
import * as SwarmCommunications from "./communications"
import { DynamicAgentFactory } from "./factory"

const Worker = Schema.Struct({ agent: Schema.String, role: Schema.String, prompt: Schema.String })
const Parameters = Schema.Struct({
  workers: Schema.optional(Schema.Array(Worker)),
  maxWorkers: Schema.optional(Schema.Int.pipe(Schema.greaterThanOrEqualTo(1), Schema.lessThanOrEqualTo(12))),
  objective: Schema.String,
})

function finding(messages: MessageV2.WithParts[]) {
  const latest = MessageV2.latest(messages).finished ?? MessageV2.latest(messages).assistant
  if (!latest) return "No assistant finding was persisted."
  const message = messages.find((item) => item.info.id === latest.id)
  if (!message) return "No assistant finding was persisted."
  return message.parts.filter((part) => part.type === "text").map((part) => part.text).join("\n").trim() || "No textual assistant finding was persisted."
}

function errorText(error: unknown) {
  return error instanceof Error ? error.message : typeof error === "string" ? error : String(error)
}

function parseReview(text: string) {
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)\s*```/i)?.[1]
  const candidate = fenced ?? text.trim()
  try { return JSON.parse(candidate) } catch {
    return { verdict: "inconclusive", summary: "Reviewer did not return valid JSON matching the evidence contract.", checks: [], evidence: [], blockingFindings: ["Reviewer output could not be parsed as JSON."], uncertainties: ["Structured peer review is unavailable for this worker."] }
  }
}


/**
 * Passing command evidence must come from a completed Kilo shell ToolPart,
 * not from the review model's JSON. Only recognizable validation commands count.
 */
function verifiedExecutions(
  messages: MessageV2.WithParts[],
  reviewerSessionId: string,
) {
  const executions: Array<{
    command: string
    exitCode: number
    sourceSessionId: string
    messageId: string
    partId: string
    callId: string
    outputSha256: string
  }> = []
  for (const message of messages) {
    for (const part of message.parts) {
      if (part.type !== "tool" || part.tool !== "bash" || part.state.status !== "completed") continue
      const command = part.state.input.command
      const exitCode = part.state.metadata.exit
      if (typeof command !== "string" || !SwarmVerification.isValidationCommand(command)) continue
      if (typeof exitCode !== "number" || !Number.isInteger(exitCode)) continue
      executions.push({
        command,
        exitCode,
        sourceSessionId: reviewerSessionId,
        messageId: message.info.id,
        partId: part.id,
        callId: part.callID,
        outputSha256: createHash("sha256").update(part.state.output).digest("hex"),
      })
    }
  }
  return executions
}


function selectAgentForRole(input: {
  role: string
  preferredAgents: readonly string[]
  availableAgents: readonly Agent.Info[]
}) {
  const eligible = input.availableAgents.filter((candidate) =>
    (candidate.mode === "subagent" || candidate.mode === "all") &&
    candidate.hidden !== true &&
    candidate.deprecated !== true,
  )
  const fallbackNames = new Set(["general", "explore", "debug"])
  for (const preferred of input.preferredAgents.filter((name) => !fallbackNames.has(name))) {
    const match = eligible.find((candidate) => candidate.name.toLowerCase() === preferred.toLowerCase())
    if (match) return match.name
  }
  const roleKeywords: Record<string, string[]> = {
    architect: ["architect", "architecture", "design", "planner"],
    implementer: ["implementer", "implementation", "developer", "coder"],
    tester: ["tester", "testing", "test", "qa", "quality"],
    reviewer: ["reviewer", "review", "audit"],
  }
  const keywords = roleKeywords[input.role] ?? [input.role.toLowerCase()]
  const semanticMatch = eligible.find((candidate) =>
    keywords.some((keyword) =>
      [candidate.name, candidate.displayName ?? "", candidate.description ?? ""]
        .join(" ")
        .toLowerCase()
        .includes(keyword),
    ),
  )
  const fallbackPreferred = input.preferredAgents
    .map((preferred) => eligible.find((candidate) => candidate.name.toLowerCase() === preferred.toLowerCase()))
    .find((candidate) => candidate !== undefined)
  const fallback = semanticMatch ??
    fallbackPreferred ??
    eligible.find((candidate) => candidate.name === "general") ??
    eligible.find((candidate) => candidate.name === "debug") ??
    eligible[0]
  if (!fallback) throw new Error("No visible subagent is available for objective-only swarm mode.")
  return fallback.name
}

export const SwarmTool: Tool.Info<typeof Parameters> = {
  id: "swarm",
  init: () => Effect.succeed({
    description: "Run a durable team swarm with configured workers or an objective-only staged specialist plan. Workers coordinate on the session board and receive independent evidence-backed peer review before work is accepted.",
    parameters: Parameters,
    execute: (args, ctx) => Effect.gen(function* () {
       if (!args.objective.trim()) return yield* Effect.fail(new Error("swarm objective must not be empty"))
      const caller = yield* Agent.get(ctx.agent)
      const sessions = yield* Session.Service
      const parent = yield* sessions.get(ctx.sessionID as SessionID)
      const config = yield* Config.Service
      const flags = yield* RuntimeFlags.Service
      const cfg = yield* config.get()

       let defaultTeamPlan: ReturnType<typeof DynamicAgentFactory.generateDefaultTeamPlanFromObjective> | undefined
       let workers: Array<{ agent: string; role: string; prompt: string }>
       if (args.workers === undefined) {
         defaultTeamPlan = DynamicAgentFactory.generateDefaultTeamPlanFromObjective(args.objective)
         const availableAgents = yield* Agent.list()
         if (!availableAgents.some((candidate) =>
           (candidate.mode === "subagent" || candidate.mode === "all") &&
           candidate.hidden !== true &&
           candidate.deprecated !== true,
         )) {
           return yield* Effect.fail(new Error("Objective-only swarm requires at least one visible configured subagent."))
         }
         workers = defaultTeamPlan.tasks.map((task) => ({
           agent: selectAgentForRole({ role: task.role, preferredAgents: task.preferredAgents, availableAgents }),
           role: task.role,
           prompt: task.prompt,
         }))
       } else {
         workers = args.workers
       }
       if (workers.length === 0) return yield* Effect.fail(new Error("swarm requires at least one worker"))
       if (workers.length > 12) return yield* Effect.fail(new Error("swarm supports at most 12 workers per invocation"))
       if (workers.length < 2) return yield* Effect.fail(new Error("verified swarm mode requires at least two workers so work can be independently peer-reviewed"))
      let boardEnabled = BoardEnabled.on(cfg, flags)
      let boardUnavailableReason: string | undefined
      let boardHistoryError: string | undefined
      // Internal BoardStore calls obey the same feature setting and permissions
      // as public board_read / board_post tools; the swarm tool cannot bypass them.
      // The board is advisory, so declined requests degrade coordination without
      // discarding the work performed by the swarm.
      if (boardEnabled) {
        const permissionResult = yield* Effect.exit(Effect.gen(function* () {
          yield* ctx.ask({ permission: "board_read", patterns: ["*"], always: ["*"], metadata: {} })
          yield* ctx.ask({ permission: "board_post", patterns: ["*"], always: ["*"], metadata: {} })
        }))
        if (permissionResult._tag === "Failure") {
          if (Cause.hasInterrupts(permissionResult.cause)) return yield* Effect.interrupt
          boardEnabled = false
          boardUnavailableReason = Cause.pretty(permissionResult.cause)
        }
      }
      const resolved = yield* Effect.forEach(workers, (worker) => Agent.get(worker.agent), { concurrency: "unbounded" })
      for (const [index, subagent] of resolved.entries()) DynamicAgentFactory.plan({ parentID: parent.id, description: workers[index]?.role ?? subagent.name, caller, subagent, session: parent })
      const base = ctx.callID ? `cor_swarm_${ctx.callID}` : `cor_swarm_${Date.now()}`
      if (boardEnabled) {
        const boardInit = yield* Effect.exit(Effect.gen(function* () {
          yield* BoardStore.scope(parent.id as SessionID)
          yield* BoardStore.post({
            sessionID: parent.id as SessionID,
            messageID: `${base}_objective`,
            callID: base,
            to: "ALL",
            type: "INFO",
            body: `SWARM OBJECTIVE: ${args.objective}\nWorkers: ${workers.map((worker) => `${worker.role} (${worker.agent})`).join(", ")}\nTeam protocol: read shared board context before acting; post discoveries, blockers, questions, and verified findings for peers; challenge contradictions with evidence; do not claim acceptance without independent review.`,
          })
        }))
        if (boardInit._tag === "Failure") {
          if (Cause.hasInterrupts(boardInit.cause)) return yield* Effect.interrupt
          boardEnabled = false
          boardUnavailableReason = Cause.pretty(boardInit.cause)
        }
      }
      const workerLimit = args.maxWorkers ?? defaultTeamPlan?.execution.maxWorkers ?? workers.length
       const requiredReviews = defaultTeamPlan?.verification.independentReviewsPerTask ?? 1
       const swarmId = yield* SwarmState.createSwarm({ maxWorkers: workerLimit, requiredReviews })
      const planned = workers.map((worker, index) => ({
        swarmId, parentSessionId: parent.id, description: worker.role,
        prompt: [`Shared objective: ${args.objective}`, `Role: ${worker.role}`, `Task: ${worker.prompt}`, "", "TEAM COORDINATION PROTOCOL:", "Inspect the shared agent board before substantive work when available.", "Post concise discoveries, blockers, questions, and verified findings so peers can build on your work.", "If another worker has already solved part of your task, build on that evidence instead of duplicating it. If you disagree, post the contradiction and supporting evidence.", "Before finishing, post your strongest evidence-backed result and every remaining uncertainty to the shared board when board_post is available; always return a final handoff in your task result."].join("\n"),
        caller, subagent: resolved[index], actor: ctx.agent, corrId: `${base}_${index}` as any,
        boardEnabled,
         ...(defaultTeamPlan?.tasks[index]?.readOnly ? { readOnly: true } : {}),
      }))
      
       let batch: {
         completed: Array<{ index: number; result: SwarmRuntime.PlannedResult }>
         failed: Array<{ index: number; error: unknown }>
       }
       if (defaultTeamPlan) {
         const stagedCompleted: Array<{ index: number; result: SwarmRuntime.PlannedResult }> = []
         const stagedFailed: Array<{ index: number; error: unknown }> = []
         const stageFindings = new Map<string, string>()
         let blockedBy: string | undefined
         for (const [index, task] of defaultTeamPlan.tasks.entries()) {
           if (blockedBy) {
             stagedFailed.push({
               index,
               error: new Error("Blocked because prerequisite task " + blockedBy + " failed; dependent work was not executed."),
             })
             continue
           }
           const missingDependency = task.dependsOn.find((dependency) => !stageFindings.has(dependency))
           if (missingDependency) {
             stagedFailed.push({
               index,
               error: new Error("Blocked because prerequisite task " + missingDependency + " has no completed handoff."),
             })
             blockedBy = missingDependency
             continue
           }
           const dependencyContext = task.dependsOn
             .map((dependency) => "Prerequisite " + dependency + " output:\n" + (stageFindings.get(dependency) ?? "No handoff was persisted."))
             .join("\n\n")
           const stageBatch = yield* SwarmRuntime.runPlannedBatchSettled({
             workers: [{
               ...planned[index]!,
               prompt: [
                 planned[index]!.prompt,
                 "STAGED EXECUTION: respect the declared task dependencies; do not act as though a prerequisite succeeded unless its handoff appears below.",
                 "Current stage: " + task.id + " — " + task.title,
                 "Prerequisites: " + (task.dependsOn.join(", ") || "none"),
                 dependencyContext ? "PREREQUISITE HANDOFFS:\n" + dependencyContext : "No prerequisite handoff is required for this stage.",
               ].join("\n\n"),
             }],
             concurrency: 1,
           })
           if (stageBatch.completed[0]) {
             const result = stageBatch.completed[0].result
             stagedCompleted.push({ index, result })
             const transcript = yield* MessageV2.stream(result.sessionId as SessionID)
             stageFindings.set(task.id, finding(transcript).slice(0, 6000) || "The stage completed without a persisted textual handoff.")
           }
           if (stageBatch.failed[0]) {
             stagedFailed.push({ index, error: stageBatch.failed[0].error })
             blockedBy = task.id
           } else if (!stageBatch.completed[0]) {
             stagedFailed.push({ index, error: new Error("The stage returned no completed result.") })
             blockedBy = task.id
           }
         }
         batch = { completed: stagedCompleted, failed: stagedFailed }
       } else {
         batch = yield* SwarmRuntime.runPlannedBatchSettled({ workers: planned, concurrency: args.maxWorkers ?? workers.length })
       }
      const reports = yield* Effect.forEach(batch.completed, (entry) => MessageV2.stream(entry.result.sessionId as SessionID), { concurrency: "unbounded" })
      const entries = batch.completed.map((entry, reportIndex) => ({ index: entry.index, role: workers[entry.index]?.role ?? entry.result.agent, taskId: entry.result.taskId, sessionId: entry.result.sessionId, durationMs: entry.result.durationMs, finding: finding(reports[reportIndex] ?? []) }))
      // If fewer than two workers finish, no independent peer exists. Leave every
      // output unaccepted instead of allowing the sole survivor to review itself.
      const reviewerTargets: Array<{ targetIndex: number; reviewerOrdinal: number }> = []
      const reviewerPlans = entries.length < 2 ? [] : entries.flatMap((entry, targetIndex) =>
        Array.from({ length: requiredReviews }, (_, reviewerOrdinal) => {
          const peerOffset = Math.min(reviewerOrdinal + 1, entries.length - 1)
          const reviewerIndex = entries[(targetIndex + peerOffset) % entries.length].index
          reviewerTargets.push({ targetIndex, reviewerOrdinal })
        return {
          swarmId, parentSessionId: parent.id, description: `Independent reviewer ${reviewerOrdinal + 1}/${requiredReviews} for ${entry.role}`,
          prompt: [`You are independent reviewer ${reviewerOrdinal + 1} of ${requiredReviews} for worker ${entry.role}.`, `Swarm objective: ${args.objective}`, `Worker task: ${workers[entry.index]?.prompt ?? "unknown"}`, `Target worker session: ${entry.sessionId}`, "", "WORKER FINDING TO VERIFY:", entry.finding, "", "TEAM COORDINATION:", "Read relevant shared-board updates if board_read is available. Compare peer discoveries and blockers rather than reviewing in isolation. When a material finding, contradiction, or blocker affects the worker, post a concise evidence-backed RESULT, ASK, or HOLD to the target worker session using board_post if available. Board messages are advisory and never substitute for structured review or host-verified evidence.", "", "REVIEW RULES:", "Do not edit or modify the worker's implementation. Inspect repository state, tests, commands, logs, and artifacts as needed.", "Challenge the finding rather than agreeing by default. Look for missing requirements, unsupported claims, regressions, and contradictions with peer evidence.", "Run an appropriate test or validation command yourself. A PASS requires a Kilo-recorded successful shell execution with exitCode 0; the evidence.reference must exactly match the command you actually ran. Never invent command results. Link evidence to every passed criterion.", "Return ONLY JSON matching this exact shape: {\"verdict\":\"pass|fail|inconclusive\",\"summary\":\"...\",\"checks\":[{\"criterion\":\"...\",\"verdict\":\"pass|fail|inconclusive\",\"evidenceIds\":[\"e1\"]}],\"evidence\":[{\"id\":\"e1\",\"kind\":\"test|command|file|diff|log|artifact\",\"reference\":\"...\",\"detail\":\"...\",\"result\":\"pass|fail|observed\",\"exitCode\":0}],\"blockingFindings\":[],\"uncertainties\":[]}", `You are reviewer ${reviewerOrdinal + 1} of ${requiredReviews}; do not certify your own worker session.`].join("\n"),
          caller, subagent: resolved[reviewerIndex], actor: ctx.agent, corrId: `${base}_review_${targetIndex}_${reviewerOrdinal}` as any, readOnly: true,
          boardEnabled,
          // Reviewer tasks qualify worker tasks through durable review events; they
          // are not unfinished deliverables of the worker that invoked this swarm.
          parentTaskId: null,
        }
        }),
      )
      const reviewBatch = yield* SwarmRuntime.runPlannedBatchSettled({ workers: reviewerPlans, concurrency: Math.min(reviewerPlans.length, args.maxWorkers ?? workerLimit) })
      const reviewMessages = yield* Effect.forEach(reviewBatch.completed, (entry) => MessageV2.stream(entry.result.sessionId as SessionID), { concurrency: "unbounded" })
      const reviews = yield* Effect.forEach(reviewBatch.completed, (reviewEntry) => Effect.gen(function* () {
        // completed[] is compacted when a reviewer fails; use the original reviewer-plan
        // index, not the compacted array position, to preserve target identity.
        const target = entries[reviewerTargets[reviewEntry.index]?.targetIndex ?? -1]
        if (!target) return yield* Effect.fail(new Error(`review result has no target for plan index ${reviewEntry.index}`))
        const reportIndex = reviewBatch.completed.findIndex((entry) => entry.index === reviewEntry.index)
        const reviewTranscript = reviewMessages[reportIndex] ?? []
        const report = parseReview(finding(reviewTranscript))
        const executedCommands = verifiedExecutions(reviewTranscript, reviewEntry.result.sessionId)
        let reviewDurablyRecorded = true
        const recorded = yield* SwarmVerification.recordReview({
          taskId: target.taskId,
          reviewerTaskId: reviewEntry.result.taskId,
          reviewerSessionId: reviewEntry.result.sessionId,
          reviewerAgent: reviewEntry.result.agent,
          corrId: `${base}_review_commit_${reviewEntry.index}`,
          report,
          executedCommands,
        }).pipe(Effect.catchCause((cause) => {
          if (Cause.hasInterrupts(cause)) return Effect.interrupt
          return Effect.sync(() => {
            reviewDurablyRecorded = false
            return {
              verdict: "inconclusive" as const,
              accepted: false,
              summary: "Review could not be durably recorded.",
              blockingFindings: [Cause.pretty(cause)],
              evidence: [],
              checks: [],
            }
          })
        }))

        // A review task is an execution artifact, not an implementation deliverable.
        // Close it as reviewed only when its event was persisted; failed persistence
        // must leave a truthful failed terminal state rather than a stuck reviewing row.
        let reviewerTaskState: string | undefined
        let reviewerTaskStateFailure: string | undefined
        const reviewerTask = yield* SwarmState.getTask(reviewEntry.result.taskId)
        if (reviewerTask.state === "reviewing") {
          const nextReviewerState = reviewDurablyRecorded ? "reviewed" : "failed"
          const completion = yield* Effect.exit(SwarmState.transition({
            taskId: reviewerTask.id,
            to: nextReviewerState,
            expectedVersion: reviewerTask.version,
            actor: ctx.agent,
            corrId: `${base}_review_task_${reviewEntry.index}`,
          }))
          if (completion._tag === "Failure") {
            if (Cause.hasInterrupts(completion.cause)) return yield* Effect.interrupt
            reviewerTaskStateFailure = Cause.pretty(completion.cause)
          } else {
            const committedReviewer = yield* SwarmState.getTask(reviewerTask.id)
            reviewerTaskState = committedReviewer.state
            if (committedReviewer.state !== nextReviewerState) {
              reviewerTaskStateFailure = `reviewer task state is ${committedReviewer.state}; expected ${nextReviewerState}`
            }
          }
        } else {
          reviewerTaskState = reviewerTask.state
          reviewerTaskStateFailure = `reviewer task was ${reviewerTask.state}, expected reviewing before completion`
        }

        return {
          workerTaskId: target.taskId,
          reviewerTaskId: reviewEntry.result.taskId,
          reviewerOrdinal: reviewerTargets[reviewEntry.index]?.reviewerOrdinal ?? 0,
          reviewerTaskState,
          reviewerTaskStateFailure,
          ...recorded,
          accepted: recorded.accepted,
          blockingFindings: recorded.blockingFindings,
        }
      }), { concurrency: "unbounded" })

      const acceptanceFailures = new Map<string, string>()
      for (const entry of entries) {
        const workerReviews = reviews
          .filter((review) => review.workerTaskId === entry.taskId)
          .slice()
          .sort((left, right) => left.reviewerOrdinal - right.reviewerOrdinal)
        const required = workerReviews.slice(0, requiredReviews)
        if (required.length < requiredReviews) {
          acceptanceFailures.set(entry.taskId, `only ${required.length}/${requiredReviews} independent review results completed`)
          continue
        }
        if (!required.every((review) => review.accepted && review.reviewerTaskState === "reviewed" && !review.reviewerTaskStateFailure)) {
          acceptanceFailures.set(entry.taskId, "one or more required independent reviews did not pass or complete durably")
          continue
        }
        const task = yield* SwarmState.getTask(entry.taskId)
        if (task.state === "accepted") continue
        const outcome = yield* Effect.exit(SwarmState.transition({
          taskId: entry.taskId,
          to: "accepted",
          expectedVersion: task.version,
          actor: ctx.agent,
          corrId: `${base}_accept`,
        }))
        if (outcome._tag === "Failure") {
          if (Cause.hasInterrupts(outcome.cause)) return yield* Effect.interrupt
          acceptanceFailures.set(entry.taskId, Cause.pretty(outcome.cause))
        } else {
          const committed = yield* SwarmState.getTask(entry.taskId)
          if (committed.state !== "accepted") {
            acceptanceFailures.set(entry.taskId, `acceptance transition returned but task state is ${committed.state}`)
          }
        }
      }

      const qualifications = yield* Effect.forEach(entries, (entry) => Effect.gen(function* () {
        const task = yield* SwarmState.getTask(entry.taskId)
        const workerReviews = reviews.filter((review) => review.workerTaskId === entry.taskId)
        return {
          workerTaskId: entry.taskId,
          accepted: task.state === "accepted",
          reviewCount: workerReviews.length,
          passingReviewCount: workerReviews.filter((review) =>
            review.accepted && review.reviewerTaskState === "reviewed" && !review.reviewerTaskStateFailure,
          ).length,
          requiredReviews,
          reviewerTaskIds: workerReviews.map((review) => review.reviewerTaskId),
          acceptanceFailure: acceptanceFailures.get(entry.taskId),
        }
      }), { concurrency: "unbounded" })
      // Persist the qualification result on the shared board so a worker can
      // discover peer-review findings later; the board is advisory, while the
      // durable task gate remains the sole authority for acceptance.
      const reviewAnnouncements = yield* Effect.forEach(boardEnabled ? reviews : [], (review) => Effect.gen(function* () {
        const target = entries.find((entry) => entry.taskId === review.workerTaskId)
        if (!target) return { taskId: review.workerTaskId, posted: false }
        const qualification = qualifications.find((item) => item.workerTaskId === review.workerTaskId)
        const checks = Array.isArray(review.checks) ? review.checks as Array<{ criterion?: string; verdict?: string; evidenceIds?: string[] }> : []
        const evidence = Array.isArray(review.evidence) ? review.evidence as Array<{ id?: string; kind?: string; reference?: string; result?: string; exitCode?: number }> : []
        const findings = Array.isArray(review.blockingFindings) ? review.blockingFindings as string[] : []
        const body = [
          `Independent peer review: ${review.accepted ? "PASS" : "BLOCKED"}`,
          `Worker role: ${target.role}`,
          `Worker task: ${target.taskId}`,
          `Reviewer task: ${review.reviewerTaskId} (${String(review.reviewerTaskState ?? "unknown state")})`,
          `Worker acceptance: ${qualification?.accepted ? "ACCEPTED" : "NOT ACCEPTED"} (${qualification?.passingReviewCount ?? 0}/${requiredReviews} passing reviews)`,
          ...(review.reviewerTaskStateFailure ? [`Reviewer lifecycle warning: ${String(review.reviewerTaskStateFailure).slice(0, 300)}`] : []),
          `Summary: ${String(review.summary ?? "No structured review summary was recorded.").slice(0, 500)}`,
          ...(checks.length ? ["Checks:", ...checks.slice(0, 5).map((check) => `- ${String(check.verdict ?? "unknown")}: ${String(check.criterion ?? "unnamed criterion").slice(0, 180)} [${(check.evidenceIds ?? []).slice(0, 4).join(", ")}]`)] : []),
          ...(evidence.length ? ["Evidence:", ...evidence.slice(0, 4).map((item) => `- ${String(item.id ?? "evidence")}: ${String(item.kind ?? "artifact")} ${String(item.result ?? "observed")} ${String(item.reference ?? "").slice(0, 200)}${item.exitCode === undefined ? "" : ` (exit ${item.exitCode})`}`)] : []),
          ...(findings.length ? ["Blocking findings:", ...findings.slice(0, 5).map((item) => `- ${String(item).slice(0, 250)}`)] : []),
          "This board note is advisory. Acceptance is determined only by the durable evidence gate.",
        ].join("\n").slice(0, 3900)
        const posted = yield* BoardStore.post({
          sessionID: parent.id as SessionID,
          messageID: ctx.messageID,
          callID: `${base}_review_notice_${review.reviewerTaskId}`,
          to: target.sessionId,
          type: review.accepted ? "RESULT" : "HOLD",
          body,
        }).pipe(
          Effect.as(true),
          Effect.catchCause((cause) => Cause.hasInterrupts(cause) ? Effect.interrupt : Effect.succeed(false)),
        )
        return { taskId: target.taskId, posted }
      }), { concurrency: "unbounded" })
      const reviewerFailureAnnouncements = yield* Effect.forEach(boardEnabled ? reviewBatch.failed : [], (entry) => Effect.gen(function* () {
        const targetInfo = reviewerTargets[entry.index]
        const target = targetInfo ? entries[targetInfo.targetIndex] : undefined
        if (!target) return { workerTaskId: undefined, reviewerTaskId: undefined, posted: false }
        const body = [
          "Independent peer review: UNAVAILABLE",
          `Worker role: ${target.role}`,
          `Worker task: ${target.taskId}`,
          "No passing review was recorded. The worker must remain unaccepted until an independent review succeeds.",
          `Reviewer execution error: ${errorText(entry.error).slice(0, 1400)}`,
          "This board note is advisory; the durable acceptance gate remains authoritative.",
        ].join("\n").slice(0, 3900)
        const posted = yield* BoardStore.post({
          sessionID: parent.id as SessionID,
          messageID: ctx.messageID,
          callID: `${base}_review_unavailable_${target.taskId}_${targetInfo?.reviewerOrdinal ?? entry.index}`,
          to: target.sessionId,
          type: "HOLD",
          body,
        }).pipe(
          Effect.as(true),
          Effect.catchCause((cause) => Cause.hasInterrupts(cause) ? Effect.interrupt : Effect.succeed(false)),
        )
        return { workerTaskId: target.taskId, reviewerTaskId: undefined, posted }
      }), { concurrency: "unbounded" })
      // A single surviving worker has no independent peer to review it. Explicitly
      // notify that worker rather than leaving it with a stale impression of completion.
      const unavailablePeerAnnouncements = yield* Effect.forEach(
        boardEnabled && entries.length < 2 ? entries.filter((entry) => !reviews.some((review) => review.workerTaskId === entry.taskId)) : [],
        (target) => Effect.gen(function* () {
          const body = [
            "Independent peer review: UNAVAILABLE",
            `Worker role: ${target.role}`,
            `Worker task: ${target.taskId}`,
            "Fewer than two workers completed successfully, so an independent reviewer was unavailable. This output is not accepted.",
            "The coordinator must arrange an independent review before treating the implementation as complete.",
            "This board note is advisory; the durable acceptance gate remains authoritative.",
          ].join("\n").slice(0, 3900)
          const posted = yield* BoardStore.post({
            sessionID: parent.id as SessionID,
            messageID: ctx.messageID,
            callID: `${base}_no_peer_review_${target.taskId}`,
            to: target.sessionId,
            type: "HOLD",
            body,
          }).pipe(Effect.as(true), Effect.catchCause((cause) => Cause.hasInterrupts(cause) ? Effect.interrupt : Effect.succeed(false)))
          return { workerTaskId: target.taskId, posted }
        }),
        { concurrency: "unbounded" },
      )
      const boardHistory = boardEnabled
        ? yield* SwarmCommunications.collect(
            (cursor) => BoardStore.read({
              sessionID: parent.id as SessionID,
              limit: 50,
              ...(cursor ? { since: cursor } : {}),
            }).pipe(Effect.map((page) => ({
              hasMore: page.hasMore,
              ...(page.cursor ? { cursor: page.cursor } : {}),
              messages: page.messages
                .filter((message) => message.from !== "main")
                .map((message) => ({
                  from: message.fromLabel ?? message.from,
                  to: message.toLabel ?? message.to,
                  type: message.type,
                  body: message.body,
                  id: message.id,
                })),
            }))),
            { maxPages: 20, maxMessages: 30, maxBodyChars: 1200 },
          ).pipe(Effect.catchCause((cause) => {
            if (Cause.hasInterrupts(cause)) return Effect.interrupt
            return Effect.sync(() => {
              boardHistoryError = Cause.pretty(cause)
              return { scanned: 0, truncated: false, messages: [] as SwarmCommunications.Message[] }
            })
          }))
        : { scanned: 0, truncated: false, messages: [] as SwarmCommunications.Message[] }
      const communications = boardHistory.messages
      const failures = batch.failed.map((entry) => ({ role: workers[entry.index]?.role ?? "worker", error: errorText(entry.error) }))
      const reviewerFailures = reviewBatch.failed.map((entry) => {
        const targetInfo = reviewerTargets[entry.index]
        const target = targetInfo ? entries[targetInfo.targetIndex] : undefined
        return {
          role: "reviewer",
          targetRole: target?.role ?? "unknown worker",
          reviewerOrdinal: targetInfo?.reviewerOrdinal ?? 0,
          error: errorText(entry.error),
        }
      })
      const reviewerStateFailures = reviews.filter((review) => review.reviewerTaskStateFailure).map((review) => ({
        workerTaskId: review.workerTaskId,
        reviewerTaskId: review.reviewerTaskId,
        error: review.reviewerTaskStateFailure,
      }))
      const accepted = qualifications.filter((qualification) => qualification.accepted).length
      return {
        title: `Swarm completed ${entries.length}/${workers.length} workers; ${accepted}/${entries.length} independently verified`,
        output: [
          `Swarm objective: ${args.objective}`, `Workers completed: ${entries.length}/${workers.length}`, `Independent peer reviews completed: ${reviews.length}/${entries.length * requiredReviews}`, `Reviewer failures: ${reviewerFailures.length}`, `Reviewer lifecycle failures: ${reviewerStateFailures.length}`, `Peer-review board notices posted: ${reviewAnnouncements.filter((item) => item.posted).length + reviewerFailureAnnouncements.filter((item) => item.posted).length + unavailablePeerAnnouncements.filter((item) => item.posted).length}/${reviewAnnouncements.length + reviewerFailureAnnouncements.length + unavailablePeerAnnouncements.length}`, ...(entries.length < 2 ? ["Independent review unavailable: fewer than two workers completed successfully."] : []), `Workers accepted after evidence gate: ${accepted}/${entries.length}`, `Team communications: ${communications.length}${boardHistory.truncated ? ` (latest messages shown; ${boardHistory.scanned} observed)` : ""}`,
          ...(boardUnavailableReason ? [`Shared-board coordination unavailable: ${boardUnavailableReason.slice(0, 500)}`] : []),
          ...(boardHistoryError ? [`Shared-board history read failed: ${boardHistoryError.slice(0, 500)}`] : []),
          ...entries.flatMap((entry, index) => {
            const workerReviews = reviews
              .filter((item) => item.workerTaskId === entry.taskId)
              .slice()
              .sort((left, right) => left.reviewerOrdinal - right.reviewerOrdinal)
            const review = workerReviews[workerReviews.length - 1]
            const qualification = qualifications.find((item) => item.workerTaskId === entry.taskId)
            const details = review as (typeof review & {
              executionEvidence?: Array<{ command: string; exitCode: number | null; partId: string; messageId: string; callId: string; outputSha256: string }>
              executionEvidenceProblems?: string[]
            })
            const checks = Array.isArray(review?.checks)
              ? (review.checks as Array<{ criterion?: string; verdict?: string; evidenceIds?: string[] }>)
              : []
            const evidence = Array.isArray(review?.evidence)
              ? (review.evidence as Array<{ id?: string; kind?: string; reference?: string; detail?: string; result?: string; exitCode?: number }>)
              : []
            const executions = Array.isArray(details?.executionEvidence) ? details.executionEvidence : []
            const executionProblems = Array.isArray(details?.executionEvidenceProblems) ? details.executionEvidenceProblems : []
            return [
              `\n## ${index + 1}. ${entry.role}`,
              entry.finding,
              `Independent reviews: ${qualification?.passingReviewCount ?? 0}/${requiredReviews} passing; task ${qualification?.accepted ? "ACCEPTED" : "NOT ACCEPTED"}`,
              ...workerReviews.map((item) => `Reviewer ${item.reviewerOrdinal + 1} (${item.reviewerTaskId}): ${item.verdict} — ${String(item.summary ?? "No summary").slice(0, 300)}`),
              ...(qualification?.acceptanceFailure ? [`Acceptance gate: ${qualification.acceptanceFailure}`] : []),
              ...(checks.length
                ? ["Verification checks:", ...checks.slice(0, 6).map((check) =>
                    `- ${String(check.verdict ?? "unknown")}: ${String(check.criterion ?? "unnamed criterion").slice(0, 200)} [evidence: ${(check.evidenceIds ?? []).slice(0, 6).join(", ")}]`,
                  )]
                : []),
              ...(evidence.length
                ? ["Review evidence:", ...evidence.slice(0, 6).map((item) =>
                    `- ${String(item.id ?? "evidence")} ${String(item.kind ?? "artifact")}/${String(item.result ?? "observed")}: ${String(item.reference ?? "").slice(0, 240)}${item.exitCode === undefined ? "" : ` (exit ${item.exitCode})`}${item.detail ? ` — ${String(item.detail).slice(0, 240)}` : ""}`,
                  )]
                : []),
              ...(executions.length
                ? [
                    "Host-recorded validation executions:",
                    ...executions.slice(0, 4).map((execution) =>
                      `- ${execution.command} (exit ${String(execution.exitCode)}; part=${execution.partId}; message=${execution.messageId}; call=${execution.callId}; sha256=${execution.outputSha256.slice(0, 16)}…)`,
                    ),
                  ]
                : []),
              ...(executionProblems.length
                ? [`Execution evidence rejected: ${executionProblems.slice(0, 4).map((problem) => String(problem).slice(0, 220)).join("; ")}`]
                : []),
              ...(review?.blockingFindings?.length ? [`Blocking findings: ${review.blockingFindings.slice(0, 6).join("; ")}`] : []),
              `(task=${entry.taskId}, session=${entry.sessionId}, duration=${entry.durationMs}ms)`,
            ]
          }),
          ...(communications.length > 0 ? ["\n## Team communication", ...communications.map((message) => `- ${message.from} → ${message.to} [${message.type}]: ${message.body}`)] : []),
          ...failures.flatMap((entry) => [`\n## FAILED: ${entry.role}`, entry.error]),
          ...reviewerFailures.flatMap((entry) => [`\n## REVIEW FAILED: ${entry.role} (target: ${entry.targetRole})`, entry.error]),
          ...reviewerStateFailures.flatMap((entry) => [`\n## REVIEW TASK LIFECYCLE FAILED: ${entry.reviewerTaskId} (target: ${entry.workerTaskId})`, String(entry.error)]),
        ].join("\n"),
        metadata: { swarmId, workerCount: entries.length, failedWorkerCount: failures.length, reviewerFailureCount: reviewerFailures.length, reviewerStateFailureCount: reviewerStateFailures.length, requiredReviews, independentlyVerifiedCount: accepted, reviewAnnouncements, reviewerFailureAnnouncements, unavailablePeerAnnouncements, workers: entries, reviews, qualifications, failures, reviewerFailures, reviewerStateFailures, communications, communicationMessagesScanned: boardHistory.scanned, communicationsTruncated: boardHistory.truncated, boardUnavailableReason, boardHistoryError },
      }
    }),
  }),
}
