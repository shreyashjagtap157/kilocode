import { Cause, Effect, Schema } from "effect"
import { createHash } from "node:crypto"
import * as Tool from "@/tool/tool"
import { Agent } from "@/agent/agent"
import { Session } from "@/session/session"
import { SessionID } from "@/session/schema"
import * as MessageV2 from "@/session/message-v2"
import { BoardStore } from "@/kilocode/board/store"
import { SwarmRuntime } from "./runtime"
import { SwarmState } from "./state"
import { SwarmVerification } from "./verification"
import { DynamicAgentFactory } from "./factory"

const Worker = Schema.Struct({ agent: Schema.String, role: Schema.String, prompt: Schema.String })
const Parameters = Schema.Struct({
  workers: Schema.Array(Worker),
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

export const SwarmTool: Tool.Info<typeof Parameters> = {
  id: "swarm",
  init: () => Effect.succeed({
    description: "Run multiple configured Kilo specialist agents concurrently as a durable team swarm. Workers share Kilo's persistent session board, communicate with peers, then independently cross-review each other's completed work with structured evidence before a worker can be marked accepted.",
    parameters: Parameters,
    execute: (args, ctx) => Effect.gen(function* () {
      if (args.workers.length === 0) return yield* Effect.fail(new Error("swarm requires at least one worker"))
      if (args.workers.length > 12) return yield* Effect.fail(new Error("swarm supports at most 12 workers per invocation"))
      if (args.workers.length < 2) return yield* Effect.fail(new Error("verified swarm mode requires at least two workers so work can be independently peer-reviewed"))
      const caller = yield* Agent.get(ctx.agent)
      const sessions = yield* Session.Service
      const parent = yield* sessions.get(ctx.sessionID as SessionID)
      const resolved = yield* Effect.forEach(args.workers, (worker) => Agent.get(worker.agent), { concurrency: "unbounded" })
      for (const [index, subagent] of resolved.entries()) DynamicAgentFactory.plan({ parentID: parent.id, description: args.workers[index]?.role ?? subagent.name, caller, subagent, session: parent })
      yield* BoardStore.scope(parent.id as SessionID)
      const base = ctx.callID ? `cor_swarm_${ctx.callID}` : `cor_swarm_${Date.now()}`
      yield* BoardStore.post({ sessionID: parent.id as SessionID, messageID: `${base}_objective`, callID: base, to: "ALL", type: "INFO", body: `SWARM OBJECTIVE: ${args.objective}\nWorkers: ${args.workers.map((worker) => `${worker.role} (${worker.agent})`).join(", ")}\nTeam protocol: read shared board context before acting; post discoveries, blockers, questions, and verified findings for peers; challenge contradictions with evidence; do not claim acceptance without independent review.` })
      const swarmId = yield* SwarmState.createSwarm({ maxWorkers: args.maxWorkers ?? args.workers.length })
      const planned = args.workers.map((worker, index) => ({
        swarmId, parentSessionId: parent.id, description: worker.role,
        prompt: [`Shared objective: ${args.objective}`, `Role: ${worker.role}`, `Task: ${worker.prompt}`, "", "TEAM COORDINATION PROTOCOL:", "Inspect the shared agent board before substantive work when available.", "Post concise discoveries, blockers, questions, and verified findings so peers can build on your work.", "If another worker has already solved part of your task, build on that evidence instead of duplicating it. If you disagree, post the contradiction and supporting evidence.", "Before finishing, post your strongest evidence-backed result and every remaining uncertainty to the shared board."].join("\n"),
        caller, subagent: resolved[index], actor: ctx.agent, corrId: `${base}_${index}` as any,
      }))
      const batch = yield* SwarmRuntime.runPlannedBatchSettled({ workers: planned, concurrency: args.maxWorkers ?? args.workers.length })
      const reports = yield* Effect.forEach(batch.completed, (entry) => MessageV2.stream(entry.result.sessionId as SessionID), { concurrency: "unbounded" })
      const entries = batch.completed.map((entry, reportIndex) => ({ index: entry.index, role: args.workers[entry.index]?.role ?? entry.result.agent, taskId: entry.result.taskId, sessionId: entry.result.sessionId, durationMs: entry.result.durationMs, finding: finding(reports[reportIndex] ?? []) }))
      // If fewer than two workers finish, no independent peer exists. Leave every
      // output unaccepted instead of allowing the sole survivor to review itself.
      const reviewerPlans = entries.length < 2 ? [] : entries.map((entry, reviewIndex) => {
        const reviewerIndex = entries[(reviewIndex + 1) % entries.length].index
        return {
          swarmId, parentSessionId: parent.id, description: `Peer reviewer for ${entry.role}`,
          prompt: [`You are the independent peer reviewer for worker ${entry.role}.`, `Swarm objective: ${args.objective}`, `Worker task: ${args.workers[entry.index]?.prompt ?? "unknown"}`, `Target worker session: ${entry.sessionId}`, "", "WORKER FINDING TO VERIFY:", entry.finding, "", "TEAM COORDINATION:", "Read relevant shared-board updates if board_read is available. Compare peer discoveries and blockers rather than reviewing in isolation. When a material finding, contradiction, or blocker affects the worker, post a concise evidence-backed RESULT, ASK, or HOLD to the target worker session using board_post if available. Board messages are advisory and never substitute for structured review or host-verified evidence.", "", "REVIEW RULES:", "Do not edit or modify the worker's implementation. Inspect repository state, tests, commands, logs, and artifacts as needed.", "Challenge the finding rather than agreeing by default. Look for missing requirements, unsupported claims, regressions, and contradictions with peer evidence.", "Run an appropriate test or validation command yourself. A PASS requires a Kilo-recorded successful shell execution with exitCode 0; the evidence.reference must exactly match the command you actually ran. Never invent command results. Link evidence to every passed criterion.", "Return ONLY JSON matching this exact shape: {\"verdict\":\"pass|fail|inconclusive\",\"summary\":\"...\",\"checks\":[{\"criterion\":\"...\",\"verdict\":\"pass|fail|inconclusive\",\"evidenceIds\":[\"e1\"]}],\"evidence\":[{\"id\":\"e1\",\"kind\":\"test|command|file|diff|log|artifact\",\"reference\":\"...\",\"detail\":\"...\",\"result\":\"pass|fail|observed\",\"exitCode\":0}],\"blockingFindings\":[],\"uncertainties\":[]}", `You are reviewer ${reviewIndex + 1}; do not certify your own worker session.`].join("\n"),
          caller, subagent: resolved[reviewerIndex], actor: ctx.agent, corrId: `${base}_review_${reviewIndex}` as any, readOnly: true,
          // Reviewer tasks qualify worker tasks through durable review events; they
          // are not unfinished deliverables of the worker that invoked this swarm.
          parentTaskId: null,
        }
      })
      const reviewBatch = yield* SwarmRuntime.runPlannedBatchSettled({ workers: reviewerPlans, concurrency: Math.min(args.workers.length, args.maxWorkers ?? args.workers.length) })
      const reviewMessages = yield* Effect.forEach(reviewBatch.completed, (entry) => MessageV2.stream(entry.result.sessionId as SessionID), { concurrency: "unbounded" })
      const reviews = yield* Effect.forEach(reviewBatch.completed, (reviewEntry) => Effect.gen(function* () {
        // completed[] is compacted when a reviewer fails; use the original reviewer-plan
        // index, not the compacted array position, to preserve target identity.
        const target = entries[reviewEntry.index]
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
        }).pipe(Effect.catchCause((cause) => Effect.sync(() => {
          reviewDurablyRecorded = false
          return {
            verdict: "inconclusive" as const,
            accepted: false,
            summary: "Review could not be durably recorded.",
            blockingFindings: [Cause.pretty(cause)],
            evidence: [],
            checks: [],
          }
        })))

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

        let acceptanceFailure: string | undefined
        if (recorded.accepted) {
          const task = yield* SwarmState.getTask(target.taskId)
          const outcome = yield* Effect.exit(SwarmState.transition({
            taskId: target.taskId,
            to: "accepted",
            expectedVersion: task.version,
            actor: ctx.agent,
            corrId: `${base}_accept_${reviewEntry.index}`,
          }))
          if (outcome._tag === "Failure") {
            acceptanceFailure = Cause.pretty(outcome.cause)
          } else {
            const committed = yield* SwarmState.getTask(target.taskId)
            if (committed.state !== "accepted") acceptanceFailure = `acceptance transition returned but task state is ${committed.state}`
          }
        }
        return {
          workerTaskId: target.taskId,
          reviewerTaskId: reviewEntry.result.taskId,
          reviewerTaskState,
          reviewerTaskStateFailure,
          ...recorded,
          accepted: recorded.accepted && !acceptanceFailure,
          blockingFindings: [...recorded.blockingFindings, ...(acceptanceFailure ? [acceptanceFailure] : [])],
        }
      }), { concurrency: "unbounded" })
      // Persist the qualification result on the shared board so a worker can
      // discover peer-review findings later; the board is advisory, while the
      // durable task gate remains the sole authority for acceptance.
      const reviewAnnouncements = yield* Effect.forEach(reviews, (review) => Effect.gen(function* () {
        const target = entries.find((entry) => entry.taskId === review.workerTaskId)
        if (!target) return { taskId: review.workerTaskId, posted: false }
        const checks = Array.isArray(review.checks) ? review.checks as Array<{ criterion?: string; verdict?: string; evidenceIds?: string[] }> : []
        const evidence = Array.isArray(review.evidence) ? review.evidence as Array<{ id?: string; kind?: string; reference?: string; result?: string; exitCode?: number }> : []
        const findings = Array.isArray(review.blockingFindings) ? review.blockingFindings as string[] : []
        const body = [
          `Independent peer review: ${review.accepted ? "PASS" : "BLOCKED"}`,
          `Worker role: ${target.role}`,
          `Worker task: ${target.taskId}`,
          `Reviewer task: ${review.reviewerTaskId} (${String(review.reviewerTaskState ?? "unknown state")})`,
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
          callID: `${base}_review_notice_${review.workerTaskId}`,
          to: target.sessionId,
          type: review.accepted ? "RESULT" : "HOLD",
          body,
        }).pipe(
          Effect.as(true),
          Effect.catchCause(() => Effect.succeed(false)),
        )
        return { taskId: target.taskId, posted }
      }), { concurrency: "unbounded" })
      const reviewerFailureAnnouncements = yield* Effect.forEach(reviewBatch.failed, (entry) => Effect.gen(function* () {
        const target = entries[entry.index]
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
          callID: `${base}_review_unavailable_${target.taskId}`,
          to: target.sessionId,
          type: "HOLD",
          body,
        }).pipe(
          Effect.as(true),
          Effect.catchCause(() => Effect.succeed(false)),
        )
        return { workerTaskId: target.taskId, reviewerTaskId: undefined, posted }
      }), { concurrency: "unbounded" })
      const board = yield* BoardStore.read({ sessionID: parent.id as SessionID, limit: 100 })
      const communications = board.messages.filter((message) => message.from !== "main").map((message) => ({ from: message.fromLabel ?? message.from, to: message.toLabel ?? message.to, type: message.type, body: message.body, id: message.id }))
      const failures = batch.failed.map((entry) => ({ role: args.workers[entry.index]?.role ?? "worker", error: errorText(entry.error) }))
      const reviewerFailures = reviewBatch.failed.map((entry) => ({
        role: entries[entry.index]?.role ?? "reviewer",
        targetRole: entries[entry.index]?.role ?? "unknown worker",
        error: errorText(entry.error),
      }))
      const reviewerStateFailures = reviews.filter((review) => review.reviewerTaskStateFailure).map((review) => ({
        workerTaskId: review.workerTaskId,
        reviewerTaskId: review.reviewerTaskId,
        error: review.reviewerTaskStateFailure,
      }))
      const accepted = reviews.filter((review) => review.accepted).length
      return {
        title: `Swarm completed ${entries.length}/${args.workers.length} workers; ${accepted}/${entries.length} independently verified`,
        output: [
          `Swarm objective: ${args.objective}`, `Workers completed: ${entries.length}/${args.workers.length}`, `Independent peer reviews: ${reviews.length}/${entries.length}`, `Reviewer failures: ${reviewerFailures.length}`, `Reviewer lifecycle failures: ${reviewerStateFailures.length}`, `Peer-review board notices posted: ${reviewAnnouncements.filter((item) => item.posted).length + reviewerFailureAnnouncements.filter((item) => item.posted).length}/${reviewAnnouncements.length + reviewerFailureAnnouncements.length}`, ...(entries.length < 2 ? ["Independent review unavailable: fewer than two workers completed successfully."] : []), `Workers accepted after evidence gate: ${accepted}/${entries.length}`, `Team communications: ${communications.length}`,
          ...entries.flatMap((entry, index) => { const review = reviews.find((item) => item.workerTaskId === entry.taskId); return [`\n## ${index + 1}. ${entry.role}`, entry.finding, `Review: ${review?.verdict ?? "missing"}${review?.accepted ? " (ACCEPTED)" : " (NOT ACCEPTED)"}`, ...(review?.blockingFindings?.length ? [`Blocking findings: ${review.blockingFindings.join("; ")}`] : []), `(task=${entry.taskId}, session=${entry.sessionId}, duration=${entry.durationMs}ms)`] }),
          ...(communications.length > 0 ? ["\n## Team communication", ...communications.map((message) => `- ${message.from} → ${message.to} [${message.type}]: ${message.body}`)] : []),
          ...failures.flatMap((entry) => [`\n## FAILED: ${entry.role}`, entry.error]),
          ...reviewerFailures.flatMap((entry) => [`\n## REVIEW FAILED: ${entry.role} (target: ${entry.targetRole})`, entry.error]),
          ...reviewerStateFailures.flatMap((entry) => [`\n## REVIEW TASK LIFECYCLE FAILED: ${entry.reviewerTaskId} (target: ${entry.workerTaskId})`, String(entry.error)]),
        ].join("\n"),
        metadata: { swarmId, workerCount: entries.length, failedWorkerCount: failures.length, reviewerFailureCount: reviewerFailures.length, reviewerStateFailureCount: reviewerStateFailures.length, independentlyVerifiedCount: accepted, reviewAnnouncements, reviewerFailureAnnouncements, workers: entries, reviews, failures, reviewerFailures, reviewerStateFailures, communications },
      }
    }),
  }),
}
