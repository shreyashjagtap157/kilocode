import { Cause, Effect, Schema } from "effect"
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
          prompt: [`You are the independent peer reviewer for worker ${entry.role}.`, `Swarm objective: ${args.objective}`, `Worker task: ${args.workers[entry.index]?.prompt ?? "unknown"}`, "", "WORKER FINDING TO VERIFY:", entry.finding, "", "REVIEW RULES:", "Do not edit or modify the worker's implementation. Inspect repository state, tests, commands, logs, and artifacts as needed.", "Challenge the finding rather than agreeing by default. Look for missing requirements, unsupported claims, regressions, and contradictions with peer evidence.", "Run an appropriate test or validation command yourself when possible. A PASS requires at least one passing test/command with exitCode 0 and evidence linked to every passed criterion.", "Return ONLY JSON matching this exact shape: {\"verdict\":\"pass|fail|inconclusive\",\"summary\":\"...\",\"checks\":[{\"criterion\":\"...\",\"verdict\":\"pass|fail|inconclusive\",\"evidenceIds\":[\"e1\"]}],\"evidence\":[{\"id\":\"e1\",\"kind\":\"test|command|file|diff|log|artifact\",\"reference\":\"...\",\"detail\":\"...\",\"result\":\"pass|fail|observed\",\"exitCode\":0}],\"blockingFindings\":[],\"uncertainties\":[]}", `You are reviewer ${reviewIndex + 1}; do not certify your own worker session.`].join("\n"),
          caller, subagent: resolved[reviewerIndex], actor: ctx.agent, corrId: `${base}_review_${reviewIndex}` as any, readOnly: true,
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
        const report = parseReview(finding(reviewMessages[reportIndex] ?? []))
        const recorded = yield* SwarmVerification.recordReview({
          taskId: target.taskId,
          reviewerTaskId: reviewEntry.result.taskId,
          reviewerSessionId: reviewEntry.result.sessionId,
          reviewerAgent: reviewEntry.result.agent,
          corrId: `${base}_review_commit_${reviewEntry.index}`,
          report,
        }).pipe(Effect.catchCause((cause) => Effect.succeed({ verdict: "inconclusive" as const, accepted: false, blockingFindings: [Cause.pretty(cause)], evidence: [], checks: [] })))

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
          ...recorded,
          accepted: recorded.accepted && !acceptanceFailure,
          blockingFindings: [...recorded.blockingFindings, ...(acceptanceFailure ? [acceptanceFailure] : [])],
        }
      }), { concurrency: "unbounded" })
      const board = yield* BoardStore.read({ sessionID: parent.id as SessionID, limit: 100 })
      const communications = board.messages.filter((message) => message.from !== "main").map((message) => ({ from: message.fromLabel ?? message.from, to: message.toLabel ?? message.to, type: message.type, body: message.body, id: message.id }))
      const failures = batch.failed.map((entry) => ({ role: args.workers[entry.index]?.role ?? "worker", error: errorText(entry.error) }))
      const reviewerFailures = reviewBatch.failed.map((entry) => ({
        role: entries[entry.index]?.role ?? "reviewer",
        targetRole: entries[entry.index]?.role ?? "unknown worker",
        error: errorText(entry.error),
      }))
      const accepted = reviews.filter((review) => review.accepted).length
      return {
        title: `Swarm completed ${entries.length}/${args.workers.length} workers; ${accepted}/${entries.length} independently verified`,
        output: [
          `Swarm objective: ${args.objective}`, `Workers completed: ${entries.length}/${args.workers.length}`, `Independent peer reviews: ${reviews.length}/${entries.length}`, `Reviewer failures: ${reviewerFailures.length}`, ...(entries.length < 2 ? ["Independent review unavailable: fewer than two workers completed successfully."] : []), `Workers accepted after evidence gate: ${accepted}/${entries.length}`, `Team communications: ${communications.length}`,
          ...entries.flatMap((entry, index) => { const review = reviews.find((item) => item.workerTaskId === entry.taskId); return [`\n## ${index + 1}. ${entry.role}`, entry.finding, `Review: ${review?.verdict ?? "missing"}${review?.accepted ? " (ACCEPTED)" : " (NOT ACCEPTED)"}`, ...(review?.blockingFindings?.length ? [`Blocking findings: ${review.blockingFindings.join("; ")}`] : []), `(task=${entry.taskId}, session=${entry.sessionId}, duration=${entry.durationMs}ms)`] }),
          ...(communications.length > 0 ? ["\n## Team communication", ...communications.map((message) => `- ${message.from} → ${message.to} [${message.type}]: ${message.body}`)] : []),
          ...failures.flatMap((entry) => [`\n## FAILED: ${entry.role}`, entry.error]),
          ...reviewerFailures.flatMap((entry) => [`\n## REVIEW FAILED: ${entry.role} (target: ${entry.targetRole})`, entry.error]),
        ].join("\n"),
        metadata: { swarmId, workerCount: entries.length, failedWorkerCount: failures.length, reviewerFailureCount: reviewerFailures.length, independentlyVerifiedCount: accepted, workers: entries, reviews, failures, reviewerFailures, communications },
      }
    }),
  }),
}
