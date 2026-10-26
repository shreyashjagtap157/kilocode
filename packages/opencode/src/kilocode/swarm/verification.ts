import { Effect, Schema } from "effect"
import { sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SwarmTaskReviewBlocked, SwarmTaskReviewPassed } from "./events"

export const ReviewEvidence = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["test", "command", "file", "diff", "log", "artifact"]),
  reference: Schema.String,
  detail: Schema.String,
  result: Schema.Literals(["pass", "fail", "observed"]),
  exitCode: Schema.optional(Schema.Int),
})

export const ReviewCheck = Schema.Struct({
  criterion: Schema.String,
  verdict: Schema.Literals(["pass", "fail", "inconclusive"]),
  evidenceIds: Schema.Array(Schema.String),
})

export const ReviewReportSchema = Schema.Struct({
  verdict: Schema.Literals(["pass", "fail", "inconclusive"]),
  summary: Schema.String,
  checks: Schema.Array(ReviewCheck),
  evidence: Schema.Array(ReviewEvidence),
  blockingFindings: Schema.Array(Schema.String),
  uncertainties: Schema.Array(Schema.String),
})

export type ReviewReport = Schema.Schema.Type<typeof ReviewReportSchema>

/** Execution evidence extracted from Kilo's persisted tool transcript, not asserted by the reviewer model. */
export const VerifiedExecutionSchema = Schema.Struct({
  command: Schema.String,
  exitCode: Schema.NullOr(Schema.Int),
  sourceSessionId: Schema.String,
  messageId: Schema.String,
  partId: Schema.String,
  callId: Schema.String,
  outputSha256: Schema.String,
})
export type VerifiedExecution = Schema.Schema.Type<typeof VerifiedExecutionSchema>

type TaskRow = { id: string; swarm_id: string; state: string }

export namespace SwarmVerification {
  export class InvalidReview extends Schema.TaggedErrorClass<InvalidReview>()("SwarmVerification.InvalidReview", { message: Schema.String }) {}
  export class AcceptanceBlocked extends Schema.TaggedErrorClass<AcceptanceBlocked>()("SwarmVerification.AcceptanceBlocked", { message: Schema.String }) {}

  const decode = (value: unknown) => {
    try {
      return { report: Schema.decodeUnknownSync(ReviewReportSchema)(value), malformed: false }
    } catch (error) {
      return {
        malformed: true,
        report: {
          verdict: "inconclusive" as const,
          summary: "Reviewer output failed the structured evidence contract.",
          checks: [], evidence: [],
          blockingFindings: [error instanceof Error ? error.message : String(error)],
          uncertainties: ["Review output could not be parsed."],
        },
      }
    }
  }

  const normalizeCommand = (command: string) => command.trim().replace(/\\s+/g, " ")

  const problems = (report: ReviewReport, malformed: boolean, executions: readonly VerifiedExecution[], reviewerSessionId: string) => {
    const result: string[] = []
    if (malformed) result.push("review output failed schema validation")
    if (report.verdict !== "pass") result.push("review verdict is not pass")
    if (!report.summary.trim()) result.push("review summary is empty")
    if (report.checks.length === 0) result.push("no acceptance criteria were checked")
    if (report.blockingFindings.length) result.push("review contains blocking findings")
    if (report.uncertainties.length) result.push("review contains unresolved uncertainties")
    const evidence = new Map(report.evidence.map((item) => [item.id, item]))
    if (evidence.size !== report.evidence.length) result.push("duplicate evidence id")
    for (const item of report.evidence) {
      if (!item.id.trim()) result.push("evidence id is empty")
      if (!item.reference.trim()) result.push(`evidence ${item.id} has no reference`)
      if (!item.detail.trim()) result.push(`evidence ${item.id} has no details`)
    }
    const referencedEvidence = new Set<string>()
    for (const check of report.checks) {
      if (!check.criterion.trim()) result.push("acceptance criterion is empty")
      if (check.verdict !== "pass") result.push(`criterion did not pass: ${check.criterion}`)
      if (!check.evidenceIds.length) result.push(`criterion has no evidence: ${check.criterion}`)
      let hasPassingEvidence = false
      for (const id of check.evidenceIds) {
        const item = evidence.get(id)
        if (!item) {
          result.push(`missing evidence: ${id}`)
          continue
        }
        referencedEvidence.add(id)
        if (item.result === "pass") hasPassingEvidence = true
      }
      if (check.verdict === "pass" && check.evidenceIds.length > 0 && !hasPassingEvidence) {
        result.push(`criterion has no passing linked evidence: ${check.criterion}`)
      }
    }
    for (const item of report.evidence) {
      if (item.id.trim() && !referencedEvidence.has(item.id)) result.push(`evidence is not linked to an acceptance criterion: ${item.id}`)
    }
    // The reviewer may describe evidence, but it cannot manufacture a successful
    // process exit. Require an exact normalized command match to a completed Kilo
    // shell invocation persisted in this reviewer's own session transcript.
    const passingExecution = report.evidence.some((item) =>
      (item.kind === "test" || item.kind === "command") &&
      item.result === "pass" &&
      item.exitCode === 0 &&
      item.reference.trim() &&
      item.detail.trim() &&
      executions.some((execution) =>
        execution.sourceSessionId === reviewerSessionId &&
        execution.exitCode === 0 &&
        normalizeCommand(execution.command) === normalizeCommand(item.reference)))
    if (!passingExecution) result.push("no passing test/validation command matched to host-recorded reviewer execution evidence")
    return [...new Set(result)]
  }

  export const recordReview = Effect.fn("SwarmVerification.recordReview")(function* (input: {
    taskId: string
    reviewerTaskId: string
    reviewerSessionId: string
    reviewerAgent: string
    corrId: string
    report: unknown
    executedCommands: readonly VerifiedExecution[]
  }) {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const target = yield* db.get<TaskRow>(sql`SELECT id, swarm_id, state FROM kilo_swarm_task WHERE id = ${input.taskId}`).pipe(Effect.orDie)
    const reviewer = yield* db.get<TaskRow>(sql`SELECT id, swarm_id, state FROM kilo_swarm_task WHERE id = ${input.reviewerTaskId}`).pipe(Effect.orDie)
    if (!target || !reviewer) return yield* Effect.fail(new InvalidReview({ message: "review target/reviewer task does not exist" }))
    if (target.id === reviewer.id) return yield* Effect.fail(new InvalidReview({ message: "a task cannot review itself" }))
    if (target.swarm_id !== reviewer.swarm_id) return yield* Effect.fail(new InvalidReview({ message: "reviewer must belong to the same swarm" }))
    if (target.state !== "reviewing" || reviewer.state !== "reviewing") return yield* Effect.fail(new InvalidReview({ message: "target and reviewer must both be in reviewing state" }))
    const reviewerBinding = yield* db
      .get<{ session_id: string; swarm_id: string }>(sql`SELECT session_id, swarm_id FROM kilo_swarm_worker_binding WHERE task_id = ${input.reviewerTaskId}`)
      .pipe(Effect.orDie)
    if (!reviewerBinding || reviewerBinding.session_id !== input.reviewerSessionId || reviewerBinding.swarm_id !== reviewer.swarm_id) {
      return yield* Effect.fail(new InvalidReview({ message: "reviewer session is not the durable session bound to the reviewer task" }))
    }

    const executions = Schema.decodeUnknownSync(Schema.Array(VerifiedExecutionSchema))(input.executedCommands)
    const invalidExecution = executions.find((execution) =>
      !execution.command.trim() ||
      execution.sourceSessionId !== input.reviewerSessionId ||
      !execution.messageId.trim() ||
      !execution.partId.trim() ||
      !execution.callId.trim() ||
      !/^[0-9a-f]{64}$/.test(execution.outputSha256))
    if (invalidExecution) return yield* Effect.fail(new InvalidReview({ message: "host execution evidence is malformed or belongs to another session" }))

    const decoded = decode(input.report)
    const failures = problems(decoded.report, decoded.malformed, executions, input.reviewerSessionId)
    const verdict = decoded.report.verdict === "pass" && failures.length ? "inconclusive" : decoded.report.verdict
    const blocking = [...new Set([...decoded.report.blockingFindings, ...failures])]
    const payload = {
      taskId: input.taskId as any, swarmId: target.swarm_id, reviewerTaskId: input.reviewerTaskId as any,
      reviewerSessionId: input.reviewerSessionId, reviewerAgent: input.reviewerAgent, verdict,
      summary: decoded.report.summary, evidenceJson: JSON.stringify(decoded.report.evidence),
      executionEvidenceJson: JSON.stringify(executions),
      checksJson: JSON.stringify(decoded.report.checks), blockingFindingsJson: JSON.stringify(blocking),
      uncertaintiesJson: JSON.stringify(decoded.report.uncertainties), corrId: input.corrId as any,
    }
    // The preflight read above provides useful error messages; revalidate both task
    // identities and states inside EventV2's transaction so a concurrent state change
    // cannot append a passing review for a task/reviewer that is no longer eligible.
    const commit = () =>
      Effect.gen(function* () {
        const currentTarget = yield* db
          .get<TaskRow>(sql`SELECT id, swarm_id, state FROM kilo_swarm_task WHERE id = ${input.taskId}`)
          .pipe(Effect.orDie)
        const currentReviewer = yield* db
          .get<TaskRow>(sql`SELECT id, swarm_id, state FROM kilo_swarm_task WHERE id = ${input.reviewerTaskId}`)
          .pipe(Effect.orDie)
        const currentReviewerBinding = yield* db
          .get<{ session_id: string; swarm_id: string }>(sql`SELECT session_id, swarm_id FROM kilo_swarm_worker_binding WHERE task_id = ${input.reviewerTaskId}`)
          .pipe(Effect.orDie)
        if (
          !currentTarget ||
          !currentReviewer ||
          !currentReviewerBinding ||
          currentReviewerBinding.session_id !== input.reviewerSessionId ||
          currentReviewerBinding.swarm_id !== target.swarm_id ||
          currentTarget.id === currentReviewer.id ||
          currentTarget.swarm_id !== target.swarm_id ||
          currentReviewer.swarm_id !== target.swarm_id ||
          currentTarget.state !== "reviewing" ||
          currentReviewer.state !== "reviewing"
        ) {
          return yield* Effect.die(new InvalidReview({ message: "review target or reviewer changed before the review event committed" }))
        }
      })
    if (verdict === "pass") {
      yield* events.publish(SwarmTaskReviewPassed, { ...payload, verdict: "pass" }, { commit })
    } else {
      yield* events.publish(
        SwarmTaskReviewBlocked,
        { ...payload, verdict: verdict === "fail" ? "fail" : "inconclusive" },
        { commit },
      )
    }
    return { verdict, accepted: verdict === "pass", blockingFindings: blocking, evidence: decoded.report.evidence, checks: decoded.report.checks }
  })

  export const requireAcceptance = Effect.fn("SwarmVerification.requireAcceptance")(function* (taskId: string) {
    const { db } = yield* Database.Service
    const task = yield* db.get<{ id: string; state: string }>(sql`SELECT id, state FROM kilo_swarm_task WHERE id = ${taskId}`).pipe(Effect.orDie)
    if (!task) return yield* Effect.fail(new AcceptanceBlocked({ message: `task does not exist: ${taskId}` }))
    const review = yield* db.get<{ count: number }>(sql`SELECT COUNT(*) AS count FROM event WHERE aggregate_id = ${taskId} AND type LIKE 'swarm.task.review_passed%'`).pipe(Effect.orDie)
    if (!review || review.count < 1) return yield* Effect.fail(new AcceptanceBlocked({ message: `task ${taskId} has no durable passing independent review` }))
    return { taskId, reviewCount: review.count }
  })
}
