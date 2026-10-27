import { describe, expect, test } from "bun:test"
import { SwarmVerification } from "@/kilocode/swarm/verification"

describe("SwarmVerification command evidence", () => {
  test("accepts explicit test, lint, typecheck, and build invocations", () => {
    const valid = [
      "bun test test/kilocode/swarm/state.test.ts",
      "bun run test:swarm",
      "npm test",
      "npm run lint",
      "pnpm --filter @scope/package test",
      "cargo test",
      "cargo fmt --check",
      "go test ./...",
      "python -m pytest",
      "pytest -q",
      "tsc --noEmit",
      "eslint src",
      "deno test",
      "Invoke-Pester",
      "CI=1 bun run typecheck",
    ]
    for (const command of valid) {
      expect(SwarmVerification.isValidationCommand(command), command).toBe(true)
    }
  })

  test("rejects arbitrary commands that only mention test/check words", () => {
    const invalid = [
      "git status test",
      'echo "bun test passed"',
      'printf "test passed"',
      'pwsh -Command "Write-Output test"',
      "npm view test",
      "cat test-results.txt",
      "echo check",
      "sleep 1; printf 'validation passed'",
      "true || bun test test/kilocode/swarm/state.test.ts",
      "bun test test/kilocode/swarm/state.test.ts; echo done",
      'echo "&& bun test"',
    ]
    for (const command of invalid) {
      expect(SwarmVerification.isValidationCommand(command), command).toBe(false)
    }
  })

  test("rejects a passing criterion linked to failing evidence", () => {
    const problems = SwarmVerification.qualificationProblems({
      summary: "A pass cannot override evidence that contradicts the criterion.",
      evidenceJson: JSON.stringify([
        { id: "e1", kind: "command", reference: "bun test", detail: "A command passed.", result: "pass", exitCode: 0 },
        { id: "e2", kind: "file", reference: "src/auth.ts", detail: "A required authentication check is missing.", result: "fail" },
      ]),
      checksJson: JSON.stringify([{ criterion: "authentication meets the requirement", verdict: "pass", evidenceIds: ["e1", "e2"] }]),
      blockingFindingsJson: "[]",
      uncertaintiesJson: "[]",
      executionEvidenceJson: JSON.stringify([{
        command: "bun test",
        exitCode: 0,
        sourceSessionId: "ses_reviewer",
        messageId: "msg_reviewer",
        partId: "prt_reviewer",
        callId: "call_reviewer",
        outputSha256: "a".repeat(64),
      }]),
      reviewerSessionId: "ses_reviewer",
    })
    expect(problems.some((problem) => problem.includes("criterion cites failing evidence"))).toBe(true)
  })

  test("rejects reviewer evidence claiming a pass with a nonzero exit code", () => {
    const problems = SwarmVerification.qualificationProblems({
      summary: "A failed command cannot be reported as passing.",
      evidenceJson: JSON.stringify([
        { id: "e1", kind: "command", reference: "bun test", detail: "The command failed.", result: "pass", exitCode: 1 },
      ]),
      checksJson: JSON.stringify([{ criterion: "tests pass", verdict: "pass", evidenceIds: ["e1"] }]),
      blockingFindingsJson: "[]",
      uncertaintiesJson: "[]",
      executionEvidenceJson: JSON.stringify([{
        command: "bun test",
        exitCode: 0,
        sourceSessionId: "ses_reviewer",
        messageId: "msg_reviewer",
        partId: "prt_reviewer",
        callId: "call_reviewer",
        outputSha256: "a".repeat(64),
      }]),
      reviewerSessionId: "ses_reviewer",
    })
    expect(problems.some((problem) => problem.includes("claims pass with nonzero exit code"))).toBe(true)
  })

})
