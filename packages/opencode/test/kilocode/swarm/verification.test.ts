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
})
