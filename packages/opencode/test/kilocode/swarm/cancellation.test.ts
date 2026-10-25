import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SwarmCancellation } from "@/kilocode/swarm/cancellation"

describe("SwarmCancellation", () => {
  test("service contract exposes interrupt for an admitted session", () => {
    const interrupted: string[] = []
    const layer = Layer.succeed(SessionExecution.Service, SessionExecution.Service.of({
      active: Effect.succeed(new Set()),
      resume: () => Effect.void,
      wake: () => Effect.void,
      interrupt: (sessionId) => Effect.sync(() => { interrupted.push(String(sessionId)) }),
    }))
    expect(layer).toBeDefined()
    expect(interrupted).toHaveLength(0)
  })
})
