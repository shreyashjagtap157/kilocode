import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { SwarmRuntime } from "@/kilocode/swarm/runtime"

describe("SwarmRuntime", () => {
  test("rejects malformed task ids before touching execution", async () => {
    const result = await Effect.runPromiseExit(
      SwarmRuntime.runSession({
        taskId: "bad",
        ownerId: "wrk_test",
        sessionId: "ses_test",
        actor: "act_test",
        corrId: "cor_test",
      }),
    )
    expect(result._tag).toBe("Failure")
  })
})
