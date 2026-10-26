import { describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SwarmCancellation } from "@/kilocode/swarm/cancellation"
import { SwarmDispatcher } from "@/kilocode/swarm/dispatcher"
import { SwarmState } from "@/kilocode/swarm/state"
import { SwarmWorker } from "@/kilocode/swarm/worker"
import { tmpdir } from "../../fixture/fixture"

const use = (effect: any, file: string, execution: SessionExecution.Interface) => {
  const db = Database.layerFromPath(file)
  const events = EventV2.layerWith().pipe(Layer.provide(db))
  const exec = Layer.succeed(SessionExecution.Service, execution)
  return Effect.runPromise(Effect.provide(Layer.mergeAll(db, events, exec))(Effect.scoped(effect)))
}

const dbFile = (tmp: any) => (tmp as any).path + "/swarm.db"

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

  test("interrupts the active bound session and releases its exact claim", async () => {
    await using tmp = await tmpdir()
    const interrupted: string[] = []
    await use(
      Effect.gen(function* () {
        const swarmId = yield* SwarmState.createSwarm({ maxWorkers: 1 })
        const taskId = yield* SwarmState.createTask({ swarmId, actor: "act_cancel", corrId: "cor_cancel" as any })
        const admission = yield* SwarmDispatcher.admit({ taskId, ownerId: "wrk_cancel", leaseMs: 30_000, actor: "act_cancel", corrId: "cor_cancel" as any })
        yield* SwarmWorker.bindSession({ taskId, sessionId: "ses_cancel", leaseId: admission.leaseId, fence: admission.fence })
        const result = yield* SwarmCancellation.request({ taskId, actor: "act_cancel", corrId: "cor_cancel_request" as any })
        expect(result.cancelled).toBe(true)
        expect(result.interrupted).toBe(true)
        expect(result.sessionId).toBe("ses_cancel")
        expect(interrupted).toEqual(["ses_cancel"])
        const task = yield* SwarmState.getTask(taskId)
        expect(task.state).toBe("cancelled")
      }),
      dbFile(tmp),
      {
        active: Effect.succeed(new Set()),
        resume: () => Effect.void,
        wake: () => Effect.void,
        interrupt: (sessionId) => Effect.sync(() => { interrupted.push(String(sessionId)) }),
      },
    )
  })
})
