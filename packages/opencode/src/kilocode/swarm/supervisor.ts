// Kilo Swarm M3 — lease supervision primitives.
// The supervisor owns no provider execution; it keeps an admitted lease alive.
import { Effect, Schema } from "effect"
import { SwarmClaims } from "./claims"

export namespace SwarmSupervisor {
  export class InvalidInterval extends Schema.TaggedErrorClass<InvalidInterval>()("SwarmSupervisor.InvalidInterval", {
    message: Schema.String,
  }) {}

  export type Input = {
    taskId: string
    leaseId: string
    fence: number
    leaseMs: number
    intervalMs: number
  }

  export const validate = (input: Input) => {
    if (!Number.isSafeInteger(input.leaseMs) || input.leaseMs <= 0)
      return Effect.fail(new InvalidInterval({ message: "leaseMs must be a positive safe integer" }))
    if (!Number.isSafeInteger(input.intervalMs) || input.intervalMs <= 0 || input.intervalMs >= input.leaseMs)
      return Effect.fail(new InvalidInterval({ message: "intervalMs must be positive and smaller than leaseMs" }))
    return Effect.succeed(input)
  }

  export const renewOnce = Effect.fn("SwarmSupervisor.renewOnce")(function* (input: Input) {
    yield* validate(input)
    return yield* SwarmClaims.renew({ taskId: input.taskId, leaseId: input.leaseId, fence: input.fence, leaseMs: input.leaseMs })
  })

  /** Renewal fails closed: stale ownership, expiry, cancellation, and terminal state stop renewal. */
  export const supervise = <A, E, R>(input: Input, worker: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      yield* validate(input)
      const renewal = Effect.gen(function* () {
        while (true) {
          yield* Effect.sleep(input.intervalMs)
          yield* renewOnce(input)
        }
      })
      // Race the worker against supervision. If the lease can no longer be renewed,
      // the supervisor wins and the worker scope is interrupted by Effect's race.
      return yield* Effect.raceFirst(worker, renewal.pipe(Effect.as(undefined as never)))
    })
}
