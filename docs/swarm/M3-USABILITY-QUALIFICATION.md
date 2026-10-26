# Kilo Swarm M3 — Usability Qualification

## Implemented capability

The Kilo-owned swarm runtime now exposes a `swarm` builtin tool. It accepts a bounded list of configured specialist agents, creates a durable swarm and one child Kilo session per specialist, derives each worker's permission ceiling from the existing Kilo delegation rules, admits each worker through the fenced swarm claim/reservation layer, runs workers through the existing Kilo `SessionExecution` service, supervises the lease while execution is active, and returns the latest persisted assistant finding from each worker session.

## Useful-work criteria

A swarm is considered useful only when all of the following are true:

1. Multiple specialists can execute concurrently without exceeding the configured swarm capacity.
2. Every child session is subject to the caller/session/agent permission intersection.
3. Worker execution is durable and fenced; stale workers cannot transition or publish completion for a newer generation.
4. Cancellation can interrupt the active Kilo session and durably cancel the task.
5. Worker lease renewal keeps long-running work admitted and stops when ownership is lost.
6. Worker findings are returned to the caller rather than requiring manual inspection of child sessions.
7. Migration, swarm regression tests, and OpenCode typecheck pass in CI.

## Deliberate limitations

- The current tool is a parallel specialist executor, not a fully autonomous recursive coordinator.
- It does not invent agents or permissions; workers must reference configured Kilo agents.
- It does not automatically merge conflicting worker changes into the repository.
- It does not claim provider-level physical interruption when a process has already lost durable ownership.
- CI must provide the final validation evidence before M3 is marked qualified.
