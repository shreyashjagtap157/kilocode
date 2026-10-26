# Kilo Swarm M3 — Usability Qualification

## Implemented capability

The Kilo-owned swarm runtime exposes a `swarm` builtin tool. It accepts a bounded list of configured specialist agents, preflights their subagent eligibility and permission plans, creates a durable swarm and one child Kilo session per specialist, reuses Kilo's existing subagent model-resolution policy, derives each worker's permission ceiling from the existing Kilo delegation rules, admits each worker through the fenced swarm claim/reservation layer, queues the real child prompt through `SessionPrompt`, waits on Kilo's `SessionDrain` for the actual provider/tool execution to finish, supervises the lease while execution is active, and returns the latest persisted assistant finding from each worker session.

Batch execution now uses settled worker outcomes: a failed specialist is reported without discarding successful findings from other specialists, while each failed worker's task is still cancelled by its own runtime failure boundary.

The runtime also exposes a cancellation boundary that durably records cancellation before interrupting a bound Kilo child session, preventing the cancelled worker from renewing or committing completion under its old fence.

## Useful-work criteria

A swarm is considered useful only when all of the following are true:

1. Multiple specialists can execute concurrently without exceeding the configured swarm capacity.
2. Every child session is subject to the caller/session/agent permission intersection.
3. Primary agents are rejected as swarm workers, matching ordinary Kilo task delegation.
4. Worker model selection follows Kilo's existing configured/default/provider resolution rather than a second model-selection implementation.
5. Worker execution is durable and fenced; stale workers cannot transition or publish completion for a newer generation.
6. Cancellation can interrupt the active Kilo session and durably cancel the task.
7. Worker lease renewal keeps long-running work admitted and stops when ownership is lost.
8. Worker findings are returned to the caller rather than requiring manual inspection of child sessions.
9. Partial failures do not discard successful specialist findings.
10. Migration, swarm regression tests, and OpenCode typecheck pass in CI.
11. A real configured provider/model run demonstrates that independent specialists produce usable findings that the parent agent can synthesize.

## Deliberate limitations

- The current tool is a parallel specialist executor, not a fully autonomous recursive coordinator.
- It does not invent agents or permissions; workers must reference configured Kilo agents.
- It does not automatically merge conflicting worker changes into the repository.
- It does not claim provider-level physical interruption when a process has already lost durable ownership.
- CI and a real provider-backed workload must provide the final validation evidence before M3 is marked qualified.
