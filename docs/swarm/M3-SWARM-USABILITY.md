# Kilo Swarm M3 — usability qualification

## Implemented runtime path

The Kilo-owned swarm runtime provides a real worker path rather than only state contracts:

1. resolve a specialized Kilo agent;
2. preflight that it is a valid subagent rather than a primary agent;
3. derive its permission ceiling from caller + parent session + worker policy;
4. reuse Kilo's existing subagent model-resolution policy;
5. create a child Kilo session with that ceiling;
6. create a durable swarm task;
7. admit a worker with a fenced lease and capacity reservation;
8. persist the task/session binding;
9. enqueue the worker prompt through the existing `SessionPrompt.Service`;
10. wait on the existing `SessionDrain.Service` for the actual provider/tool turn to finish;
11. renew the lease while execution is active;
12. interrupt the active Kilo execution on cancellation;
13. reject stale completion using the durable fence;
14. release the claim and capacity reservation after completion/failure/cancellation;
15. transition successful work to `reviewing`.

`runPlannedBatch` provides fail-fast semantics for callers that require all workers, while `runPlannedBatchSettled` preserves successful specialist results when another worker fails. The public `swarm` builtin uses the settled path so useful findings are not discarded by one failed specialist.

## Product integration

`SwarmTool` is now part of `KiloToolRegistry.infos()`, is initialized by `KiloToolRegistry.build()`, and is exposed by `KiloToolRegistry.extra()`. A compatibility import at `packages/opencode/src/kilocode/tool/swarm.ts` re-exports the canonical implementation rather than maintaining a duplicate tool.

## Usefulness validation

The focused runtime test suite includes:

- planned child-session creation through the Kilo Session service boundary;
- durable task admission and completion to `reviewing`;
- four specialized workers executed through a two-worker concurrency bound;
- an execution probe asserting actual overlap (`maxActive > 1`) while never exceeding the configured bound (`maxActive <= 2`);
- queued-task durable cancellation;
- worker binding replacement protection;
- claim and worker-capacity release after successful execution.

These tests establish the core orchestration usefulness properties. They are not a substitute for a real provider-backed workload.

## Security/authority boundaries

- Worker permissions are derived by `DynamicAgentFactory` and never widened by swarm.
- Primary agents cannot be used as swarm workers.
- Board messages remain advisory and cannot mutate authoritative task state.
- Claims, reservations, lifecycle transitions, cancellation and fencing remain authoritative in the swarm state layer.
- Provider/model execution continues through Kilo's existing session/prompt execution path.
- A lease expiration is not treated as proof that an external provider invocation physically stopped.

## Final qualification gate

M3 is **implemented but not yet production-qualified** until:

1. the focused GitHub Actions workflow executes successfully on the current `main` head;
2. migration/schema checks pass;
3. the complete swarm regression suite passes;
4. OpenCode typecheck passes;
5. a real configured provider/model run demonstrates that multiple specialists produce useful, distinct findings which the parent agent can synthesize;
6. real cancellation/recovery behavior is observed against an actual running child session.

## Validation evidence status

The current GitHub status for the latest swarm changes remains `pending` with no reported status checks. Local execution is also unavailable in this environment because direct GitHub network access/DNS is unavailable. The repository therefore intentionally distinguishes implemented coverage from executed CI/provider evidence rather than claiming a successful build without evidence.
