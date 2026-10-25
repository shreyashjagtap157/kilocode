# Kilo Swarm M3 — usability qualification

## Implemented runtime path

The Kilo-owned swarm runtime now provides a real worker path rather than only state contracts:

1. resolve a specialized Kilo agent;
2. derive its permission ceiling from caller + parent session + worker policy;
3. create a child Kilo session with that ceiling;
4. create a durable swarm task;
5. admit a worker with a fenced lease and capacity reservation;
6. persist the task/session binding;
7. enqueue the worker prompt through the existing `SessionPrompt.Service`;
8. run the existing `SessionExecution.Service` under lease supervision;
9. renew the lease while execution is active;
10. interrupt the active Kilo execution on cancellation;
11. reject stale completion using the durable fence;
12. transition successful work to `reviewing`.

`runPlannedBatch` adds bounded parallel execution for multiple specialized workers. It does not create a second model/provider runtime.

## Usefulness validation

The focused runtime test suite now includes:

- real planned worker creation through a fake Kilo Session service;
- durable task admission and completion to `reviewing`;
- four specialized workers executed through a two-worker concurrency bound;
- an execution probe asserting actual overlap (`maxActive > 1`) while never exceeding the configured bound (`maxActive <= 2`).

This establishes the core usefulness property: independent specialist work can execute concurrently while the swarm's durable capacity ceiling remains authoritative.

## Security/authority boundaries

- Worker permissions are derived by `DynamicAgentFactory` and never widened by swarm.
- Board messages remain advisory and cannot mutate authoritative task state.
- Claims, reservations, lifecycle transitions, cancellation and fencing remain authoritative in the swarm state layer.
- Provider/model execution continues through Kilo's existing session execution path.
- A lease expiration is not treated as proof that an external provider invocation physically stopped.

## Remaining product integration gate

The runtime/tool implementation is intentionally separated from the normal Task tool. The final product gate is wiring the new `SwarmTool` into the builtin tool registry and adding the corresponding registry dependency graph entry. Until that integration is committed and the repository's actual CI/test run reports success, M3 must not be described as fully production-qualified.

## Validation evidence status

The current GitHub commit status for the latest swarm changes is still `pending` with no reported status checks. Local execution is also unavailable in this environment because direct GitHub network access/DNS is unavailable. Therefore this document distinguishes implemented test coverage from executed CI evidence rather than claiming a successful build without evidence.
