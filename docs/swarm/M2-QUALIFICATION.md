# Kilo Swarm M2 / M3 Qualification

Status: **M2 FOUNDATION IMPLEMENTED; M3 EXECUTION BRIDGE UNDER ACTIVE QUALIFICATION**

## M2

M2 provides authoritative SQLite-backed swarm/task state, EventV2 atomic transitions, CAS versions/fences, idempotency, cancellation, durable claims, worker-capacity reservations, lease renewal/release, and deterministic expiry reconciliation. The existing focused workflow is the repeatable M2 qualification path.

## M3 implemented so far

The M3 boundary now consists of:

1. `SwarmDispatcher.admit()` — durable `queued -> reserved -> dispatched` admission with claim/fence/capacity ownership.
2. `SwarmDispatcher.startExecution()` — fenced `dispatched -> running` transition.
3. `SwarmDispatcher.beforeExecution()` — execution-time ownership validation.
4. `SwarmDispatcher.assertCompletionOwnership()` — stale/cancelled workers cannot publish completion state.
5. `SwarmWorker.bindSession()` — durable task-to-existing-Kilo-session binding, including lease and fence generation.
6. `SwarmWorker.resumeSession()` — bridge into Kilo's existing `SessionExecution.Service`; swarm does not implement a second provider/model runner.
7. `SwarmRecovery.reconcile()` — expires claims/reservations and invalidates bindings whose exact lease/fence generation is no longer active.
8. Regression tests for admission, execution, durable binding, cancellation, and binding recovery.

The worker/session binding is persisted in `kilo_swarm_worker_binding` and has a unique session index. The migration is registered as `202610260001_swarm_worker_binding`.

Nested swarm workers are linked through `kilo_swarm_task.parent_id` to the currently executing parent task, resolved only from a session whose binding, task fence, active claim, and active capacity reservation all match. Root sessions create parentless tasks. Reviewer tasks are deliberately created as root audit tasks; their review events qualify the worker they reviewed, so a reviewer does not accidentally become an unfinished deliverable that blocks the parent. The task-created event decoder remains permissive for historical M3 rows that used a session ID in `parent_id`, while new task creation writes only task IDs or `null`.

## Important execution semantics

A lease is an ownership generation, not a physical kill switch. Expiration or cancellation cannot retroactively prove that an already-running provider request stopped. Instead:

- no new execution begins without an active matching claim;
- execution state advances only under the matching fence;
- completion checks the exact lease/fence generation again;
- recovery marks the binding expired when that generation is no longer active;
- stale workers therefore cannot commit newer-generation swarm state.

## Independent peer-review and evidence-backed acceptance

The user-facing swarm tool now runs a distinct reviewer wave after the worker wave. Reviewers use separate durable tasks/sessions, are denied edit permission, and cannot delegate additional tasks. The shared team board remains the communication surface for worker progress, blockers, findings, and handoffs; board messages are advisory and never grant lifecycle authority.

A passing review must contain structured acceptance criteria, evidence references linked to each passing criterion, no blocking findings or unresolved uncertainties, and a successful validation command. A model-authored claim that a command passed is insufficient on its own. Execution evidence carries the reviewer session/message/part/call identity, the actual command, exit code, and SHA-256 output digest. At qualification time, Kilo checks that this proof matches the persisted completed shell-tool part in the bound reviewer session.

\`SwarmState.transition(... to: "accepted")\` enforces the gate both before the transition and again inside EventV2's CAS transaction. It reads the latest review outcome (a later blocked review overrides an earlier pass), validates the reviewer task and current session binding, revalidates the structured evidence, matches the host-recorded tool part and output hash, and refuses acceptance while any direct child task remains outside \`accepted\`/\`integrated\`. Reviewer failures, missing reviewers, invalid proof, stale bindings, and failed acceptance transitions remain visible as unaccepted results rather than being silently reported as successful.

Regression coverage has been added for missing reviews, invalid evidence, a directly published forged pass, reviewer/session mismatch, state changes in commit hooks, a later blocked review overriding a pass, failed commands, output-hash mismatch, and unfinished child tasks. These test cases must still run successfully in CI before this gate is considered qualified. A committed test is not itself a passing test result.

## Remaining qualification gates

M3 is **not qualified** until all of the following are proven by tests/CI:

- real worker/session creation is wired to the existing Kilo session factory with the intended permission ceiling;
- task payload/correlation semantics are persisted rather than supplied as an external assumption;
- long-running execution has supervised lease renewal;
- cancellation invokes the existing session interruption mechanism where the active process owns that execution;
- provider result/error persistence is fenced at its authoritative write boundary;
- restart recovery can discover eligible bindings and safely resume or quarantine them without duplicate provider execution;
- duplicate dispatch, cancellation races, stale fences, capacity exhaustion, lease expiry, and cross-process cases pass deterministically;
- migration/schema generation checks pass;
- the complete focused test suite passes in CI.

Until then the system is deliberately described as **M3 under qualification**, not production-complete.
