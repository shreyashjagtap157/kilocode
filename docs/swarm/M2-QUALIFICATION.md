# Kilo Swarm M2 / M3 Qualification

Status: **M2 FOUNDATION IMPLEMENTED; M3 ADMISSION IMPLEMENTED, EXECUTION INTEGRATION NOT YET QUALIFIED**

## M2 scope

M2 is the durable admission/state foundation for the Kilo-owned swarm runtime. It provides:

- authoritative swarm/task state backed by SQLite;
- EventV2-backed atomic lifecycle transitions;
- optimistic version and fence checks;
- durable task idempotency records;
- cancellation markers and cancellation idempotency;
- durable task claims with fencing leases;
- per-swarm worker-capacity reservations;
- atomic claim + reservation admission;
- atomic lease renewal + reservation renewal;
- exact-fence release + reservation release;
- rejection of claims after cancellation or terminal state;
- rejection of lease renewal after cancellation or terminal state;
- recovery/re-admission semantics for expired reservations;
- concurrency, rollback, migration, and cross-process regression coverage;
- focused CI qualification through `.github/workflows/m2-swarm-foundation.yml`.

## M3 work now implemented

`packages/opencode/src/kilocode/swarm/dispatcher.ts` adds the durable dispatch boundary:

1. require the task to be `queued`;
2. acquire a durable claim and worker-capacity reservation;
3. advance `queued -> reserved` with the existing CAS/event machinery;
4. advance `reserved -> dispatched` with the same machinery;
5. release the claim if the admission sequence fails before provider execution;
6. revalidate the fence immediately before execution;
7. advance `dispatched -> running` only after ownership validation;
8. require the same lease/fence to be valid before a worker may commit completion state.

The implementation intentionally does not pretend that a lease expiry can physically interrupt a provider request already in flight. Stale completion must instead fail the durable ownership gate.

## Lease-token hardening

The claim API previously generated a second token that was returned to callers but never persisted. That created an unusable credential. The API now treats the persisted `lease_id` as the durable per-admission token (`token === leaseId`), while the monotonically increasing task `fence` remains the stale-worker generation guard.

## Current tests added

`packages/opencode/test/kilocode/swarm/dispatcher.test.ts` covers:

- queued -> reserved -> dispatched admission;
- durable fence/lease identity;
- dispatched -> running only with valid ownership;
- cancellation preventing the execution gate.

The existing swarm suite remains the broader qualification suite.

## Required qualification command set

The focused workflow is the authoritative repeatable qualification path:

```text
packages/core:
  bun script/migration.ts --check
  bun script/migration.ts
  bun test test/event.test.ts test/database-migration.test.ts --timeout 60000

packages/opencode:
  bun test test/kilocode/swarm --timeout 60000

repository root:
  git diff --exit-code -- packages/core/src/database/schema.gen.ts
```

The workflow also verifies that the generated schema is unchanged after generation. It runs on relevant swarm/database changes and can be started manually.

## Remaining M3 qualification gates

M3 is **not** declared qualified yet. The remaining gates are:

1. bind an admitted task to the existing Kilo session/provider execution path;
2. persist the worker/session correlation needed for restart/recovery;
3. provide lease renewal supervision for long-running provider calls;
4. prevent stale workers from writing terminal results after losing their fence;
5. define durable provider-error/cancellation result semantics;
6. enforce `DynamicAgentFactory` permission ceilings at worker creation;
7. add duplicate-dispatch, stale-fence, cancellation-race, capacity-exhaustion, and restart tests around the real execution path;
8. run and record CI evidence before declaring M3 qualified.

## Safety boundary

The swarm runtime must never treat advisory messages, provider output, or an expired worker's local state as authority. Durable state, EventV2 commit semantics, task version, cancellation marker, lease identity, and fence remain authoritative.
