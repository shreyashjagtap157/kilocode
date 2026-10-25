# Kilo Swarm M2 Qualification

Status: **FOUNDATION IMPLEMENTED; M3 DISPATCH REMAINS NO-GO**

## Scope

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

## Current main-line evidence

The M2 implementation is on `main`. Recent hardening commits include:

- `934eb628e1397f0ce4864fa7c3d470f1a0dfcc02` — atomic/idempotent cancellation;
- `9d91a38d730f71b94ee6bc86563817bea8e7a0e4` — no forward lifecycle transitions after cancellation;
- `48dea54eebcecb92fb1344d999ff17d70719c17a` — conflicting idempotency requests rejected;
- `023eb817b00c3180a07d840d3d27b543f3cc434f` — malformed idempotency records fail closed;
- `dff17dbb3ccda413b5642ad91dae8119a445deea` — stored task idempotency results validated;
- `cf59d3aaa2fb6019fef10a36c8729df375f63ce0` — stored cancellation replay results validated;
- `761725f399b8658b8c88cf708cc517c22f396196` — first-result idempotency semantics preserved;
- `2f868513a67c84faf393b80a72443a782e814e3d` — lease renewal blocked after cancellation/terminal state;
- `8eac80508c66c68d168dfb0a3f985a418ce20048` — manual M2 qualification workflow dispatch enabled.

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

## Safety boundary

M2 does **not** provide:

- background worker dispatch;
- automatic task polling/resumption;
- reclaim of an actually running provider/model invocation;
- physical interruption of a provider/model call;
- autonomous execution outside the existing Kilo permission ceiling;
- budget enforcement/debit/reconciliation;
- artifact persistence/review/integration;
- approval orchestration;
- swarm UI/runtime wiring.

A lease expiry is a durable admission fact, not proof that a previously launched model invocation has physically stopped.

## M3 entry gate

M3 should not be enabled merely because M2 state tables exist. Before implementing or enabling dispatch, qualification must establish:

1. a worker-session creation path that uses the existing Kilo session/provider machinery rather than duplicating it;
2. an atomic dispatch/admission transaction that binds task state, claim fence, worker identity, and correlation ID;
3. a durable worker lifecycle mapping for `reserved -> dispatched -> running` and terminal/error paths;
4. cancellation behavior that prevents new admission and marks durable state, while explicitly distinguishing that from physical provider interruption;
5. lease-loss behavior that prevents stale workers from committing task results;
6. permission derivation through `DynamicAgentFactory` and the existing Kilo task permission machinery;
7. deterministic single-process and cross-process tests for duplicate dispatch, stale fences, cancellation races, capacity exhaustion, and restart recovery;
8. CI evidence for the complete focused suite before M3 is declared qualified.

Until those conditions are implemented and tested, M3 remains **NO-GO**.
