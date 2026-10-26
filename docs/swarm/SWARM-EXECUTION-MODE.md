# Swarm execution mode

Kilo's swarm capability is a native primary agent mode as well as a callable orchestration tool. It is registered by Kilo's agent initialization, so it is available in the installed CLI regardless of the current project directory.

## What the mode does

When `Swarm` is selected as the active primary mode, the agent is instructed to:

1. decompose a substantive request into distinct specialist workstreams;
2. select appropriate configured specialist agents;
3. execute independent work concurrently through the durable `swarm` tool;
4. coordinate through Kilo's persistent shared agent board when it is enabled;
5. sequence dependent work rather than creating conflicting concurrent edits;
6. preserve successful findings when individual workers fail;
7. use independent verification/testing for implementation tasks;
8. synthesize worker findings and peer communications into one result.

The runtime continues to enforce the swarm's worker limit, permission ceiling, durable task admission, fencing, leases, cancellation, and bounded concurrency.

## Installation and legacy compatibility

The native mode is registered in `packages/opencode/src/kilocode/agent/index.ts`. It inherits the normal code-agent permission rules and adds explicit permission for the `swarm` tool and, when the shared board is enabled, `board_read` and `board_post`. Explicit user permission rules are applied afterward, so the user can still deny these tools.

The repository retains a `.kilocodemodes` definition as a compatibility/example entry. The `ModesMigrator` reserves the `swarm` slug and skips that legacy custom-mode entry, preventing it from shadowing or duplicating the native mode. The mode-install test verifies both native registration and the legacy migration behavior.

## Selection semantics

Swarm is an **execution strategy**. It does not replace model selection or sessions:

- model selection still chooses the model/provider used by the parent and worker sessions;
- session selection still determines the conversation/workspace context;
- Swarm determines how substantive work is decomposed and executed across specialist child sessions.

This keeps swarm orthogonal to models and sessions, so changing the model does not disable swarm orchestration and changing sessions does not destroy swarm task durability.

## Validation

`packages/opencode/test/kilocode/swarm/mode-install.test.ts` exercises the actual mode-registration function and the real `ModesMigrator` against a temporary project. It checks that Swarm is a visible native primary agent, that its team/board instructions are present, and that the required orchestration/board permissions are granted.

This is a source-level regression test, not proof that a production installer has been built or installed. The broader swarm qualification suite and the installed-CLI/provider benchmark remain necessary to validate durable execution, bounded concurrency, cancellation, fencing, peer communication, real provider execution, and usefulness.
