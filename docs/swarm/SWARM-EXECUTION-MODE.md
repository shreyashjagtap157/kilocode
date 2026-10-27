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
8. run a distinct reviewer wave with separate durable reviewer tasks/sessions;
9. synthesize worker findings and peer communications into one result.

The runtime continues to enforce the swarm's worker limit, permission ceiling, durable task admission, fencing, leases, cancellation, and bounded concurrency. Direct use of the shared board is guarded by `BoardEnabled` and requests both `board_read` and `board_post` permissions first. If the board is explicitly disabled, workers still run, but swarm will not seed, read, or post shared-board messages.

## Review and acceptance semantics

A reviewer runs in a different Kilo task/session from the worker being reviewed. Reviewer sessions have an explicit edit-deny permission and cannot delegate through the task/todo tools. The reviewer identity is resolved from the configured specialist team, but the current design does not guarantee a different model or provider; independence here means separate task/session and execution provenance, not guaranteed model diversity.

The reviewer must return structured criteria and link evidence to each passing criterion. A successful command claimed in the report is not trusted by itself: the evidence reference must exactly match the command in the completed shell-tool part persisted by Kilo in that exact reviewer session, including the exit code, part/message/call identity, and output hash. The state transition to `accepted` rechecks the latest review result and this host-recorded evidence inside the durable CAS transaction. A later blocked review overrides an earlier pass, unfinished direct child tasks block acceptance, and failed/missing reviews remain unaccepted. Reviewer tasks close as terminal `reviewed` only after a durable review event is recorded; a reviewer task's `reviewed` state is not implementation approval. If fewer than two workers complete, the surviving worker receives an explicit HOLD notice because independent review could not be performed.

When fewer than two workers complete, Swarm does not let the sole survivor review itself; the result remains unaccepted and the tool reports that independent review was unavailable. These source-level safeguards are implemented, but they must not be described as fully qualified until the focused swarm tests and typecheck run successfully in CI and real provider execution validates the stored transcript shape.

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
