# Swarm execution mode

Kilo's swarm capability is exposed as an agentic execution mode, not only as a callable implementation primitive.

## What the mode does

When `Swarm` is selected as the active primary mode, the agent is instructed to:

1. decompose a substantive request into distinct specialist workstreams;
2. select appropriate configured specialist agents;
3. execute independent work concurrently through the durable `swarm` tool;
4. sequence dependent work rather than creating conflicting concurrent edits;
5. preserve successful findings when individual workers fail;
6. use independent verification/testing for implementation tasks;
7. synthesize worker findings into one result.

The runtime continues to enforce the swarm's worker limit, permission ceiling, durable task admission, fencing, leases, cancellation, and bounded concurrency.

## Installation

The repository contains `.kilocodemodes` with a `swarm` mode definition. Kilo's existing `ModesMigrator` reads project `.kilocodemodes` and converts custom modes into primary agents. The swarm mode intentionally does not use one of Kilo's reserved native mode slugs, so it is installed as a selectable custom primary mode.

For an installed Kilo distribution, place the same `customModes` entry in the user's supported global `custom_modes.yaml` location if a project-local mode is not desired.

## Selection semantics

Swarm is an **execution strategy**. It does not replace model selection or sessions:

- model selection still chooses the model/provider used by the parent and worker sessions;
- session selection still determines the conversation/workspace context;
- Swarm determines how substantive work is decomposed and executed across specialist child sessions.

This keeps swarm orthogonal to models and sessions, so changing the model does not disable swarm orchestration and changing sessions does not destroy swarm task durability.

## Validation

`packages/opencode/test/kilocode/swarm/mode-install.test.ts` verifies that the repository's `.kilocodemodes` definition is accepted by the real `ModesMigrator` and produces a visible primary `swarm` agent with the expected prompt and permissions.

The broader swarm qualification suite remains responsible for validating durable execution, bounded concurrency, cancellation, fencing, worker findings, and integration with Kilo's existing provider/session execution path.
