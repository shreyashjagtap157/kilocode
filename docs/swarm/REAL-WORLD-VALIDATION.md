# Kilo Swarm Real-World Validation

## Purpose

This document is the final evidence checklist for determining whether Kilo's swarm mode is useful in practice, rather than merely implemented.

## Automated gate

The focused GitHub Actions workflow must pass migration/schema validation, durable-event tests, the complete `packages/opencode/test/kilocode/swarm` suite, OpenCode typechecking, and generated-schema cleanliness. The workflow is configured for both pull requests and pushes to `main`. See `.github/workflows/m2-swarm-foundation.yml`.

## Functional gate

A real configured provider/model run must demonstrate all of the following:

1. The parent agent can invoke the `swarm` tool.
2. At least three distinct specialist workers can be admitted.
3. Workers receive distinct roles/objectives.
4. Workers execute through normal Kilo session/provider machinery.
5. Workers can run concurrently up to the configured capacity.
6. The parent receives distinct persisted findings from successful workers.
7. One worker failure does not erase successful findings from other workers.
8. Cancellation interrupts an active Kilo session and leaves no authoritative completion behind.
9. Lease loss prevents stale workers from committing completion.
10. Restart/recovery does not create duplicate authoritative execution.
11. Worker permissions never exceed the derived Kilo permission ceiling.
12. Each worker result eligible for acceptance is reviewed by a separate reviewer task/session; self-review is never substituted when too few workers finish.
13. Passing criteria link to evidence, and a claimed passing command reference exactly matches a completed persisted Kilo shell-tool part in that reviewer session, including command, exit code, part/message/call identity, and output digest.
14. A later blocked review overrides an earlier pass, and a parent task remains unaccepted while any direct child is not accepted/integrated.
15. Reviewer failures, missing reviewers, mismatched sessions, failed commands, or forged evidence remain visible as unaccepted outcomes; a lone surviving worker receives an explicit HOLD notice when no independent reviewer can be scheduled.
16. A reviewer task transitions to terminal `reviewed` only after its pass/fail/inconclusive event is durably recorded; failed review persistence is not silently treated as completion, and `reviewed` never means the implementation was accepted.

## Usefulness benchmark

Use a task with independently verifiable output, such as repository investigation or code-review preparation. Compare:

- single-agent baseline;
- swarm result with 3-4 specialists;
- wall-clock duration;
- number of independent findings;
- duplicate/contradictory findings;
- findings accepted by a verifier/reviewer;
- failures and recoveries.

Swarm mode is useful only when the parallel specialist result provides materially better coverage, confidence, or turnaround than the single-agent baseline for the selected task.

## Evidence policy

Do not mark this document as qualified from unit tests alone. Mocked tests establish orchestration correctness. Real provider execution establishes operational usefulness. CI establishes repeatability. Both are required for a successful qualification claim.

For the evidence-backed review gate, a committed regression test is not a test result. The focused swarm tests and OpenCode typecheck must execute and pass in CI, and a real configured provider/model run must demonstrate that the reviewer's persisted session parts contain the same command, exit code, and output digest used by the acceptance gate. Until that evidence exists, describe the gate as implemented but not CI-qualified.
