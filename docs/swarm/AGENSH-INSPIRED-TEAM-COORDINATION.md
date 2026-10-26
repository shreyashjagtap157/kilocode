# Agensh-inspired team coordination for Kilo Swarm

Microsoft's open-source **Agensh** describes a self-organized multi-agent harness in which workers concurrently gather context, claim sub-tasks, share findings, verify results, and merge progress asynchronously. Its infrastructure uses a shared workspace, message interface, and shared context, rather than requiring every interaction to pass through a central orchestrator. The released harness scales its research setup to 1,024 agents.

Kilo Swarm adopts the useful coordination principles without copying Agensh's infrastructure or claiming the same scale:

| Agensh principle | Kilo implementation |
| --- | --- |
| Shared workspace | Kilo child sessions operate in the same project/workspace context |
| Message interface | Kilo's persistent shared agent board (`BoardStore`, `board_read`, `board_post`) |
| Shared context | Parent objective + durable board history + worker findings |
| Discover/claim work | Durable swarm task admission, claims, capacity reservations and fencing |
| Concurrent workers | Bounded `SwarmRuntime` execution |
| Peer communication | Workers are instructed to read/post board updates while working |
| Verification | Dedicated verifier/reviewer worker roles and persisted findings |
| Merge progress | Parent swarm tool synthesizes worker findings and board communications |
| Recovery | Durable claims, leases, worker bindings and recovery logic |

## Deliberate differences

Kilo remains centrally initiated: a parent Kilo session selects Swarm mode and starts a bounded team. Workers do not autonomously create unlimited workers or bypass Kilo's permission model. This preserves predictable resource usage and Kilo's existing security boundaries.

The team board is nevertheless peer-to-peer in behavior: a worker can discover another worker's progress, communicate a blocker, challenge a finding, or publish a verified handoff without waiting for the parent to copy the information into another prompt.

## Why this matters

Parallel prompts alone are not a team. A useful team needs a shared state and communication mechanism. Kilo already has a persistent agent board, enabled by default unless explicitly disabled, so Swarm now uses that existing infrastructure instead of inventing a second message bus.

The Swarm tool seeds the common objective on the board, tells each worker to use the board throughout its task, and includes the resulting peer communications in the parent's synthesis context. This makes team interaction observable and testable.

## Validation requirement

Agensh's published results demonstrate that increasing agent count can improve benchmark performance in its own harness and workload, but those numbers must not be generalized to Kilo. Kilo must validate usefulness independently with its own workloads, provider configuration, latency, cost, correctness, and failure/recovery measurements.
