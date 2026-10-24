import { Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SwarmClaims } from "@/kilocode/swarm/claims"

const [file, taskId, ownerId] = process.argv.slice(2)
if (!file || !taskId || !ownerId) throw new Error("Expected database path, task ID, and owner ID")

const db = Database.layerFromPath(file)
const events = EventV2.layerWith().pipe(Layer.provide(db))
const layer = Layer.mergeAll(db, events)
const result = await Effect.runPromise(
  Effect.provide(layer)(
    Effect.scoped(
      SwarmClaims.claim({
        taskId,
        ownerId,
        leaseMs: 60_000,
        actor: "act_process_test",
        corrId: `cor_${ownerId}` as any,
      }).pipe(
        Effect.map((claim) => ({ ok: true as const, ownerId, fence: claim.fence, leaseId: claim.leaseId })),
        Effect.catch((error) => Effect.succeed({
          ok: false as const,
          ownerId,
          tag: (error as { _tag?: string })._tag ?? "unknown",
          message: (error as { message?: string }).message ?? String(error),
        })),
      ),
    ),
  ),
)

process.stdout.write(JSON.stringify(result) + "\n")
