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
      Effect.either(
        SwarmClaims.claim({
          taskId,
          ownerId,
          leaseMs: 60_000,
          actor: "act_process_test",
          corrId: `cor_${ownerId}` as any,
        }),
      ),
    ),
  ),
)

if (result._tag === "Right") {
  process.stdout.write(JSON.stringify({ ok: true, ownerId, fence: result.right.fence, leaseId: result.right.leaseId }) + "\n")
} else {
  process.stdout.write(JSON.stringify({ ok: false, ownerId, tag: result.left._tag, message: result.left.message }) + "\n")
}
