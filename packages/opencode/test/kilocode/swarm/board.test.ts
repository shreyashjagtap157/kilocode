import { describe, expect, test } from "bun:test"
import { Effect, Schema } from "effect"
import { sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { BoardStore } from "@/kilocode/board/store"
import { SessionID } from "@/session/schema"
import { SwarmBoard } from "@/kilocode/swarm/board"
import { SwarmMessage } from "@/kilocode/swarm/schema"

const use = <A, E>(effect: Effect.Effect<A, E, Database.Service>, file = ":memory:") =>
  Effect.runPromise(Effect.provide(Database.layerFromPath(file))(Effect.scoped(effect)))

const id = (value: string) => SessionID.make(`ses_${value}`)

const seed = (db: Database.Interface["db"]) =>
  Effect.gen(function* () {
    yield* db.run(
      sql`INSERT INTO project (id, worktree, time_created, time_updated, sandboxes) VALUES ('p', '/', 1, 1, '[]')`,
    )
    yield* db.run(sql`
      INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated)
      VALUES ('ses_root', 'p', 'root', '/', 'Root', 'test', 1, 1)
    `)
    yield* db.run(sql`
      INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
      VALUES ('msg_objective', 'ses_root', 'user', 1, 1, 1, ${JSON.stringify({ text: "Coordinate the swarm", synthetic: false })})
    `)
  })

const message = Schema.decodeUnknownSync(SwarmMessage)({
  kind: "finding",
  detail: "found the widget",
  id: "smsg_1",
  corrId: "cor_1",
  from: "act_main",
  to: "ALL",
  version: 1,
} as const)

describe("SwarmBoard", () => {
  test("AC8: encode/decode round-trips a typed message across the advisory board wire shape", () => {
    const wire = SwarmBoard.encode(message)
    expect(wire.type).toBe("RESULT")
    expect(SwarmBoard.decode(wire)).toEqual(message)
  })

  test("AC8: message kinds map onto existing BoardStore kinds", () => {
    expect(SwarmBoard.kind("status")).toBe("INFO")
    expect(SwarmBoard.kind("question")).toBe("ASK")
    expect(SwarmBoard.kind("finding")).toBe("RESULT")
    expect(SwarmBoard.kind("hold")).toBe("HOLD")
    expect(SwarmBoard.kind("veto")).toBe("VETO")
    expect(SwarmBoard.swarmKind("VETO")).toBe("veto")
    expect(() => SwarmBoard.decode({ type: "HOLD", body: JSON.stringify(message) })).toThrow(/does not match/)
  })

  test("AC8/AC10: the board layer is advisory only with no authority surface", () => {
    expect(SwarmBoard.advisory).toEqual({ authoritative: false, locks: false, claims: false, authority: false })
    const api = Object.keys(SwarmBoard)
    expect(api.filter((key) => /state|lock|claim|lease|authoriz|grant|reserve/i.test(key))).toEqual([])
  })

  test("AC8: a typed message survives a real BoardStore post/read round-trip", async () => {
    await use(
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        yield* seed(db)

        const record = yield* SwarmBoard.post({
          sessionID: id("root"),
          messageID: "msg_swarm_1",
          to: "ALL",
          message,
        })
        expect(record.type).toBe("RESULT")

        const page = yield* BoardStore.read({ sessionID: id("root") })
        const stored = page.messages.at(-1)
        expect(stored).toBeDefined()
        expect(SwarmBoard.decode({ type: stored!.type, body: stored!.body })).toEqual(message)
      }),
    )
  })
})
