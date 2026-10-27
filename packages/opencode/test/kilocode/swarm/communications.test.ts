import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import * as SwarmCommunications from "@/kilocode/swarm/communications"

const message = (id: string, body = "peer update") => ({
  id,
  from: "worker-session",
  to: "main",
  type: "RESULT",
  body,
})

describe("SwarmCommunications.collect", () => {
  test("pages through shared history and retains only the newest bounded window", async () => {
    const calls: Array<string | undefined> = []
    const readPage = (cursor?: string) => {
      calls.push(cursor)
      if (!cursor) {
        return Effect.succeed({
          messages: Array.from({ length: 30 }, (_, i) => message("m" + i)),
          cursor: "page-one",
          hasMore: true,
        })
      }
      return Effect.succeed({
        messages: Array.from({ length: 30 }, (_, i) => message("m" + (i + 30))),
        cursor: "page-two",
        hasMore: false,
      })
    }

    const result = await Effect.runPromise(
      SwarmCommunications.collect(readPage, { maxPages: 5, maxMessages: 30, maxBodyChars: 1200 }),
    )
    expect(calls).toEqual([undefined, "page-one"])
    expect(result.scanned).toBe(60)
    expect(result.truncated).toBe(true)
    expect(result.messages).toHaveLength(30)
    expect(result.messages[0]?.id).toBe("m30")
    expect(result.messages[29]?.id).toBe("m59")
  })

  test("stops safely when a page says more data exists but provides no cursor", async () => {
    let calls = 0
    const result = await Effect.runPromise(
      SwarmCommunications.collect(() => {
        calls += 1
        return Effect.succeed({ messages: [message("first")], hasMore: true })
      }),
    )
    expect(calls).toBe(1)
    expect(result.scanned).toBe(1)
    expect(result.truncated).toBe(true)
  })

  test("stops safely when a cursor fails to advance", async () => {
    let calls = 0
    const result = await Effect.runPromise(
      SwarmCommunications.collect((cursor) => {
        calls += 1
        return Effect.succeed({
          messages: [message("page-" + calls)],
          cursor: cursor ?? "same-cursor",
          hasMore: true,
        })
      }),
    )
    expect(calls).toBe(2)
    expect(result.scanned).toBe(2)
    expect(result.truncated).toBe(true)
  })

  test("caps scanning when the board keeps growing", async () => {
    let calls = 0
    const result = await Effect.runPromise(
      SwarmCommunications.collect((cursor) => {
        calls += 1
        return Effect.succeed({
          messages: [message("m" + calls)],
          cursor: "cursor-" + calls,
          hasMore: true,
        })
      }, { maxPages: 3, maxMessages: 10 }),
    )
    expect(calls).toBe(3)
    expect(result.scanned).toBe(3)
    expect(result.truncated).toBe(true)
  })

  test("bounds individual message bodies and reports truncation", async () => {
    const result = await Effect.runPromise(
      SwarmCommunications.collect(
        () => Effect.succeed({
          messages: [message("long", "0123456789")],
          cursor: "last",
          hasMore: false,
        }),
        { maxBodyChars: 5 },
      ),
    )
    expect(result.messages[0]?.body).toBe("01234 … [truncated]")
    expect(result.truncated).toBe(true)
  })
})
