import { Effect } from "effect"

export type Message = {
  id: string
  from: string
  to: string
  type: string
  body: string
}

export type Page = {
  messages: readonly Message[]
  cursor?: string
  hasMore: boolean
}

export type History = {
  /** Number of non-root messages observed while paging (not a global board total). */
  scanned: number
  /** True if any board pages or individual messages were omitted/truncated. */
  truncated: boolean
  messages: Message[]
}

export type Options = {
  maxPages?: number
  maxMessages?: number
  maxBodyChars?: number
}

/**
 * Read a bounded, newest-message window from a cursor-paged shared board.
 *
 * The callback receives the previous page cursor. A repeated/missing cursor while
 * hasMore=true fails closed instead of looping or silently repeating messages.
 * A page cap makes the operation bounded even if the board keeps changing.
 */
export function collect<E, R>(
  readPage: (cursor?: string) => Effect.Effect<Page, E, R>,
  options: Options = {},
): Effect.Effect<History, E, R> {
  return Effect.gen(function* () {
    const maxPages = options.maxPages ?? 20
    const maxMessages = options.maxMessages ?? 30
    const maxBodyChars = options.maxBodyChars ?? 1200
    if (!Number.isSafeInteger(maxPages) || maxPages < 1) throw new Error("maxPages must be a positive safe integer")
    if (!Number.isSafeInteger(maxMessages) || maxMessages < 1) throw new Error("maxMessages must be a positive safe integer")
    if (!Number.isSafeInteger(maxBodyChars) || maxBodyChars < 1) throw new Error("maxBodyChars must be a positive safe integer")

    let cursor: string | undefined
    let hasMore = true
    let truncated = false
    let pages = 0
    const seenCursors = new Set<string>()
    const collected: Message[] = []
    while (hasMore && pages < maxPages) {
      const page = yield* readPage(cursor)
      pages += 1
      // Never append a page whose cursor repeats or loops back: its rows may be
      // replayed from an earlier page and would duplicate team evidence.
      if (cursor !== undefined && (!page.cursor || page.cursor === cursor || seenCursors.has(page.cursor))) {
        if (page.hasMore || page.messages.length > 0) truncated = true
        hasMore = false
        break
      }
      collected.push(...page.messages)
      hasMore = page.hasMore
      if (!hasMore) break
      if (!page.cursor) {
        truncated = true
        break
      }
      seenCursors.add(page.cursor)
      cursor = page.cursor
    }

    if (hasMore) truncated = true
    if (collected.length > maxMessages) truncated = true
    const messages = collected.slice(-maxMessages).map((message) => {
      if (message.body.length <= maxBodyChars) return message
      truncated = true
      return { ...message, body: message.body.slice(0, maxBodyChars) + " … [truncated]" }
    })
    return { scanned: collected.length, truncated, messages }
  })
}
