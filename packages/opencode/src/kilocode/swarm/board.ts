/**
 * Kilo Swarm M1 — typed advisory blackboard contract (foundation only).
 *
 * Layers typed `SwarmMessage` contracts on the existing advisory `BoardStore`
 * (`packages/opencode/src/kilocode/board/store.ts`). It does not build a second
 * messaging system: it reuses `BoardStore.Kind` and `BoardStore.post`, mapping
 * swarm message kinds onto the board's existing kinds.
 *
 * This layer is explicitly advisory. It has no locks, no claims and no
 * authority, and it MUST NOT be used as authoritative task state (re-audit §11).
 * `SwarmMessage` carries no task-state and no permission fields, so a board
 * record can never mutate a task or authorize an action.
 */
import { Effect, Schema } from "effect"
import { BoardStore } from "@/kilocode/board/store"
import type { SessionID } from "@/session/schema"
import { SwarmMessage, SwarmMessageKind } from "./schema"

export namespace SwarmBoard {
  /** Explicit advisory marker: nothing here is authoritative. */
  export const advisory = {
    authoritative: false,
    locks: false,
    claims: false,
    authority: false,
  } as const

  const toBoard: Record<SwarmMessageKind, BoardStore.Kind> = {
    status: "INFO",
    question: "ASK",
    finding: "RESULT",
    hold: "HOLD",
    veto: "VETO",
  }

  const toSwarm: Record<BoardStore.Kind, SwarmMessageKind> = {
    INFO: "status",
    ASK: "question",
    RESULT: "finding",
    HOLD: "hold",
    VETO: "veto",
  }

  export function kind(value: SwarmMessageKind): BoardStore.Kind {
    return toBoard[value]
  }

  export function swarmKind(value: BoardStore.Kind): SwarmMessageKind {
    return toSwarm[value]
  }

  /** Encode a typed swarm message onto the advisory board wire shape. */
  export function encode(message: SwarmMessage): { type: BoardStore.Kind; body: string } {
    return { type: kind(message.kind), body: JSON.stringify(message) }
  }

  /** Decode a board record back into a typed swarm message, checking kind consistency. */
  export function decode(input: { type: BoardStore.Kind; body: string }): SwarmMessage {
    const message = Schema.decodeUnknownSync(SwarmMessage)(JSON.parse(input.body))
    if (kind(message.kind) !== input.type)
      throw new Error(`Swarm message kind "${message.kind}" does not match board type "${input.type}"`)
    return message
  }

  /** Reuse `BoardStore.post` to publish an advisory typed message. */
  export const post = (input: {
    sessionID: SessionID
    messageID: string
    callID?: string
    to: string
    message: SwarmMessage
  }) =>
    Effect.gen(function* () {
      const wire = encode(input.message)
      return yield* BoardStore.post({
        sessionID: input.sessionID,
        messageID: input.messageID,
        ...(input.callID === undefined ? {} : { callID: input.callID }),
        to: input.to,
        type: wire.type,
        body: wire.body,
      })
    })
}
