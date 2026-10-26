import { describe, expect, test } from "bun:test"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { Permission } from "@/permission"
import * as KiloAgent from "@/kilocode/agent"
import { ModesMigrator } from "@/kilocode/modes-migrator"

describe("Swarm execution mode installation", () => {
  test("native Swarm is globally registered and legacy project definitions do not shadow it", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "kilo-swarm-mode-"))
    try {
      const source = await readFile(path.resolve(import.meta.dir, "../../../../../.kilocodemodes"), "utf8")
      await writeFile(path.join(root, ".kilocodemodes"), source)

      const migrated = await ModesMigrator.migrate({ projectDir: root, skipGlobalPaths: true })
      expect(migrated.skipped.some((item) => item.slug === "swarm")).toBe(true)
      expect(migrated.agents.swarm).toBeUndefined()

      const empty = Permission.fromConfig({})
      const agents: Record<string, any> = {
        code: {
          name: "code",
          description: "Base coding agent",
          mode: "primary",
          native: true,
          permission: empty,
          options: {},
        },
      }
      KiloAgent.patchAgents(
        agents,
        empty,
        empty,
        { mcpRules: {}, defaultsPatch: empty, board: true },
        root,
        [],
      )

      const swarm = agents.swarm
      expect(swarm).toBeDefined()
      expect(swarm.name).toBe("swarm")
      expect(swarm.mode).toBe("primary")
      expect(swarm.native).toBe(true)
      expect(swarm.prompt).toContain("durable `swarm` tool")
      expect(swarm.prompt).toContain("shared Kilo agent board")
      expect(swarm.prompt).toContain("post useful discoveries")
      for (const permission of ["swarm", "board_read", "board_post"]) {
        expect(swarm.permission.some((rule: any) => rule.permission === permission && rule.action === "allow")).toBe(true)
      }
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})
