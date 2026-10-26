import { describe, expect, test } from "bun:test"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { ModesMigrator } from "@/kilocode/modes-migrator"

describe("Swarm execution mode installation", () => {
  test("project .kilocodemodes installs Swarm as a primary team agent", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "kilo-swarm-mode-"))
    try {
      const source = await readFile(path.resolve(import.meta.dir, "../../../../../.kilocodemodes"), "utf8")
      await writeFile(path.join(root, ".kilocodemodes"), source)

      const result = await ModesMigrator.migrate({ projectDir: root, skipGlobalPaths: true })
      const swarm = result.agents.swarm

      expect(result.skipped.some((item) => item.slug === "swarm")).toBe(false)
      expect(swarm).toBeDefined()
      expect(swarm.mode).toBe("primary")
      expect(swarm.description).toContain("Durable multi-agent orchestration")
      expect(swarm.prompt).toContain("durable swarm tool")
      expect(swarm.prompt).toContain("shared agent board")
      expect(swarm.prompt).toContain("post useful discoveries")
      expect(swarm.permission).toMatchObject({
        read: "allow",
        edit: "allow",
        bash: "allow",
        swarm: "allow",
        board_read: "allow",
        board_post: "allow",
      })
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})
