import { describe, expect, test } from "bun:test"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { ModesMigrator } from "@/kilocode/modes-migrator"

describe("Swarm execution mode installation", () => {
  test("project .kilocodemodes installs Swarm as a primary agent", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "kilo-swarm-mode-"))
    try {
      const source = await readFile(path.resolve(import.meta.dir, "../../../../.kilocodemodes"), "utf8")
      await writeFile(path.join(root, ".kilocodemodes"), source)

      const result = await ModesMigrator.migrate({ projectDir: root, skipGlobalPaths: true })
      const swarm = result.agents.swarm

      expect(result.skipped.some((item) => item.slug === "swarm")).toBe(false)
      expect(swarm).toBeDefined()
      expect(swarm.mode).toBe("primary")
      expect(swarm.description).toContain("Durable multi-agent orchestration")
      expect(swarm.prompt).toContain("durable swarm tool")
      expect(swarm.permission).toEqual(expect.arrayContaining([
        expect.objectContaining({ permission: "read", action: "allow" }),
        expect.objectContaining({ permission: "edit", action: "allow" }),
        expect.objectContaining({ permission: "bash", action: "allow" }),
        expect.objectContaining({ permission: "swarm", action: "allow" }),
      ]))
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})
