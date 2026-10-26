import { afterEach, describe, expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { assembleCliPackage } from "../../script/kilocode/cli-package"

const roots: string[] = []
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => fs.rm(root, { recursive: true, force: true })))
})

describe("CLI wrapper package assembly", () => {
  test("creates an installable package with the platform binary dependency and postinstall", async () => {
    const temp = await fs.mkdtemp(path.join(os.tmpdir(), "kilo-cli-package-"))
    roots.push(temp)
    const root = path.join(temp, "repo", "packages", "opencode")
    await fs.mkdir(path.join(root, "bin", "kilocode"), { recursive: true })
    await fs.mkdir(path.join(root, "script"), { recursive: true })
    await fs.mkdir(path.resolve(root, "..", ".."), { recursive: true })
    await fs.writeFile(path.join(root, "bin", "kilo"), "#!/usr/bin/env node\nprocess.exit(0)\n")
    await fs.writeFile(path.join(root, "bin", "kilocode", "windows-avx2.cjs"), "exports.supportsAvx2 = () => true\n")
    await fs.writeFile(path.join(root, "script", "postinstall.mjs"), "process.exit(0)\n")
    await fs.writeFile(path.join(root, "README.md"), "# Kilo CLI\n")
    await fs.writeFile(path.resolve(root, "..", "..", "LICENSE"), "MIT\n")

    const binaries = { "@kilocode/cli-windows-x64": "0.0.0-smoke" }
    const output = await assembleCliPackage({
      root,
      distDir: path.join(root, "dist"),
      metadata: {
        name: "@kilocode/cli",
        license: "MIT",
        keywords: ["cli", "kilo"],
        private: false,
      },
      version: "0.0.0-smoke",
      binaries,
    })
    const manifest = JSON.parse(await fs.readFile(path.join(output, "package.json"), "utf8"))

    expect(manifest).toMatchObject({
      name: "@kilocode/cli",
      version: "0.0.0-smoke",
      bin: { kilo: "./bin/kilo", kilocode: "./bin/kilo" },
      scripts: { postinstall: "node ./postinstall.mjs" },
      optionalDependencies: binaries,
      os: ["darwin", "linux", "win32"],
      cpu: ["arm64", "x64"],
    })
    expect(await fs.readFile(path.join(output, "bin", "kilo"), "utf8")).toContain("process.exit(0)")
    expect(await fs.readFile(path.join(output, "bin", "kilocode", "windows-avx2.cjs"), "utf8")).toContain("supportsAvx2")
    expect(await fs.readFile(path.join(output, "postinstall.mjs"), "utf8")).toContain("process.exit(0)")
    expect(await fs.readFile(path.join(output, "README.md"), "utf8")).toContain("Kilo CLI")
    expect(await fs.readFile(path.join(output, "LICENSE"), "utf8")).toContain("MIT")
  })
})
