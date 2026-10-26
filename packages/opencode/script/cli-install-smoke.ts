#!/usr/bin/env bun

import { $ } from "bun"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import pkg from "../package.json"
import { assembleCliPackage } from "./kilocode/cli-package"

const root = path.resolve(import.meta.dir, "..")
const dist = path.join(root, "dist")
const scope = path.join(dist, "@kilocode")
const platform = process.platform === "win32" ? "windows" : process.platform
const packageName = `${pkg.name}-${platform}-${process.arch}`
const platformDir = path.join(dist, ...packageName.split("/"))
const platformManifestPath = path.join(platformDir, "package.json")

if (!(await fs.stat(platformManifestPath).catch(() => undefined))) {
  throw new Error(`Expected host CLI package is missing after build: ${platformManifestPath}`)
}

const platformManifest = JSON.parse(await fs.readFile(platformManifestPath, "utf8")) as {
  name: string
  version: string
}
if (platformManifest.name !== packageName) {
  throw new Error(`Expected ${packageName}, found ${platformManifest.name}`)
}

const binaries: Record<string, string> = {}
for (const entry of await fs.readdir(scope, { withFileTypes: true })) {
  if (!entry.isDirectory() || !entry.name.startsWith("cli-")) continue
  const manifestPath = path.join(scope, entry.name, "package.json")
  const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8")) as { name: string; version: string }
  binaries[manifest.name] = manifest.version
}
if (Object.keys(binaries).length === 0) {
  throw new Error(`No built platform packages were found in ${scope}`)
}

const wrapperDir = await assembleCliPackage({
  root,
  distDir: dist,
  metadata: pkg,
  version: platformManifest.version,
  binaries,
})

const temp = await fs.mkdtemp(path.join(os.tmpdir(), "kilo-installed-package-smoke-"))
const packDir = path.join(temp, "packs")
const installDir = path.join(temp, "install")
await fs.mkdir(packDir, { recursive: true })
await fs.mkdir(installDir, { recursive: true })

async function pack(directory: string) {
  const output = await $`npm pack --json --pack-destination ${packDir}`.cwd(directory).quiet().text()
  const result = JSON.parse(output) as { filename: string }[]
  if (result.length !== 1 || !result[0]?.filename) {
    throw new Error(`npm pack did not produce exactly one tarball for ${directory}: ${output}`)
  }
  return path.join(packDir, result[0].filename)
}

try {
  // Match release packaging's executable permissions before packing on Unix.
  if (process.platform !== "win32") {
    await $`chmod -R 755 ${platformDir}`
    await $`chmod -R 755 ${wrapperDir}`
  }

  const platformTarball = await pack(platformDir)
  const wrapperTarball = await pack(wrapperDir)

  // Install both local tarballs into a clean prefix. The wrapper's optional dependency
  // must resolve to the matching platform package without relying on the source tree.
  await $`npm install --prefix ${installDir} --no-audit --no-fund ${platformTarball} ${wrapperTarball}`

  const wrapper = path.join(installDir, "node_modules", ...pkg.name.split("/"), "bin", "kilo")
  const versionOutput = (await $`node ${wrapper} --version`.text()).trim()
  if (!versionOutput.includes(platformManifest.version)) {
    throw new Error(`Installed CLI reported an unexpected version: ${versionOutput}; expected ${platformManifest.version}`)
  }

  await $`node ${wrapper} --help`
  console.log(`Installed package smoke passed: ${pkg.name}@${platformManifest.version}`)
  console.log(`Platform package: ${packageName}`)
  console.log(`Installed wrapper: ${wrapper}`)
  console.log(`Version output: ${versionOutput}`)
} finally {
  await fs.rm(temp, { recursive: true, force: true })
}
