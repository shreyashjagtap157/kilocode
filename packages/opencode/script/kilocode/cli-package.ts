import fs from "node:fs/promises"
import path from "node:path"

export type CliPackageMetadata = {
  name: string
  license: string
  keywords: string[]
  private: boolean
}

export async function assembleCliPackage(input: {
  root: string
  distDir: string
  metadata: CliPackageMetadata
  version: string
  binaries: Record<string, string>
}) {
  const out = path.join(input.distDir, input.metadata.name)
  await fs.mkdir(out, { recursive: true })
  await fs.cp(path.join(input.root, "bin"), path.join(out, "bin"), { recursive: true })
  await fs.copyFile(path.join(input.root, "script", "postinstall.mjs"), path.join(out, "postinstall.mjs"))
  await fs.copyFile(path.resolve(input.root, "..", "..", "LICENSE"), path.join(out, "LICENSE"))
  await fs.copyFile(path.join(input.root, "README.md"), path.join(out, "README.md"))

  const pkg = {
    name: input.metadata.name,
    bin: {
      kilo: "./bin/kilo",
      kilocode: "./bin/kilo",
    },
    scripts: {
      postinstall: "node ./postinstall.mjs",
    },
    version: input.version,
    license: input.metadata.license,
    keywords: input.metadata.keywords,
    private: input.metadata.private,
    os: ["darwin", "linux", "win32"],
    cpu: ["arm64", "x64"],
    optionalDependencies: input.binaries,
    repository: {
      type: "git",
      url: "https://github.com/Kilo-Org/kilocode",
    },
  }
  await fs.writeFile(path.join(out, "package.json"), JSON.stringify(pkg, null, 2) + "\n")
  return out
}
