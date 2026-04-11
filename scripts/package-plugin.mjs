import { cp, mkdir, readFile, rm } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

const execFileAsync = promisify(execFile)
const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const repoRoot = path.resolve(__dirname, '..')

const packageJson = JSON.parse(
  await readFile(path.join(repoRoot, 'package.json'), 'utf8'),
)
const pluginName = packageJson.name
const iconPath =
  typeof packageJson?.logseq?.icon === 'string' &&
  packageJson.logseq.icon.trim().length > 0
    ? packageJson.logseq.icon.replace(/^\.\//, '')
    : 'icon.png'

if (typeof pluginName !== 'string' || pluginName.trim().length === 0) {
  throw new Error('package.json must define a non-empty "name" field.')
}

const releaseRoot = path.join(repoRoot, '.release')
const pluginRoot = path.join(releaseRoot, pluginName)
const outputZip = path.join(repoRoot, `${pluginName}.zip`)

await rm(pluginRoot, { recursive: true, force: true })
await rm(outputZip, { force: true })
await mkdir(pluginRoot, { recursive: true })

const copyPaths = ['README.md', 'package.json', iconPath, 'LICENSE.md', 'dist']

for (const relativePath of copyPaths) {
  await cp(path.join(repoRoot, relativePath), path.join(pluginRoot, relativePath), {
    recursive: true,
  })
}

await execFileAsync('zip', ['-r', outputZip, pluginName], {
  cwd: releaseRoot,
})

console.log(outputZip)
