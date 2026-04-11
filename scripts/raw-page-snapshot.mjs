#!/usr/bin/env node
// @ts-nocheck

import fs from 'node:fs/promises'
import path from 'node:path'

const DEFAULT_GRAPH_PATH = '/Users/hsk/Logseq/roam'
const DEFAULT_SNAPSHOT_DIR = path.resolve(
  process.cwd(),
  'logs',
  'raw-page-snapshots',
)
const DEFAULT_DIFF_DIR = path.resolve(process.cwd(), 'logs', 'raw-page-diffs')

const usage = `Usage:
  node scripts/raw-page-snapshot.mjs capture --page-name "<page-name>" [--graph-path "<graph-path>"]
  node scripts/raw-page-snapshot.mjs diff --page-name "<page-name>" [--graph-path "<graph-path>"]

Examples:
  node scripts/raw-page-snapshot.mjs capture --page-name "ReadwiseHighlights/20260411-001540/Tweets from 卫斯理"
  node scripts/raw-page-snapshot.mjs diff --page-name "ReadwiseHighlights/20260411-001540/Tweets from 卫斯理"
`

const parseArgs = (argv) => {
  const [, , command, ...rest] = argv
  const options = {
    command,
    pageName: '',
    graphPath: DEFAULT_GRAPH_PATH,
    snapshotDir: DEFAULT_SNAPSHOT_DIR,
    diffDir: DEFAULT_DIFF_DIR,
  }

  for (let index = 0; index < rest.length; index += 1) {
    const arg = rest[index]
    const value = rest[index + 1]

    if (arg === '--page-name' && typeof value === 'string') {
      options.pageName = value
      index += 1
      continue
    }

    if (arg === '--graph-path' && typeof value === 'string') {
      options.graphPath = value
      index += 1
      continue
    }

    if (arg === '--snapshot-dir' && typeof value === 'string') {
      options.snapshotDir = path.resolve(value)
      index += 1
      continue
    }

    if (arg === '--diff-dir' && typeof value === 'string') {
      options.diffDir = path.resolve(value)
      index += 1
      continue
    }

    throw new Error(`Unknown or incomplete argument: ${arg}`)
  }

  if (!['capture', 'diff'].includes(options.command)) {
    throw new Error('Missing or invalid command.')
  }

  if (!options.pageName) {
    throw new Error('Missing required --page-name.')
  }

  return options
}

const buildPageFileStem = (pageName) => pageName.replaceAll('/', '___')

const resolvePageFilePath = (graphPath, pageName) =>
  path.join(graphPath, 'pages', `${buildPageFileStem(pageName)}.org`)

const buildSnapshotPath = (snapshotDir, pageName) =>
  path.join(snapshotDir, `${buildPageFileStem(pageName)}.json`)

const timestamp = () => {
  const now = new Date()
  const pad = (value) => String(value).padStart(2, '0')
  return `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`
}

const buildDiffReportPath = (diffDir, pageName) =>
  path.join(diffDir, `${buildPageFileStem(pageName)}-${timestamp()}.txt`)

const formatExcerpt = (lines, startLine, endLine) =>
  lines
    .slice(startLine, endLine)
    .map((line, index) => `${startLine + index + 1}: ${line}`)
    .join('\n')

const buildDiffResult = (before, after, pageName, absolutePath) => {
  if (before === after) {
    return {
      changed: false,
      firstDiffLine: null,
      beforeExcerpt: '',
      afterExcerpt: '',
      report: [
        `${pageName}`,
        `Source: file (${absolutePath})`,
        '',
        'No raw file changes detected.',
      ].join('\n'),
    }
  }

  const beforeLines = before.split('\n')
  const afterLines = after.split('\n')
  let firstDiff = 0

  while (
    firstDiff < beforeLines.length &&
    firstDiff < afterLines.length &&
    beforeLines[firstDiff] === afterLines[firstDiff]
  ) {
    firstDiff += 1
  }

  let beforeEnd = beforeLines.length - 1
  let afterEnd = afterLines.length - 1

  while (
    beforeEnd >= firstDiff &&
    afterEnd >= firstDiff &&
    beforeLines[beforeEnd] === afterLines[afterEnd]
  ) {
    beforeEnd -= 1
    afterEnd -= 1
  }

  const contextStart = Math.max(0, firstDiff - 2)
  const beforeContextEnd = Math.min(beforeLines.length, beforeEnd + 3)
  const afterContextEnd = Math.min(afterLines.length, afterEnd + 3)
  const beforeExcerpt = formatExcerpt(beforeLines, contextStart, beforeContextEnd)
  const afterExcerpt = formatExcerpt(afterLines, contextStart, afterContextEnd)

  return {
    changed: true,
    firstDiffLine: firstDiff + 1,
    beforeExcerpt,
    afterExcerpt,
    report: [
      `${pageName} @ line ${firstDiff + 1}`,
      `Source: file (${absolutePath})`,
      '',
      'Before Excerpt:',
      beforeExcerpt,
      '',
      'After Excerpt:',
      afterExcerpt,
      '',
      'Before Full Page:',
      before,
      '',
      'After Full Page:',
      after,
    ].join('\n'),
  }
}

const ensureDir = async (dirPath) => {
  await fs.mkdir(dirPath, { recursive: true })
}

const captureSnapshot = async ({ pageName, graphPath, snapshotDir }) => {
  const absolutePath = resolvePageFilePath(graphPath, pageName)
  const content = await fs.readFile(absolutePath, 'utf8')

  await ensureDir(snapshotDir)
  const snapshotPath = buildSnapshotPath(snapshotDir, pageName)
  const snapshotPayload = {
    schemaVersion: 1,
    capturedAt: new Date().toISOString(),
    pageName,
    graphPath,
    absolutePath,
    content,
  }

  await fs.writeFile(snapshotPath, JSON.stringify(snapshotPayload, null, 2), 'utf8')

  console.log(
    `Captured raw snapshot for ${pageName} from ${absolutePath} (${content.split('\n').length} lines).`,
  )
  console.log(`Snapshot saved to ${snapshotPath}`)
}

const diffSnapshot = async ({ pageName, graphPath, snapshotDir, diffDir }) => {
  const snapshotPath = buildSnapshotPath(snapshotDir, pageName)
  const rawSnapshot = await fs.readFile(snapshotPath, 'utf8')
  const snapshot = JSON.parse(rawSnapshot)
  const absolutePath = resolvePageFilePath(graphPath, pageName)
  const currentContent = await fs.readFile(absolutePath, 'utf8')
  const diff = buildDiffResult(snapshot.content, currentContent, pageName, absolutePath)

  await ensureDir(diffDir)
  const reportPath = buildDiffReportPath(diffDir, pageName)
  await fs.writeFile(reportPath, diff.report, 'utf8')

  console.log(diff.report)
  console.log('')
  console.log(`Diff report saved to ${reportPath}`)
}

const main = async () => {
  try {
    const options = parseArgs(process.argv)

    if (options.command === 'capture') {
      await captureSnapshot(options)
      return
    }

    if (options.command === 'diff') {
      await diffSnapshot(options)
      return
    }
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error))
    console.error('')
    console.error(usage)
    process.exitCode = 1
  }
}

await main()
