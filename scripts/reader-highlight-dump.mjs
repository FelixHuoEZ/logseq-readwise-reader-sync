#!/usr/bin/env node
// @ts-nocheck

import fs from 'node:fs/promises'
import path from 'node:path'

const READER_API_BASE_URL = 'https://readwise.io/api/v3'
const DEFAULT_OUTPUT_DIR = path.resolve(
  process.cwd(),
  'logs',
  'reader-highlight-dumps',
)

const usage = `Usage:
  node scripts/reader-highlight-dump.mjs [--token "<token>"] [--limit 10] [--updated-after "<ISO8601>"] [--with-html-content]

Examples:
  READWISE_ACCESS_TOKEN=xxx node scripts/reader-highlight-dump.mjs --limit 5
  node scripts/reader-highlight-dump.mjs --token "xxx" --limit 5 --updated-after "2026-04-01T00:00:00Z"
`

const parseArgs = (argv) => {
  const [, , ...rest] = argv
  const options = {
    token: process.env.READWISE_ACCESS_TOKEN ?? '',
    limit: 5,
    updatedAfter: '',
    withHtmlContent: false,
    outputDir: DEFAULT_OUTPUT_DIR,
  }

  for (let index = 0; index < rest.length; index += 1) {
    const arg = rest[index]
    const value = rest[index + 1]

    if (arg === '--token' && typeof value === 'string') {
      options.token = value
      index += 1
      continue
    }

    if (arg === '--limit' && typeof value === 'string') {
      const parsed = Number.parseInt(value, 10)
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid --limit value: ${value}`)
      }
      options.limit = parsed
      index += 1
      continue
    }

    if (arg === '--updated-after' && typeof value === 'string') {
      options.updatedAfter = value
      index += 1
      continue
    }

    if (arg === '--output-dir' && typeof value === 'string') {
      options.outputDir = path.resolve(value)
      index += 1
      continue
    }

    if (arg === '--with-html-content') {
      options.withHtmlContent = true
      continue
    }

    throw new Error(`Unknown or incomplete argument: ${arg}`)
  }

  if (!options.token) {
    throw new Error(
      'Missing Readwise access token. Pass --token or set READWISE_ACCESS_TOKEN.',
    )
  }

  return options
}

const ensureDir = async (dirPath) => {
  await fs.mkdir(dirPath, { recursive: true })
}

const timestamp = () => {
  const now = new Date()
  const pad = (value) => String(value).padStart(2, '0')
  return `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`
}

const buildOutputPath = (outputDir) =>
  path.join(outputDir, `reader-highlights-${timestamp()}.json`)

const buildRequestUrl = ({ limit, updatedAfter, withHtmlContent }) => {
  const url = new URL(`${READER_API_BASE_URL}/list/`)
  url.searchParams.set('category', 'highlight')
  url.searchParams.set('limit', String(limit))
  if (updatedAfter) {
    url.searchParams.set('updatedAfter', updatedAfter)
  }
  if (withHtmlContent) {
    url.searchParams.set('withHtmlContent', 'true')
  }
  return url.toString()
}

const summarizeResult = (result) => ({
  id: result.id ?? null,
  url: result.url ?? null,
  parent_id: result.parent_id ?? null,
  source_url: result.source_url ?? null,
  title: result.title ?? null,
  author: result.author ?? null,
  category: result.category ?? null,
  location: result.location ?? null,
  created_at: result.created_at ?? null,
  updated_at: result.updated_at ?? null,
  has_html_content:
    typeof result.html_content === 'string' && result.html_content.length > 0,
  keys: Object.keys(result).sort(),
})

const main = async () => {
  try {
    const options = parseArgs(process.argv)
    const requestUrl = buildRequestUrl(options)

    const response = await fetch(requestUrl, {
      method: 'GET',
      headers: {
        Authorization: `Token ${options.token}`,
      },
    })

    if (!response.ok) {
      const body = await response.text()
      throw new Error(
        `Reader highlight list request failed: ${response.status} ${response.statusText}\n${body}`,
      )
    }

    const payload = await response.json()
    const output = {
      fetchedAt: new Date().toISOString(),
      requestUrl,
      count: payload.count ?? null,
      nextPageCursor: payload.nextPageCursor ?? null,
      resultCount: Array.isArray(payload.results) ? payload.results.length : 0,
      results: Array.isArray(payload.results) ? payload.results : [],
      summaries: Array.isArray(payload.results)
        ? payload.results.map(summarizeResult)
        : [],
    }

    await ensureDir(options.outputDir)
    const outputPath = buildOutputPath(options.outputDir)
    await fs.writeFile(outputPath, JSON.stringify(output, null, 2), 'utf8')

    console.log(`Fetched ${output.resultCount} Reader highlight document(s).`)
    console.log(`Saved raw payload to ${outputPath}`)
    console.log('')
    console.log(JSON.stringify(output.summaries, null, 2))
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error))
    console.error('')
    console.error(usage)
    process.exitCode = 1
  }
}

await main()
