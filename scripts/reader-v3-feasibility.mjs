#!/usr/bin/env node
// @ts-nocheck

import fs from 'node:fs/promises'
import path from 'node:path'

const V2_BASE_URL = 'https://readwise.io/api/v2'
const V3_BASE_URL = 'https://readwise.io/api/v3'
const DEFAULT_OUTPUT_DIR = path.resolve(
  process.cwd(),
  'logs',
  'reader-v3-feasibility',
)

const usage = `Usage:
  node scripts/reader-v3-feasibility.mjs [--token "<token>"] [--max-books 20]

Examples:
  READWISE_ACCESS_TOKEN=xxx node scripts/reader-v3-feasibility.mjs --max-books 20
  node scripts/reader-v3-feasibility.mjs --token "xxx" --max-books 20
`

const stripWrappingLinkSyntax = (value) => {
  const trimmed = value.trim()

  const orgMatch = trimmed.match(/^\[\[([^[\]]+)\]\[[^[\]]+\]\]$/)
  if (orgMatch) return orgMatch[1] ?? trimmed

  const simpleBracketMatch = trimmed.match(/^\[\[([^[\]]+)\]\]$/)
  if (simpleBracketMatch) return simpleBracketMatch[1] ?? trimmed

  return trimmed
}

const normalizeComparableUrlV1 = (value) => {
  if (typeof value !== 'string') return null
  const stripped = stripWrappingLinkSyntax(value).trim()
  if (stripped.length === 0) return null
  return stripped.replace(/\/+$/, '')
}

const normalizeText = (value) => {
  if (typeof value !== 'string') return ''
  return value.replace(/\s+/g, ' ').trim()
}

const parseArgs = (argv) => {
  const [, , ...rest] = argv
  const options = {
    token: process.env.READWISE_ACCESS_TOKEN ?? '',
    maxBooks: 20,
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

    if (arg === '--max-books' && typeof value === 'string') {
      const parsed = Number.parseInt(value, 10)
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid --max-books value: ${value}`)
      }
      options.maxBooks = parsed
      index += 1
      continue
    }

    if (arg === '--output-dir' && typeof value === 'string') {
      options.outputDir = path.resolve(value)
      index += 1
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

const buildBaseHeaders = (token) => ({
  Authorization: `Token ${token}`,
  'Content-Type': 'application/json',
})

const fetchJson = async (url, token) => {
  const response = await fetch(url, {
    method: 'GET',
    headers: buildBaseHeaders(token),
  })

  if (!response.ok) {
    const body = await response.text()
    throw new Error(`${response.status} ${response.statusText}: ${body}`)
  }

  return response.json()
}

const buildExportUrl = ({ updatedAfter, pageCursor }) => {
  const url = new URL(`${V2_BASE_URL}/export/`)
  if (updatedAfter) url.searchParams.set('updatedAfter', updatedAfter)
  if (pageCursor) url.searchParams.set('pageCursor', pageCursor)
  return url.toString()
}

const buildReaderListUrl = ({ category, updatedAfter, pageCursor, limit = 100 }) => {
  const url = new URL(`${V3_BASE_URL}/list/`)
  if (category) url.searchParams.set('category', category)
  if (updatedAfter) url.searchParams.set('updatedAfter', updatedAfter)
  if (pageCursor) url.searchParams.set('pageCursor', pageCursor)
  if (limit) url.searchParams.set('limit', String(limit))
  return url.toString()
}

const collectBookCandidateUrls = (book) =>
  [book.source_url, book.unique_url]
    .map((value) => normalizeComparableUrlV1(value))
    .filter((value, index, array) => value && array.indexOf(value) === index)

const pickBookUpdatedAt = (book) => {
  const timestamps = Array.isArray(book.highlights)
    ? book.highlights.flatMap((highlight) =>
        [highlight.updated_at, highlight.created_at].filter(Boolean),
      )
    : []

  if (timestamps.length === 0) {
    return null
  }

  return timestamps.reduce((latest, current) =>
    current > latest ? current : latest,
  )
}

const loadRecentExportBooks = async (token, maxBooks) => {
  const books = []
  let pageCursor

  while (true) {
    const payload = await fetchJson(buildExportUrl({ pageCursor }), token)
    books.push(...(payload.results ?? []))

    if (books.length >= maxBooks) {
      return books.slice(0, maxBooks)
    }

    if (!payload.nextPageCursor) {
      return books
    }

    pageCursor = payload.nextPageCursor
  }
}

const loadReaderDocumentsForBooks = async (token, books, pageLimit = 20) => {
  const candidateUrlToBookIds = new Map()
  const unresolvedBookIds = new Set()
  const matchedDocs = new Map()
  const allVisitedDocs = []
  let pageCursor
  let pagesVisited = 0

  for (const book of books) {
    const candidateUrls = collectBookCandidateUrls(book)
    if (candidateUrls.length === 0) continue
    unresolvedBookIds.add(book.user_book_id)
    for (const candidateUrl of candidateUrls) {
      const existing = candidateUrlToBookIds.get(candidateUrl) ?? []
      if (!existing.includes(book.user_book_id)) {
        existing.push(book.user_book_id)
      }
      candidateUrlToBookIds.set(candidateUrl, existing)
    }
  }

  while (pagesVisited < pageLimit) {
    pagesVisited += 1
    const payload = await fetchJson(
      buildReaderListUrl({ pageCursor, limit: 100 }),
      token,
    )

    for (const document of payload.results ?? []) {
      allVisitedDocs.push(document)
      if (document.parent_id != null) continue

      const candidates = [
        normalizeComparableUrlV1(document.source_url),
        normalizeComparableUrlV1(document.url),
      ].filter((value, index, array) => value && array.indexOf(value) === index)

      for (const candidate of candidates) {
        const bookIds = candidateUrlToBookIds.get(candidate)
        if (!bookIds || bookIds.length === 0) continue
        for (const bookId of bookIds) {
          if (!unresolvedBookIds.has(bookId)) continue
          matchedDocs.set(bookId, document)
          unresolvedBookIds.delete(bookId)
        }
      }
    }

    if (!payload.nextPageCursor || unresolvedBookIds.size === 0) {
      return {
        matchedDocs,
        unresolvedBookIds,
        pagesVisited,
        visitedDocCount: allVisitedDocs.length,
      }
    }

    pageCursor = payload.nextPageCursor
  }

  return {
    matchedDocs,
    unresolvedBookIds,
    pagesVisited,
    visitedDocCount: allVisitedDocs.length,
  }
}

const loadReaderHighlightsForParents = async (
  token,
  parentIds,
  updatedAfter,
  pageLimit = 50,
) => {
  const targetParentIds = new Set(parentIds)
  const highlightsByParentId = new Map()
  let pageCursor
  let pagesVisited = 0
  let visitedHighlightCount = 0

  while (pagesVisited < pageLimit) {
    pagesVisited += 1
    const payload = await fetchJson(
      buildReaderListUrl({
        category: 'highlight',
        updatedAfter,
        pageCursor,
        limit: 100,
      }),
      token,
    )

    for (const result of payload.results ?? []) {
      visitedHighlightCount += 1
      const parentId = result.parent_id
      if (!parentId || !targetParentIds.has(parentId)) continue
      const existing = highlightsByParentId.get(parentId) ?? []
      existing.push(result)
      highlightsByParentId.set(parentId, existing)
    }

    if (!payload.nextPageCursor) {
      return { highlightsByParentId, pagesVisited, visitedHighlightCount }
    }

    pageCursor = payload.nextPageCursor
  }

  return { highlightsByParentId, pagesVisited, visitedHighlightCount }
}

const buildFieldCoverage = (matchedDocs) => {
  const fields = [
    'title',
    'author',
    'category',
    'source_url',
    'url',
    'summary',
    'notes',
    'image_url',
    'published_date',
    'tags',
    'site_name',
  ]

  const values = Object.fromEntries(fields.map((field) => [field, 0]))
  const total = matchedDocs.length

  for (const doc of matchedDocs) {
    for (const field of fields) {
      const value = doc[field]
      const present =
        value != null &&
        ((typeof value === 'string' && value.length > 0) ||
          (typeof value === 'object' && Object.keys(value).length > 0))
      if (present) {
        values[field] += 1
      }
    }
  }

  return Object.fromEntries(
    Object.entries(values).map(([field, count]) => [
      field,
      {
        count,
        total,
        ratio: total > 0 ? Number((count / total).toFixed(3)) : 0,
      },
    ]),
  )
}

const buildPerBookComparison = (books, matchedDocs, highlightsByParentId) =>
  books.map((book) => {
    const matchedDoc = matchedDocs.get(book.user_book_id) ?? null
    const exportHighlights = Array.isArray(book.highlights) ? book.highlights : []
    const readerHighlights = matchedDoc
      ? highlightsByParentId.get(matchedDoc.id) ?? []
      : []

    const exportTexts = new Set(
      exportHighlights.map((highlight) => normalizeText(highlight.text)).filter(Boolean),
    )
    const readerTexts = new Set(
      readerHighlights.map((highlight) => normalizeText(highlight.content)).filter(Boolean),
    )

    let overlapCount = 0
    for (const text of exportTexts) {
      if (readerTexts.has(text)) overlapCount += 1
    }

    return {
      userBookId: book.user_book_id,
      title: book.title,
      category: book.category,
      sourceUrl: book.source_url,
      exportHighlightCount: exportHighlights.length,
      readerDocumentMatched: matchedDoc != null,
      readerDocumentId: matchedDoc?.id ?? null,
      readerDocumentUrl: matchedDoc?.url ?? null,
      readerHighlightCount: readerHighlights.length,
      exactTextOverlapCount: overlapCount,
      exactTextOverlapRatio:
        exportTexts.size > 0
          ? Number((overlapCount / exportTexts.size).toFixed(3))
          : null,
    }
  })

const buildFeasibilitySummary = ({
  sampleSize,
  matchedDocumentCount,
  perBook,
  fieldCoverage,
}) => {
  const docsMatchedRatio =
    sampleSize > 0 ? Number((matchedDocumentCount / sampleSize).toFixed(3)) : 0
  const booksWithExactHighlightCoverage = perBook.filter(
    (item) =>
      item.readerDocumentMatched &&
      item.exportHighlightCount === item.readerHighlightCount &&
      item.exactTextOverlapRatio === 1,
  ).length
  const booksWithSomeHighlightMismatch = perBook.filter(
    (item) =>
      item.readerDocumentMatched &&
      (item.exportHighlightCount !== item.readerHighlightCount ||
        item.exactTextOverlapRatio !== 1),
  ).length

  return {
    docsMatchedRatio,
    booksWithExactHighlightCoverage,
    booksWithSomeHighlightMismatch,
    canSupportCurrentPageMetadata:
      fieldCoverage.title.ratio === 1 &&
      fieldCoverage.author.ratio === 1 &&
      fieldCoverage.category.ratio === 1 &&
      fieldCoverage.url.ratio === 1,
    needsMigrationForIdentityModel: true,
    needsNewCheckpointModel: true,
    suitableAsDirectDropInReplacement: false,
  }
}

const main = async () => {
  try {
    const options = parseArgs(process.argv)
    const books = await loadRecentExportBooks(options.token, options.maxBooks)
    const matchedDocsResult = await loadReaderDocumentsForBooks(
      options.token,
      books,
    )
    const matchedDocs = matchedDocsResult.matchedDocs

    const sampleUpdatedTimestamps = books
      .map((book) => pickBookUpdatedAt(book))
      .filter(Boolean)
      .sort()
    const earliestUpdatedAfter =
      sampleUpdatedTimestamps.length > 0 ? sampleUpdatedTimestamps[0] : null

    const readerHighlightsResult = await loadReaderHighlightsForParents(
      options.token,
      [...matchedDocs.values()].map((doc) => doc.id),
      earliestUpdatedAfter,
    )

    const matchedDocList = [...matchedDocs.values()]
    const fieldCoverage = buildFieldCoverage(matchedDocList)
    const perBook = buildPerBookComparison(
      books,
      matchedDocs,
      readerHighlightsResult.highlightsByParentId,
    )

    const report = {
      generatedAt: new Date().toISOString(),
      sampleSize: books.length,
      exportSample: books.map((book) => ({
        userBookId: book.user_book_id,
        title: book.title,
        category: book.category,
        sourceUrl: book.source_url,
        uniqueUrl: book.unique_url,
        exportHighlightCount: Array.isArray(book.highlights)
          ? book.highlights.length
          : 0,
        updatedAt: pickBookUpdatedAt(book),
      })),
      readerDocumentMatching: {
        pagesVisited: matchedDocsResult.pagesVisited,
        visitedDocCount: matchedDocsResult.visitedDocCount,
        matchedDocumentCount: matchedDocs.size,
        unmatchedBookIds: [...matchedDocsResult.unresolvedBookIds],
      },
      readerHighlightMatching: {
        updatedAfter: earliestUpdatedAfter,
        pagesVisited: readerHighlightsResult.pagesVisited,
        visitedHighlightCount: readerHighlightsResult.visitedHighlightCount,
      },
      fieldCoverage,
      perBook,
      feasibilitySummary: buildFeasibilitySummary({
        sampleSize: books.length,
        matchedDocumentCount: matchedDocs.size,
        perBook,
        fieldCoverage,
      }),
    }

    await ensureDir(options.outputDir)
    const outputPath = path.join(
      options.outputDir,
      `reader-v3-feasibility-${timestamp()}.json`,
    )
    await fs.writeFile(outputPath, JSON.stringify(report, null, 2), 'utf8')

    console.log(`Saved report to ${outputPath}`)
    console.log('')
    console.log(
      JSON.stringify(
        {
          sampleSize: report.sampleSize,
          readerDocumentMatching: report.readerDocumentMatching,
          readerHighlightMatching: report.readerHighlightMatching,
          feasibilitySummary: report.feasibilitySummary,
          firstFiveBooks: report.perBook.slice(0, 5),
        },
        null,
        2,
      ),
    )
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error))
    console.error('')
    console.error(usage)
    process.exitCode = 1
  }
}

await main()
