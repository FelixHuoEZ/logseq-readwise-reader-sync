import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

import { loadCurrentGraphContextV1 } from './load-current-graph-context'
import { normalizeComparableUrlV1 } from './normalize-comparable-url'
import type {
  GraphPageCandidateV1,
  GraphPageSnapshotSourceV1,
  GraphSnapshotV1,
} from './types'

const READWISE_PAGE_PREFIX = 'ReadwiseHighlights___'

const toPageTitle = (page: GraphPageSnapshotSourceV1): string =>
  (typeof page.originalName === 'string' && page.originalName) ||
  (typeof page.title === 'string' && page.title) ||
  (typeof page.name === 'string' && page.name) ||
  ''

const toPagePath = (page: GraphPageSnapshotSourceV1): string | null => {
  if (typeof page.path === 'string' && page.path.length > 0) {
    return page.path
  }

  if (
    page.file &&
    typeof page.file === 'object' &&
    'path' in page.file &&
    typeof page.file.path === 'string' &&
    page.file.path.length > 0
  ) {
    return page.file.path
  }

  return null
}

const safeDecodeURIComponent = (value: string): string => {
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

const stripFileExtension = (value: string): string =>
  value.replace(/\.(md|markdown|org)$/i, '')

const stripWrappingQuotes = (value: string): string =>
  value.replace(/^["'“”‘’]+/, '').replace(/["'“”‘’]+$/, '')

const normalizeBridgeTitle = (value: string): string =>
  stripWrappingQuotes(value)
    .replace(/^\\#/, '#')
    .replace(/\s+/g, ' ')
    .trim()

const normalizePropertyKey = (value: string): string =>
  value.toLowerCase().replace(/[^a-z0-9]/g, '')

const findPropertyValue = (
  properties: Record<string, unknown> | undefined,
  expectedKeys: string[],
): unknown => {
  if (!properties) return null

  const expected = new Set(expectedKeys.map(normalizePropertyKey))

  for (const [key, value] of Object.entries(properties)) {
    if (expected.has(normalizePropertyKey(key))) {
      return value
    }
  }

  return null
}

const extractStringValues = (value: unknown): string[] => {
  if (typeof value === 'string') {
    const trimmed = value.trim()
    return trimmed.length > 0 ? [trimmed] : []
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return [String(value)]
  }

  if (Array.isArray(value)) {
    return value.flatMap((item) => extractStringValues(item))
  }

  return []
}

const extractReadwiseBookId = (
  page: GraphPageSnapshotSourceV1,
): number | null => {
  const rawValue = findPropertyValue(page.properties, ['rw-id', 'rwid'])
  const [firstValue] = extractStringValues(rawValue)

  if (!firstValue) return null

  const parsed = Number.parseInt(firstValue, 10)
  return Number.isFinite(parsed) ? parsed : null
}

const extractCanonicalUrls = (
  page: GraphPageSnapshotSourceV1,
): string[] => {
  const keys = ['link', 'rw-source-url', 'rw-unique-url', 'rw-readwise-url']
  const urls = new Set<string>()

  for (const key of keys) {
    const rawValue = findPropertyValue(page.properties, [key])
    for (const candidate of extractStringValues(rawValue)) {
      const normalized = normalizeComparableUrlV1(candidate)
      if (normalized) {
        urls.add(normalized)
      }
    }
  }

  return [...urls]
}

const buildReadwiseBridgeTitles = (
  page: GraphPageSnapshotSourceV1,
): string[] => {
  const values = new Set<string>()
  const path = toPagePath(page)
  const rawCandidates = [toPageTitle(page)]

  if (path) {
    rawCandidates.push(path.split('/').pop() ?? path)
  }

  for (const rawCandidate of rawCandidates) {
    const decoded = stripFileExtension(safeDecodeURIComponent(rawCandidate))

    if (!decoded.startsWith(READWISE_PAGE_PREFIX)) {
      continue
    }

    const withoutPrefix = decoded.slice(READWISE_PAGE_PREFIX.length).trim()
    const variants = [
      withoutPrefix,
      withoutPrefix.replace(/___$/, '').trim(),
      withoutPrefix.replace(/\.{3}___$/, '').trim(),
      withoutPrefix.replace(/…___$/, '').trim(),
    ]

    for (const variant of variants) {
      const normalized = normalizeBridgeTitle(variant)
      if (normalized.length > 0) {
        values.add(normalized)
      }
    }
  }

  return [...values]
}

const toPageCandidate = (
  page: GraphPageSnapshotSourceV1,
  matchKind: GraphPageCandidateV1['matchKind'] = 'exact_title',
): GraphPageCandidateV1 => ({
  pageUuid: page.uuid,
  pageTitle: toPageTitle(page),
  path: toPagePath(page),
  matchKind,
})

const pushCandidate = (
  index: Record<string, GraphPageCandidateV1[]>,
  key: string,
  candidate: GraphPageCandidateV1,
) => {
  const existingCandidates = index[key] ?? []
  if (existingCandidates.some((existing) => existing.pageUuid === candidate.pageUuid)) {
    return
  }

  existingCandidates.push(candidate)
  index[key] = existingCandidates
}

export const buildGraphSnapshotV1 = (
  graphId: string,
  pages: GraphPageSnapshotSourceV1[],
): GraphSnapshotV1 => {
  const pageUuidExists: Record<string, boolean> = {}
  const pagesByExactTitle: Record<string, GraphPageCandidateV1[]> = {}
  const pagesByBridgeTitle: Record<string, GraphPageCandidateV1[]> = {}
  const pagesByReadwiseBookId: Record<string, GraphPageCandidateV1[]> = {}
  const pagesByCanonicalUrl: Record<string, GraphPageCandidateV1[]> = {}

  for (const page of pages) {
    if (typeof page.uuid !== 'string' || page.uuid.length === 0) continue

    pageUuidExists[page.uuid] = true

    const pageTitle = toPageTitle(page)
    if (pageTitle.length > 0) {
      pushCandidate(
        pagesByExactTitle,
        pageTitle,
        toPageCandidate(page, 'exact_title'),
      )
    }

    for (const bridgeTitle of buildReadwiseBridgeTitles(page)) {
      pushCandidate(
        pagesByBridgeTitle,
        bridgeTitle,
        toPageCandidate(page, 'readwise_page_title'),
      )
    }

    const readwiseBookId = extractReadwiseBookId(page)
    if (readwiseBookId != null) {
      pushCandidate(
        pagesByReadwiseBookId,
        String(readwiseBookId),
        toPageCandidate(page, 'rw_id'),
      )
    }

    for (const canonicalUrl of extractCanonicalUrls(page)) {
      pushCandidate(
        pagesByCanonicalUrl,
        canonicalUrl,
        toPageCandidate(page, 'property_url'),
      )
    }
  }

  return {
    graphId,
    pageUuidExists,
    pagesByExactTitle,
    pagesByBridgeTitle,
    pagesByReadwiseBookId,
    pagesByCanonicalUrl,
  }
}

export const loadCurrentGraphSnapshotV1 = async (): Promise<GraphSnapshotV1> => {
  const [graphContext, pages] = await Promise.all([
    loadCurrentGraphContextV1(),
    logseq.Editor.getAllPages(),
  ])

  return buildGraphSnapshotV1(
    graphContext.graphId,
    (pages ?? []) as PageEntity[],
  )
}
