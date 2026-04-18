import type { GraphReaderSyncCacheV1 } from '../cache/reader-sync-cache'
import {
  describeUnknownError,
  logReadwiseDebug,
  logReadwiseInfo,
  logReadwiseWarn,
} from '../logging'
import type { ReaderDocument } from '../types'
import type { ReadwiseClient } from './index'
import {
  decideReaderDocumentHighlightDetailsStrategy,
  tryEnrichReaderDocumentHighlightsViaMcp,
  reuseCachedReaderDocumentHighlightDetails,
} from './reader-document-highlights'

export type ReaderPreviewLoadMode =
  | 'full-library-scan'
  | 'incremental-window'
  | 'cached-full-rebuild'
  | 'snapshot-only-refresh'

export type ReaderParentMetadataMode = 'cache_first' | 'always_refresh'

export interface ReaderPreviewBook {
  document: ReaderDocument
  highlights: ReaderDocument[]
  highlightCoverage: ReaderPreviewLoadMode
}

export interface ReaderPreviewLoadStats {
  highlightPagesScanned: number
  highlightsScanned: number
  parentDocumentsIdentified: number
  pagesTargeted: number
  pagesProcessed: number
  estimatedHighlightPages: number | null
  estimatedHighlightResults: number | null
  latestHighlightUpdatedAt: string | null
  usedCachedHighlightSnapshot: boolean
  staleHighlightDeletionRisk: boolean
  completeHighlightSnapshotRefreshed: boolean
  parentMetadataCacheHits: number
  parentMetadataRemoteFetches: number
  documentHighlightDetailCalls: number
  documentHighlightDetailSkippedNoParentMetadata: number
  documentHighlightDetailSkippedNoRichMedia: number
  documentHighlightDetailSkippedVideo: number
  documentHighlightDetailSkippedResolved: number
  documentHighlightDetailMissingInReader: number
  fetchHighlightsDurationMs: number
  fetchDocumentsDurationMs: number
}

export type ReaderPreviewLoadPhase = 'fetch-highlights' | 'fetch-documents'

export interface ReaderPreviewLoadResumeState {
  phase: ReaderPreviewLoadPhase
  mode: ReaderPreviewLoadMode
  maxDocuments: number | null
  maxHighlightPages: number | null
  targetParentIds: string[]
  highlightsByParent: Array<[string, ReaderDocument[]]>
  seenHighlightIds: string[]
  latestHighlightByParent: Array<[string, ReaderDocument]>
  totalHighlights: number
  pageNumber: number
  pageCursor: string | null
  initialTotalPages: number | null
  initialTotalResults: number | null
  fetchHighlightsDurationMs: number
  selectedParentIds: string[]
  previewBooks: ReaderPreviewBook[]
  documentIndex: number
  fetchDocumentsDurationMs: number
}

export class ReaderPreviewLoadResumeError extends Error {
  resumeState: ReaderPreviewLoadResumeState

  constructor(message: string, resumeState: ReaderPreviewLoadResumeState) {
    super(message)
    this.name = 'ReaderPreviewLoadResumeError'
    this.resumeState = resumeState
  }
}

export const isReaderPreviewLoadResumeError = (
  error: unknown,
): error is ReaderPreviewLoadResumeError =>
  error instanceof ReaderPreviewLoadResumeError

export interface ReaderPreviewLoadResult {
  books: ReaderPreviewBook[]
  stats: ReaderPreviewLoadStats
}

export interface LoadReaderPreviewBooksProgress {
  phase:
    | 'fetch-highlights'
    | 'fetch-notes'
    | 'refresh-snapshot'
    | 'fetch-documents'
  pageNumber?: number
  totalPages?: number
  totalResults?: number
  uniqueParents?: number
  totalHighlights?: number
  totalNotes?: number
  completed?: number
  total?: number
  pageTitle?: string | null
}

export interface LoadReaderPreviewBooksOptions {
  maxDocuments?: number | null
  mode?: ReaderPreviewLoadMode
  maxHighlightPages?: number
  updatedAfter?: string
  resumeState?: ReaderPreviewLoadResumeState
  previewCache?: GraphReaderSyncCacheV1 | null
  parentMetadataMode?: ReaderParentMetadataMode
  readerAuthToken?: string | null
  logPrefix?: string
  onProgress?: (progress: LoadReaderPreviewBooksProgress) => void
}

export interface LoadReaderPreviewBooksByParentIdsOptions {
  parentIds: readonly string[]
  previewCache: GraphReaderSyncCacheV1
  parentMetadataMode?: ReaderParentMetadataMode
  readerAuthToken?: string | null
  logPrefix?: string
  highlightCoverage?: ReaderPreviewLoadMode
}

export interface LoadReaderPreviewBooksByParentIdsResult {
  books: ReaderPreviewBook[]
  unresolvedParentIds: string[]
  parentMetadataCacheHits: number
  parentMetadataRemoteFetches: number
  documentHighlightDetailCalls: number
  documentHighlightDetailSkippedNoRichMedia: number
  documentHighlightDetailSkippedVideo: number
  documentHighlightDetailSkippedResolved: number
  documentHighlightDetailMissingInReader: number
  fetchDocumentsDurationMs: number
}

const sortByCreatedAtAscending = (left: ReaderDocument, right: ReaderDocument) =>
  left.created_at.localeCompare(right.created_at)

const sortByUpdatedAtDescending = (left: ReaderDocument, right: ReaderDocument) =>
  right.updated_at.localeCompare(left.updated_at)

const getDocumentContentText = (document: ReaderDocument) =>
  typeof document.content === 'string' ? document.content.trim() : ''

const getDocumentNotesText = (document: ReaderDocument) =>
  typeof document.notes === 'string' ? document.notes.trim() : ''

const joinUniqueNonEmptySections = (
  values: readonly (string | null | undefined)[],
): string | null => {
  const seen = new Set<string>()
  const sections: string[] = []

  for (const value of values) {
    const trimmed = typeof value === 'string' ? value.trim() : ''
    if (trimmed.length === 0 || seen.has(trimmed)) continue

    seen.add(trimmed)
    sections.push(trimmed)
  }

  return sections.length > 0 ? sections.join('\n\n') : null
}

const mergeHighlightWithAttachedNotes = (
  highlight: ReaderDocument,
  noteDocuments: readonly ReaderDocument[],
): ReaderDocument => {
  const sortedNotes = [...noteDocuments].sort(sortByCreatedAtAscending)
  const mergedNotes = joinUniqueNonEmptySections(
    sortedNotes.length > 0
      ? sortedNotes.map((note) => getDocumentContentText(note))
      : [getDocumentNotesText(highlight)],
  )
  const latestNoteUpdatedAt = getLatestUpdatedAt(sortedNotes)
  const mergedUpdatedAt =
    latestNoteUpdatedAt != null && latestNoteUpdatedAt > highlight.updated_at
      ? latestNoteUpdatedAt
      : highlight.updated_at

  if (
    mergedNotes === getDocumentNotesText(highlight) &&
    mergedUpdatedAt === highlight.updated_at
  ) {
    return highlight
  }

  return {
    ...highlight,
    notes: mergedNotes,
    updated_at: mergedUpdatedAt,
  }
}

const uniqueParentIds = (parentIds: readonly string[]) =>
  [...new Set(parentIds.filter((parentId) => typeof parentId === 'string' && parentId.length > 0))]

const sleep = async (milliseconds: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, milliseconds)
  })

const isRetriableReaderListError = (error: unknown) => {
  const message = describeUnknownError(error)

  return (
    error instanceof TypeError ||
    /Failed to fetch|NetworkError|ERR_CONNECTION_CLOSED|ERR_CONNECTION_RESET|ERR_NETWORK_CHANGED|ERR_INTERNET_DISCONNECTED|fetch/i.test(
      message,
    )
  )
}

const listReaderDocumentsWithRetry = async (
  client: ReadwiseClient,
  params: Parameters<ReadwiseClient['listReaderDocuments']>[0],
  options: {
    logPrefix?: string
    stage: 'fetch-highlights' | 'fetch-notes' | 'fetch-documents'
    pageNumber?: number
    parentId?: string
  },
) => {
  let lastError: unknown = null
  const totalAttempts = 3

  for (let attempt = 0; attempt < totalAttempts; attempt += 1) {
    try {
      return await client.listReaderDocuments(params)
    } catch (error) {
      lastError = error

      if (!isRetriableReaderListError(error) || attempt === totalAttempts - 1) {
        break
      }

      if (options.logPrefix) {
        logReadwiseWarn(options.logPrefix, 'Reader API list request failed; retrying', {
          stage: options.stage,
          pageNumber: options.pageNumber ?? null,
          parentId: options.parentId ?? null,
          attempt: attempt + 1,
          totalAttempts,
          formattedError: describeUnknownError(error),
        })
      }

      await sleep(1000 * (attempt + 1))
    }
  }

  const lastMessage = describeUnknownError(lastError)
  const stageLabel =
    options.stage === 'fetch-highlights'
      ? `Reader highlight scan request${
          options.pageNumber != null ? ` at page ${options.pageNumber}` : ''
        }`
      : options.stage === 'fetch-notes'
        ? `Reader note scan request${
            options.pageNumber != null ? ` at page ${options.pageNumber}` : ''
          }`
      : `Reader parent document fetch${
          options.parentId ? ` for ${options.parentId}` : ''
        }`
  const detail =
    lastMessage === 'Failed to fetch'
      ? 'Network request failed before Readwise returned a response.'
      : lastMessage

  throw new Error(
    `${stageLabel} failed after ${totalAttempts} attempt(s). ${detail}`,
  )
}

const getLatestUpdatedAt = (documents: Iterable<ReaderDocument>) => {
  let latest: string | null = null

  for (const document of documents) {
    if (typeof document.updated_at !== 'string') continue
    if (latest == null || document.updated_at > latest) {
      latest = document.updated_at
    }
  }

  return latest
}

const flattenHighlightsByParent = (highlightsByParent: Map<string, ReaderDocument[]>) =>
  [...highlightsByParent.values()].flat()

export const loadReaderPreviewBooksByParentIds = async (
  client: ReadwiseClient,
  options: LoadReaderPreviewBooksByParentIdsOptions,
): Promise<LoadReaderPreviewBooksByParentIdsResult> => {
  const parentIds = uniqueParentIds(options.parentIds)
  if (parentIds.length === 0) {
    return {
      books: [],
      unresolvedParentIds: [],
      parentMetadataCacheHits: 0,
      parentMetadataRemoteFetches: 0,
      documentHighlightDetailCalls: 0,
      documentHighlightDetailSkippedNoRichMedia: 0,
      documentHighlightDetailSkippedVideo: 0,
      documentHighlightDetailSkippedResolved: 0,
      documentHighlightDetailMissingInReader: 0,
      fetchDocumentsDurationMs: 0,
    }
  }

  const fetchDocumentsStartedAt = Date.now()
  const parentMetadataMode = options.parentMetadataMode ?? 'cache_first'
  const unresolvedParentIds: string[] = []
  let parentMetadataCacheHits = 0
  let parentMetadataRemoteFetches = 0
  let documentHighlightDetailCalls = 0
  let documentHighlightDetailSkippedNoRichMedia = 0
  let documentHighlightDetailSkippedVideo = 0
  let documentHighlightDetailSkippedResolved = 0
  let documentHighlightDetailMissingInReader = 0
  let highlightsByParent = new Map<string, ReaderDocument[]>()
  let cachedParentDocuments = new Map<string, ReaderDocument>()
  const enrichedHighlightsToPersist: ReaderDocument[] = []

  try {
    highlightsByParent = await options.previewCache.loadGroupedHighlightsByParent(parentIds)
  } catch (error) {
    if (options.logPrefix) {
      logReadwiseWarn(
        options.logPrefix,
        'failed to load cached highlights for queued retry pages',
        {
          parentCount: parentIds.length,
          formattedError: describeUnknownError(error),
        },
      )
    }

    return {
      books: [],
      unresolvedParentIds: parentIds,
      parentMetadataCacheHits,
      parentMetadataRemoteFetches,
      documentHighlightDetailCalls,
      documentHighlightDetailSkippedNoRichMedia,
      documentHighlightDetailSkippedVideo,
      documentHighlightDetailSkippedResolved,
      documentHighlightDetailMissingInReader,
      fetchDocumentsDurationMs: Date.now() - fetchDocumentsStartedAt,
    }
  }

  if (parentMetadataMode === 'cache_first') {
    try {
      cachedParentDocuments = await options.previewCache.getCachedParentDocuments(
        parentIds,
      )
    } catch (error) {
      if (options.logPrefix) {
        logReadwiseWarn(
          options.logPrefix,
          'failed to load cached parent metadata for queued retry pages',
          {
            parentCount: parentIds.length,
            formattedError: describeUnknownError(error),
          },
        )
      }
    }
  }

  const fetchedParentDocuments: ReaderDocument[] = []
  const books: ReaderPreviewBook[] = []

  for (const parentId of parentIds) {
    const highlights = [...(highlightsByParent.get(parentId) ?? [])].sort(
      sortByCreatedAtAscending,
    )

    if (highlights.length === 0) {
      unresolvedParentIds.push(parentId)

      if (options.logPrefix) {
        logReadwiseWarn(
          options.logPrefix,
          'queued retry page has no cached highlights; leaving it queued for a future run',
          {
            parentId,
          },
        )
      }

      continue
    }

    const cachedDocument =
      parentMetadataMode === 'cache_first'
        ? cachedParentDocuments.get(parentId) ?? null
        : null
    let document = cachedDocument

    if (document) {
      parentMetadataCacheHits += 1
    } else {
      try {
        const response = await listReaderDocumentsWithRetry(
          client,
          {
            id: parentId,
            limit: 1,
            withHtmlContent: true,
          },
          {
            logPrefix: options.logPrefix,
            stage: 'fetch-documents',
            parentId,
          },
        )

        document = response.results[0] ?? null
      } catch (error) {
        unresolvedParentIds.push(parentId)

        if (options.logPrefix) {
          logReadwiseWarn(
            options.logPrefix,
            'failed to reload queued retry page parent metadata; keeping it queued',
            {
              parentId,
              formattedError: describeUnknownError(error),
            },
          )
        }

        continue
      }

      if (document) {
        parentMetadataRemoteFetches += 1
        fetchedParentDocuments.push(document)
      }
    }

    if (!document) {
      unresolvedParentIds.push(parentId)
      continue
    }

    const enrichedHighlightsResult = await tryEnrichReaderDocumentHighlightsViaMcp({
      token: options.readerAuthToken,
      document,
      highlights,
      logPrefix: options.logPrefix,
    })

    if (enrichedHighlightsResult.attempted) {
      documentHighlightDetailCalls += 1
    }
    if (enrichedHighlightsResult.missingInReader) {
      documentHighlightDetailMissingInReader += 1
    }
    switch (enrichedHighlightsResult.skippedReason) {
      case 'video':
        documentHighlightDetailSkippedVideo += 1
        break
      case 'no_rich_media':
        documentHighlightDetailSkippedNoRichMedia += 1
        break
      case 'already_resolved':
        documentHighlightDetailSkippedResolved += 1
        break
      default:
        break
    }

    const resolvedHighlights = [...enrichedHighlightsResult.highlights].sort(
      sortByCreatedAtAscending,
    )

    if (enrichedHighlightsResult.changedCount > 0) {
      highlightsByParent.set(parentId, resolvedHighlights)
      enrichedHighlightsToPersist.push(...resolvedHighlights)
    }

    books.push({
      document,
      highlights: resolvedHighlights,
      highlightCoverage: options.highlightCoverage ?? 'cached-full-rebuild',
    })
  }

  if (enrichedHighlightsToPersist.length > 0) {
    try {
      await options.previewCache.putHighlights(enrichedHighlightsToPersist)
    } catch (error) {
      if (options.logPrefix) {
        logReadwiseWarn(
          options.logPrefix,
          'failed to persist enriched Reader highlights for queued retry pages',
          {
            highlightCount: enrichedHighlightsToPersist.length,
            formattedError: describeUnknownError(error),
          },
        )
      }
    }
  }

  if (fetchedParentDocuments.length > 0) {
    try {
      await options.previewCache.putParentDocuments(fetchedParentDocuments)
    } catch (error) {
      if (options.logPrefix) {
        logReadwiseWarn(
          options.logPrefix,
          'failed to persist refreshed parent metadata for queued retry pages',
          {
            documentCount: fetchedParentDocuments.length,
            formattedError: describeUnknownError(error),
          },
        )
      }
    }
  }

  return {
    books,
    unresolvedParentIds,
    parentMetadataCacheHits,
    parentMetadataRemoteFetches,
    documentHighlightDetailCalls,
    documentHighlightDetailSkippedNoRichMedia,
    documentHighlightDetailSkippedVideo,
    documentHighlightDetailSkippedResolved,
    documentHighlightDetailMissingInReader,
    fetchDocumentsDurationMs: Date.now() - fetchDocumentsStartedAt,
  }
}

export const loadReaderPreviewBooks = async (
  client: ReadwiseClient,
  options: LoadReaderPreviewBooksOptions = {},
): Promise<ReaderPreviewLoadResult> => {
  const mode = options.mode ?? 'full-library-scan'
  const maxDocuments =
    options.maxDocuments === null
      ? null
      : typeof options.maxDocuments === 'number' && Number.isFinite(options.maxDocuments)
        ? options.maxDocuments > 0
          ? Math.floor(options.maxDocuments)
          : null
        : mode === 'incremental-window'
          ? null
          : 20
  const effectiveMaxHighlightPages =
    typeof options.maxHighlightPages === 'number' && options.maxHighlightPages > 0
      ? Math.floor(options.maxHighlightPages)
      : null
  const resumeState = options.resumeState
  const previewCache = options.previewCache ?? null
  const parentMetadataMode =
    options.parentMetadataMode ??
    (mode === 'incremental-window' ? 'cache_first' : 'always_refresh')
  const targetParentIds = [...(resumeState?.targetParentIds ?? [])]
  const seenParentIds = new Set(targetParentIds)
  const highlightsByParent = new Map<string, ReaderDocument[]>(
    resumeState?.highlightsByParent ?? [],
  )
  const highlightsById = new Map<string, ReaderDocument>()
  for (const highlights of highlightsByParent.values()) {
    for (const highlight of highlights) {
      highlightsById.set(highlight.id, highlight)
    }
  }
  const seenHighlightIds = new Set(resumeState?.seenHighlightIds ?? [])
  const latestHighlightByParent = new Map<string, ReaderDocument>(
    resumeState?.latestHighlightByParent ?? [],
  )
  const resumePhase = resumeState?.phase ?? 'fetch-highlights'
  let totalHighlights = resumeState?.totalHighlights ?? 0
  let highlightPageNumber = resumeState?.pageNumber ?? 0
  let highlightPageCursor =
    resumePhase === 'fetch-highlights' ? resumeState?.pageCursor ?? null : null
  let highlightInitialTotalPages = resumeState?.initialTotalPages ?? null
  let highlightInitialTotalResults = resumeState?.initialTotalResults ?? null
  let notePageNumber = 0
  let notePageCursor: string | null = null
  let noteInitialTotalPages: number | null = null
  let noteInitialTotalResults: number | null = null
  let latestHighlightUpdatedAt = getLatestUpdatedAt(latestHighlightByParent.values())
  let usedCachedHighlightSnapshot = false
  let staleHighlightDeletionRisk = false
  let completeHighlightSnapshotRefreshed = false
  let parentMetadataCacheHits = 0
  let parentMetadataRemoteFetches = 0
  let documentHighlightDetailCalls = 0
  let documentHighlightDetailSkippedNoParentMetadata = 0
  let documentHighlightDetailSkippedNoRichMedia = 0
  let documentHighlightDetailSkippedVideo = 0
  let documentHighlightDetailSkippedResolved = 0
  let documentHighlightDetailMissingInReader = 0
  const fetchHighlightsStartedAt =
    Date.now() - (resumeState?.fetchHighlightsDurationMs ?? 0)

  const sortedParentIds = () =>
    [...targetParentIds].sort((left, right) => {
      const leftLatest = latestHighlightByParent.get(left)
      const rightLatest = latestHighlightByParent.get(right)
      if (!leftLatest && !rightLatest) return 0
      if (!leftLatest) return 1
      if (!rightLatest) return -1
      return sortByUpdatedAtDescending(leftLatest, rightLatest)
    })

  const buildResumeState = (
    phase: ReaderPreviewLoadPhase,
    extra: Partial<ReaderPreviewLoadResumeState> = {},
  ): ReaderPreviewLoadResumeState => ({
    phase,
    mode,
    maxDocuments,
    maxHighlightPages: effectiveMaxHighlightPages,
    targetParentIds: [...targetParentIds],
    highlightsByParent: [...highlightsByParent.entries()],
    seenHighlightIds: [...seenHighlightIds],
    latestHighlightByParent: [...latestHighlightByParent.entries()],
    totalHighlights,
    pageNumber: extra.pageNumber ?? highlightPageNumber,
    pageCursor: extra.pageCursor ?? highlightPageCursor,
    initialTotalPages: extra.initialTotalPages ?? highlightInitialTotalPages,
    initialTotalResults:
      extra.initialTotalResults ?? highlightInitialTotalResults,
    fetchHighlightsDurationMs: Date.now() - fetchHighlightsStartedAt,
    selectedParentIds: extra.selectedParentIds ?? [],
    previewBooks: extra.previewBooks ?? [],
    documentIndex: extra.documentIndex ?? 0,
    fetchDocumentsDurationMs: extra.fetchDocumentsDurationMs ?? 0,
  })

  const upsertResolvedHighlight = (highlight: ReaderDocument) => {
    const parentId =
      typeof highlight.parent_id === 'string' && highlight.parent_id.length > 0
        ? highlight.parent_id
        : null
    if (!parentId) return false

    if (
      typeof highlight.updated_at === 'string' &&
      (latestHighlightUpdatedAt == null ||
        highlight.updated_at > latestHighlightUpdatedAt)
    ) {
      latestHighlightUpdatedAt = highlight.updated_at
    }

    if (!seenParentIds.has(parentId)) {
      seenParentIds.add(parentId)
      targetParentIds.push(parentId)
    }

    const existingHighlights = highlightsByParent.get(parentId) ?? []
    const existingIndex = existingHighlights.findIndex(
      (existingHighlight) => existingHighlight.id === highlight.id,
    )

    if (existingIndex >= 0) {
      existingHighlights[existingIndex] = highlight
    } else {
      existingHighlights.push(highlight)
      if (!seenHighlightIds.has(highlight.id)) {
        seenHighlightIds.add(highlight.id)
        totalHighlights += 1
      }
    }

    highlightsByParent.set(parentId, existingHighlights)
    highlightsById.set(highlight.id, highlight)

    const latestExisting = latestHighlightByParent.get(parentId)
    if (
      !latestExisting ||
      latestExisting.id === highlight.id ||
      highlight.updated_at > latestExisting.updated_at
    ) {
      latestHighlightByParent.set(parentId, highlight)
    }

    return true
  }

  const ingestHighlight = (highlight: ReaderDocument) => {
    if (seenHighlightIds.has(highlight.id)) return false

    const text = getDocumentContentText(highlight)
    if (text.length === 0) return false

    return upsertResolvedHighlight(highlight)
  }

  if (
    mode === 'cached-full-rebuild' &&
    resumePhase === 'fetch-highlights'
  ) {
    if (!previewCache) {
      throw new Error('Cached rebuild requires local Reader cache support.')
    }

    const cacheState = await previewCache.getHighlightCacheState()
    if (!cacheState?.hasFullLibrarySnapshot) {
      throw new Error(
        'Cached rebuild requires a successful Full Reconcile first to build a complete local highlight snapshot.',
      )
    }

    const cachedHighlightsByParent = await previewCache.loadGroupedHighlightsByParent()
    usedCachedHighlightSnapshot = true
    staleHighlightDeletionRisk = cacheState.staleDeletionRisk
    latestHighlightUpdatedAt =
      cacheState.latestHighlightUpdatedAt ?? latestHighlightUpdatedAt

    for (const highlights of cachedHighlightsByParent.values()) {
      for (const highlight of highlights) {
        ingestHighlight(highlight)
      }
    }

    options.onProgress?.({
      phase: 'fetch-highlights',
      pageNumber: 1,
      totalPages: 1,
      totalResults: cacheState.highlightCount,
      uniqueParents: targetParentIds.length,
      totalHighlights,
    })
  } else {
    let remoteHighlightScanExhaustive = resumePhase !== 'fetch-highlights'
    let remoteNoteScanExhaustive = resumePhase === 'fetch-documents'
    let totalNotes = 0

    if (resumePhase === 'fetch-highlights') {
      while (true) {
      if (
        effectiveMaxHighlightPages != null &&
        highlightPageNumber >= effectiveMaxHighlightPages
      ) {
        break
      }

      const nextPageNumber = highlightPageNumber + 1
      const pageStartedAt = Date.now()
      let response: Awaited<ReturnType<ReadwiseClient['listReaderDocuments']>>

      try {
        response = await listReaderDocumentsWithRetry(
          client,
          {
            category: 'highlight',
            limit: 100,
            pageCursor: highlightPageCursor ?? undefined,
            updatedAfter: options.updatedAfter,
            withHtmlContent: true,
          },
          {
            logPrefix: options.logPrefix,
            stage: 'fetch-highlights',
            pageNumber: nextPageNumber,
          },
        )
      } catch (error) {
        throw new ReaderPreviewLoadResumeError(
          describeUnknownError(error),
          buildResumeState('fetch-highlights'),
        )
      }

      highlightPageNumber = nextPageNumber

      if (highlightInitialTotalPages == null) {
        highlightInitialTotalPages =
          typeof response.count === 'number' && response.count > 0
            ? Math.ceil(response.count / 100)
            : null
        highlightInitialTotalResults =
          typeof response.count === 'number' && response.count > 0
            ? response.count
            : null
      }

      for (const highlight of response.results) {
        ingestHighlight(highlight)
      }

      const estimatedTotalPages = Math.max(
        highlightInitialTotalPages ??
          effectiveMaxHighlightPages ??
          highlightPageNumber,
        highlightPageNumber,
      )
      const displayTotalPages =
        effectiveMaxHighlightPages != null
          ? Math.min(estimatedTotalPages, effectiveMaxHighlightPages)
          : estimatedTotalPages

      if (options.logPrefix) {
        logReadwiseDebug(options.logPrefix, 'fetched highlight page', {
          pageNumber: highlightPageNumber,
          estimatedTotalPages: displayTotalPages,
          estimatedTotalResults: highlightInitialTotalResults,
          responseResultCount: response.results.length,
          totalHighlights,
          uniqueParents: targetParentIds.length,
          hasNextPage: !!response.nextPageCursor,
          cappedByDebugLimit:
            effectiveMaxHighlightPages != null &&
            highlightPageNumber >= effectiveMaxHighlightPages,
          pageDurationMs: Date.now() - pageStartedAt,
        })
      }

      options.onProgress?.({
        phase: 'fetch-highlights',
        pageNumber: highlightPageNumber,
        totalPages: displayTotalPages,
        totalResults: highlightInitialTotalResults ?? undefined,
        uniqueParents: targetParentIds.length,
        totalHighlights,
      })

      highlightPageCursor = response.nextPageCursor

      if (!response.nextPageCursor) {
        remoteHighlightScanExhaustive = true
        break
      }

      if (
        mode === 'incremental-window' &&
        maxDocuments != null &&
        targetParentIds.length >= maxDocuments
      ) {
        break
      }
    }
    }

    const noteDocumentsByHighlightId = new Map<string, ReaderDocument[]>()
    const seenNoteIds = new Set<string>()

    const recordHighlightNote = (noteDocument: ReaderDocument) => {
      const highlightId =
        typeof noteDocument.parent_id === 'string' &&
        noteDocument.parent_id.length > 0
          ? noteDocument.parent_id
          : null
      if (!highlightId || seenNoteIds.has(noteDocument.id)) return false

      seenNoteIds.add(noteDocument.id)

      const text = getDocumentContentText(noteDocument)
      if (text.length === 0) return false

      totalNotes += 1
      const existingNotes = noteDocumentsByHighlightId.get(highlightId) ?? []
      existingNotes.push(noteDocument)
      noteDocumentsByHighlightId.set(highlightId, existingNotes)
      return true
    }

    if (resumePhase === 'fetch-highlights') {
      while (true) {
      if (
        effectiveMaxHighlightPages != null &&
        notePageNumber >= effectiveMaxHighlightPages
      ) {
        remoteNoteScanExhaustive = false
        break
      }

      const nextPageNumber = notePageNumber + 1
      const pageStartedAt = Date.now()
      let response: Awaited<ReturnType<ReadwiseClient['listReaderDocuments']>>

      try {
        response = await listReaderDocumentsWithRetry(
          client,
          {
            category: 'note',
            limit: 100,
            pageCursor: notePageCursor ?? undefined,
            updatedAfter: options.updatedAfter,
          },
          {
            logPrefix: options.logPrefix,
            stage: 'fetch-notes',
            pageNumber: nextPageNumber,
          },
        )
      } catch (error) {
        throw new ReaderPreviewLoadResumeError(
          describeUnknownError(error),
          buildResumeState('fetch-highlights', {
            // Note-only failures should retry from a clean fetch-highlights pass.
            // This keeps the existing resume model simple and avoids advancing
            // note state without a stable serialized note-page cursor.
            pageNumber: 0,
            pageCursor: null,
            initialTotalPages: null,
            initialTotalResults: null,
          }),
        )
      }

      notePageNumber = nextPageNumber

      if (noteInitialTotalPages == null) {
        noteInitialTotalPages =
          typeof response.count === 'number' && response.count > 0
            ? Math.ceil(response.count / 100)
            : null
        noteInitialTotalResults =
          typeof response.count === 'number' && response.count > 0
            ? response.count
            : null
      }

      for (const noteDocument of response.results) {
        recordHighlightNote(noteDocument)
      }

      const estimatedTotalPages = Math.max(
        noteInitialTotalPages ?? effectiveMaxHighlightPages ?? notePageNumber,
        notePageNumber,
      )
      const displayTotalPages =
        effectiveMaxHighlightPages != null
          ? Math.min(estimatedTotalPages, effectiveMaxHighlightPages)
          : estimatedTotalPages

      if (options.logPrefix) {
        logReadwiseDebug(options.logPrefix, 'fetched note page', {
          pageNumber: notePageNumber,
          estimatedTotalPages: displayTotalPages,
          estimatedTotalResults: noteInitialTotalResults,
          responseResultCount: response.results.length,
          trackedHighlightNotes: noteDocumentsByHighlightId.size,
          totalNotes,
          hasNextPage: !!response.nextPageCursor,
          cappedByDebugLimit:
            effectiveMaxHighlightPages != null &&
            notePageNumber >= effectiveMaxHighlightPages,
          pageDurationMs: Date.now() - pageStartedAt,
        })
      }

      options.onProgress?.({
        phase: 'fetch-notes',
        pageNumber: notePageNumber,
        totalPages: displayTotalPages,
        totalResults: noteInitialTotalResults ?? undefined,
        uniqueParents: targetParentIds.length,
        totalHighlights,
        totalNotes,
      })

      notePageCursor = response.nextPageCursor

      if (!response.nextPageCursor) {
        remoteNoteScanExhaustive = true
        break
      }

      if (
        mode === 'incremental-window' &&
        maxDocuments != null &&
        targetParentIds.length >= maxDocuments
      ) {
        remoteNoteScanExhaustive = false
        break
      }
    }
    }

    const noteParentHighlightIds = [...noteDocumentsByHighlightId.keys()].filter(
      (highlightId) => !highlightsById.has(highlightId),
    )

    let unresolvedNoteParentHighlightIds = noteParentHighlightIds
    let refreshSnapshotStepTotal = 1 + (previewCache ? 1 : 0)
    let refreshSnapshotStepCompleted = 0

    const emitRefreshSnapshotProgress = () => {
      options.onProgress?.({
        phase: 'refresh-snapshot',
        completed: refreshSnapshotStepCompleted,
        total: refreshSnapshotStepTotal,
        uniqueParents: targetParentIds.length,
        totalHighlights,
        totalNotes,
      })
    }

    emitRefreshSnapshotProgress()

    if (noteParentHighlightIds.length > 0 && previewCache) {
      try {
        const cachedHighlights = await previewCache.getCachedHighlightsByIds(
          noteParentHighlightIds,
        )

        for (const highlight of cachedHighlights.values()) {
          ingestHighlight(highlight)
        }
      } catch (error) {
        if (options.logPrefix) {
          logReadwiseWarn(
            options.logPrefix,
            'failed to load cached highlights for changed Reader notes',
            {
              requestedHighlightCount: noteParentHighlightIds.length,
              formattedError: describeUnknownError(error),
            },
          )
        }
      }

      unresolvedNoteParentHighlightIds = [...noteDocumentsByHighlightId.keys()].filter(
        (highlightId) => !highlightsById.has(highlightId),
      )
    }

    refreshSnapshotStepTotal =
      1 + unresolvedNoteParentHighlightIds.length + (previewCache ? 1 : 0)
    refreshSnapshotStepCompleted += 1
    emitRefreshSnapshotProgress()

    for (const [index, highlightId] of unresolvedNoteParentHighlightIds.entries()) {
      try {
        const response = await listReaderDocumentsWithRetry(
          client,
          {
            id: highlightId,
            limit: 1,
            withHtmlContent: true,
          },
          {
            logPrefix: options.logPrefix,
            stage: 'fetch-documents',
            parentId: highlightId,
          },
        )
        const highlight = response.results[0] ?? null
        if (highlight) {
          ingestHighlight(highlight)
        }
      } catch (error) {
        if (options.logPrefix) {
          logReadwiseWarn(
            options.logPrefix,
            'failed to fetch a highlight referenced by a changed Reader note',
            {
              highlightId,
              formattedError: describeUnknownError(error),
            },
          )
        }
      }

      refreshSnapshotStepCompleted = Math.min(
        refreshSnapshotStepTotal,
        1 + index + 1,
      )
      emitRefreshSnapshotProgress()
    }

    let mergedHighlightNoteCount = 0
    let unresolvedHighlightNoteCount = 0

    for (const [highlightId, noteDocuments] of noteDocumentsByHighlightId.entries()) {
      const highlight = highlightsById.get(highlightId)
      if (!highlight) {
        unresolvedHighlightNoteCount += 1
        continue
      }

      const mergedHighlight = mergeHighlightWithAttachedNotes(
        highlight,
        noteDocuments,
      )
      upsertResolvedHighlight(mergedHighlight)
      mergedHighlightNoteCount += 1
    }

    if (options.logPrefix && noteDocumentsByHighlightId.size > 0) {
      logReadwiseInfo(options.logPrefix, 'merged Reader note documents into highlights', {
        trackedHighlightsWithNotes: noteDocumentsByHighlightId.size,
        mergedHighlightNoteCount,
        unresolvedHighlightNoteCount,
      })
    }

    if (
      mode === 'snapshot-only-refresh' &&
      typeof options.readerAuthToken === 'string' &&
      options.readerAuthToken.trim().length > 0 &&
      targetParentIds.length > 0
    ) {
      let cachedHighlightsById = new Map<string, ReaderDocument>()
      let cachedParentDocuments = new Map<string, ReaderDocument>()

      if (previewCache) {
        const currentHighlightIds = flattenHighlightsByParent(highlightsByParent).map(
          (highlight) => highlight.id,
        )

        if (currentHighlightIds.length > 0) {
          try {
            cachedHighlightsById = await previewCache.getCachedHighlightsByIds(
              currentHighlightIds,
            )
          } catch (error) {
            if (options.logPrefix) {
              logReadwiseWarn(
                options.logPrefix,
                'failed to load cached enriched Reader highlights before snapshot MCP gating',
                {
                  highlightCount: currentHighlightIds.length,
                  formattedError: describeUnknownError(error),
                },
              )
            }
          }
        }

        try {
          cachedParentDocuments = await previewCache.getCachedParentDocuments(
            targetParentIds,
          )
        } catch (error) {
          if (options.logPrefix) {
            logReadwiseWarn(
              options.logPrefix,
              'failed to load cached parent metadata before snapshot MCP gating',
              {
                parentCount: targetParentIds.length,
                formattedError: describeUnknownError(error),
              },
            )
          }
        }
      }

      const unresolvedSnapshotParentIds: string[] = []
      let cacheReuseChangedHighlightCount = 0

      for (const parentId of targetParentIds) {
        const highlights = highlightsByParent.get(parentId) ?? []
        if (highlights.length === 0) continue

        const cachedReuseResult = reuseCachedReaderDocumentHighlightDetails(
          highlights,
          cachedHighlightsById,
        )

        if (cachedReuseResult.changedCount > 0) {
          highlightsByParent.set(parentId, cachedReuseResult.highlights)

          for (const highlight of cachedReuseResult.highlights) {
            highlightsById.set(highlight.id, highlight)
          }

          cacheReuseChangedHighlightCount += cachedReuseResult.changedCount
        }

        const cachedDocument = cachedParentDocuments.get(parentId) ?? null
        const detailStrategy = decideReaderDocumentHighlightDetailsStrategy({
          document: cachedDocument,
          highlights: cachedReuseResult.highlights,
        })

        if (!detailStrategy.shouldEnrich) {
          switch (detailStrategy.reason) {
            case 'missing_parent_metadata':
              documentHighlightDetailSkippedNoParentMetadata += 1
              break
            case 'no_rich_media':
              documentHighlightDetailSkippedNoRichMedia += 1
              break
            case 'video':
              documentHighlightDetailSkippedVideo += 1
              break
            case 'already_resolved':
              documentHighlightDetailSkippedResolved += 1
              break
            default:
              break
          }
          continue
        }

        unresolvedSnapshotParentIds.push(parentId)
      }

      if (options.logPrefix) {
        logReadwiseDebug(
          options.logPrefix,
          'prepared snapshot-only MCP enrichment targets',
          {
            targetParentCount: targetParentIds.length,
            cachedParentMetadataCount: cachedParentDocuments.size,
            cacheReuseChangedHighlightCount,
            documentHighlightDetailSkippedResolved,
            unresolvedParentCount: unresolvedSnapshotParentIds.length,
            documentHighlightDetailSkippedNoRichMedia,
            documentHighlightDetailSkippedNoParentMetadata,
            documentHighlightDetailSkippedVideo,
          },
        )
      }

      refreshSnapshotStepTotal += unresolvedSnapshotParentIds.length
      emitRefreshSnapshotProgress()

      for (const [index, parentId] of unresolvedSnapshotParentIds.entries()) {
        const highlights = highlightsByParent.get(parentId) ?? []
        const cachedDocument = cachedParentDocuments.get(parentId) ?? null

        if (cachedDocument && highlights.length > 0) {
          const enrichmentResult = await tryEnrichReaderDocumentHighlightsViaMcp({
            token: options.readerAuthToken,
            document: cachedDocument,
            highlights,
            logPrefix: options.logPrefix,
          })

          if (enrichmentResult.attempted) {
            documentHighlightDetailCalls += 1
          }
          if (enrichmentResult.missingInReader) {
            documentHighlightDetailMissingInReader += 1
          }

          if (enrichmentResult.changedCount > 0) {
            highlightsByParent.set(parentId, enrichmentResult.highlights)

            for (const highlight of enrichmentResult.highlights) {
              highlightsById.set(highlight.id, highlight)
            }

            if (options.logPrefix) {
              logReadwiseDebug(
                options.logPrefix,
                'enriched Reader snapshot highlights via MCP',
                {
                  readerDocumentId: parentId,
                  fetchedHighlights: enrichmentResult.fetchedCount,
                  changedHighlights: enrichmentResult.changedCount,
                },
              )
            }
          }
        }

        refreshSnapshotStepCompleted = Math.min(
          refreshSnapshotStepTotal,
          1 + unresolvedNoteParentHighlightIds.length + index + 1,
        )
        emitRefreshSnapshotProgress()
      }
    }

    if (previewCache) {
      const allHighlights = flattenHighlightsByParent(highlightsByParent)

      try {
        if (mode === 'full-library-scan' || mode === 'snapshot-only-refresh') {
          if (remoteHighlightScanExhaustive && remoteNoteScanExhaustive) {
            await previewCache.replaceHighlightsFromFullScan(
              allHighlights,
              latestHighlightUpdatedAt,
            )
            completeHighlightSnapshotRefreshed = true
          } else if (options.logPrefix) {
            logReadwiseWarn(
              options.logPrefix,
              'skipped refreshing the complete Reader highlight snapshot because the full scan did not exhaust the remote highlight library',
              {
                mode,
                highlightCount: allHighlights.length,
                highlightPagesScanned: highlightPageNumber,
                maxHighlightPages: effectiveMaxHighlightPages,
                hasMoreRemotePages: highlightPageCursor != null,
                hasMoreRemoteNotePages: notePageCursor != null,
              },
            )
          }
        } else if (mode === 'incremental-window') {
          await previewCache.upsertHighlightsFromIncremental(
            allHighlights,
            latestHighlightUpdatedAt,
          )
        }
      } catch (error) {
        if (options.logPrefix) {
          logReadwiseWarn(options.logPrefix, 'failed to persist Reader highlight cache', {
            mode,
            highlightCount: allHighlights.length,
            formattedError: describeUnknownError(error),
          })
        }
      }

      refreshSnapshotStepCompleted = refreshSnapshotStepTotal
      emitRefreshSnapshotProgress()
    }
  }

  const fetchHighlightsDurationMs = Date.now() - fetchHighlightsStartedAt
  if (mode === 'snapshot-only-refresh') {
    return {
      books: [],
      stats: {
        highlightPagesScanned: highlightPageNumber,
        highlightsScanned: totalHighlights,
        parentDocumentsIdentified: targetParentIds.length,
        pagesTargeted: 0,
        pagesProcessed: 0,
        estimatedHighlightPages: highlightInitialTotalPages,
        estimatedHighlightResults: highlightInitialTotalResults,
        latestHighlightUpdatedAt,
        usedCachedHighlightSnapshot,
        staleHighlightDeletionRisk,
        completeHighlightSnapshotRefreshed,
        parentMetadataCacheHits: 0,
        parentMetadataRemoteFetches: 0,
        documentHighlightDetailCalls,
        documentHighlightDetailSkippedNoParentMetadata,
        documentHighlightDetailSkippedNoRichMedia,
        documentHighlightDetailSkippedVideo,
        documentHighlightDetailSkippedResolved,
        documentHighlightDetailMissingInReader,
        fetchHighlightsDurationMs,
        fetchDocumentsDurationMs: 0,
      },
    }
  }

  const selectedParentIds =
    resumePhase === 'fetch-documents' &&
    resumeState != null &&
    resumeState.selectedParentIds.length > 0
      ? [...resumeState.selectedParentIds]
      : (() => {
          const sorted = sortedParentIds()
          return maxDocuments == null ? sorted : sorted.slice(0, maxDocuments)
        })()
  const previewBooks = [...(resumeState?.previewBooks ?? [])]
  let documentIndex =
    resumePhase === 'fetch-documents' && resumeState != null
      ? resumeState.documentIndex
      : 0
  const fetchDocumentsStartedAt =
    Date.now() - (resumeState?.fetchDocumentsDurationMs ?? 0)
  const fetchedParentDocuments: ReaderDocument[] = []
  const enrichedHighlightsToPersist: ReaderDocument[] = []
  let cachedParentDocuments = new Map<string, ReaderDocument>()
  let cachedHighlightsById = new Map<string, ReaderDocument>()

  if (previewCache) {
    if (parentMetadataMode === 'cache_first') {
      try {
        cachedParentDocuments = await previewCache.getCachedParentDocuments(
          selectedParentIds,
        )
      } catch (error) {
        if (options.logPrefix) {
          logReadwiseWarn(options.logPrefix, 'failed to load Reader parent metadata cache', {
            parentCount: selectedParentIds.length,
            formattedError: describeUnknownError(error),
          })
        }
      }
    }

    const selectedHighlightIds = selectedParentIds.flatMap(
      (parentId) => (highlightsByParent.get(parentId) ?? []).map((highlight) => highlight.id),
    )

    if (selectedHighlightIds.length > 0) {
      try {
        cachedHighlightsById = await previewCache.getCachedHighlightsByIds(
          selectedHighlightIds,
        )
      } catch (error) {
        if (options.logPrefix) {
          logReadwiseWarn(
            options.logPrefix,
            'failed to load cached enriched Reader highlights before parent fetch',
            {
              highlightCount: selectedHighlightIds.length,
              formattedError: describeUnknownError(error),
            },
          )
        }
      }
    }
  }

  options.onProgress?.({
    phase: 'fetch-documents',
    completed: documentIndex,
    total: selectedParentIds.length,
    pageTitle: null,
  })

  for (; documentIndex < selectedParentIds.length; documentIndex += 1) {
    const parentId = selectedParentIds[documentIndex]!
    const cachedDocument =
      parentMetadataMode === 'cache_first'
        ? cachedParentDocuments.get(parentId) ?? null
        : null
    let document = cachedDocument

    if (document) {
      parentMetadataCacheHits += 1
    } else {
      let response: Awaited<ReturnType<ReadwiseClient['listReaderDocuments']>>

      try {
        response = await listReaderDocumentsWithRetry(
          client,
          {
            id: parentId,
            limit: 1,
            withHtmlContent: true,
          },
          {
            logPrefix: options.logPrefix,
            stage: 'fetch-documents',
            parentId,
          },
        )
      } catch (error) {
        throw new ReaderPreviewLoadResumeError(
          describeUnknownError(error),
          buildResumeState('fetch-documents', {
            selectedParentIds,
            previewBooks,
            documentIndex,
            fetchDocumentsDurationMs: Date.now() - fetchDocumentsStartedAt,
          }),
        )
      }

      document = response.results[0] ?? null
      if (document) {
        parentMetadataRemoteFetches += 1
        fetchedParentDocuments.push(document)
      }
    }

    options.onProgress?.({
      phase: 'fetch-documents',
      completed: documentIndex + 1,
      total: selectedParentIds.length,
      pageTitle: document?.title ?? parentId,
    })

    if (!document) continue

    const cacheReuseResult = reuseCachedReaderDocumentHighlightDetails(
      highlightsByParent.get(parentId) ?? [],
      cachedHighlightsById,
    )
    const highlights = [...cacheReuseResult.highlights].sort(
      sortByCreatedAtAscending,
    )

    if (cacheReuseResult.changedCount > 0) {
      highlightsByParent.set(parentId, highlights)
    }

    const enrichedHighlightsResult = await tryEnrichReaderDocumentHighlightsViaMcp({
      token: options.readerAuthToken,
      document,
      highlights,
      logPrefix: options.logPrefix,
    })
    const resolvedHighlights = [...enrichedHighlightsResult.highlights].sort(
      sortByCreatedAtAscending,
    )

    if (
      cacheReuseResult.changedCount > 0 ||
      enrichedHighlightsResult.changedCount > 0
    ) {
      highlightsByParent.set(parentId, resolvedHighlights)
      enrichedHighlightsToPersist.push(...resolvedHighlights)
    }

    previewBooks.push({
      document,
      highlights: resolvedHighlights,
      highlightCoverage: mode,
    })
  }

  if (previewCache && enrichedHighlightsToPersist.length > 0) {
    try {
      await previewCache.putHighlights(enrichedHighlightsToPersist)
    } catch (error) {
      if (options.logPrefix) {
        logReadwiseWarn(
          options.logPrefix,
          'failed to persist enriched Reader highlights after parent fetch',
          {
            highlightCount: enrichedHighlightsToPersist.length,
            formattedError: describeUnknownError(error),
          },
        )
      }
    }
  }

  if (previewCache && fetchedParentDocuments.length > 0) {
    try {
      await previewCache.putParentDocuments(fetchedParentDocuments)
    } catch (error) {
      if (options.logPrefix) {
        logReadwiseWarn(options.logPrefix, 'failed to persist Reader parent metadata cache', {
          documentCount: fetchedParentDocuments.length,
          formattedError: describeUnknownError(error),
        })
      }
    }
  }

  return {
    books: previewBooks,
    stats: {
      highlightPagesScanned: highlightPageNumber,
      highlightsScanned: totalHighlights,
      parentDocumentsIdentified: targetParentIds.length,
      pagesTargeted: selectedParentIds.length,
      pagesProcessed: previewBooks.length,
      estimatedHighlightPages: highlightInitialTotalPages,
      estimatedHighlightResults: highlightInitialTotalResults,
      latestHighlightUpdatedAt,
      usedCachedHighlightSnapshot,
      staleHighlightDeletionRisk,
      completeHighlightSnapshotRefreshed,
      parentMetadataCacheHits,
      parentMetadataRemoteFetches,
      documentHighlightDetailCalls,
      documentHighlightDetailSkippedNoParentMetadata,
      documentHighlightDetailSkippedNoRichMedia,
      documentHighlightDetailSkippedVideo,
      documentHighlightDetailSkippedResolved,
      documentHighlightDetailMissingInReader,
      fetchHighlightsDurationMs,
      fetchDocumentsDurationMs: Date.now() - fetchDocumentsStartedAt,
    },
  }
}
