import type { GraphReaderSyncCacheV1 } from '../cache/reader-sync-cache'
import {
  describeUnknownError,
  logReadwiseDebug,
  logReadwiseWarn,
} from '../logging'
import type { ReaderDocument } from '../types'
import type { ReadwiseClient } from './index'

export type ReaderPreviewLoadMode =
  | 'full-library-scan'
  | 'incremental-window'
  | 'cached-full-rebuild'

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
  phase: 'fetch-highlights' | 'fetch-documents'
  pageNumber?: number
  totalPages?: number
  totalResults?: number
  uniqueParents?: number
  totalHighlights?: number
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
  logPrefix?: string
  onProgress?: (progress: LoadReaderPreviewBooksProgress) => void
}

const sortByCreatedAtAscending = (left: ReaderDocument, right: ReaderDocument) =>
  left.created_at.localeCompare(right.created_at)

const sortByUpdatedAtDescending = (left: ReaderDocument, right: ReaderDocument) =>
  right.updated_at.localeCompare(left.updated_at)

const getHighlightText = (document: ReaderDocument) =>
  typeof document.content === 'string' ? document.content.trim() : ''

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
    stage: 'fetch-highlights' | 'fetch-documents'
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
  const seenHighlightIds = new Set(resumeState?.seenHighlightIds ?? [])
  const latestHighlightByParent = new Map<string, ReaderDocument>(
    resumeState?.latestHighlightByParent ?? [],
  )
  let totalHighlights = resumeState?.totalHighlights ?? 0
  let pageNumber = resumeState?.pageNumber ?? 0
  let pageCursor = resumeState?.pageCursor ?? null
  let initialTotalPages = resumeState?.initialTotalPages ?? null
  let initialTotalResults = resumeState?.initialTotalResults ?? null
  let latestHighlightUpdatedAt = getLatestUpdatedAt(latestHighlightByParent.values())
  let usedCachedHighlightSnapshot = false
  let staleHighlightDeletionRisk = false
  let completeHighlightSnapshotRefreshed = false
  let parentMetadataCacheHits = 0
  let parentMetadataRemoteFetches = 0
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
    pageNumber,
    pageCursor,
    initialTotalPages,
    initialTotalResults,
    fetchHighlightsDurationMs: Date.now() - fetchHighlightsStartedAt,
    selectedParentIds: extra.selectedParentIds ?? [],
    previewBooks: extra.previewBooks ?? [],
    documentIndex: extra.documentIndex ?? 0,
    fetchDocumentsDurationMs: extra.fetchDocumentsDurationMs ?? 0,
  })

  const ingestHighlight = (highlight: ReaderDocument) => {
    const parentId =
      typeof highlight.parent_id === 'string' && highlight.parent_id.length > 0
        ? highlight.parent_id
        : null
    if (!parentId || seenHighlightIds.has(highlight.id)) return false

    const text = getHighlightText(highlight)
    if (text.length === 0) return false

    seenHighlightIds.add(highlight.id)
    totalHighlights += 1

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

    const existing = highlightsByParent.get(parentId) ?? []
    existing.push(highlight)
    highlightsByParent.set(parentId, existing)

    const latestExisting = latestHighlightByParent.get(parentId)
    if (!latestExisting || highlight.updated_at > latestExisting.updated_at) {
      latestHighlightByParent.set(parentId, highlight)
    }

    return true
  }

  if (
    mode === 'cached-full-rebuild' &&
    (resumeState?.phase ?? 'fetch-highlights') === 'fetch-highlights'
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
  } else if ((resumeState?.phase ?? 'fetch-highlights') === 'fetch-highlights') {
    let remoteHighlightScanExhaustive = false

    while (true) {
      if (
        effectiveMaxHighlightPages != null &&
        pageNumber >= effectiveMaxHighlightPages
      ) {
        break
      }

      const nextPageNumber = pageNumber + 1
      const pageStartedAt = Date.now()
      let response: Awaited<ReturnType<ReadwiseClient['listReaderDocuments']>>

      try {
        response = await listReaderDocumentsWithRetry(
          client,
          {
            category: 'highlight',
            limit: 100,
            pageCursor: pageCursor ?? undefined,
            updatedAfter: options.updatedAfter,
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

      pageNumber = nextPageNumber

      if (initialTotalPages == null) {
        initialTotalPages =
          typeof response.count === 'number' && response.count > 0
            ? Math.ceil(response.count / 100)
            : null
        initialTotalResults =
          typeof response.count === 'number' && response.count > 0
            ? response.count
            : null
      }

      for (const highlight of response.results) {
        ingestHighlight(highlight)
      }

      const estimatedTotalPages = Math.max(
        initialTotalPages ?? effectiveMaxHighlightPages ?? pageNumber,
        pageNumber,
      )
      const displayTotalPages =
        effectiveMaxHighlightPages != null
          ? Math.min(estimatedTotalPages, effectiveMaxHighlightPages)
          : estimatedTotalPages

      if (options.logPrefix) {
        logReadwiseDebug(options.logPrefix, 'fetched highlight page', {
          pageNumber,
          estimatedTotalPages: displayTotalPages,
          estimatedTotalResults: initialTotalResults,
          responseResultCount: response.results.length,
          totalHighlights,
          uniqueParents: targetParentIds.length,
          hasNextPage: !!response.nextPageCursor,
          cappedByDebugLimit:
            effectiveMaxHighlightPages != null &&
            pageNumber >= effectiveMaxHighlightPages,
          pageDurationMs: Date.now() - pageStartedAt,
        })
      }

      options.onProgress?.({
        phase: 'fetch-highlights',
        pageNumber,
        totalPages: displayTotalPages,
        totalResults: initialTotalResults ?? undefined,
        uniqueParents: targetParentIds.length,
        totalHighlights,
      })

      pageCursor = response.nextPageCursor

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

    if (previewCache) {
      const allHighlights = flattenHighlightsByParent(highlightsByParent)

      try {
        if (mode === 'full-library-scan') {
          if (remoteHighlightScanExhaustive) {
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
                highlightPagesScanned: pageNumber,
                maxHighlightPages: effectiveMaxHighlightPages,
                hasMoreRemotePages: pageCursor != null,
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
    }
  }

  const fetchHighlightsDurationMs = Date.now() - fetchHighlightsStartedAt
  const selectedParentIds =
    resumeState?.phase === 'fetch-documents' && resumeState.selectedParentIds.length > 0
      ? [...resumeState.selectedParentIds]
      : (() => {
          const sorted = sortedParentIds()
          return maxDocuments == null ? sorted : sorted.slice(0, maxDocuments)
        })()
  const previewBooks = [...(resumeState?.previewBooks ?? [])]
  let documentIndex =
    resumeState?.phase === 'fetch-documents' ? resumeState.documentIndex : 0
  const fetchDocumentsStartedAt =
    Date.now() - (resumeState?.fetchDocumentsDurationMs ?? 0)
  const fetchedParentDocuments: ReaderDocument[] = []
  let cachedParentDocuments = new Map<string, ReaderDocument>()

  if (previewCache && parentMetadataMode === 'cache_first') {
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

    previewBooks.push({
      document,
      highlights: [...(highlightsByParent.get(parentId) ?? [])].sort(
        sortByCreatedAtAscending,
      ),
      highlightCoverage: mode,
    })
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
      highlightPagesScanned: pageNumber,
      highlightsScanned: totalHighlights,
      parentDocumentsIdentified: targetParentIds.length,
      pagesTargeted: selectedParentIds.length,
      pagesProcessed: previewBooks.length,
      estimatedHighlightPages: initialTotalPages,
      estimatedHighlightResults: initialTotalResults,
      latestHighlightUpdatedAt,
      usedCachedHighlightSnapshot,
      staleHighlightDeletionRisk,
      completeHighlightSnapshotRefreshed,
      parentMetadataCacheHits,
      parentMetadataRemoteFetches,
      fetchHighlightsDurationMs,
      fetchDocumentsDurationMs: Date.now() - fetchDocumentsStartedAt,
    },
  }
}
