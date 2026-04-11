import type { ReaderDocument } from '../types'
import {
  describeUnknownError,
  logReadwiseDebug,
  logReadwiseWarn,
} from '../logging'
import type { ReadwiseClient } from './index'

export interface ReaderPreviewBook {
  document: ReaderDocument
  highlights: ReaderDocument[]
  highlightCoverage: 'full-library-scan' | 'recent-window'
}

export interface ReaderPreviewLoadStats {
  highlightPagesScanned: number
  highlightsScanned: number
  parentDocumentsIdentified: number
  pagesTargeted: number
  pagesProcessed: number
  estimatedHighlightPages: number | null
  estimatedHighlightResults: number | null
  fetchHighlightsDurationMs: number
  fetchDocumentsDurationMs: number
}

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
  mode?: 'full-library-scan' | 'recent-window'
  maxHighlightPages?: number
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
      : 20
  const effectiveMaxHighlightPages =
    typeof options.maxHighlightPages === 'number' && options.maxHighlightPages > 0
      ? Math.floor(options.maxHighlightPages)
      : mode === 'recent-window'
        ? 20
        : null
  const targetParentIds: string[] = []
  const seenParentIds = new Set<string>()
  const highlightsByParent = new Map<string, ReaderDocument[]>()
  const seenHighlightIds = new Set<string>()
  const latestHighlightByParent = new Map<string, ReaderDocument>()
  let totalHighlights = 0
  let pageNumber = 0
  let pageCursor: string | undefined
  let initialTotalPages: number | null = null
  let initialTotalResults: number | null = null
  const fetchHighlightsStartedAt = Date.now()

  while (true) {
    if (
      effectiveMaxHighlightPages != null &&
      pageNumber >= effectiveMaxHighlightPages
    ) {
      break
    }

    pageNumber += 1
    const pageStartedAt = Date.now()
    const response = await listReaderDocumentsWithRetry(
      client,
      {
      category: 'highlight',
      limit: 100,
      pageCursor,
      },
      {
        logPrefix: options.logPrefix,
        stage: 'fetch-highlights',
        pageNumber,
      },
    )

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
      const parentId =
        typeof highlight.parent_id === 'string' && highlight.parent_id.length > 0
          ? highlight.parent_id
          : null
      if (!parentId || seenHighlightIds.has(highlight.id)) continue

      const text = getHighlightText(highlight)
      if (text.length === 0) continue

      seenHighlightIds.add(highlight.id)
      totalHighlights += 1

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
    }

    const estimatedTotalPages = Math.max(
      initialTotalPages ??
        effectiveMaxHighlightPages ??
        pageNumber,
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

    if (
      !response.nextPageCursor ||
      (mode === 'recent-window' &&
        maxDocuments != null &&
        targetParentIds.length >= maxDocuments)
    ) {
      break
    }

    pageCursor = response.nextPageCursor
  }

  const fetchHighlightsDurationMs = Date.now() - fetchHighlightsStartedAt

  const previewBooks: ReaderPreviewBook[] = []
  const selectedParentIds =
    mode === 'full-library-scan'
      ? (() => {
          const sorted = [...targetParentIds].sort((left, right) => {
            const leftLatest = latestHighlightByParent.get(left)
            const rightLatest = latestHighlightByParent.get(right)
            if (!leftLatest && !rightLatest) return 0
            if (!leftLatest) return 1
            if (!rightLatest) return -1
            return sortByUpdatedAtDescending(leftLatest, rightLatest)
          })

          return maxDocuments == null ? sorted : sorted.slice(0, maxDocuments)
        })()
      : maxDocuments == null
        ? [...targetParentIds]
        : targetParentIds.slice(0, maxDocuments)
  const fetchDocumentsStartedAt = Date.now()
  options.onProgress?.({
    phase: 'fetch-documents',
    completed: 0,
    total: selectedParentIds.length,
    pageTitle: null,
  })

  for (let index = 0; index < selectedParentIds.length; index += 1) {
    const parentId = selectedParentIds[index]!
    const response = await listReaderDocumentsWithRetry(
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
    const document = response.results[0]

    options.onProgress?.({
      phase: 'fetch-documents',
      completed: index + 1,
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
      fetchHighlightsDurationMs,
      fetchDocumentsDurationMs: Date.now() - fetchDocumentsStartedAt,
    },
  }
}
