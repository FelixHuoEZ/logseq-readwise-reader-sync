import type { ReaderDocument } from '../types'
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
  maxDocuments?: number
  mode?: 'full-library-scan' | 'recent-window'
  maxHighlightPages?: number
  onProgress?: (progress: LoadReaderPreviewBooksProgress) => void
}

const sortByCreatedAtAscending = (left: ReaderDocument, right: ReaderDocument) =>
  left.created_at.localeCompare(right.created_at)

const sortByUpdatedAtDescending = (left: ReaderDocument, right: ReaderDocument) =>
  right.updated_at.localeCompare(left.updated_at)

const getHighlightText = (document: ReaderDocument) =>
  typeof document.content === 'string' ? document.content.trim() : ''

export const loadReaderPreviewBooks = async (
  client: ReadwiseClient,
  options: LoadReaderPreviewBooksOptions = {},
): Promise<ReaderPreviewLoadResult> => {
  const mode = options.mode ?? 'full-library-scan'
  const maxDocuments =
    typeof options.maxDocuments === 'number' && options.maxDocuments > 0
      ? Math.floor(options.maxDocuments)
      : 20
  const maxHighlightPages =
    typeof options.maxHighlightPages === 'number' && options.maxHighlightPages > 0
      ? Math.floor(options.maxHighlightPages)
      : 20
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
    if (mode === 'recent-window' && pageNumber >= maxHighlightPages) {
      break
    }

    pageNumber += 1
    const response = await client.listReaderDocuments({
      category: 'highlight',
      limit: 100,
      pageCursor,
    })

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

    options.onProgress?.({
      phase: 'fetch-highlights',
      pageNumber,
      totalPages: Math.max(initialTotalPages ?? 0, pageNumber) || undefined,
      totalResults: initialTotalResults ?? undefined,
      uniqueParents: targetParentIds.length,
      totalHighlights,
    })

    if (
      !response.nextPageCursor ||
      (mode === 'recent-window' && targetParentIds.length >= maxDocuments)
    ) {
      break
    }

    pageCursor = response.nextPageCursor
  }

  const fetchHighlightsDurationMs = Date.now() - fetchHighlightsStartedAt

  const previewBooks: ReaderPreviewBook[] = []
  const selectedParentIds =
    mode === 'full-library-scan'
      ? [...targetParentIds]
          .sort((left, right) => {
            const leftLatest = latestHighlightByParent.get(left)
            const rightLatest = latestHighlightByParent.get(right)
            if (!leftLatest && !rightLatest) return 0
            if (!leftLatest) return 1
            if (!rightLatest) return -1
            return sortByUpdatedAtDescending(leftLatest, rightLatest)
          })
          .slice(0, maxDocuments)
      : targetParentIds.slice(0, maxDocuments)
  const fetchDocumentsStartedAt = Date.now()

  for (let index = 0; index < selectedParentIds.length; index += 1) {
    const parentId = selectedParentIds[index]!
    const response = await client.listReaderDocuments({
      id: parentId,
      limit: 1,
    })
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
      fetchHighlightsDurationMs,
      fetchDocumentsDurationMs: Date.now() - fetchDocumentsStartedAt,
    },
  }
}
