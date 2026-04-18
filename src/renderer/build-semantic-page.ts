import { format, isValid, parseISO } from 'date-fns'

import type { NormalizedHighlight } from '../normalizer'
import type {
  PageRenderContext,
  SemanticHighlight,
  SemanticMetadataEntry,
  SemanticPage,
  SemanticSyncHeader,
} from './types'

const toYmd = (value: string | null) => {
  if (!value) return null

  const parsed = parseISO(value)
  if (!isValid(parsed)) return null

  return format(parsed, 'yyyy-MM-dd')
}

const buildMetadataEntries = (
  context: PageRenderContext,
): SemanticMetadataEntry[] => {
  const { book, runtime } = context

  return [
    { key: 'rw-id', value: String(book.userBookId) },
    { key: 'AUTHOR', value: book.author },
    { key: 'CATEGORIES', value: book.category },
    { key: 'LINK', value: book.sourceUrl ?? book.uniqueUrl },
    {
      key: 'TAGS',
      value:
        book.documentTags.length > 0
          ? ` ${book.documentTags.join('  ,  ')}  ,  `
          : null,
    },
    { key: 'DATE', value: toYmd(runtime.syncDate) },
    { key: 'PUBLISHED', value: toYmd(book.publishedDate) },
    { key: 'SAVED', value: toYmd(book.savedDate) },
  ]
}

const buildSyncHeader = (context: PageRenderContext): SemanticSyncHeader => {
  const { runtime } = context

  if (runtime.isNewPage) {
    return {
      kind: 'first_sync',
      text: `Highlights first synced by [[Readwise]] [[${runtime.syncDate}]]`,
    }
  }

  if (runtime.hasNewHighlights) {
    return {
      kind: 'new_highlights',
      text: `New highlights added [[${runtime.syncDate}]] at ${runtime.syncTime}`,
    }
  }

  return {
    kind: 'none',
    text: null,
  }
}

const normalizeHighlightTags = (highlight: NormalizedHighlight): string[] => {
  const seen = new Set<string>()
  const tags: string[] = []

  for (const tag of highlight.tags) {
    const name = tag.name.trim()
    if (name.length === 0 || seen.has(name)) continue

    seen.add(name)
    tags.push(name)
  }

  return tags
}

const buildSemanticHighlight = (
  highlight: NormalizedHighlight,
  computeUuid: (locationUrl: string) => string,
  readerDocumentUrl: string | null,
): SemanticHighlight => {
  const uuidSource = highlight.locationUrl ?? highlight.readwiseUrl

  return {
    highlightId: highlight.id,
    uuid: computeUuid(uuidSource),
    text: highlight.text,
    imageUrl: highlight.imageUrl,
    locationLabel: highlight.locationLabel,
    locationUrl: readerDocumentUrl ?? highlight.locationUrl,
    createdDate: toYmd(highlight.highlightedAt ?? highlight.createdAt) ?? '',
    tags: normalizeHighlightTags(highlight),
    note: highlight.note,
  }
}

export const buildSemanticPage = (
  context: PageRenderContext,
  computeUuid: (locationUrl: string) => string,
): SemanticPage => ({
  format: context.runtime.format,
  pageTitle: context.book.title,
  metadata: buildMetadataEntries(context),
  pageNote: {
    imageUrl: context.book.coverImageUrl,
    summary: context.book.summary,
  },
  syncHeader: buildSyncHeader(context),
  highlights: context.book.highlights.map((highlight) =>
    buildSemanticHighlight(
      highlight,
      computeUuid,
      context.book.readerDocumentUrl,
    ),
  ),
})
