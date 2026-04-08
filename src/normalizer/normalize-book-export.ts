import type { ExportedBook } from '../types'
import { normalizeHighlight } from './normalize-highlight'
import type { NormalizedBookExport } from './types'

const toArray = <T,>(value: T[] | null | undefined): T[] =>
  Array.isArray(value) ? value : []

const pickMostRecentTimestamp = (book: ExportedBook) => {
  const timestamps = toArray(book.highlights).flatMap((highlight) =>
    [highlight.updated_at, highlight.created_at].filter(
      (value): value is string => typeof value === 'string' && value.length > 0,
    ),
  )

  if (timestamps.length === 0) {
    return new Date(0).toISOString()
  }

  return timestamps.reduce((latest, current) =>
    current > latest ? current : latest,
  )
}

export const normalizeBookExport = (
  book: ExportedBook,
): NormalizedBookExport => {
  const highlights = toArray(book.highlights)
  const documentTags = toArray(book.book_tags)

  return {
    userBookId: book.user_book_id,
    isDeleted: book.is_deleted,
    title: book.title,
    readableTitle: book.readable_title,
    author: book.author || null,
    category: book.category,
    source: book.source || null,
    sourceUrl: book.source_url || null,
    uniqueUrl: book.unique_url || null,
    readwiseUrl: book.readwise_url,
    coverImageUrl: book.cover_image_url,
    documentTags: documentTags.map((tag) => tag.name).filter(Boolean),
    documentNote: book.document_note,
    summary: book.summary,
    publishedDate: null,
    updatedAt: pickMostRecentTimestamp(book),
    highlights: highlights.map((highlight) =>
      normalizeHighlight(book.user_book_id, highlight),
    ),
  }
}
