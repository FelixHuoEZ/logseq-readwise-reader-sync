import type { ExportedBook } from '../types'

const collectBookTimestamps = (book: ExportedBook): string[] => {
  if (!Array.isArray(book.highlights)) {
    return []
  }

  return book.highlights.flatMap((highlight) =>
    [highlight.updated_at, highlight.created_at].filter(
      (value): value is string => typeof value === 'string' && value.length > 0,
    ),
  )
}

export const deriveNextUpdatedAfterV1 = (
  books: ExportedBook[],
  previousUpdatedAfter: string | null,
): string | null => {
  const timestamps = books.flatMap((book) => collectBookTimestamps(book))

  if (timestamps.length === 0) {
    return previousUpdatedAfter
  }

  return timestamps.reduce((latest, current) =>
    current > latest ? current : latest,
  )
}
