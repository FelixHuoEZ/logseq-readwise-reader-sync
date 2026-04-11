import { normalizeComparableUrlV1 } from '../graph'
import type { ExportedBook } from '../types'
import type { ReadwiseClient } from './index'

const collectBookCandidateUrls = (book: ExportedBook): string[] =>
  [book.source_url, book.unique_url]
    .map((value) => normalizeComparableUrlV1(value))
    .filter((value, index, array): value is string =>
      typeof value === 'string' && value.length > 0 && array.indexOf(value) === index,
    )

export const loadReaderDocumentUrlsForBooks = async (
  client: ReadwiseClient,
  books: ExportedBook[],
  options: {
    updatedAfter?: string
  } = {},
): Promise<Map<number, string>> => {
  const candidateUrlToBookIds = new Map<string, number[]>()
  const unresolvedBookIds = new Set<number>()

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

  if (unresolvedBookIds.size === 0) {
    return new Map()
  }

  const matchedReaderUrls = new Map<number, string>()
  let pageCursor: string | undefined

  while (true) {
    const response = await client.listReaderDocuments({
      updatedAfter: options.updatedAfter,
      pageCursor,
    })

    for (const document of response.results) {
      const candidates = [
        normalizeComparableUrlV1(document.source_url),
        normalizeComparableUrlV1(document.url),
      ].filter((value, index, array): value is string =>
        typeof value === 'string' && value.length > 0 && array.indexOf(value) === index,
      )

      for (const candidate of candidates) {
        const bookIds = candidateUrlToBookIds.get(candidate)
        if (!bookIds || bookIds.length === 0) continue

        for (const bookId of bookIds) {
          if (!unresolvedBookIds.has(bookId)) continue
          matchedReaderUrls.set(bookId, document.url)
          unresolvedBookIds.delete(bookId)
        }
      }
    }

    if (!response.nextPageCursor || unresolvedBookIds.size === 0) {
      return matchedReaderUrls
    }

    pageCursor = response.nextPageCursor
  }
}
