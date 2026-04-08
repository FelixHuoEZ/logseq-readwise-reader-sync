import type { ExportParams, ExportedBook } from '../types'
import type { ReadwiseClient } from './index'

export interface LoadAllExportedBooksOptions {
  maxBooks?: number
  onPage?: (info: {
    pageNumber: number
    pageResultCount: number
    totalFetched: number
    maxBooks: number | null
  }) => void
}

export const loadAllExportedBooks = async (
  client: ReadwiseClient,
  params: ExportParams = {},
  options: LoadAllExportedBooksOptions = {},
): Promise<ExportedBook[]> => {
  const results: ExportedBook[] = []
  let pageCursor = params.pageCursor
  let pageNumber = 0
  const maxBooks =
    typeof options.maxBooks === 'number' && options.maxBooks > 0
      ? Math.floor(options.maxBooks)
      : null

  while (true) {
    pageNumber += 1
    const response = await client.exportHighlights({
      ...params,
      pageCursor,
    })

    results.push(...response.results)
    options.onPage?.({
      pageNumber,
      pageResultCount: response.results.length,
      totalFetched: results.length,
      maxBooks,
    })

    if (maxBooks != null && results.length >= maxBooks) {
      return results.slice(0, maxBooks)
    }

    if (!response.nextPageCursor) {
      return results
    }

    pageCursor = response.nextPageCursor
  }
}
