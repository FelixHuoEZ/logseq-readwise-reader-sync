import type { ExportParams, ExportedBook } from '../types'
import type { ReadwiseClient } from './index'

export const loadAllExportedBooks = async (
  client: ReadwiseClient,
  params: ExportParams = {},
): Promise<ExportedBook[]> => {
  const results: ExportedBook[] = []
  let pageCursor = params.pageCursor

  while (true) {
    const response = await client.exportHighlights({
      ...params,
      pageCursor,
    })

    results.push(...response.results)

    if (!response.nextPageCursor) {
      return results
    }

    pageCursor = response.nextPageCursor
  }
}
