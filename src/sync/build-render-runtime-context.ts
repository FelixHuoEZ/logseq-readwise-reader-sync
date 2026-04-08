import { format } from 'date-fns'

import type { NormalizedBookExport } from '../normalizer'
import type { RenderRuntimeContext } from '../renderer'
import type { GraphStateV1, PageIndexEntryV1 } from '../state'

const hasRemoteChanges = (
  book: NormalizedBookExport,
  existingPageIndexEntry: PageIndexEntryV1 | null,
): boolean => {
  if (!existingPageIndexEntry) return false
  if (book.isDeleted) return false
  if (existingPageIndexEntry.lastRemoteUpdatedAt == null) return true
  if (book.updatedAt > existingPageIndexEntry.lastRemoteUpdatedAt) return true

  return (
    existingPageIndexEntry.lastSeenHighlightCount != null &&
    existingPageIndexEntry.lastSeenHighlightCount !== book.highlights.length
  )
}

export const buildRenderRuntimeContextV1 = (
  book: NormalizedBookExport,
  existingPageIndexEntry: PageIndexEntryV1 | null,
  documentFormat: GraphStateV1['documentFormat'],
  startedAt: string,
): RenderRuntimeContext => {
  const startedAtDate = new Date(startedAt)

  return {
    format: documentFormat,
    syncDate: format(startedAtDate, 'yyyy-MM-dd'),
    syncTime: format(startedAtDate, 'HH:mm'),
    isNewPage: existingPageIndexEntry == null,
    hasNewHighlights: hasRemoteChanges(book, existingPageIndexEntry),
  }
}
