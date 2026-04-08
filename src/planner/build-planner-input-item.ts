import type { GraphSnapshotV1 } from '../graph'
import type { NormalizedBookExport } from '../normalizer'
import type { RenderedPage } from '../renderer'
import type { GraphStateV1 } from '../state'
import type { PlannerInputItemV1 } from './types'

export const buildPlannerInputItemV1 = (
  book: NormalizedBookExport,
  renderedPage: RenderedPage,
  graphState: GraphStateV1,
  graphSnapshot: GraphSnapshotV1,
): PlannerInputItemV1 => {
  const existingPageIndexEntry =
    graphState.pageIndex[String(book.userBookId)] ?? null
  const exactTitleCandidates = graphSnapshot.pagesByExactTitle[book.title] ?? []
  const mappedPageUuid = existingPageIndexEntry?.pageUuid ?? null
  const acceptedPendingRelink =
    graphState.pendingRelinkQueue.find(
      (entry) =>
        entry.userBookId === book.userBookId && entry.status === 'accepted',
    ) ?? null

  return {
    book,
    renderedPage,
    existingPageIndexEntry,
    exactTitleCandidates,
    mappedPageExists: mappedPageUuid
      ? graphSnapshot.pageUuidExists[mappedPageUuid] === true
      : false,
    acceptedPendingRelink,
  }
}
