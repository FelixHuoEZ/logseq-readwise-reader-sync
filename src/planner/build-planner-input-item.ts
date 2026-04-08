import { normalizeComparableUrlV1, type GraphSnapshotV1 } from '../graph'
import type { NormalizedBookExport } from '../normalizer'
import type { RenderedPage } from '../renderer'
import type { GraphStateV1 } from '../state'
import type { PlannerInputItemV1 } from './types'

const stripWrappingQuotes = (value: string): string =>
  value.replace(/^["'“”‘’]+/, '').replace(/["'“”‘’]+$/, '')

const normalizeBridgeTitle = (value: string): string =>
  stripWrappingQuotes(value).replace(/\s+/g, ' ').trim()

const mergeCandidates = (
  ...candidateLists: Array<PlannerInputItemV1['exactTitleCandidates']>
): PlannerInputItemV1['exactTitleCandidates'] => {
  const merged: PlannerInputItemV1['exactTitleCandidates'] = []

  for (const candidateList of candidateLists) {
    for (const candidate of candidateList) {
      if (merged.some((existing) => existing.pageUuid === candidate.pageUuid)) {
        continue
      }
      merged.push(candidate)
    }
  }

  return merged
}

export const buildPlannerInputItemV1 = (
  book: NormalizedBookExport,
  renderedPage: RenderedPage,
  graphState: GraphStateV1,
  graphSnapshot: GraphSnapshotV1,
): PlannerInputItemV1 => {
  const existingPageIndexEntry =
    graphState.pageIndex[String(book.userBookId)] ?? null
  const exactTitleCandidates = mergeCandidates(
    graphSnapshot.pagesByExactTitle[book.title] ?? [],
    graphSnapshot.pagesByBridgeTitle[normalizeBridgeTitle(book.title)] ?? [],
  )
  const propertyMatchCandidates = mergeCandidates(
    ...[
      book.uniqueUrl,
      book.sourceUrl,
      book.readwiseUrl,
    ].map((rawUrl) => {
      const normalized = normalizeComparableUrlV1(rawUrl)
      return normalized
        ? graphSnapshot.pagesByCanonicalUrl[normalized] ?? []
        : []
    }),
  )
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
    propertyMatchCandidates,
    mappedPageExists: mappedPageUuid
      ? graphSnapshot.pageUuidExists[mappedPageUuid] === true
      : false,
    acceptedPendingRelink,
  }
}
