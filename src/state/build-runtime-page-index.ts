import type { GraphSnapshotV1 } from '../graph'

import type { GraphStateV1, PageIndexEntryV1 } from './types'

const toCachedEntry = (
  entry: PageIndexEntryV1,
  graphSnapshot: GraphSnapshotV1,
): PageIndexEntryV1 => ({
  ...entry,
  status:
    graphSnapshot.pageUuidExists[entry.pageUuid] === true
      ? entry.status
      : 'missing',
  identitySource: entry.identitySource ?? 'settings_cache',
})

const toPagePropertyEntry = (
  userBookId: number,
  candidate: GraphSnapshotV1['pagesByReadwiseBookId'][string][number],
  cachedEntry: PageIndexEntryV1 | null,
): PageIndexEntryV1 => ({
  userBookId,
  pageUuid: candidate.pageUuid,
  pageTitle: candidate.pageTitle || cachedEntry?.pageTitle || '',
  status: 'active',
  lastRemoteUpdatedAt: cachedEntry?.lastRemoteUpdatedAt ?? null,
  lastAppliedAt: cachedEntry?.lastAppliedAt ?? null,
  lastAppliedRenderHash: cachedEntry?.lastAppliedRenderHash ?? null,
  lastSeenHighlightCount: cachedEntry?.lastSeenHighlightCount ?? null,
  lastKnownPagePath: candidate.path ?? cachedEntry?.lastKnownPagePath ?? null,
  identitySource: 'page_property',
})

export const buildRuntimePageIndexV1 = (
  graphState: GraphStateV1,
  graphSnapshot: GraphSnapshotV1,
): GraphStateV1['pageIndex'] => {
  const runtimePageIndex: GraphStateV1['pageIndex'] = {}

  for (const [bookId, entry] of Object.entries(graphState.pageIndex)) {
    runtimePageIndex[bookId] = toCachedEntry(entry, graphSnapshot)
  }

  for (const [bookId, candidates] of Object.entries(
    graphSnapshot.pagesByReadwiseBookId,
  )) {
    if (candidates.length !== 1) {
      continue
    }

    const userBookId = Number.parseInt(bookId, 10)
    if (!Number.isFinite(userBookId)) {
      continue
    }

    runtimePageIndex[bookId] = toPagePropertyEntry(
      userBookId,
      candidates[0]!,
      runtimePageIndex[bookId] ?? null,
    )
  }

  return runtimePageIndex
}

export const buildRuntimeGraphStateV1 = (
  graphState: GraphStateV1,
  graphSnapshot: GraphSnapshotV1,
): GraphStateV1 => ({
  ...graphState,
  pageIndex: buildRuntimePageIndexV1(graphState, graphSnapshot),
})
