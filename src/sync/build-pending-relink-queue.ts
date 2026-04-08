import type { PendingRelinkEntryV1 } from '../state'

import type { PreparedSyncItemV1 } from './types'

const buildQueueKey = (userBookId: number, candidatePageUuid: string): string =>
  `${userBookId}:${candidatePageUuid}`

export const buildPendingRelinkQueueV1 = (
  items: PreparedSyncItemV1[],
  existingQueue: PendingRelinkEntryV1[],
  detectedAt: string,
): PendingRelinkEntryV1[] => {
  const existingByKey = new Map(
    existingQueue.map((entry) => [
      buildQueueKey(entry.userBookId, entry.candidatePageUuid),
      entry,
    ]),
  )

  const nextQueue: PendingRelinkEntryV1[] = []

  for (const item of items) {
    const candidates = item.plannerInput.exactTitleCandidates

    if (item.plannerInput.acceptedPendingRelink || candidates.length === 0) {
      continue
    }

    for (const candidate of candidates) {
      const key = buildQueueKey(item.book.userBookId, candidate.pageUuid)
      const existing = existingByKey.get(key)

      nextQueue.push(
        existing ?? {
          userBookId: item.book.userBookId,
          remoteTitle: item.book.title,
          candidatePageUuid: candidate.pageUuid,
          candidatePageTitle: candidate.pageTitle,
          detectedAt,
          reason: 'title_exact_match',
          status: 'pending',
        },
      )
    }
  }

  return nextQueue
}
