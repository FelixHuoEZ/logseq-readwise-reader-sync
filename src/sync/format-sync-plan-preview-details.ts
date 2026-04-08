import type { PreparedSyncPlanV1 } from './types'

type ActionRow = {
  userBookId: number
  title: string
  action: string
  reasonCode: string
  checkpoint: 'block' | 'ok'
  target: string
}

type ItemRow = {
  userBookId: number
  title: string
  highlights: number
  mappedPageExists: boolean
  identitySource: string
  titleCandidateCount: number
  propertyCandidateCount: number
  acceptedRelink: boolean
  renderHash: string
}

const toActionTarget = (
  action: PreparedSyncPlanV1['plan']['actions'][number],
): string => {
  switch (action.type) {
    case 'update_page':
    case 'mark_page_deleted':
      return action.pageUuid
    case 'relink_page':
      return action.targetPageUuid
    case 'skip_page':
      return action.pageUuid ?? '-'
    case 'create_page':
      return '-'
  }
}

export const buildSyncPlanActionRowsV1 = (
  prepared: PreparedSyncPlanV1,
): ActionRow[] =>
  prepared.plan.actions.map((action) => ({
    userBookId: action.userBookId,
    title: action.remoteTitle,
    action: action.type,
    reasonCode: action.reasonCode,
    checkpoint: action.blocksCheckpoint ? 'block' : 'ok',
    target: toActionTarget(action),
  }))

export const buildSyncPlanItemRowsV1 = (
  prepared: PreparedSyncPlanV1,
): ItemRow[] =>
  prepared.items.map((item) => ({
    userBookId: item.book.userBookId,
    title: item.book.title,
    highlights: item.book.highlights.length,
    mappedPageExists: item.plannerInput.mappedPageExists,
    identitySource: item.plannerInput.existingPageIndexEntry?.identitySource ?? '-',
    titleCandidateCount: item.plannerInput.exactTitleCandidates.length,
    propertyCandidateCount: item.plannerInput.propertyMatchCandidates.length,
    acceptedRelink: item.plannerInput.acceptedPendingRelink != null,
    renderHash: item.renderedPage.renderHash,
  }))
