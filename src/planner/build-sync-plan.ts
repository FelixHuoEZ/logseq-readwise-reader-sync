import type { GraphStateV1 } from '../state'
import type {
  PlannerInputItemV1,
  SyncPlanSummaryV1,
  SyncPlanV1,
  SyncRunContextV1,
} from './types'
import {
  PAGE_ACTION_ORDER_V1,
  type PageSyncActionV1,
} from './types'
import { buildCheckpointDecisionV1 } from './build-checkpoint-decision'
import { planPageActionV1 } from './plan-page-action'

const sortActions = (
  actions: PageSyncActionV1[],
): PageSyncActionV1[] =>
  [...actions].sort((left, right) => {
    const leftIndex = PAGE_ACTION_ORDER_V1.indexOf(left.type)
    const rightIndex = PAGE_ACTION_ORDER_V1.indexOf(right.type)

    if (leftIndex !== rightIndex) {
      return leftIndex - rightIndex
    }

    return left.userBookId - right.userBookId
  })

const buildSummary = (
  actions: PageSyncActionV1[],
): SyncPlanSummaryV1 => ({
  createCount: actions.filter((action) => action.type === 'create_page').length,
  updateCount: actions.filter((action) => action.type === 'update_page').length,
  skipCount: actions.filter((action) => action.type === 'skip_page').length,
  relinkCount: actions.filter((action) => action.type === 'relink_page').length,
  deleteCount: actions.filter((action) => action.type === 'mark_page_deleted').length,
  pendingRelinkCount: actions.filter(
    (action) =>
      action.type === 'skip_page' &&
      action.skipReason === 'awaiting_manual_relink',
  ).length,
})

const pickNextUpdatedAfter = (items: PlannerInputItemV1[]) => {
  const timestamps = items
    .map((item) => item.book.updatedAt)
    .filter((value): value is string => typeof value === 'string' && value.length > 0)

  if (timestamps.length === 0) return null

  return timestamps.reduce((latest, current) =>
    current > latest ? current : latest,
  )
}

export const buildSyncPlanV1 = (
  runContext: SyncRunContextV1,
  items: PlannerInputItemV1[],
  graphState: GraphStateV1,
): SyncPlanV1 => {
  const actions = sortActions(
    items.map((item) => planPageActionV1(item, graphState)),
  )
  const checkpointDecision = buildCheckpointDecisionV1(
    actions,
    runContext.checkpointBeforeRun,
    pickNextUpdatedAfter(items),
  )

  return {
    runContext,
    actions,
    summary: buildSummary(actions),
    checkpointDecision,
  }
}
