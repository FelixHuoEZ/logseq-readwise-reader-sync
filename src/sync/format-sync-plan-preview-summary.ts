import type { PreparedSyncPlanV1 } from './types'

export const formatSyncPlanPreviewSummaryV1 = (
  prepared: PreparedSyncPlanV1,
): string => {
  const { summary, checkpointDecision } = prepared.plan
  const parts = [
    `Preview ready`,
    `create ${summary.createCount}`,
    `update ${summary.updateCount}`,
    `relink ${summary.relinkCount}`,
    `archive ${summary.deleteCount}`,
    `skip ${summary.skipCount}`,
    `pending relink ${summary.pendingRelinkCount}`,
  ]

  const checkpointPart = checkpointDecision.shouldCommit
    ? checkpointDecision.nextUpdatedAfter
      ? `checkpoint would advance to ${checkpointDecision.nextUpdatedAfter}`
      : 'checkpoint would stay unchanged'
    : `checkpoint blocked: ${checkpointDecision.reasons.join(', ')}`

  return `${parts.join(' | ')} | ${checkpointPart} | preview only, no pages written`
}
