import type { SyncPlanV1 } from '../planner'
import type { LastRunSummaryV1 } from '../state'

export const buildLastRunSummaryFromPlanV1 = (
  plan: SyncPlanV1,
  completedAt: string,
): LastRunSummaryV1 => ({
  runId: plan.runContext.runId,
  completedAt,
  createCount: plan.summary.createCount,
  updateCount: plan.summary.updateCount,
  skipCount: plan.summary.skipCount,
  relinkCount: plan.summary.relinkCount,
  deleteCount: plan.summary.deleteCount,
  blockedCheckpoint: plan.checkpointDecision.reasons.length > 0,
  blockedCheckpointReasons: plan.checkpointDecision.reasons,
})
