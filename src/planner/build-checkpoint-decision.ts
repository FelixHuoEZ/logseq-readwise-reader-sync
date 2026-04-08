import type { GraphCheckpointStateV1 } from '../graph'
import type {
  CheckpointDecisionV1,
  PageSyncActionV1,
} from './types'

const deriveReasons = (
  actions: PageSyncActionV1[],
): CheckpointDecisionV1['reasons'] => {
  if (actions.some((action) => action.blocksCheckpoint)) {
    return ['planner_incomplete']
  }

  return []
}

export const buildCheckpointDecisionV1 = (
  actions: PageSyncActionV1[],
  checkpointBeforeRun: GraphCheckpointStateV1 | null,
  nextUpdatedAfter: string | null,
): CheckpointDecisionV1 => {
  const reasons = deriveReasons(actions)

  return {
    shouldCommit: reasons.length === 0,
    nextUpdatedAfter:
      reasons.length === 0 ? nextUpdatedAfter : checkpointBeforeRun?.updatedAfter ?? null,
    reasons,
  }
}
