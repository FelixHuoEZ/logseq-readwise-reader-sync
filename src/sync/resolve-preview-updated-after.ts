import type { UserConfigV1 } from '../config'
import type { GraphCheckpointStateV1 } from '../graph'

export const resolvePreviewUpdatedAfterV1 = (
  checkpointState: GraphCheckpointStateV1 | null,
  userConfig: UserConfigV1,
): string | null =>
  checkpointState?.updatedAfter ??
  userConfig.legacyLastSyncTimestamp ??
  null
