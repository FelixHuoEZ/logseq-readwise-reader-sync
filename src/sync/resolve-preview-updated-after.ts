import type { UserConfigV1 } from '../config'
import type { GraphStateV1 } from '../state'

export const resolvePreviewUpdatedAfterV1 = (
  graphState: GraphStateV1,
  userConfig: UserConfigV1,
): string | null =>
  graphState.checkpoint?.updatedAfter ??
  userConfig.legacyLastSyncTimestamp ??
  null
