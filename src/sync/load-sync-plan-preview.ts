import { createReadwiseClient, loadAllExportedBooks } from '../api'
import { readUserConfig } from '../config'
import { loadCurrentGraphSnapshotV1 } from '../graph'
import { buildRuntimeGraphStateV1, loadPluginStateV1 } from '../state'

import { prepareSyncPlanV1 } from './prepare-sync-plan'
import { resolvePreviewUpdatedAfterV1 } from './resolve-preview-updated-after'
import type { PreparedSyncPlanV1 } from './types'

export const loadSyncPlanPreviewV1 = async (): Promise<PreparedSyncPlanV1> => {
  const userConfig = readUserConfig()

  if (userConfig.apiToken.length === 0) {
    throw new Error('Readwise API token is not configured.')
  }

  const pluginState = await loadPluginStateV1()
  const graphState = pluginState.activeGraph
  const client = createReadwiseClient(userConfig.apiToken)
  const updatedAfter = resolvePreviewUpdatedAfterV1(graphState, userConfig)
  const maxBooks = updatedAfter == null ? 20 : undefined
  const rawBooks = await loadAllExportedBooks(client, {
    updatedAfter: updatedAfter ?? undefined,
    includeDeleted: true,
  }, {
    maxBooks,
  })
  const graphSnapshot = await loadCurrentGraphSnapshotV1()
  const runtimeGraphState = buildRuntimeGraphStateV1(
    graphState,
    graphSnapshot,
  )

  return prepareSyncPlanV1({
    rawBooks,
    graphState: runtimeGraphState,
    graphSnapshot,
  })
}
