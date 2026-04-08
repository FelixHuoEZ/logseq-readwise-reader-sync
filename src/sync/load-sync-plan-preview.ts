import { createReadwiseClient, loadAllExportedBooks } from '../api'
import { readUserConfig } from '../config'
import { loadCurrentGraphSnapshotV1 } from '../graph'
import { loadPluginStateV1 } from '../state'

import { prepareSyncPlanV1 } from './prepare-sync-plan'
import type { PreparedSyncPlanV1 } from './types'

export const loadSyncPlanPreviewV1 = async (): Promise<PreparedSyncPlanV1> => {
  const userConfig = readUserConfig()

  if (userConfig.apiToken.length === 0) {
    throw new Error('Readwise API token is not configured.')
  }

  const pluginState = await loadPluginStateV1()
  const graphState = pluginState.activeGraph
  const client = createReadwiseClient(userConfig.apiToken)
  const rawBooks = await loadAllExportedBooks(client, {
    updatedAfter: graphState.checkpoint?.updatedAfter ?? undefined,
    includeDeleted: true,
  })
  const graphSnapshot = await loadCurrentGraphSnapshotV1()

  return prepareSyncPlanV1({
    rawBooks,
    graphState,
    graphSnapshot,
  })
}
