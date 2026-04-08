import { createReadwiseClient, loadAllExportedBooks } from '../api'
import { readUserConfig } from '../config'
import {
  loadCurrentGraphSnapshotV1,
  loadGraphCheckpointStateV1,
} from '../graph'
import {
  buildRuntimeGraphStateV1,
  loadPluginStateV1,
  savePluginStateV1,
} from '../state'

import { buildLastRunSummaryFromPlanV1 } from './build-last-run-summary'
import { buildPendingRelinkQueueV1 } from './build-pending-relink-queue'
import {
  buildSyncPlanActionRowsV1,
  buildSyncPlanItemRowsV1,
} from './format-sync-plan-preview-details'
import { formatSyncPlanPreviewSummaryV1 } from './format-sync-plan-preview-summary'
import { prepareSyncPlanV1 } from './prepare-sync-plan'
import { resolvePreviewUpdatedAfterV1 } from './resolve-preview-updated-after'
import type { PreparedSyncPlanV1 } from './types'

const createRunId = (): string =>
  globalThis.crypto?.randomUUID?.() ??
  `run-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`

export const runSyncPlanPreviewV1 =
  async (): Promise<PreparedSyncPlanV1> => {
    console.info('[Readwise V1 Preview] starting pipeline')
    const userConfig = readUserConfig()

    if (userConfig.apiToken.length === 0) {
      throw new Error('Readwise API token is not configured.')
    }

    const [pluginState, checkpointBeforeRun] = await Promise.all([
      loadPluginStateV1(),
      loadGraphCheckpointStateV1(),
    ])
    const runId = createRunId()
    const startedAt = new Date().toISOString()

    pluginState.activeGraph.runState = {
      status: 'planning',
      phase: 'fetching',
      runId,
      startedAt,
      endedAt: null,
      message: 'Loading Readwise export for V1 sync preview...',
      activeUserBookId: null,
    }
    await savePluginStateV1(pluginState)
    console.info('[Readwise V1 Preview] fetching export')

    try {
      const client = createReadwiseClient(userConfig.apiToken)
      const updatedAfter = resolvePreviewUpdatedAfterV1(
        checkpointBeforeRun,
        userConfig,
      )
      const maxBooks = updatedAfter == null ? 20 : undefined
      console.info('[Readwise V1 Preview] updatedAfter', updatedAfter)
      console.info('[Readwise V1 Preview] sampleMode', updatedAfter == null)
      console.info('[Readwise V1 Preview] checkpointBeforeRun', checkpointBeforeRun)
      if (maxBooks != null) {
        pluginState.activeGraph.runState = {
          ...pluginState.activeGraph.runState,
          message:
            'Loading Readwise export for V1 sync preview (sample mode: first 20 pages)...',
        }
        await savePluginStateV1(pluginState)
      }
      const rawBooks = await loadAllExportedBooks(client, {
        updatedAfter: updatedAfter ?? undefined,
        includeDeleted: true,
      }, {
        maxBooks,
        onPage: ({ pageNumber, pageResultCount, totalFetched, maxBooks }) => {
          console.info('[Readwise V1 Preview] export page', {
            pageNumber,
            pageResultCount,
            totalFetched,
            maxBooks,
          })
        },
      })
      console.info('[Readwise V1 Preview] fetched export', rawBooks.length)

      pluginState.activeGraph.runState = {
        ...pluginState.activeGraph.runState,
        phase: 'planning',
        message: `Planning V1 preview for ${rawBooks.length} Readwise page(s)...`,
      }
      await savePluginStateV1(pluginState)

      console.info('[Readwise V1 Preview] loading graph snapshot')
      const graphSnapshot = await loadCurrentGraphSnapshotV1()
      console.info('[Readwise V1 Preview] loaded graph snapshot', {
        graphId: graphSnapshot.graphId,
        knownPageUuids: Object.keys(graphSnapshot.pageUuidExists).length,
        titledPages: Object.keys(graphSnapshot.pagesByExactTitle).length,
        bridgeTitles: Object.keys(graphSnapshot.pagesByBridgeTitle).length,
        rwIdPages: Object.keys(graphSnapshot.pagesByReadwiseBookId).length,
        urlCandidates: Object.keys(graphSnapshot.pagesByCanonicalUrl).length,
      })
      pluginState.activeGraph = buildRuntimeGraphStateV1(
        pluginState.activeGraph,
        graphSnapshot,
      )
      console.info('[Readwise V1 Preview] hydrated runtime page index', {
        entries: Object.keys(pluginState.activeGraph.pageIndex).length,
        pagePropertyEntries: Object.values(pluginState.activeGraph.pageIndex).filter(
          (entry) => entry.identitySource === 'page_property',
        ).length,
      })
      console.info('[Readwise V1 Preview] preparing plan')
      const prepared = prepareSyncPlanV1({
        rawBooks,
        graphState: pluginState.activeGraph,
        graphSnapshot,
        checkpointBeforeRun,
        startedAt,
        runId,
      })
      console.info('[Readwise V1 Preview] prepared plan')
      const completedAt = new Date().toISOString()
      const summary = formatSyncPlanPreviewSummaryV1(prepared)

      pluginState.activeGraph.pendingRelinkQueue = buildPendingRelinkQueueV1(
        prepared.items,
        pluginState.activeGraph.pendingRelinkQueue,
        startedAt,
      )
      pluginState.activeGraph.lastRunSummary = buildLastRunSummaryFromPlanV1(
        prepared.plan,
        completedAt,
      )
      pluginState.activeGraph.runState = {
        status: 'success',
        phase: 'idle',
        runId,
        startedAt,
        endedAt: completedAt,
        message: summary,
        activeUserBookId: null,
      }

      await savePluginStateV1(pluginState)

      const actionRows = buildSyncPlanActionRowsV1(prepared)
      const itemRows = buildSyncPlanItemRowsV1(prepared)

      console.info('[Readwise V1 Preview] summary', summary)
      console.info('[Readwise V1 Preview] checkpointDecision', prepared.plan.checkpointDecision)
      console.info('[Readwise V1 Preview] plan', prepared.plan)
      console.info('[Readwise V1 Preview] items', prepared.items)
      if (typeof console.table === 'function') {
        console.table(actionRows)
        console.table(itemRows)
      } else {
        console.info('[Readwise V1 Preview] actionRows', actionRows)
        console.info('[Readwise V1 Preview] itemRows', itemRows)
      }
      logseq.UI.showMsg(summary)

      return prepared
    } catch (error) {
      const completedAt = new Date().toISOString()
      const message =
        error instanceof Error ? error.message : String(error)

      pluginState.activeGraph.runState = {
        status: 'error',
        phase: 'idle',
        runId,
        startedAt,
        endedAt: completedAt,
        message,
        activeUserBookId: null,
      }
      pluginState.activeGraph.lastFailureAt = completedAt
      pluginState.activeGraph.lastFailureSummary = message

      await savePluginStateV1(pluginState)
      logseq.UI.showMsg(`V1 preview failed: ${message}`)

      throw error
    }
  }
