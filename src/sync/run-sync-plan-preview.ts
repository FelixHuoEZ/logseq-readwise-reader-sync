import { createReadwiseClient, loadAllExportedBooks } from '../api'
import { readUserConfig } from '../config'
import { loadCurrentGraphSnapshotV1 } from '../graph'
import { loadPluginStateV1, savePluginStateV1 } from '../state'

import { buildLastRunSummaryFromPlanV1 } from './build-last-run-summary'
import { buildPendingRelinkQueueV1 } from './build-pending-relink-queue'
import { formatSyncPlanPreviewSummaryV1 } from './format-sync-plan-preview-summary'
import { prepareSyncPlanV1 } from './prepare-sync-plan'
import type { PreparedSyncPlanV1 } from './types'

const createRunId = (): string =>
  globalThis.crypto?.randomUUID?.() ??
  `run-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`

export const runSyncPlanPreviewV1 =
  async (): Promise<PreparedSyncPlanV1> => {
    const userConfig = readUserConfig()

    if (userConfig.apiToken.length === 0) {
      throw new Error('Readwise API token is not configured.')
    }

    const pluginState = await loadPluginStateV1()
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

    try {
      const client = createReadwiseClient(userConfig.apiToken)
      const rawBooks = await loadAllExportedBooks(client, {
        updatedAfter: pluginState.activeGraph.checkpoint?.updatedAfter ?? undefined,
        includeDeleted: true,
      })

      pluginState.activeGraph.runState = {
        ...pluginState.activeGraph.runState,
        phase: 'planning',
        message: `Planning V1 preview for ${rawBooks.length} Readwise page(s)...`,
      }
      await savePluginStateV1(pluginState)

      const graphSnapshot = await loadCurrentGraphSnapshotV1()
      const prepared = prepareSyncPlanV1({
        rawBooks,
        graphState: pluginState.activeGraph,
        graphSnapshot,
        startedAt,
        runId,
      })
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

      console.info('[Readwise V1 Preview] plan', prepared.plan)
      console.info('[Readwise V1 Preview] items', prepared.items)
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
