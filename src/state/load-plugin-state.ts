import { loadCurrentGraphContextV1 } from '../graph'
import { createEmptyPluginStateV1 } from './defaults'
import type { GraphStateV1, PluginStateV1 } from './types'

const PLUGIN_STATE_JSON_KEY = 'pluginStateJson'

const isObjectRecord = (
  value: unknown,
): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null

const isPluginStateV1 = (value: unknown): value is PluginStateV1 => {
  if (!isObjectRecord(value)) return false
  if (value.schemaVersion !== 1) return false
  if (!isObjectRecord(value.activeGraph)) return false
  return typeof value.machineId === 'string'
}

const coerceGraphStateV1 = (
  value: unknown,
  fallback: GraphStateV1,
  graphName: string,
): GraphStateV1 => {
  if (!isObjectRecord(value)) {
    return fallback
  }

  return {
    graphId:
      typeof value.graphId === 'string' && value.graphId.length > 0
        ? value.graphId
        : fallback.graphId,
    graphName,
    readwiseAccountId:
      typeof value.readwiseAccountId === 'string'
        ? value.readwiseAccountId
        : fallback.readwiseAccountId,
    archiveNamespace:
      value.archiveNamespace === 'Readwise Archived'
        ? value.archiveNamespace
        : fallback.archiveNamespace,
    uuidCompatMode:
      value.uuidCompatMode === 'rw-location-url-v1'
        ? value.uuidCompatMode
        : fallback.uuidCompatMode,
    documentFormat:
      value.documentFormat === 'markdown' ? 'markdown' : fallback.documentFormat,
    runState: isObjectRecord(value.runState)
      ? {
          status:
            value.runState.status === 'planning' ||
            value.runState.status === 'applying' ||
            value.runState.status === 'success' ||
            value.runState.status === 'error' ||
            value.runState.status === 'aborted'
              ? value.runState.status
              : fallback.runState.status,
          phase:
            value.runState.phase === 'fetching' ||
            value.runState.phase === 'normalizing' ||
            value.runState.phase === 'planning' ||
            value.runState.phase === 'writing' ||
            value.runState.phase === 'finalizing'
              ? value.runState.phase
              : fallback.runState.phase,
          runId:
            typeof value.runState.runId === 'string'
              ? value.runState.runId
              : null,
          startedAt:
            typeof value.runState.startedAt === 'string'
              ? value.runState.startedAt
              : null,
          endedAt:
            typeof value.runState.endedAt === 'string'
              ? value.runState.endedAt
              : null,
          message:
            typeof value.runState.message === 'string'
              ? value.runState.message
              : null,
          activeUserBookId:
            typeof value.runState.activeUserBookId === 'number'
              ? value.runState.activeUserBookId
              : null,
        }
      : fallback.runState,
    pageIndex: isObjectRecord(value.pageIndex)
      ? (value.pageIndex as GraphStateV1['pageIndex'])
      : fallback.pageIndex,
    pendingRelinkQueue: Array.isArray(value.pendingRelinkQueue)
      ? (value.pendingRelinkQueue as GraphStateV1['pendingRelinkQueue'])
      : fallback.pendingRelinkQueue,
    lastRunSummary: isObjectRecord(value.lastRunSummary)
      ? (value.lastRunSummary as unknown as GraphStateV1['lastRunSummary'])
      : fallback.lastRunSummary,
    lastSuccessAt:
      typeof value.lastSuccessAt === 'string' ? value.lastSuccessAt : null,
    lastFailureAt:
      typeof value.lastFailureAt === 'string' ? value.lastFailureAt : null,
    lastFailureSummary:
      typeof value.lastFailureSummary === 'string'
        ? value.lastFailureSummary
        : null,
  }
}

export const loadPluginStateV1 = async (): Promise<PluginStateV1> => {
  const { graphId, graphName } = await loadCurrentGraphContextV1()
  const raw = logseq.settings?.[PLUGIN_STATE_JSON_KEY]

  if (typeof raw !== 'string' || raw.trim() === '') {
    return createEmptyPluginStateV1(graphId, graphName)
  }

  try {
    const parsed: unknown = JSON.parse(raw)
    if (!isPluginStateV1(parsed)) {
      return createEmptyPluginStateV1(graphId, graphName)
    }

    const fallback = createEmptyPluginStateV1(graphId, graphName)

    if (parsed.activeGraph.graphId === graphId) {
      return {
        ...parsed,
        activeGraph: coerceGraphStateV1(parsed.activeGraph, fallback.activeGraph, graphName),
      }
    }

    return {
      schemaVersion: 1,
      machineId: parsed.machineId,
      activeGraph: fallback.activeGraph,
    }
  } catch {
    return createEmptyPluginStateV1(graphId, graphName)
  }
}
