import { createEmptyPluginStateV1 } from './defaults'
import type { PluginStateV1 } from './types'

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

const getCurrentGraphContext = () => {
  const graphId = window.location.pathname || 'unknown-graph'
  const graphName =
    document.title.trim() || window.location.pathname || 'Current Graph'

  return { graphId, graphName }
}

export const loadPluginStateV1 = async (): Promise<PluginStateV1> => {
  const { graphId, graphName } = getCurrentGraphContext()
  const raw = logseq.settings?.[PLUGIN_STATE_JSON_KEY]

  if (typeof raw !== 'string' || raw.trim() === '') {
    return createEmptyPluginStateV1(graphId, graphName)
  }

  try {
    const parsed: unknown = JSON.parse(raw)
    if (!isPluginStateV1(parsed)) {
      return createEmptyPluginStateV1(graphId, graphName)
    }

    if (parsed.activeGraph.graphId === graphId) {
      return {
        ...parsed,
        activeGraph: {
          ...parsed.activeGraph,
          graphName,
        },
      }
    }

    return {
      schemaVersion: 1,
      machineId: parsed.machineId,
      activeGraph: createEmptyPluginStateV1(graphId, graphName).activeGraph,
    }
  } catch {
    return createEmptyPluginStateV1(graphId, graphName)
  }
}
