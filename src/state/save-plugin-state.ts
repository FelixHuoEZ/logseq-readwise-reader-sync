import type { PluginStateV1 } from './types'

const PLUGIN_STATE_JSON_KEY = 'pluginStateJson'

const sanitizePluginStateV1 = (state: PluginStateV1): PluginStateV1 => ({
  schemaVersion: 1,
  machineId: state.machineId,
  activeGraph: {
    graphId: state.activeGraph.graphId,
    graphName: state.activeGraph.graphName,
    readwiseAccountId: state.activeGraph.readwiseAccountId,
    archiveNamespace: state.activeGraph.archiveNamespace,
    uuidCompatMode: state.activeGraph.uuidCompatMode,
    documentFormat: state.activeGraph.documentFormat,
    runState: state.activeGraph.runState,
    pageIndex: state.activeGraph.pageIndex,
    pendingRelinkQueue: state.activeGraph.pendingRelinkQueue,
    lastRunSummary: state.activeGraph.lastRunSummary,
    lastSuccessAt: state.activeGraph.lastSuccessAt,
    lastFailureAt: state.activeGraph.lastFailureAt,
    lastFailureSummary: state.activeGraph.lastFailureSummary,
  },
})

export const savePluginStateV1 = async (
  state: PluginStateV1,
): Promise<void> => {
  await logseq.updateSettings({
    [PLUGIN_STATE_JSON_KEY]: JSON.stringify(sanitizePluginStateV1(state)),
  })
}
