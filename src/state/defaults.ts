import type {
  GraphStateV1,
  PluginStateV1,
  RunStateV1,
} from './types'

const MACHINE_ID_STORAGE_KEY = 'logseq-readwise-plugin/machine-id-v1'

const resolveMachineId = () => {
  const existing = window.localStorage.getItem(MACHINE_ID_STORAGE_KEY)
  if (existing) return existing

  const next =
    globalThis.crypto?.randomUUID?.() ??
    `machine-${Date.now().toString(36)}-${Math.random()
      .toString(36)
      .slice(2, 10)}`

  window.localStorage.setItem(MACHINE_ID_STORAGE_KEY, next)
  return next
}

export const createEmptyRunStateV1 = (): RunStateV1 => ({
  status: 'idle',
  phase: 'idle',
  runId: null,
  startedAt: null,
  endedAt: null,
  message: null,
  activeUserBookId: null,
})

export const createEmptyGraphStateV1 = (
  graphId: string,
  graphName: string,
): GraphStateV1 => ({
  graphId,
  graphName,
  readwiseAccountId: null,
  archiveNamespace: 'Readwise Archived',
  uuidCompatMode: 'rw-location-url-v1',
  documentFormat: 'org',
  runState: createEmptyRunStateV1(),
  pageIndex: {},
  pendingRelinkQueue: [],
  lastRunSummary: null,
  lastSuccessAt: null,
  lastFailureAt: null,
  lastFailureSummary: null,
})

export const createEmptyPluginStateV1 = (
  graphId: string,
  graphName: string,
): PluginStateV1 => ({
  schemaVersion: 1,
  machineId: resolveMachineId(),
  activeGraph: createEmptyGraphStateV1(graphId, graphName),
})
