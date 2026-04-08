import type { GraphCheckpointStateV1, GraphSnapshotV1 } from '../graph'
import type { SyncRunContextV1 } from '../planner'
import type { GraphStateV1 } from '../state'

const createRunId = (): string =>
  globalThis.crypto?.randomUUID?.() ??
  `run-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`

export const buildSyncRunContextV1 = (
  graphState: GraphStateV1,
  graphSnapshot: GraphSnapshotV1,
  checkpointBeforeRun: GraphCheckpointStateV1 | null,
  startedAt: string = new Date().toISOString(),
  runId: string = createRunId(),
): SyncRunContextV1 => ({
  runId,
  graphId: graphSnapshot.graphId,
  graphName: graphState.graphName,
  startedAt,
  checkpointBeforeRun,
  documentFormat: graphState.documentFormat,
})
