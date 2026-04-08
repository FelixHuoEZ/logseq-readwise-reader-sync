import type { GraphCheckpointStateV1, GraphSnapshotV1 } from '../graph'
import type { NormalizedBookExport } from '../normalizer'
import type { PlannerInputItemV1, SyncPlanV1, SyncRunContextV1 } from '../planner'
import type { RenderRuntimeContext, RenderedPage } from '../renderer'
import type { GraphStateV1 } from '../state'
import type { ExportedBook } from '../types'

export interface PreparedSyncItemV1 {
  rawBook: ExportedBook
  book: NormalizedBookExport
  runtime: RenderRuntimeContext
  renderedPage: RenderedPage
  plannerInput: PlannerInputItemV1
}

export interface PrepareSyncPlanParamsV1 {
  rawBooks: ExportedBook[]
  graphState: GraphStateV1
  graphSnapshot: GraphSnapshotV1
  checkpointBeforeRun: GraphCheckpointStateV1 | null
  startedAt?: string
  runId?: string
}

export interface PreparedSyncPlanV1 {
  graphState: GraphStateV1
  graphSnapshot: GraphSnapshotV1
  runContext: SyncRunContextV1
  items: PreparedSyncItemV1[]
  plan: SyncPlanV1
}
