export type StoredDocumentFormatV1 = 'org' | 'markdown'

export type RunStatusV1 =
  | 'idle'
  | 'planning'
  | 'applying'
  | 'success'
  | 'error'
  | 'aborted'

export type RunPhaseV1 =
  | 'idle'
  | 'fetching'
  | 'normalizing'
  | 'planning'
  | 'writing'
  | 'finalizing'

export type PendingRelinkStatusV1 = 'pending' | 'accepted' | 'rejected'

export type CheckpointBlockReasonV1 =
  | 'apply_failed'
  | 'graph_switched'
  | 'run_aborted'
  | 'planner_incomplete'

export interface PluginStateV1 {
  schemaVersion: 1
  machineId: string
  activeGraph: GraphStateV1
}

export interface GraphStateV1 {
  graphId: string
  graphName: string
  readwiseAccountId: string | null
  archiveNamespace: 'Readwise Archived'
  uuidCompatMode: 'rw-location-url-v1'
  documentFormat: StoredDocumentFormatV1
  runState: RunStateV1
  pageIndex: Record<string, PageIndexEntryV1>
  pendingRelinkQueue: PendingRelinkEntryV1[]
  lastRunSummary: LastRunSummaryV1 | null
  lastSuccessAt: string | null
  lastFailureAt: string | null
  lastFailureSummary: string | null
}

export interface RunStateV1 {
  status: RunStatusV1
  phase: RunPhaseV1
  runId: string | null
  startedAt: string | null
  endedAt: string | null
  message: string | null
  activeUserBookId: number | null
}

export interface PageIndexEntryV1 {
  userBookId: number
  pageUuid: string
  pageTitle: string
  status: 'active' | 'missing' | 'deleted' | 'needs_relink'
  lastRemoteUpdatedAt: string | null
  lastAppliedAt: string | null
  lastAppliedRenderHash: string | null
  lastSeenHighlightCount: number | null
  lastKnownPagePath: string | null
  identitySource?: 'page_property' | 'settings_cache'
}

export interface PendingRelinkEntryV1 {
  userBookId: number
  remoteTitle: string
  candidatePageUuid: string
  candidatePageTitle: string
  detectedAt: string
  reason: 'title_exact_match' | 'property_match'
  status: PendingRelinkStatusV1
}

export interface LastRunSummaryV1 {
  runId: string
  completedAt: string
  createCount: number
  updateCount: number
  skipCount: number
  relinkCount: number
  deleteCount: number
  blockedCheckpoint: boolean
  blockedCheckpointReasons: CheckpointBlockReasonV1[]
}
