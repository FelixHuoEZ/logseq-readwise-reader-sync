import type { GraphPageCandidateV1, GraphSnapshotV1 } from '../graph'
import type { NormalizedBookExport } from '../normalizer'
import type { RenderedPage } from '../renderer'
import type {
  CheckpointBlockReasonV1,
  GraphStateV1,
  PageIndexEntryV1,
  PendingRelinkEntryV1,
  SyncCheckpointV1,
} from '../state'
export type { GraphPageCandidateV1, GraphSnapshotV1 } from '../graph'

export interface PlannerInputItemV1 {
  book: NormalizedBookExport
  renderedPage: RenderedPage
  existingPageIndexEntry: PageIndexEntryV1 | null
  exactTitleCandidates: GraphPageCandidateV1[]
  propertyMatchCandidates: GraphPageCandidateV1[]
  mappedPageExists: boolean
  acceptedPendingRelink: PendingRelinkEntryV1 | null
}

export interface SyncRunContextV1 {
  runId: string
  graphId: string
  graphName: string
  startedAt: string
  checkpointBeforeRun: SyncCheckpointV1 | null
  documentFormat: GraphStateV1['documentFormat']
}

export type PlanReasonCodeV1 =
  | 'missing_mapping'
  | 'mapped_page_exists_remote_unchanged'
  | 'mapped_page_exists_remote_changed'
  | 'mapped_page_missing'
  | 'title_exact_match_pending_confirmation'
  | 'property_match_pending_confirmation'
  | 'remote_deleted_local_exists'
  | 'remote_deleted_local_missing'
  | 'manual_pause'
  | 'redundant_retry'

export interface PageSyncActionBaseV1 {
  actionId: string
  userBookId: number
  remoteTitle: string
  reasonCode: PlanReasonCodeV1
  reasonDetail: string
  blocksCheckpoint: boolean
}

export interface CreatePageActionV1 extends PageSyncActionBaseV1 {
  type: 'create_page'
  renderedPage: RenderedPage
}

export interface UpdatePageActionV1 extends PageSyncActionBaseV1 {
  type: 'update_page'
  pageUuid: string
  renderedPage: RenderedPage
  previousRenderHash: string | null
  nextRenderHash: string
}

export type SkipReasonV1 =
  | 'remote_unchanged'
  | 'local_missing_but_deleted_remote'
  | 'redundant_retry'
  | 'manual_pause'
  | 'awaiting_manual_relink'

export interface SkipPageActionV1 extends PageSyncActionBaseV1 {
  type: 'skip_page'
  skipReason: SkipReasonV1
  pageUuid: string | null
  renderedPage: RenderedPage | null
}

export interface RelinkPageActionV1 extends PageSyncActionBaseV1 {
  type: 'relink_page'
  targetPageUuid: string
  candidatePageTitle: string
  renderedPage: RenderedPage
}

export interface MarkPageDeletedActionV1 extends PageSyncActionBaseV1 {
  type: 'mark_page_deleted'
  pageUuid: string
  archiveNamespace: 'Readwise Archived'
}

export type PageSyncActionV1 =
  | CreatePageActionV1
  | UpdatePageActionV1
  | SkipPageActionV1
  | RelinkPageActionV1
  | MarkPageDeletedActionV1

export interface CheckpointDecisionV1 {
  shouldCommit: boolean
  nextUpdatedAfter: string | null
  reasons: CheckpointBlockReasonV1[]
}

export interface SyncPlanSummaryV1 {
  createCount: number
  updateCount: number
  skipCount: number
  relinkCount: number
  deleteCount: number
  pendingRelinkCount: number
}

export interface SyncPlanV1 {
  runContext: SyncRunContextV1
  actions: PageSyncActionV1[]
  summary: SyncPlanSummaryV1
  checkpointDecision: CheckpointDecisionV1
}

export const PAGE_ACTION_ORDER_V1: ReadonlyArray<PageSyncActionV1['type']> = [
  'relink_page',
  'create_page',
  'update_page',
  'mark_page_deleted',
  'skip_page',
]
