export interface GraphPageCandidateV1 {
  pageUuid: string
  pageTitle: string
  path: string | null
  matchKind?:
    | 'exact_title'
    | 'readwise_page_title'
    | 'rw_id'
    | 'property_url'
}

export interface GraphSnapshotV1 {
  graphId: string
  pageUuidExists: Record<string, boolean>
  pagesByExactTitle: Record<string, GraphPageCandidateV1[]>
  pagesByBridgeTitle: Record<string, GraphPageCandidateV1[]>
  pagesByReadwiseBookId: Record<string, GraphPageCandidateV1[]>
  pagesByCanonicalUrl: Record<string, GraphPageCandidateV1[]>
}

export interface GraphPageSnapshotSourceV1 {
  uuid: string
  name: string
  title?: string
  originalName?: string
  path?: string | null
  file?: Record<string, unknown> | null
  properties?: Record<string, unknown>
}
