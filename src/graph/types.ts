export interface GraphPageCandidateV1 {
  pageUuid: string
  pageTitle: string
  path: string | null
}

export interface GraphSnapshotV1 {
  graphId: string
  pageUuidExists: Record<string, boolean>
  pagesByExactTitle: Record<string, GraphPageCandidateV1[]>
}

export interface GraphPageSnapshotSourceV1 {
  uuid: string
  name: string
  title?: string
  originalName?: string
  path?: string | null
  file?: Record<string, unknown> | null
}
