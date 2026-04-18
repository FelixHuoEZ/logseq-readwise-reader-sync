import type { NormalizedBookExport, NormalizedHighlight } from '../normalizer'

export type DocumentFormat = 'org' | 'markdown'

export interface RenderRuntimeContext {
  format: DocumentFormat
  syncDate: string
  syncTime: string
  isNewPage: boolean
  hasNewHighlights: boolean
}

export interface PageRenderContext {
  book: NormalizedBookExport
  runtime: RenderRuntimeContext
}

export interface HighlightRenderContext {
  book: Pick<
    NormalizedBookExport,
    'userBookId' | 'title' | 'author' | 'category' | 'readwiseUrl'
  >
  highlight: NormalizedHighlight
  compatibleUuid: string
}

export interface SemanticMetadataEntry {
  key: string
  value: string | null
}

export interface SemanticPageNote {
  imageUrl: string | null
  summary: string | null
}

export interface SemanticSyncHeader {
  kind: 'first_sync' | 'new_highlights' | 'none'
  text: string | null
}

export interface SemanticHighlight {
  highlightId: number | string
  uuid: string
  text: string
  imageUrl: string | null
  locationLabel: string | null
  locationUrl: string | null
  createdDate: string
  tags: string[]
  note: string | null
}

export interface SemanticPage {
  format: DocumentFormat
  pageTitle: string
  metadata: SemanticMetadataEntry[]
  pageNote: SemanticPageNote | null
  syncHeader: SemanticSyncHeader
  highlights: SemanticHighlight[]
}

export interface EmittedBlock {
  text: string
  children?: EmittedBlock[]
}

export interface EmitResult {
  format: DocumentFormat
  pageProperties: Array<{
    key: string
    value: string | null
  }>
  metadataText: string
  pageNoteText: string | null
  syncHeaderText: string | null
  highlightBlocks: EmittedBlock[]
  bodyText: string
  pageContentText: string
  outputText: string
}

export interface RenderHashInput {
  pageTitle: string
  metadata: SemanticMetadataEntry[]
  pageNote: SemanticPageNote | null
  highlights: Array<{
    uuid: string
    text: string
    imageUrl: string | null
    locationLabel: string | null
    locationUrl: string | null
    createdDate: string
    tags: string[]
    note: string | null
  }>
}

export interface RenderedPage {
  format: DocumentFormat
  userBookId: number
  pageTitle: string
  semanticPage: SemanticPage
  emitResult: EmitResult
  renderHashInput: RenderHashInput
  renderHash: string
}
