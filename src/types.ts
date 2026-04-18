export interface Tag {
  id: number
  name: string
}

export type BookCategory =
  | 'books'
  | 'articles'
  | 'tweets'
  | 'supplementals'
  | 'podcasts'

export interface ExportedHighlight {
  id: number
  is_deleted: boolean
  text: string
  location: number
  location_type: string
  note: string | null
  color: string
  highlighted_at: string | null
  created_at: string
  updated_at: string
  external_id: string | null
  end_location: number | null
  url: string | null
  book_id: number
  tags: Tag[]
  is_favorite: boolean
  is_discard: boolean
  readwise_url: string
  image_url?: string | null
}

export interface ExportedBook {
  user_book_id: number
  is_deleted: boolean
  title: string
  author: string
  readable_title: string
  source: string
  cover_image_url: string | null
  unique_url: string | null
  book_tags: Tag[]
  category: BookCategory
  document_note: string | null
  summary: string | null
  readwise_url: string
  source_url: string | null
  external_id: string | null
  asin: string | null
  highlights: ExportedHighlight[]
}

export type ExportedBookIdentity = Pick<ExportedBook, 'user_book_id' | 'title'>

export interface ReaderDocument {
  id: string
  url: string
  parent_id: string | null
  source_url: string | null
  title: string | null
  author: string | null
  source: string | null
  category: string | null
  location: string | null
  tags: Record<string, unknown> | null
  site_name: string | null
  word_count: number | null
  reading_time: string | null
  created_at: string
  updated_at: string
  published_date: string | null
  summary: string | null
  image_url: string | null
  content: string | null
  notes: string | null
  reading_progress: number | null
  first_opened_at: string | null
  last_opened_at: string | null
  saved_at: string | null
  last_moved_at: string | null
  html_content?: string | null
}

export interface ReadwisePageProp {
  key: string
  schema: {
    type: string
    cardinality?: 'many' | 'one'
  }
}

export interface ExportParams {
  updatedAfter?: string
  ids?: number[]
  includeDeleted?: boolean
  pageCursor?: string
}

export interface ExportResponse {
  count: number
  nextPageCursor: string | null
  results: ExportedBook[]
}

export interface ReaderListParams {
  id?: string
  updatedAfter?: string
  location?: string
  category?: string
  limit?: number
  pageCursor?: string
  withHtmlContent?: boolean
}

export interface ReaderListResponse {
  count: number
  nextPageCursor: string | null
  results: ReaderDocument[]
}

export type SyncStatus = 'idle' | 'fetching' | 'syncing' | 'completed' | 'error'
