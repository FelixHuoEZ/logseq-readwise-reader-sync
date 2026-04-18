import type { BookCategory } from '../types'

export interface NormalizedHighlightTag {
  id?: number | null
  name: string
}

export interface NormalizedHighlight {
  id: number
  bookId: number
  isDeleted: boolean
  text: string
  imageUrl: string | null
  note: string | null
  location: number | null
  locationType: string | null
  locationLabel: string | null
  locationUrl: string | null
  highlightedAt: string | null
  createdAt: string
  updatedAt: string
  readwiseUrl: string
  tags: NormalizedHighlightTag[]
}

export interface NormalizedBookExport {
  userBookId: number
  isDeleted: boolean
  title: string
  readableTitle: string
  author: string | null
  category: BookCategory
  source: string | null
  sourceUrl: string | null
  uniqueUrl: string | null
  readwiseUrl: string
  readerDocumentUrl: string | null
  coverImageUrl: string | null
  documentTags: string[]
  documentNote: string | null
  summary: string | null
  publishedDate: string | null
  savedDate: string | null
  updatedAt: string
  highlights: NormalizedHighlight[]
}
