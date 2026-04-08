import type { ExportedHighlight } from '../types'
import type { NormalizedHighlight } from './types'

const buildLocationLabel = (
  highlight: ExportedHighlight,
): string | null => {
  if (highlight.location == null) return null
  return String(highlight.location)
}

export const normalizeHighlight = (
  bookId: number,
  highlight: ExportedHighlight,
): NormalizedHighlight => ({
  id: highlight.id,
  bookId,
  isDeleted: highlight.is_deleted,
  text: highlight.text,
  note: highlight.note,
  location: highlight.location ?? null,
  locationType: highlight.location_type ?? null,
  locationLabel: buildLocationLabel(highlight),
  locationUrl: highlight.readwise_url ?? null,
  highlightedAt: highlight.highlighted_at,
  createdAt: highlight.created_at,
  updatedAt: highlight.updated_at,
  readwiseUrl: highlight.readwise_url,
  tags: highlight.tags.map((tag) => ({
    id: tag.id,
    name: tag.name,
  })),
})
