import type { RenderHashInput, SemanticPage } from './types'

export const buildRenderHashInput = (page: SemanticPage): RenderHashInput => ({
  pageTitle: page.pageTitle,
  metadata: page.metadata.map((entry) => ({
    key: entry.key,
    value: entry.value,
  })),
  pageNote: page.pageNote
    ? {
        imageUrl: page.pageNote.imageUrl,
        text: page.pageNote.text,
      }
    : null,
  highlights: page.highlights.map((highlight) => ({
    uuid: highlight.uuid,
    text: highlight.text,
    imageUrl: highlight.imageUrl,
    locationLabel: highlight.locationLabel,
    locationUrl: highlight.locationUrl,
    createdDate: highlight.createdDate,
    tags: [...highlight.tags],
    note: highlight.note,
  })),
})
