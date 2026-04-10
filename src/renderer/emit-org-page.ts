import type { EmitResult, EmittedBlock, SemanticPage } from './types'

const getMetadataValue = (
  page: SemanticPage,
  key: string,
): string | null =>
  page.metadata.find((entry) => entry.key === key)?.value ?? null

const emitPropertyLine = (
  key: string,
  value: string | null,
  formatter?: (nextValue: string) => string,
) => `:${key}: ${value ? (formatter ? formatter(value) : value) : ''}`

const wrapWikiLink = (value: string) => `[[${value}]]`

const emitMetadataText = (page: SemanticPage) => {
  const author = getMetadataValue(page, 'AUTHOR')
  const category = getMetadataValue(page, 'CATEGORIES')
  const link = getMetadataValue(page, 'LINK')
  const tags = getMetadataValue(page, 'TAGS')
  const date = getMetadataValue(page, 'DATE')
  const published = getMetadataValue(page, 'PUBLISHED')
  const imageLine = page.pageNote?.imageUrl
    ? wrapWikiLink(page.pageNote.imageUrl)
    : ''
  const summaryLine = page.pageNote?.summary ?? ''

  return [
    ':PROPERTIES:',
    emitPropertyLine('AUTHOR', author, wrapWikiLink),
    emitPropertyLine('CATEGORIES', category),
    emitPropertyLine('LINK', link),
    emitPropertyLine('TAGS', tags),
    emitPropertyLine('DATE', date, wrapWikiLink),
    emitPropertyLine('PUBLISHED', published, wrapWikiLink),
    ':END:',
    '#+BEGIN_NOTE',
    imageLine,
    summaryLine,
    '#+END_NOTE',
  ].join('\n')
}

const emitHighlightMainText = (
  highlight: SemanticPage['highlights'][number],
) => {
  const locationSuffix =
    highlight.locationLabel && highlight.locationUrl
      ? ` ([[${highlight.locationUrl}][${highlight.locationLabel}]])`
      : highlight.locationLabel
        ? ` (${highlight.locationLabel})`
        : ''

  const tagSuffix =
    highlight.tags.length > 0
      ? `${highlight.tags.map((tag) => `  ,  [[${tag}]]`).join('')} `
      : ''

  const [firstLine = '', ...restLines] = highlight.text.split('\n')
  const restText = restLines.join('\n')
  const noteSection = highlight.note ? [`* *Note*: ${highlight.note}`] : []
  const trailingSections = [restText, ...noteSection].filter(
    (section): section is string => section.length > 0,
  )

  return [
    `** ${firstLine}${locationSuffix}`,
    ':PROPERTIES:',
    `:created: [[${highlight.createdDate}]]`,
    `:tags: [[ReadwiseHighlights]]${tagSuffix}`,
    `:id: ${highlight.uuid}`,
    ':END:',
    ...trailingSections.flatMap((section) => ['', section]),
  ].join('\n')
}

const emitHighlightBlocks = (page: SemanticPage): EmittedBlock[] =>
  page.highlights.map((highlight) => ({
    text: emitHighlightMainText(highlight),
  }))

export const emitOrgPage = (page: SemanticPage): EmitResult => {
  const metadataText = emitMetadataText(page)
  const syncHeaderText = page.syncHeader.text
    ? `* ${page.syncHeader.text}`
    : ''
  const highlightBlocks = emitHighlightBlocks(page)
  const highlightTexts = highlightBlocks.map((block) =>
    [block.text, ...(block.children?.map((child) => child.text) ?? [])].join('\n'),
  )
  const contentText = [syncHeaderText, ...highlightTexts]
    .filter((part): part is string => typeof part === 'string' && part.length > 0)
    .join('\n')

  const outputParts = [metadataText, contentText].filter(
    (part): part is string => typeof part === 'string' && part.length > 0,
  )

  return {
    format: 'org',
    metadataText,
    syncHeaderText,
    highlightBlocks,
    outputText: outputParts.join('\n\n'),
  }
}
