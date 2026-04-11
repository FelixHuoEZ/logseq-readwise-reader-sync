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

const normalizeBoundaryBlankLines = (value: string) => {
  const lines = value.split('\n')
  const normalizeLine = (line: string) =>
    line
      .replaceAll('\u200B', '')
      .replaceAll('\u200C', '')
      .replaceAll('\u200D', '')
      .replaceAll('\uFEFF', '')
      .trim()

  let firstContentLine = 0
  while (
    firstContentLine < lines.length &&
    normalizeLine(lines[firstContentLine] ?? '').length === 0
  ) {
    firstContentLine += 1
  }

  let lastContentLine = lines.length - 1
  while (
    lastContentLine >= firstContentLine &&
    normalizeLine(lines[lastContentLine] ?? '').length === 0
  ) {
    lastContentLine -= 1
  }

  return lines.slice(firstContentLine, lastContentLine + 1).join('\n')
}

const emitMetadataText = (page: SemanticPage) => {
  const readwiseId = getMetadataValue(page, 'rw-id')
  const readerId = getMetadataValue(page, 'rw-reader-id')
  const author = getMetadataValue(page, 'AUTHOR')
  const category = getMetadataValue(page, 'CATEGORIES')
  const link = getMetadataValue(page, 'LINK')
  const tags = getMetadataValue(page, 'TAGS')
  const date = getMetadataValue(page, 'DATE')
  const published = getMetadataValue(page, 'PUBLISHED')
  const reservedKeys = new Set([
    'rw-id',
    'rw-reader-id',
    'AUTHOR',
    'CATEGORIES',
    'LINK',
    'TAGS',
    'DATE',
    'PUBLISHED',
  ])
  const extraMetadataLines = page.metadata
    .filter((entry) => !reservedKeys.has(entry.key))
    .map((entry) => emitPropertyLine(entry.key, entry.value))
  const imageLine = page.pageNote?.imageUrl
    ? wrapWikiLink(page.pageNote.imageUrl)
    : ''
  const summaryLine = page.pageNote?.summary ?? ''
  const noteLines = [imageLine, summaryLine].filter((line) => line.trim().length > 0)
  const noteSection =
    noteLines.length > 0
      ? ['#+BEGIN_NOTE', ...noteLines, '#+END_NOTE']
      : []

  return [
    ':PROPERTIES:',
    ...(readwiseId != null ? [emitPropertyLine('rw-id', readwiseId)] : []),
    ...(readerId != null ? [emitPropertyLine('rw-reader-id', readerId)] : []),
    emitPropertyLine('AUTHOR', author, wrapWikiLink),
    emitPropertyLine('CATEGORIES', category),
    emitPropertyLine('LINK', link),
    emitPropertyLine('TAGS', tags),
    emitPropertyLine('DATE', date, wrapWikiLink),
    emitPropertyLine('PUBLISHED', published, wrapWikiLink),
    ...extraMetadataLines,
    ':END:',
    ...noteSection,
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

  const emittedTags = ['[[ReadwiseHighlights]]', ...highlight.tags.map((tag) => `[[${tag}]]`)]
    .join('  ,  ')

  const [firstLine = '', ...restLines] = highlight.text.split('\n')
  const restText = normalizeBoundaryBlankLines(restLines.join('\n'))
  const noteSection = highlight.note ? [`* *Note*: ${highlight.note}`] : []
  const trailingSections = [restText, ...noteSection].filter(
    (section): section is string => section.length > 0,
  )

  return [
    `** ${firstLine}${locationSuffix}`,
    ':PROPERTIES:',
    `:created: [[${highlight.createdDate}]]`,
    `:tags: ${emittedTags}`,
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
