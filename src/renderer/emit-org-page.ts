import type { EmitResult, EmittedBlock, SemanticPage } from './types'

const getMetadataValue = (page: SemanticPage, key: string): string | null =>
  page.metadata.find((entry) => entry.key === key)?.value ?? null

const emitPropertyLine = (
  key: string,
  value: string | null,
  formatter?: (nextValue: string) => string,
) => `:${key}: ${value ? (formatter ? formatter(value) : value) : ''}`

const wrapWikiLink = (value: string) => `[[${value}]]`

export const buildPageProperties = (
  page: SemanticPage,
): Array<{ key: string; value: string | null }> => {
  const readwiseId = getMetadataValue(page, 'rw-id')
  const readerId = getMetadataValue(page, 'rw-reader-id')
  const author = getMetadataValue(page, 'AUTHOR')
  const category = getMetadataValue(page, 'CATEGORIES')
  const link = getMetadataValue(page, 'LINK')
  const tags = getMetadataValue(page, 'TAGS')
  const date = getMetadataValue(page, 'DATE')
  const published = getMetadataValue(page, 'PUBLISHED')
  const saved = getMetadataValue(page, 'SAVED')
  const summary = getMetadataValue(page, 'summary')
  const reservedKeys = new Set([
    'rw-id',
    'rw-reader-id',
    'AUTHOR',
    'CATEGORIES',
    'LINK',
    'TAGS',
    'DATE',
    'PUBLISHED',
    'SAVED',
    'summary',
  ])
  const extraMetadataEntries = page.metadata.filter(
    (entry) => !reservedKeys.has(entry.key),
  )

  return [
    ...(readwiseId ? [{ key: 'rw-id', value: readwiseId }] : []),
    ...(readerId ? [{ key: 'rw-reader-id', value: readerId }] : []),
    { key: 'AUTHOR', value: author ? wrapWikiLink(author) : null },
    { key: 'CATEGORIES', value: category },
    { key: 'LINK', value: link },
    { key: 'TAGS', value: tags },
    { key: 'DATE', value: date ? wrapWikiLink(date) : null },
    { key: 'PUBLISHED', value: published ? wrapWikiLink(published) : null },
    { key: 'SAVED', value: saved ? wrapWikiLink(saved) : null },
    { key: 'summary', value: summary },
    ...extraMetadataEntries.map((entry) => ({
      key: entry.key,
      value: entry.value,
    })),
  ]
}

export const emitPageNoteText = (page: SemanticPage): string | null => {
  const imageLine = page.pageNote?.imageUrl
    ? wrapWikiLink(page.pageNote.imageUrl)
    : ''
  const noteText = normalizeBoundaryBlankLines(page.pageNote?.text ?? '')
  const noteLines = [imageLine, noteText].filter((line) => line.trim().length > 0)

  if (noteLines.length === 0) {
    return null
  }

  return ['#+BEGIN_NOTE', ...noteLines, '#+END_NOTE'].join('\n')
}

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

const emitMetadataText = (
  pageProperties: Array<{ key: string; value: string | null }>,
) => {
  return [
    ':PROPERTIES:',
    ...pageProperties.map((entry) => emitPropertyLine(entry.key, entry.value)),
    ':END:',
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

  const emittedTags = [
    '[[ReadwiseHighlights]]',
    ...highlight.tags.map((tag) => `[[${tag}]]`),
  ].join('  ,  ')

  const [firstLine = '', ...restLines] = highlight.text.split('\n')
  const restText = normalizeBoundaryBlankLines(restLines.join('\n'))
  const orderedContentSections = (
    highlight.contentSegments.length > 0
      ? highlight.contentSegments
      : highlight.imageUrl
        ? [{ kind: 'image' as const, value: highlight.imageUrl }]
        : []
  )
    .map((segment) =>
      segment.kind === 'image' ? wrapWikiLink(segment.value) : segment.value,
    )
    .map((value) => normalizeBoundaryBlankLines(value))
    .filter((value) => value.length > 0)
  const trailingSections = [restText, ...orderedContentSections].filter(
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

const emitChildNoteBlockText = (level: number, value: string) => {
  const normalizedValue = normalizeBoundaryBlankLines(value)
  if (normalizedValue.length === 0) return ''

  const prefix = '*'.repeat(level)

  return [`${prefix} #+BEGIN_NOTE`, normalizedValue, '#+END_NOTE'].join('\n')
}

const emitHighlightBlocks = (page: SemanticPage): EmittedBlock[] =>
  page.highlights.map((highlight) => {
    const children: EmittedBlock[] = []

    if (highlight.note) {
      children.push({
        text: emitChildNoteBlockText(3, highlight.note),
      })
    }

    return {
      text: emitHighlightMainText(highlight),
      children: children.length > 0 ? children : undefined,
    }
  })

export const emitOrgPage = (page: SemanticPage): EmitResult => {
  const pageProperties = buildPageProperties(page)
  const metadataText = emitMetadataText(pageProperties)
  const pageNoteText = emitPageNoteText(page)
  const highlightBlocks = emitHighlightBlocks(page)
  const highlightTexts = highlightBlocks.map((block) =>
    [block.text, ...(block.children?.map((child) => child.text) ?? [])].join(
      '\n',
    ),
  )
  const bodyText = highlightTexts
    .filter(
      (part): part is string => typeof part === 'string' && part.length > 0,
    )
    .join('\n')
  const pageContentText = [pageNoteText, bodyText]
    .filter(
      (part): part is string => typeof part === 'string' && part.length > 0,
    )
    .join('\n\n')

  const outputParts = [metadataText, pageContentText].filter(
    (part): part is string => typeof part === 'string' && part.length > 0,
  )

  return {
    format: 'org',
    pageProperties,
    metadataText,
    pageNoteText,
    syncHeaderText: null,
    highlightBlocks,
    bodyText,
    pageContentText,
    outputText: outputParts.join('\n\n'),
  }
}
