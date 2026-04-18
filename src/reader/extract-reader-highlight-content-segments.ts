export interface ReaderHighlightContentSegment {
  kind: 'text' | 'image'
  value: string
}

const MARKDOWN_IMAGE_PATTERN = /!\[[^\]]*]\(([^)]+)\)/g

const BLOCK_TAGS = new Set([
  'P',
  'DIV',
  'SECTION',
  'ARTICLE',
  'BLOCKQUOTE',
  'LI',
  'UL',
  'OL',
  'FIGURE',
  'FIGCAPTION',
  'PRE',
  'H1',
  'H2',
  'H3',
  'H4',
  'H5',
  'H6',
])

const normalizeOptionalText = (value: string | null | undefined) => {
  if (typeof value !== 'string') return null

  const normalized = value
    .replaceAll('\u00A0', ' ')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim()

  return normalized.length > 0 ? normalized : null
}

export const isLikelyProfileImageUrl = (value: string | null | undefined) =>
  typeof value === 'string' && /\/profile_images\//i.test(value)

export const normalizeReaderImageUrl = (value: string | null | undefined) => {
  const normalized = normalizeOptionalText(value)
  if (!normalized) return null
  if (isLikelyProfileImageUrl(normalized)) return null
  return normalized
}

const pushTextSegment = (
  segments: ReaderHighlightContentSegment[],
  value: string,
) => {
  const normalizedValue = normalizeOptionalText(value)
  if (!normalizedValue) return

  const previous = segments[segments.length - 1]
  if (previous?.kind === 'text') {
    previous.value = normalizeOptionalText(
      `${previous.value}\n\n${normalizedValue}`,
    ) ?? previous.value
    return
  }

  segments.push({ kind: 'text', value: normalizedValue })
}

const pushImageSegment = (
  segments: ReaderHighlightContentSegment[],
  value: string | null | undefined,
) => {
  const normalizedValue = normalizeReaderImageUrl(value)
  if (!normalizedValue) return

  const previous = segments[segments.length - 1]
  if (previous?.kind === 'image' && previous.value === normalizedValue) {
    return
  }

  segments.push({ kind: 'image', value: normalizedValue })
}

const extractSegmentsFromHtml = (
  htmlContent: string,
): ReaderHighlightContentSegment[] => {
  if (typeof DOMParser === 'undefined') return []

  try {
    const parser = new DOMParser()
    const document = parser.parseFromString(htmlContent, 'text/html')
    const segments: ReaderHighlightContentSegment[] = []
    let textBuffer = ''

    const flushTextBuffer = () => {
      if (textBuffer.length === 0) return
      pushTextSegment(segments, textBuffer)
      textBuffer = ''
    }

    const walkNode = (node: Node) => {
      if (node.nodeType === Node.TEXT_NODE) {
        textBuffer += node.textContent ?? ''
        return
      }

      if (!(node instanceof Element)) return

      if (node.tagName === 'IMG') {
        flushTextBuffer()
        pushImageSegment(segments, node.getAttribute('src'))
        return
      }

      if (node.tagName === 'BR') {
        textBuffer += '\n'
        return
      }

      const isBlock = BLOCK_TAGS.has(node.tagName)
      if (isBlock && textBuffer.trim().length > 0) {
        textBuffer += '\n\n'
      }

      for (const child of [...node.childNodes]) {
        walkNode(child)
      }

      if (isBlock) {
        textBuffer += '\n\n'
        flushTextBuffer()
      }
    }

    for (const child of [...document.body.childNodes]) {
      walkNode(child)
    }

    flushTextBuffer()
    return segments
  } catch {
    return []
  }
}

const extractSegmentsFromRichContent = (
  richContent: string,
): ReaderHighlightContentSegment[] => {
  const segments: ReaderHighlightContentSegment[] = []
  let lastIndex = 0

  for (const match of richContent.matchAll(MARKDOWN_IMAGE_PATTERN)) {
    const matchIndex = match.index ?? 0
    pushTextSegment(segments, richContent.slice(lastIndex, matchIndex))
    pushImageSegment(segments, match[1])
    lastIndex = matchIndex + match[0].length
  }

  pushTextSegment(segments, richContent.slice(lastIndex))
  return segments
}

const focusSegmentsFromPrimaryText = (
  segments: ReaderHighlightContentSegment[],
  primaryText: string | null,
  requireMatch = false,
) => {
  const normalizedPrimaryText = normalizeOptionalText(primaryText)
  if (!normalizedPrimaryText) return segments

  const matchIndex = segments.findIndex(
    (segment) =>
      segment.kind === 'text' && segment.value.includes(normalizedPrimaryText),
  )
  if (matchIndex < 0) return requireMatch ? [] : segments

  const focusedSegments = segments.slice(matchIndex).map((segment) => ({ ...segment }))
  const firstSegment = focusedSegments[0]

  if (firstSegment?.kind === 'text') {
    const primaryTextIndex = firstSegment.value.indexOf(normalizedPrimaryText)
    if (primaryTextIndex >= 0) {
      const trailingText = normalizeOptionalText(
        firstSegment.value.slice(primaryTextIndex + normalizedPrimaryText.length),
      )

      if (trailingText) {
        firstSegment.value = trailingText
      } else {
        focusedSegments.shift()
      }
    }
  }

  return focusedSegments
}

export const extractReaderHighlightContentSegments = (options: {
  richContent?: string | null
  imageUrl?: string | null
  htmlContent?: string | null
  primaryText?: string | null
  requirePrimaryTextMatch?: boolean
}): ReaderHighlightContentSegment[] => {
  const normalizedRichContent = normalizeOptionalText(options.richContent)
  const richContentSegments = normalizedRichContent
    ? extractSegmentsFromRichContent(normalizedRichContent)
    : []
  const cleanedRichContentSegments = focusSegmentsFromPrimaryText(
    richContentSegments,
    options.primaryText ?? null,
    options.requirePrimaryTextMatch ?? false,
  )

  if (cleanedRichContentSegments.length > 0) {
    return cleanedRichContentSegments
  }

  const normalizedHtmlContent = normalizeOptionalText(options.htmlContent)
  const htmlSegments = normalizedHtmlContent
    ? extractSegmentsFromHtml(normalizedHtmlContent)
    : []
  const cleanedHtmlSegments = focusSegmentsFromPrimaryText(
    htmlSegments,
    options.primaryText ?? null,
    options.requirePrimaryTextMatch ?? false,
  )

  if (cleanedHtmlSegments.length > 0) {
    return cleanedHtmlSegments
  }

  const fallbackImageUrl = normalizeReaderImageUrl(options.imageUrl)
  return fallbackImageUrl
    ? [{ kind: 'image', value: fallbackImageUrl }]
    : []
}
