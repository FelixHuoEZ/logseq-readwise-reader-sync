import {
  describeUnknownError,
  logReadwiseDebug,
  logReadwiseWarn,
} from '../logging'
import {
  extractReaderHighlightContentSegments,
  normalizeReaderImageUrl,
} from '../reader/extract-reader-highlight-content-segments'
import type { ReaderDocument } from '../types'

const READWISE_MCP_URL = 'https://mcp2.readwise.io/mcp'
const READWISE_MCP_PROTOCOL_VERSION = '2025-11-25'
const READWISE_MCP_CLIENT_INFO = {
  name: 'logseq-readwise-reader-sync',
  version: '0.1.6',
}
const READWISE_MCP_AUTH_SCHEMES = ['Token', 'Bearer'] as const
const RICH_MEDIA_HTML_PATTERN =
  /<(img|video|audio|picture|iframe|embed|object|svg|canvas)\b/i

export interface ReaderDocumentHighlightDetail {
  id: string
  content: string | null
  tags: string[]
  notes: string | null
}

export type ReaderDocumentHighlightDetailDecisionReason =
  | 'missing_parent_metadata'
  | 'video'
  | 'no_rich_media'
  | 'already_resolved'
  | 'enrich'

export interface ReaderDocumentHighlightEnrichmentResult {
  highlights: ReaderDocument[]
  changedCount: number
  fetchedCount: number
  attempted: boolean
  skippedReason: Exclude<
    ReaderDocumentHighlightDetailDecisionReason,
    'missing_parent_metadata' | 'enrich'
  > | null
  missingInReader: boolean
}

class ReadwiseMcpAuthError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ReadwiseMcpAuthError'
    this.status = status
  }
}

const normalizeOptionalText = (value: string | null | undefined) => {
  if (typeof value !== 'string') return null

  const normalized = value.trim()
  return normalized.length > 0 ? normalized : null
}

const isMissingReaderDocumentDetailsError = (error: unknown) => {
  const message =
    error instanceof Error ? error.message : String(error ?? '')

  return /error getting document from reader/i.test(message)
}

const normalizeDocumentCategory = (value: string | null | undefined) =>
  normalizeOptionalText(value)?.toLowerCase() ?? null

const normalizeComparableText = (value: string | null | undefined) =>
  normalizeOptionalText(value)?.toLowerCase() ?? null

const isYoutubeUrl = (value: string | null | undefined) => {
  const normalized = normalizeOptionalText(value)
  if (!normalized) return false

  try {
    const url = new URL(normalized)
    const hostname = url.hostname.toLowerCase()

    return (
      hostname === 'youtu.be' ||
      hostname === 'www.youtu.be' ||
      hostname === 'youtube.com' ||
      hostname.endsWith('.youtube.com') ||
      hostname === 'youtube-nocookie.com' ||
      hostname.endsWith('.youtube-nocookie.com')
    )
  } catch {
    return /(?:^|\/\/)(?:www\.)?(?:youtube\.com|youtu\.be|youtube-nocookie\.com)(?:\/|$)/i.test(
      normalized,
    )
  }
}

const isYoutubeLinkedReaderDocument = (document: ReaderDocument) => {
  if (isYoutubeUrl(document.source_url) || isYoutubeUrl(document.url)) {
    return true
  }

  const siteName = normalizeComparableText(document.site_name)
  if (siteName === 'youtube') {
    return true
  }

  const source = normalizeComparableText(document.source)
  return source === 'youtube'
}

const parseEventStreamPayload = (bodyText: string) => {
  const blocks = bodyText
    .split(/\r?\n\r?\n/g)
    .map((block) => block.trim())
    .filter((block) => block.length > 0)

  for (const block of blocks) {
    const dataLines = block
      .split(/\r?\n/g)
      .filter((line) => line.startsWith('data:'))
      .map((line) => line.slice('data:'.length).trimStart())

    if (dataLines.length === 0) continue

    return JSON.parse(dataLines.join('\n')) as Record<string, unknown>
  }

  throw new Error('Readwise MCP returned an empty event stream payload.')
}

const parseMcpResponse = async (response: Response) => {
  if (response.status === 202) {
    await response.text().catch(() => '')
    return null
  }

  const contentType = response.headers.get('content-type') ?? ''
  const bodyText = await response.text()

  if (contentType.includes('application/json')) {
    return bodyText.length > 0
      ? (JSON.parse(bodyText) as Record<string, unknown>)
      : null
  }

  if (contentType.includes('text/event-stream')) {
    return parseEventStreamPayload(bodyText)
  }

  throw new Error(
    `Unsupported Readwise MCP response content type: ${contentType || 'unknown'}.`,
  )
}

const postMcpMessage = async ({
  token,
  authScheme,
  sessionId,
  protocolVersion,
  message,
}: {
  token: string
  authScheme: (typeof READWISE_MCP_AUTH_SCHEMES)[number]
  sessionId: string | null
  protocolVersion: string | null
  message: Record<string, unknown>
}) => {
  const headers: Record<string, string> = {
    Authorization: `${authScheme} ${token}`,
    'content-type': 'application/json',
    accept: 'application/json, text/event-stream',
  }

  if (sessionId) {
    headers['mcp-session-id'] = sessionId
  }

  if (protocolVersion) {
    headers['mcp-protocol-version'] = protocolVersion
  }

  const response = await fetch(READWISE_MCP_URL, {
    method: 'POST',
    headers,
    body: JSON.stringify(message),
  })

  if (!response.ok) {
    const bodyText = await response.text().catch(() => '')
    const detail = bodyText.trim() || response.statusText

    if (response.status === 401 || response.status === 403) {
      throw new ReadwiseMcpAuthError(
        response.status,
        `Readwise MCP authentication failed: ${detail}`,
      )
    }

    throw new Error(
      `Readwise MCP request failed with ${response.status}: ${detail}`,
    )
  }

  return {
    sessionId: response.headers.get('mcp-session-id'),
    payload: await parseMcpResponse(response),
  }
}

const extractToolResult = <T>(payload: Record<string, unknown> | null): T => {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Readwise MCP tool call returned an empty payload.')
  }

  const result =
    'result' in payload && payload.result && typeof payload.result === 'object'
      ? (payload.result as Record<string, unknown>)
      : null

  if (!result) {
    throw new Error('Readwise MCP tool call did not include a result object.')
  }

  if (result.isError === true) {
    const content = Array.isArray(result.content)
      ? result.content
          .map((entry) =>
            entry && typeof entry === 'object' && 'text' in entry
              ? String(entry.text ?? '')
              : '',
          )
          .filter((value) => value.length > 0)
          .join('\n')
      : ''

    throw new Error(content || 'Readwise MCP tool call returned isError=true.')
  }

  const structuredContent =
    'structuredContent' in result &&
    result.structuredContent &&
    typeof result.structuredContent === 'object'
      ? (result.structuredContent as Record<string, unknown>)
      : null

  if (structuredContent && 'result' in structuredContent) {
    return structuredContent.result as T
  }

  const firstTextBlock = Array.isArray(result.content)
    ? result.content.find(
        (entry) =>
          entry &&
          typeof entry === 'object' &&
          entry.type === 'text' &&
          typeof entry.text === 'string',
      )
    : null

  if (firstTextBlock && typeof firstTextBlock === 'object') {
    return JSON.parse(String(firstTextBlock.text)) as T
  }

  throw new Error('Readwise MCP tool call returned no structured content.')
}

const callReadwiseMcpTool = async <T>(
  token: string,
  toolName: string,
  toolArguments: Record<string, unknown>,
): Promise<T> => {
  const normalizedToken = token.trim()
  if (normalizedToken.length === 0) {
    throw new Error('Readwise MCP requires a non-empty token.')
  }

  let lastAuthError: ReadwiseMcpAuthError | null = null

  for (const authScheme of READWISE_MCP_AUTH_SCHEMES) {
    try {
      const initializeResponse = await postMcpMessage({
        token: normalizedToken,
        authScheme,
        sessionId: null,
        protocolVersion: null,
        message: {
          jsonrpc: '2.0',
          id: 1,
          method: 'initialize',
          params: {
            protocolVersion: READWISE_MCP_PROTOCOL_VERSION,
            capabilities: {},
            clientInfo: READWISE_MCP_CLIENT_INFO,
          },
        },
      })

      const initializePayload = initializeResponse.payload
      const initializeResult =
        initializePayload &&
        typeof initializePayload === 'object' &&
        'result' in initializePayload &&
        initializePayload.result &&
        typeof initializePayload.result === 'object'
          ? (initializePayload.result as Record<string, unknown>)
          : null
      const negotiatedProtocolVersion =
        typeof initializeResult?.protocolVersion === 'string'
          ? initializeResult.protocolVersion
          : READWISE_MCP_PROTOCOL_VERSION
      const sessionId = initializeResponse.sessionId

      await postMcpMessage({
        token: normalizedToken,
        authScheme,
        sessionId,
        protocolVersion: negotiatedProtocolVersion,
        message: {
          jsonrpc: '2.0',
          method: 'notifications/initialized',
          params: {},
        },
      })

      const toolResponse = await postMcpMessage({
        token: normalizedToken,
        authScheme,
        sessionId,
        protocolVersion: negotiatedProtocolVersion,
        message: {
          jsonrpc: '2.0',
          id: 2,
          method: 'tools/call',
          params: {
            name: toolName,
            arguments: toolArguments,
          },
        },
      })

      return extractToolResult<T>(toolResponse.payload)
    } catch (error) {
      if (error instanceof ReadwiseMcpAuthError) {
        lastAuthError = error
        continue
      }

      throw error
    }
  }

  throw lastAuthError ?? new Error('Readwise MCP authentication failed.')
}

export const getReaderDocumentHighlightsViaMcp = async (
  token: string,
  documentId: string,
) => {
  const highlights = await callReadwiseMcpTool<ReaderDocumentHighlightDetail[]>(
    token,
    'reader_get_document_highlights',
    {
      document_id: documentId,
    },
  )

  return Array.isArray(highlights) ? highlights : []
}

const buildTagRecord = (tagNames: readonly string[]) => {
  const names = tagNames
    .map((tagName) => tagName.trim())
    .filter((tagName) => tagName.length > 0)

  return names.length > 0
    ? Object.fromEntries(names.map((tagName) => [tagName, true]))
    : null
}

const highlightNeedsReaderDocumentDetails = (highlight: ReaderDocument) => {
  const primaryText = highlight.content?.trim() ?? ''
  const segments = extractReaderHighlightContentSegments({
    richContent: highlight.render_content ?? highlight.content,
    imageUrl: highlight.image_url,
    htmlContent: highlight.html_content,
    primaryText,
  })

  return segments.length === 0
}

export const documentHasReaderRichMediaHints = (document: ReaderDocument) =>
  normalizeReaderImageUrl(document.image_url) != null ||
  RICH_MEDIA_HTML_PATTERN.test(document.html_content ?? '')

export const decideReaderDocumentHighlightDetailsStrategy = ({
  document,
  highlights,
}: {
  document: ReaderDocument | null | undefined
  highlights: readonly ReaderDocument[]
}): {
  shouldEnrich: boolean
  reason: ReaderDocumentHighlightDetailDecisionReason
} => {
  if (!highlights.some(highlightNeedsReaderDocumentDetails)) {
    return {
      shouldEnrich: false,
      reason: 'already_resolved',
    }
  }

  if (!document) {
    return {
      shouldEnrich: false,
      reason: 'missing_parent_metadata',
    }
  }

  const normalizedCategory = normalizeDocumentCategory(document.category)
  if (
    normalizedCategory === 'video' ||
    normalizedCategory === 'videos' ||
    isYoutubeLinkedReaderDocument(document)
  ) {
    return {
      shouldEnrich: false,
      reason: 'video',
    }
  }

  if (!documentHasReaderRichMediaHints(document)) {
    return {
      shouldEnrich: false,
      reason: 'no_rich_media',
    }
  }

  return {
    shouldEnrich: true,
    reason: 'enrich',
  }
}

export const reuseCachedReaderDocumentHighlightDetails = (
  highlights: readonly ReaderDocument[],
  cachedHighlightsById: ReadonlyMap<string, ReaderDocument> | null | undefined,
) => {
  if (!cachedHighlightsById || cachedHighlightsById.size === 0) {
    return {
      highlights: [...highlights],
      changedCount: 0,
      reusedCount: 0,
    }
  }

  let changedCount = 0
  let reusedCount = 0

  const mergedHighlights = highlights.map((highlight) => {
    const cachedHighlight = cachedHighlightsById.get(highlight.id)
    if (!cachedHighlight) return highlight
    if (cachedHighlight.updated_at !== highlight.updated_at) return highlight

    const nextRenderContent =
      highlight.render_content ?? cachedHighlight.render_content ?? null
    const nextNotes = highlight.notes ?? cachedHighlight.notes ?? null
    const nextTags = highlight.tags ?? cachedHighlight.tags

    const hasChanged =
      nextRenderContent !== (highlight.render_content ?? null) ||
      nextNotes !== (highlight.notes ?? null) ||
      nextTags !== highlight.tags

    if (!hasChanged) return highlight

    changedCount += 1
    if (cachedHighlight.render_content != null) {
      reusedCount += 1
    }

    return {
      ...highlight,
      render_content: nextRenderContent,
      notes: nextNotes,
      tags: nextTags,
    }
  })

  return {
    highlights: mergedHighlights,
    changedCount,
    reusedCount,
  }
}

export const previewBookMayNeedReaderDocumentHighlightDetails = (
  document: ReaderDocument,
  highlights: readonly ReaderDocument[],
) => {
  return decideReaderDocumentHighlightDetailsStrategy({
    document,
    highlights,
  }).shouldEnrich
}

export const mergeReaderDocumentHighlightsWithDetails = (
  highlights: readonly ReaderDocument[],
  detailList: readonly ReaderDocumentHighlightDetail[],
) => {
  const detailsById = new Map(detailList.map((detail) => [detail.id, detail]))
  let changedCount = 0

  const mergedHighlights = highlights.map((highlight) => {
    const detail = detailsById.get(highlight.id)
    if (!detail) return highlight

    const nextRenderContent = normalizeOptionalText(detail.content)
    const nextNotes = normalizeOptionalText(detail.notes) ?? highlight.notes ?? null
    const nextTags = buildTagRecord(detail.tags) ?? highlight.tags

    const hasChanged =
      nextRenderContent !== (highlight.render_content ?? null) ||
      nextNotes !== (highlight.notes ?? null) ||
      nextTags !== highlight.tags

    if (!hasChanged) return highlight

    changedCount += 1

    return {
      ...highlight,
      render_content: nextRenderContent,
      notes: nextNotes,
      tags: nextTags,
    }
  })

  return {
    highlights: mergedHighlights,
    changedCount,
  }
}

export const enrichReaderDocumentHighlightsViaMcp = async ({
  token,
  document,
  highlights,
  logPrefix,
}: {
  token: string
  document: ReaderDocument
  highlights: readonly ReaderDocument[]
  logPrefix?: string
}): Promise<ReaderDocumentHighlightEnrichmentResult> => {
  const strategy = decideReaderDocumentHighlightDetailsStrategy({
    document,
    highlights,
  })

  if (!strategy.shouldEnrich) {
    return {
      highlights: [...highlights],
      changedCount: 0,
      fetchedCount: 0,
      attempted: false,
      skippedReason:
        strategy.reason === 'video' ||
        strategy.reason === 'no_rich_media' ||
        strategy.reason === 'already_resolved'
          ? strategy.reason
          : null,
      missingInReader: false,
    }
  }

  const detailList = await getReaderDocumentHighlightsViaMcp(token, document.id)
  const mergeResult = mergeReaderDocumentHighlightsWithDetails(
    highlights,
    detailList,
  )

  if (logPrefix) {
    logReadwiseDebug(logPrefix, 'resolved Reader document highlights via MCP', {
      readerDocumentId: document.id,
      category: document.category,
      requestedHighlights: highlights.length,
      fetchedHighlights: detailList.length,
      changedHighlights: mergeResult.changedCount,
    })
  }

  return {
    highlights: mergeResult.highlights,
    changedCount: mergeResult.changedCount,
    fetchedCount: detailList.length,
    attempted: true,
    skippedReason: null,
    missingInReader: false,
  }
}

export const tryEnrichReaderDocumentHighlightsViaMcp = async ({
  token,
  document,
  highlights,
  logPrefix,
}: {
  token: string | null | undefined
  document: ReaderDocument
  highlights: readonly ReaderDocument[]
  logPrefix?: string
}): Promise<ReaderDocumentHighlightEnrichmentResult> => {
  const normalizedToken = normalizeOptionalText(token)
  if (!normalizedToken) {
    return {
      highlights: [...highlights],
      changedCount: 0,
      fetchedCount: 0,
      attempted: false,
      skippedReason: null,
      missingInReader: false,
    }
  }

  try {
    return await enrichReaderDocumentHighlightsViaMcp({
      token: normalizedToken,
      document,
      highlights,
      logPrefix,
    })
  } catch (error) {
    if (logPrefix) {
      const logFn = isMissingReaderDocumentDetailsError(error)
        ? logReadwiseDebug
        : logReadwiseWarn

      logFn(
        logPrefix,
        'failed to enrich Reader document highlights via MCP; falling back to list response',
        {
          readerDocumentId: document.id,
          formattedError: describeUnknownError(error),
        },
      )
    }

    return {
      highlights: [...highlights],
      changedCount: 0,
      fetchedCount: 0,
      attempted: true,
      skippedReason: null,
      missingInReader: isMissingReaderDocumentDetailsError(error),
    }
  }
}
