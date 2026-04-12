import type { PageEntity } from '@logseq/libs/dist/LSPlugin'
import type { ReaderSyncRetryPageEntryV1 } from '../cache'
import { logReadwiseWarn } from '../logging'

export const GRAPH_READER_SYNC_RETRY_FALLBACK_PAGE_NAME_V1 =
  'Readwise Sync Retry Fallback'

const RETRY_FALLBACK_BLOCK_UUID =
  '9b29e9f2-5745-4ee4-8a50-0cb3316d2128'

const RETRY_FALLBACK_PROPERTY_KEYS = {
  schemaVersion: 'rw-reader-retry-fallback-schema',
  savedAt: 'rw-reader-retry-fallback-saved-at',
  entryCount: 'rw-reader-retry-fallback-entry-count',
} as const

interface GraphReaderRetryFallbackPayloadV1 {
  schemaVersion: 1
  savedAt: string
  entries: ReaderSyncRetryPageEntryV1[]
}

const ensureManagedStatePage = async (pageName: string): Promise<PageEntity> => {
  const existing = await logseq.Editor.getPage(pageName)
  if (existing) {
    return existing
  }

  const created = await logseq.Editor.createPage(
    pageName,
    {},
    {
      createFirstBlock: false,
      redirect: false,
    },
  )

  if (!created) {
    throw new Error(`Failed to create managed graph state page "${pageName}".`)
  }

  return created
}

const ensureFallbackPage = async () =>
  ensureManagedStatePage(GRAPH_READER_SYNC_RETRY_FALLBACK_PAGE_NAME_V1)

const parseRetryFallbackEntry = (
  value: unknown,
): ReaderSyncRetryPageEntryV1 | null => {
  if (!value || typeof value !== 'object') return null

  const record = value as Record<string, unknown>
  const readerDocumentId =
    typeof record.readerDocumentId === 'string'
      ? record.readerDocumentId.trim()
      : ''

  if (readerDocumentId.length === 0) return null

  return {
    readerDocumentId,
    pageName:
      typeof record.pageName === 'string' && record.pageName.trim().length > 0
        ? record.pageName
        : null,
    category:
      typeof record.category === 'string' && record.category.trim().length > 0
        ? record.category
        : 'generic',
    message:
      typeof record.message === 'string' && record.message.trim().length > 0
        ? record.message
        : '',
    queuedAt:
      typeof record.queuedAt === 'string' && record.queuedAt.trim().length > 0
        ? record.queuedAt
        : new Date(0).toISOString(),
    lastSeenAt:
      typeof record.lastSeenAt === 'string' && record.lastSeenAt.trim().length > 0
        ? record.lastSeenAt
        : new Date(0).toISOString(),
  }
}

const dedupeRetryFallbackEntries = (
  entries: readonly ReaderSyncRetryPageEntryV1[],
): ReaderSyncRetryPageEntryV1[] => {
  const byDocumentId = new Map<string, ReaderSyncRetryPageEntryV1>()

  for (const entry of entries) {
    const existing = byDocumentId.get(entry.readerDocumentId)
    if (!existing || entry.lastSeenAt >= existing.lastSeenAt) {
      byDocumentId.set(entry.readerDocumentId, entry)
    }
  }

  return [...byDocumentId.values()].sort((left, right) =>
    left.readerDocumentId.localeCompare(right.readerDocumentId),
  )
}

const buildRetryFallbackPayloadV1 = (
  entries: readonly ReaderSyncRetryPageEntryV1[],
): GraphReaderRetryFallbackPayloadV1 => ({
  schemaVersion: 1,
  savedAt: new Date().toISOString(),
  entries: dedupeRetryFallbackEntries(entries),
})

const parseRetryFallbackPayload = (
  content: string | null | undefined,
): GraphReaderRetryFallbackPayloadV1 | null => {
  if (typeof content !== 'string' || content.trim().length === 0) {
    return null
  }

  try {
    const parsed = JSON.parse(content) as {
      schemaVersion?: unknown
      savedAt?: unknown
      entries?: unknown
    }

    if (parsed.schemaVersion !== 1 || !Array.isArray(parsed.entries)) {
      return null
    }

    return {
      schemaVersion: 1,
      savedAt:
        typeof parsed.savedAt === 'string' && parsed.savedAt.trim().length > 0
          ? parsed.savedAt
          : new Date(0).toISOString(),
      entries: parsed.entries
        .map((entry) => parseRetryFallbackEntry(entry))
        .filter((entry): entry is ReaderSyncRetryPageEntryV1 => entry != null),
    }
  } catch (error) {
    logReadwiseWarn(
      '[Readwise Sync]',
      'failed to parse graph retry fallback payload; ignoring saved fallback entries',
      {
        pageName: GRAPH_READER_SYNC_RETRY_FALLBACK_PAGE_NAME_V1,
        blockUuid: RETRY_FALLBACK_BLOCK_UUID,
        error,
      },
    )
    return null
  }
}

const upsertRetryFallbackBlock = async (
  page: PageEntity,
  payload: GraphReaderRetryFallbackPayloadV1,
) => {
  const content = JSON.stringify(payload, null, 2)
  const existing = await logseq.Editor.getBlock(RETRY_FALLBACK_BLOCK_UUID)

  if (existing) {
    await logseq.Editor.updateBlock(RETRY_FALLBACK_BLOCK_UUID, content)
    return
  }

  const created = await logseq.Editor.insertBlock(page.uuid, content, {
    customUUID: RETRY_FALLBACK_BLOCK_UUID,
  })

  if (!created) {
    throw new Error('Failed to create the Reader retry fallback block.')
  }
}

const removeRetryFallbackBlock = async () => {
  const existing = await logseq.Editor.getBlock(RETRY_FALLBACK_BLOCK_UUID)
  if (existing) {
    await logseq.Editor.removeBlock(RETRY_FALLBACK_BLOCK_UUID)
  }
}

export const loadGraphReaderRetryFallbackEntriesV1 = async (): Promise<
  ReaderSyncRetryPageEntryV1[]
> => {
  const block = await logseq.Editor.getBlock(RETRY_FALLBACK_BLOCK_UUID)
  const payload = parseRetryFallbackPayload(block?.content)
  return payload?.entries ?? []
}

export const saveGraphReaderRetryFallbackEntriesV1 = async (
  entries: readonly ReaderSyncRetryPageEntryV1[],
): Promise<ReaderSyncRetryPageEntryV1[]> => {
  const payload = buildRetryFallbackPayloadV1(entries)
  const page = await ensureFallbackPage()

  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    RETRY_FALLBACK_PROPERTY_KEYS.schemaVersion,
    payload.schemaVersion,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    RETRY_FALLBACK_PROPERTY_KEYS.savedAt,
    payload.savedAt,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    RETRY_FALLBACK_PROPERTY_KEYS.entryCount,
    payload.entries.length,
  )

  if (payload.entries.length === 0) {
    await removeRetryFallbackBlock()
    return []
  }

  await upsertRetryFallbackBlock(page, payload)
  return payload.entries
}

export const removeGraphReaderRetryFallbackEntriesV1 = async (
  readerDocumentIds: readonly string[],
): Promise<ReaderSyncRetryPageEntryV1[]> => {
  const ids = [...new Set(readerDocumentIds.filter((id) => id.trim().length > 0))]
  if (ids.length === 0) {
    return await loadGraphReaderRetryFallbackEntriesV1()
  }

  const existingEntries = await loadGraphReaderRetryFallbackEntriesV1()
  const nextEntries = existingEntries.filter(
    (entry) => !ids.includes(entry.readerDocumentId),
  )

  await saveGraphReaderRetryFallbackEntriesV1(nextEntries)
  return nextEntries
}
