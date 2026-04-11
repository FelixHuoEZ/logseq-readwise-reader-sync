import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

export type GraphCheckpointSourceV1 = 'full_sync' | 'incremental_sync'

export interface GraphCheckpointStateV1 {
  schemaVersion: 1
  updatedAfter: string | null
  committedAt: string
  source: GraphCheckpointSourceV1
}

export interface GraphLastFormalSyncSummaryV1 {
  schemaVersion: 1
  runKind: 'reader_full_scan'
  status: 'success' | 'partial_error' | 'failed'
  completedAt: string
  highlightPagesScanned: number
  highlightsScanned: number
  parentDocumentsIdentified: number
  pagesTargeted: number
  pagesProcessed: number
  createdCount: number
  updatedCount: number
  unchangedCount: number
  renamedCount: number
  errorCount: number
  totalDurationMs: number
  fetchHighlightsDurationMs: number
  fetchDocumentsDurationMs: number
  writePagesDurationMs: number
  failureSummary: string | null
}

export const GRAPH_CHECKPOINT_PAGE_NAME_V1 = 'Readwise Sync State'

const CHECKPOINT_PROPERTY_KEYS = {
  schemaVersion: 'rw-sync-schema',
  updatedAfter: 'rw-checkpoint-updated-after',
  committedAt: 'rw-checkpoint-committed-at',
  source: 'rw-checkpoint-source',
} as const

const LAST_FORMAL_SYNC_PROPERTY_KEYS = {
  schemaVersion: 'rw-last-formal-sync-schema',
  runKind: 'rw-last-formal-sync-kind',
  status: 'rw-last-formal-sync-status',
  completedAt: 'rw-last-formal-sync-completed-at',
  highlightPagesScanned: 'rw-last-formal-sync-highlight-pages-scanned',
  highlightsScanned: 'rw-last-formal-sync-highlights-scanned',
  parentDocumentsIdentified: 'rw-last-formal-sync-parent-documents-identified',
  pagesTargeted: 'rw-last-formal-sync-pages-targeted',
  pagesProcessed: 'rw-last-formal-sync-pages-processed',
  createdCount: 'rw-last-formal-sync-created',
  updatedCount: 'rw-last-formal-sync-updated',
  unchangedCount: 'rw-last-formal-sync-unchanged',
  renamedCount: 'rw-last-formal-sync-renamed',
  errorCount: 'rw-last-formal-sync-errors',
  totalDurationMs: 'rw-last-formal-sync-total-duration-ms',
  fetchHighlightsDurationMs: 'rw-last-formal-sync-fetch-highlights-duration-ms',
  fetchDocumentsDurationMs: 'rw-last-formal-sync-fetch-documents-duration-ms',
  writePagesDurationMs: 'rw-last-formal-sync-write-pages-duration-ms',
  failureSummary: 'rw-last-formal-sync-failure-summary',
} as const

const normalizePropertyKey = (value: string): string =>
  value.toLowerCase().replace(/[^a-z0-9]/g, '')

const readPropertyValue = (
  properties: Record<string, unknown> | undefined,
  expectedKey: string,
): unknown => {
  if (!properties) return null

  const normalizedExpected = normalizePropertyKey(expectedKey)

  for (const [key, value] of Object.entries(properties)) {
    if (normalizePropertyKey(key) === normalizedExpected) {
      return value
    }
  }

  return null
}

const extractStringValue = (value: unknown): string | null => {
  if (typeof value === 'string') {
    const trimmed = value.trim()
    return trimmed.length > 0 ? trimmed : null
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value)
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const extracted = extractStringValue(item)
      if (extracted) return extracted
    }
  }

  return null
}

const extractNumberValue = (value: unknown): number | null => {
  const stringValue = extractStringValue(value)
  if (!stringValue) return null

  const parsed = Number(stringValue)
  return Number.isFinite(parsed) ? parsed : null
}

const parseSchemaVersion = (page: PageEntity): 1 | null => {
  const rawValue = readPropertyValue(
    page.properties,
    CHECKPOINT_PROPERTY_KEYS.schemaVersion,
  )
  const parsed = Number.parseInt(extractStringValue(rawValue) ?? '', 10)

  return parsed === 1 ? 1 : null
}

const parseCheckpointSource = (
  page: PageEntity,
): GraphCheckpointSourceV1 | null => {
  const rawValue = extractStringValue(
    readPropertyValue(page.properties, CHECKPOINT_PROPERTY_KEYS.source),
  )

  if (rawValue === 'full_sync' || rawValue === 'incremental_sync') {
    return rawValue
  }

  return null
}

const parseCheckpointPage = (
  page: PageEntity | null,
): GraphCheckpointStateV1 | null => {
  if (!page) return null

  const schemaVersion = parseSchemaVersion(page)
  const committedAt = extractStringValue(
    readPropertyValue(page.properties, CHECKPOINT_PROPERTY_KEYS.committedAt),
  )
  const source = parseCheckpointSource(page)

  if (schemaVersion !== 1 || committedAt == null || source == null) {
    return null
  }

  return {
    schemaVersion,
    updatedAfter:
      extractStringValue(
        readPropertyValue(page.properties, CHECKPOINT_PROPERTY_KEYS.updatedAfter),
      ) ?? null,
    committedAt,
    source,
  }
}

const compareUpdatedAfter = (
  left: string | null,
  right: string | null,
): number => {
  if (left === right) return 0
  if (left == null) return -1
  if (right == null) return 1

  return left < right ? -1 : 1
}

const ensureCheckpointPage = async (): Promise<PageEntity> => {
  const existing = await logseq.Editor.getPage(GRAPH_CHECKPOINT_PAGE_NAME_V1)
  if (existing) {
    return existing
  }

  const created = await logseq.Editor.createPage(
    GRAPH_CHECKPOINT_PAGE_NAME_V1,
    {},
    {
      createFirstBlock: false,
      redirect: false,
    },
  )

  if (!created) {
    throw new Error('Failed to create graph checkpoint state page.')
  }

  return created
}

export const loadGraphCheckpointStateV1 =
  async (): Promise<GraphCheckpointStateV1 | null> =>
    parseCheckpointPage(
      await logseq.Editor.getPage(GRAPH_CHECKPOINT_PAGE_NAME_V1),
    )

export const pickPreferredCheckpointStateV1 = (
  currentState: GraphCheckpointStateV1 | null,
  nextState: GraphCheckpointStateV1,
): GraphCheckpointStateV1 =>
  currentState == null ||
  compareUpdatedAfter(nextState.updatedAfter, currentState.updatedAfter) >= 0
    ? nextState
    : currentState

export const saveGraphCheckpointStateV1 = async (
  nextState: GraphCheckpointStateV1,
): Promise<GraphCheckpointStateV1> => {
  const currentState = await loadGraphCheckpointStateV1()
  const preferredState = pickPreferredCheckpointStateV1(currentState, nextState)
  const page = await ensureCheckpointPage()

  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    CHECKPOINT_PROPERTY_KEYS.schemaVersion,
    preferredState.schemaVersion,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    CHECKPOINT_PROPERTY_KEYS.updatedAfter,
    preferredState.updatedAfter ?? '',
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    CHECKPOINT_PROPERTY_KEYS.committedAt,
    preferredState.committedAt,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    CHECKPOINT_PROPERTY_KEYS.source,
    preferredState.source,
  )

  return preferredState
}

export const openGraphCheckpointStatePageV1 = async (): Promise<PageEntity> => {
  const currentState = await loadGraphCheckpointStateV1()

  if (currentState == null) {
    await saveGraphCheckpointStateV1({
      schemaVersion: 1,
      updatedAfter: null,
      committedAt: new Date().toISOString(),
      source: 'full_sync',
    })
  }

  const page = await ensureCheckpointPage()
  logseq.App.pushState('page', { name: GRAPH_CHECKPOINT_PAGE_NAME_V1 })
  return page
}

export const loadGraphLastFormalSyncSummaryV1 =
  async (): Promise<GraphLastFormalSyncSummaryV1 | null> => {
    const page = await logseq.Editor.getPage(GRAPH_CHECKPOINT_PAGE_NAME_V1)
    if (!page) return null

    const schemaVersion = extractNumberValue(
      readPropertyValue(page.properties, LAST_FORMAL_SYNC_PROPERTY_KEYS.schemaVersion),
    )
    const runKind = extractStringValue(
      readPropertyValue(page.properties, LAST_FORMAL_SYNC_PROPERTY_KEYS.runKind),
    )
    const status = extractStringValue(
      readPropertyValue(page.properties, LAST_FORMAL_SYNC_PROPERTY_KEYS.status),
    )
    const completedAt = extractStringValue(
      readPropertyValue(page.properties, LAST_FORMAL_SYNC_PROPERTY_KEYS.completedAt),
    )

    if (
      schemaVersion !== 1 ||
      runKind !== 'reader_full_scan' ||
      (status !== 'success' && status !== 'partial_error' && status !== 'failed') ||
      completedAt == null
    ) {
      return null
    }

    const numberOrZero = (key: (typeof LAST_FORMAL_SYNC_PROPERTY_KEYS)[keyof typeof LAST_FORMAL_SYNC_PROPERTY_KEYS]) =>
      extractNumberValue(readPropertyValue(page.properties, key)) ?? 0

    return {
      schemaVersion: 1,
      runKind: 'reader_full_scan',
      status,
      completedAt,
      highlightPagesScanned: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.highlightPagesScanned),
      highlightsScanned: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.highlightsScanned),
      parentDocumentsIdentified: numberOrZero(
        LAST_FORMAL_SYNC_PROPERTY_KEYS.parentDocumentsIdentified,
      ),
      pagesTargeted: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.pagesTargeted),
      pagesProcessed: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.pagesProcessed),
      createdCount: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.createdCount),
      updatedCount: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.updatedCount),
      unchangedCount: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.unchangedCount),
      renamedCount: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.renamedCount),
      errorCount: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.errorCount),
      totalDurationMs: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.totalDurationMs),
      fetchHighlightsDurationMs: numberOrZero(
        LAST_FORMAL_SYNC_PROPERTY_KEYS.fetchHighlightsDurationMs,
      ),
      fetchDocumentsDurationMs: numberOrZero(
        LAST_FORMAL_SYNC_PROPERTY_KEYS.fetchDocumentsDurationMs,
      ),
      writePagesDurationMs: numberOrZero(LAST_FORMAL_SYNC_PROPERTY_KEYS.writePagesDurationMs),
      failureSummary:
        extractStringValue(
          readPropertyValue(page.properties, LAST_FORMAL_SYNC_PROPERTY_KEYS.failureSummary),
        ) ?? null,
    }
  }

export const saveGraphLastFormalSyncSummaryV1 = async (
  summary: GraphLastFormalSyncSummaryV1,
): Promise<GraphLastFormalSyncSummaryV1> => {
  const page = await ensureCheckpointPage()

  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.schemaVersion,
    summary.schemaVersion,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.runKind,
    summary.runKind,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.status,
    summary.status,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.completedAt,
    summary.completedAt,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.highlightPagesScanned,
    summary.highlightPagesScanned,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.highlightsScanned,
    summary.highlightsScanned,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.parentDocumentsIdentified,
    summary.parentDocumentsIdentified,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.pagesTargeted,
    summary.pagesTargeted,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.pagesProcessed,
    summary.pagesProcessed,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.createdCount,
    summary.createdCount,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.updatedCount,
    summary.updatedCount,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.unchangedCount,
    summary.unchangedCount,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.renamedCount,
    summary.renamedCount,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.errorCount,
    summary.errorCount,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.totalDurationMs,
    summary.totalDurationMs,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.fetchHighlightsDurationMs,
    summary.fetchHighlightsDurationMs,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.fetchDocumentsDurationMs,
    summary.fetchDocumentsDurationMs,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.writePagesDurationMs,
    summary.writePagesDurationMs,
  )
  await logseq.Editor.upsertBlockProperty(
    page.uuid,
    LAST_FORMAL_SYNC_PROPERTY_KEYS.failureSummary,
    summary.failureSummary ?? '',
  )

  return summary
}
