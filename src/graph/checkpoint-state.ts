import type { PageEntity } from '@logseq/libs/dist/LSPlugin'
import { logReadwiseWarn } from '../logging'

export type GraphCheckpointSourceV1 = 'full_sync' | 'incremental_sync'
export type GraphReaderSyncSourceV1 = 'full_reconcile' | 'incremental_sync'

export interface GraphCheckpointStateV1 {
  schemaVersion: 1
  updatedAfter: string | null
  committedAt: string
  source: GraphCheckpointSourceV1
}

export interface GraphReaderSyncStateV1 {
  schemaVersion: 1
  updatedAfter: string | null
  committedAt: string
  source: GraphReaderSyncSourceV1
}

export interface GraphLastFormalSyncSummaryV1 {
  schemaVersion: 1
  runKind: 'reader_full_scan' | 'reader_incremental' | 'reader_cached_rebuild'
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

export const GRAPH_FORMAL_SYNC_STATE_PAGE_NAME_V1 = 'Readwise Sync State'
export const GRAPH_LEGACY_CHECKPOINT_PAGE_NAME_V1 = 'Readwise Legacy Sync State'

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

const READER_SYNC_PROPERTY_KEYS = {
  schemaVersion: 'rw-reader-sync-schema',
  updatedAfter: 'rw-reader-sync-updated-after',
  committedAt: 'rw-reader-sync-committed-at',
  source: 'rw-reader-sync-source',
} as const

const LAST_FORMAL_SYNC_SUMMARY_BLOCK_UUID =
  'c2ddfe13-67d1-42df-9f0b-cd8684f16f61'
const READER_SYNC_STATE_BLOCK_UUID =
  '0ff7bc8e-6638-4d39-a5d3-89b8b2a12eb1'

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

const parseReaderSyncSource = (
  page: PageEntity,
): GraphReaderSyncSourceV1 | null => {
  const rawValue = extractStringValue(
    readPropertyValue(page.properties, READER_SYNC_PROPERTY_KEYS.source),
  )

  if (rawValue === 'full_reconcile' || rawValue === 'incremental_sync') {
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

const parseReaderSyncPage = (
  page: PageEntity | null,
): GraphReaderSyncStateV1 | null => {
  if (!page) return null

  const schemaVersion = extractNumberValue(
    readPropertyValue(page.properties, READER_SYNC_PROPERTY_KEYS.schemaVersion),
  )
  const committedAt = extractStringValue(
    readPropertyValue(page.properties, READER_SYNC_PROPERTY_KEYS.committedAt),
  )
  const source = parseReaderSyncSource(page)

  if (schemaVersion !== 1 || committedAt == null || source == null) {
    return null
  }

  return {
    schemaVersion: 1,
    updatedAfter:
      extractStringValue(
        readPropertyValue(page.properties, READER_SYNC_PROPERTY_KEYS.updatedAfter),
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
    throw new Error('Failed to create graph checkpoint state page.')
  }

  return created
}

const ensureLegacyCheckpointPage = async () =>
  ensureManagedStatePage(GRAPH_LEGACY_CHECKPOINT_PAGE_NAME_V1)

const ensureFormalSyncStatePage = async () =>
  ensureManagedStatePage(GRAPH_FORMAL_SYNC_STATE_PAGE_NAME_V1)

const formatDurationMinutes = (milliseconds: number) =>
  (milliseconds / 60000).toFixed(2)

const buildLastFormalSyncSummaryBlockContent = (
  summary: GraphLastFormalSyncSummaryV1,
) => {
  const lines = [
    'Readwise Formal Sync Summary',
    '',
    `- rw-last-formal-sync-status: ${summary.status}`,
    `- rw-last-formal-sync-kind: ${summary.runKind}`,
    `- rw-last-formal-sync-completed-at: ${summary.completedAt}`,
    `- rw-last-formal-sync-highlight-pages-scanned: ${summary.highlightPagesScanned}`,
    `- rw-last-formal-sync-highlights-scanned: ${summary.highlightsScanned}`,
    `- rw-last-formal-sync-parent-documents-identified: ${summary.parentDocumentsIdentified}`,
    `- rw-last-formal-sync-pages-targeted: ${summary.pagesTargeted}`,
    `- rw-last-formal-sync-pages-processed: ${summary.pagesProcessed}`,
    `- rw-last-formal-sync-created: ${summary.createdCount}`,
    `- rw-last-formal-sync-updated: ${summary.updatedCount}`,
    `- rw-last-formal-sync-unchanged: ${summary.unchangedCount}`,
    `- rw-last-formal-sync-renamed: ${summary.renamedCount}`,
    `- rw-last-formal-sync-errors: ${summary.errorCount}`,
    `- rw-last-formal-sync-total-duration-minutes: ${formatDurationMinutes(summary.totalDurationMs)}`,
    `- rw-last-formal-sync-fetch-highlights-duration-minutes: ${formatDurationMinutes(summary.fetchHighlightsDurationMs)}`,
    `- rw-last-formal-sync-fetch-documents-duration-minutes: ${formatDurationMinutes(summary.fetchDocumentsDurationMs)}`,
    `- rw-last-formal-sync-write-pages-duration-minutes: ${formatDurationMinutes(summary.writePagesDurationMs)}`,
  ]

  if (summary.failureSummary) {
    lines.push(`- rw-last-formal-sync-failure-summary: ${summary.failureSummary}`)
  }

  return lines.join('\n')
}

const upsertLastFormalSyncSummaryBlock = async (
  page: PageEntity,
  summary: GraphLastFormalSyncSummaryV1,
) => {
  const content = buildLastFormalSyncSummaryBlockContent(summary)
  const existing = await logseq.Editor.getBlock(LAST_FORMAL_SYNC_SUMMARY_BLOCK_UUID)

  if (existing) {
    await logseq.Editor.updateBlock(LAST_FORMAL_SYNC_SUMMARY_BLOCK_UUID, content)
    return
  }

  const created = await logseq.Editor.insertBlock(page.uuid, content, {
    customUUID: LAST_FORMAL_SYNC_SUMMARY_BLOCK_UUID,
  })

  if (!created) {
    throw new Error('Failed to create the last formal sync summary block.')
  }
}

const buildReaderSyncStateBlockContent = (state: GraphReaderSyncStateV1) =>
  [
    'Readwise Reader Sync State',
    '',
    `- ${READER_SYNC_PROPERTY_KEYS.schemaVersion}: ${state.schemaVersion}`,
    `- ${READER_SYNC_PROPERTY_KEYS.updatedAfter}: ${state.updatedAfter ?? ''}`,
    `- ${READER_SYNC_PROPERTY_KEYS.committedAt}: ${state.committedAt}`,
    `- ${READER_SYNC_PROPERTY_KEYS.source}: ${state.source}`,
  ].join('\n')

const parseStateBlockPropertyLines = (
  content: string | null | undefined,
): Record<string, string> => {
  if (typeof content !== 'string' || content.trim().length === 0) {
    return {}
  }

  const parsed = new Map<string, string>()
  const lines = content.split(/\r?\n/)

  for (const line of lines) {
    const match = line.match(/^\s*-\s+([^:]+):\s*(.*)\s*$/)
    if (!match) continue
    parsed.set(normalizePropertyKey(match[1] ?? ''), (match[2] ?? '').trim())
  }

  return Object.fromEntries(parsed)
}

const parseReaderSyncStateBlock = (
  block: { content?: string | null } | null,
): GraphReaderSyncStateV1 | null => {
  if (!block?.content) return null

  const properties = parseStateBlockPropertyLines(block.content)
  const schemaVersion = extractNumberValue(
    properties[normalizePropertyKey(READER_SYNC_PROPERTY_KEYS.schemaVersion)] ?? null,
  )
  const committedAt = extractStringValue(
    properties[normalizePropertyKey(READER_SYNC_PROPERTY_KEYS.committedAt)] ?? null,
  )
  const source = extractStringValue(
    properties[normalizePropertyKey(READER_SYNC_PROPERTY_KEYS.source)] ?? null,
  )

  if (
    schemaVersion !== 1 ||
    committedAt == null ||
    (source !== 'incremental_sync' && source !== 'full_reconcile')
  ) {
    return null
  }

  return {
    schemaVersion: 1,
    updatedAfter:
      extractStringValue(
        properties[normalizePropertyKey(READER_SYNC_PROPERTY_KEYS.updatedAfter)] ?? null,
      ) ?? null,
    committedAt,
    source,
  }
}

const upsertReaderSyncStateBlock = async (
  page: PageEntity,
  state: GraphReaderSyncStateV1,
) => {
  const content = buildReaderSyncStateBlockContent(state)
  const existing = await logseq.Editor.getBlock(READER_SYNC_STATE_BLOCK_UUID)

  if (existing) {
    await logseq.Editor.updateBlock(READER_SYNC_STATE_BLOCK_UUID, content)
    return
  }

  const created = await logseq.Editor.insertBlock(page.uuid, content, {
    customUUID: READER_SYNC_STATE_BLOCK_UUID,
  })

  if (!created) {
    throw new Error('Failed to create the Reader sync state block.')
  }
}

export const loadGraphCheckpointStateV1 =
  async (): Promise<GraphCheckpointStateV1 | null> =>
    parseCheckpointPage(
      await logseq.Editor.getPage(GRAPH_LEGACY_CHECKPOINT_PAGE_NAME_V1),
    )

export const loadGraphReaderSyncStateV1 =
  async (): Promise<GraphReaderSyncStateV1 | null> => {
    const page = await logseq.Editor.getPage(GRAPH_FORMAL_SYNC_STATE_PAGE_NAME_V1)
    const pageState = parseReaderSyncPage(page)
    if (pageState) return pageState

    return parseReaderSyncStateBlock(
      await logseq.Editor.getBlock(READER_SYNC_STATE_BLOCK_UUID),
    )
  }

export const pickPreferredCheckpointStateV1 = (
  currentState: GraphCheckpointStateV1 | null,
  nextState: GraphCheckpointStateV1,
): GraphCheckpointStateV1 =>
  currentState == null ||
  compareUpdatedAfter(nextState.updatedAfter, currentState.updatedAfter) >= 0
    ? nextState
    : currentState

export const pickPreferredReaderSyncStateV1 = (
  currentState: GraphReaderSyncStateV1 | null,
  nextState: GraphReaderSyncStateV1,
): GraphReaderSyncStateV1 =>
  currentState == null ||
  compareUpdatedAfter(nextState.updatedAfter, currentState.updatedAfter) >= 0
    ? nextState
    : currentState

export const saveGraphCheckpointStateV1 = async (
  nextState: GraphCheckpointStateV1,
): Promise<GraphCheckpointStateV1> => {
  const currentState = await loadGraphCheckpointStateV1()
  const preferredState = pickPreferredCheckpointStateV1(currentState, nextState)
  const page = await ensureLegacyCheckpointPage()

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

export const saveGraphReaderSyncStateV1 = async (
  nextState: GraphReaderSyncStateV1,
): Promise<GraphReaderSyncStateV1> => {
  await migrateLegacyCheckpointOffFormalSyncStatePageV1()

  const currentState = await loadGraphReaderSyncStateV1()
  const preferredState = pickPreferredReaderSyncStateV1(currentState, nextState)
  const page = await ensureFormalSyncStatePage()

  try {
    await logseq.Editor.upsertBlockProperty(
      page.uuid,
      READER_SYNC_PROPERTY_KEYS.schemaVersion,
      preferredState.schemaVersion,
    )
    await logseq.Editor.upsertBlockProperty(
      page.uuid,
      READER_SYNC_PROPERTY_KEYS.updatedAfter,
      preferredState.updatedAfter ?? '',
    )
    await logseq.Editor.upsertBlockProperty(
      page.uuid,
      READER_SYNC_PROPERTY_KEYS.committedAt,
      preferredState.committedAt,
    )
    await logseq.Editor.upsertBlockProperty(
      page.uuid,
      READER_SYNC_PROPERTY_KEYS.source,
      preferredState.source,
    )
  } catch (error) {
    logReadwiseWarn(
      '[Readwise Sync]',
      'failed to persist Reader sync state as page properties; falling back to managed state block only.',
      error,
    )
  }

  await upsertReaderSyncStateBlock(page, preferredState)

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

  const page = await ensureLegacyCheckpointPage()
  logseq.App.pushState('page', { name: GRAPH_LEGACY_CHECKPOINT_PAGE_NAME_V1 })
  return page
}

export const loadGraphLastFormalSyncSummaryV1 =
  async (): Promise<GraphLastFormalSyncSummaryV1 | null> => {
    const page = await logseq.Editor.getPage(GRAPH_FORMAL_SYNC_STATE_PAGE_NAME_V1)
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
      (runKind !== 'reader_full_scan' &&
        runKind !== 'reader_incremental' &&
        runKind !== 'reader_cached_rebuild') ||
      (status !== 'success' && status !== 'partial_error' && status !== 'failed') ||
      completedAt == null
    ) {
      return null
    }

    const numberOrZero = (key: (typeof LAST_FORMAL_SYNC_PROPERTY_KEYS)[keyof typeof LAST_FORMAL_SYNC_PROPERTY_KEYS]) =>
      extractNumberValue(readPropertyValue(page.properties, key)) ?? 0

    return {
      schemaVersion: 1,
      runKind,
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

const migrateLegacyCheckpointOffFormalSyncStatePageV1 = async () => {
  const formalPage = await logseq.Editor.getPage(GRAPH_FORMAL_SYNC_STATE_PAGE_NAME_V1)
  if (!formalPage) return

  const legacyState = parseCheckpointPage(formalPage)
  if (!legacyState) return

  await saveGraphCheckpointStateV1(legacyState)

  const pageBlocks = await logseq.Editor.getPageBlocksTree(
    GRAPH_FORMAL_SYNC_STATE_PAGE_NAME_V1,
  )
  const nonSummaryBlocks =
    pageBlocks?.filter((block) => block.uuid !== LAST_FORMAL_SYNC_SUMMARY_BLOCK_UUID) ?? []

  if (nonSummaryBlocks.length > 0) {
    logReadwiseWarn(
      '[Readwise Sync]',
      'legacy checkpoint props were migrated, but the formal sync state page was not recreated because it contains additional managed blocks.',
      {
        pageName: GRAPH_FORMAL_SYNC_STATE_PAGE_NAME_V1,
        nonSummaryBlockCount: nonSummaryBlocks.length,
      },
    )
    return
  }

  await logseq.Editor.deletePage(GRAPH_FORMAL_SYNC_STATE_PAGE_NAME_V1)
}

export const saveGraphLastFormalSyncSummaryV1 = async (
  summary: GraphLastFormalSyncSummaryV1,
): Promise<GraphLastFormalSyncSummaryV1> => {
  await migrateLegacyCheckpointOffFormalSyncStatePageV1()
  const page = await ensureFormalSyncStatePage()

  try {
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
  } catch (error) {
    logReadwiseWarn(
      '[Readwise Sync]',
      'failed to persist last formal sync summary as page properties; falling back to managed summary block only.',
      error,
    )
  }

  await upsertLastFormalSyncSummaryBlock(page, summary)

  return summary
}
