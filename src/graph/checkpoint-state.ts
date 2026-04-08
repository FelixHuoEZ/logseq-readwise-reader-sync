import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

export type GraphCheckpointSourceV1 = 'full_sync' | 'incremental_sync'

export interface GraphCheckpointStateV1 {
  schemaVersion: 1
  updatedAfter: string | null
  committedAt: string
  source: GraphCheckpointSourceV1
}

export const GRAPH_CHECKPOINT_PAGE_NAME_V1 = 'Readwise Sync State'

const CHECKPOINT_PROPERTY_KEYS = {
  schemaVersion: 'rw-sync-schema',
  updatedAfter: 'rw-checkpoint-updated-after',
  committedAt: 'rw-checkpoint-committed-at',
  source: 'rw-checkpoint-source',
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
