import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

import {
  assertManagedPageFileNameWithinLimits,
  buildManagedPageFileStem,
} from './readwise-page-names'

const uniqueValues = (values: string[]) =>
  values.filter(
    (value, index, array) => value.length > 0 && array.indexOf(value) === index,
  )

const collectPageAliases = (page: PageEntity): string[] =>
  uniqueValues([
    typeof page.originalName === 'string' ? page.originalName : '',
    typeof page.name === 'string' ? page.name : '',
    typeof page.title === 'string' ? page.title : '',
  ])

const normalizePropertyKey = (value: string): string =>
  value.toLowerCase().replace(/[^a-z0-9]/g, '')

const extractStringValues = (value: unknown): string[] => {
  if (typeof value === 'string') {
    const trimmed = value.trim()
    return trimmed.length > 0 ? [trimmed] : []
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return [String(value)]
  }

  if (Array.isArray(value)) {
    return value.flatMap((item) => extractStringValues(item))
  }

  return []
}

const findPropertyValue = (
  properties: Record<string, unknown> | undefined,
  expectedKeys: string[],
): unknown => {
  if (!properties) return null

  const expected = new Set(expectedKeys.map(normalizePropertyKey))

  for (const [key, value] of Object.entries(properties)) {
    if (expected.has(normalizePropertyKey(key))) {
      return value
    }
  }

  return null
}

const extractReaderDocumentIdFromProperties = (
  page: PageEntity,
): string | null => {
  const rawValue = findPropertyValue(page.properties, ['rw-reader-id'])
  const [firstValue] = extractStringValues(rawValue)
  return firstValue ?? null
}

const matchesNamespaceRoots = (page: PageEntity, namespaceRoots: string[]) => {
  const aliases = collectPageAliases(page)

  return namespaceRoots.some((namespaceRoot) =>
    aliases.some(
      (alias) =>
        alias === namespaceRoot || alias.startsWith(`${namespaceRoot}/`),
    ),
  )
}

export interface ManagedReaderPageAuditEntryV1 {
  pageTitle: string
  aliases: string[]
  pageUuid: string
  readerDocumentId: string | null
  fileStem: string
}

export interface ManagedReaderPageAuditResultV1 {
  scannedPages: number
  namespaceRoots: string[]
  duplicateReaderIds: Array<{
    readerDocumentId: string
    pages: ManagedReaderPageAuditEntryV1[]
  }>
  missingReaderIdPages: ManagedReaderPageAuditEntryV1[]
  overlongFileNamePages: Array<
    ManagedReaderPageAuditEntryV1 & {
      diagnosticMessage: string
    }
  >
}

const loadReaderDocumentId = async (
  page: PageEntity,
): Promise<string | null> => {
  const directValue = extractReaderDocumentIdFromProperties(page)
  if (directValue != null) {
    return directValue
  }

  if (!page.uuid) return null

  try {
    const rawValue = await logseq.Editor.getBlockProperty(
      page.uuid,
      'rw-reader-id',
    )
    const [firstValue] = extractStringValues(rawValue)
    return firstValue ?? null
  } catch {
    return null
  }
}

export const auditManagedReaderPagesV1 = async (
  namespaceRoots: string[],
): Promise<ManagedReaderPageAuditResultV1> => {
  const pages = ((await logseq.Editor.getAllPages()) ?? []).filter((page) =>
    matchesNamespaceRoots(page, namespaceRoots),
  )
  const entries: ManagedReaderPageAuditEntryV1[] = []

  for (const page of pages) {
    const aliases = collectPageAliases(page)
    const pageTitle =
      aliases[0] ?? page.originalName ?? page.name ?? page.title ?? ''
    const fileStem = buildManagedPageFileStem(pageTitle)
    const readerDocumentId = await loadReaderDocumentId(page)

    entries.push({
      pageTitle,
      aliases,
      pageUuid: page.uuid,
      readerDocumentId,
      fileStem,
    })
  }

  const duplicateMap = new Map<string, ManagedReaderPageAuditEntryV1[]>()
  const missingReaderIdPages: ManagedReaderPageAuditEntryV1[] = []
  const overlongFileNamePages: Array<
    ManagedReaderPageAuditEntryV1 & { diagnosticMessage: string }
  > = []

  for (const entry of entries) {
    if (entry.readerDocumentId == null) {
      missingReaderIdPages.push(entry)
    } else {
      duplicateMap.set(entry.readerDocumentId, [
        ...(duplicateMap.get(entry.readerDocumentId) ?? []),
        entry,
      ])
    }

    try {
      assertManagedPageFileNameWithinLimits(entry.pageTitle, 'org')
    } catch (error) {
      overlongFileNamePages.push({
        ...entry,
        diagnosticMessage:
          error instanceof Error ? error.message : String(error),
      })
    }
  }

  const duplicateReaderIds = [...duplicateMap.entries()]
    .filter(([, auditEntries]) => auditEntries.length > 1)
    .map(([readerDocumentId, auditEntries]) => ({
      readerDocumentId,
      pages: auditEntries,
    }))

  return {
    scannedPages: entries.length,
    namespaceRoots,
    duplicateReaderIds,
    missingReaderIdPages,
    overlongFileNamePages,
  }
}
