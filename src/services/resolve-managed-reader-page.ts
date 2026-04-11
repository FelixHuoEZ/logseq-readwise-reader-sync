import type { PageEntity } from '@logseq/libs/dist/LSPlugin'
import { logReadwiseInfo } from '../logging'

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

const collectPageAliases = (page: PageEntity): string[] =>
  [
    typeof page.originalName === 'string' ? page.originalName : '',
    typeof page.name === 'string' ? page.name : '',
    typeof page.title === 'string' ? page.title : '',
  ].filter((value, index, values) => value.length > 0 && values.indexOf(value) === index)

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

const extractReaderDocumentId = (page: PageEntity): string | null => {
  const rawValue = findPropertyValue(page.properties, ['rw-reader-id'])
  const [firstValue] = extractStringValues(rawValue)
  return firstValue ?? null
}

const matchesNamespaceRoot = (page: PageEntity, namespaceRoot: string) =>
  collectPageAliases(page).some(
    (alias) => alias === namespaceRoot || alias.startsWith(`${namespaceRoot}/`),
  )

export interface ManagedReaderPageResolutionV1 {
  page: PageEntity | null
  matchKind: 'rw-reader-id' | 'managed_title' | 'none'
}

const getPreferredPageName = (page: PageEntity): string | null =>
  collectPageAliases(page)[0] ?? null

export const resolveManagedReaderPageV1 = async ({
  readerDocumentId,
  expectedPageName,
  namespaceRoot,
}: {
  readerDocumentId: string
  expectedPageName: string
  namespaceRoot: string
}): Promise<ManagedReaderPageResolutionV1> => {
  const allPages = (await logseq.Editor.getAllPages()) ?? []
  const managedPages = allPages.filter((page) => matchesNamespaceRoot(page, namespaceRoot))
  const readerIdMatches = managedPages.filter(
    (page) => extractReaderDocumentId(page) === readerDocumentId,
  )

  if (readerIdMatches.length > 1) {
    const duplicateTitles = readerIdMatches
      .flatMap((page) => collectPageAliases(page))
      .filter((value, index, values) => values.indexOf(value) === index)

    throw new Error(
      `Multiple managed pages share rw-reader-id=${readerDocumentId}: ${duplicateTitles.join(', ')}`,
    )
  }

  if (readerIdMatches.length === 1) {
    return {
      page: readerIdMatches[0]!,
      matchKind: 'rw-reader-id',
    }
  }

  const titleMatch =
    managedPages.find((page) => collectPageAliases(page).includes(expectedPageName)) ?? null

  if (titleMatch == null) {
    return {
      page: null,
      matchKind: 'none',
    }
  }

  const titleMatchReaderId = extractReaderDocumentId(titleMatch)
  if (
    titleMatchReaderId != null &&
    titleMatchReaderId.length > 0 &&
    titleMatchReaderId !== readerDocumentId
  ) {
    throw new Error(
      `Managed page identity conflict for ${expectedPageName}: existing rw-reader-id=${titleMatchReaderId}, incoming rw-reader-id=${readerDocumentId}`,
    )
  }

  return {
    page: titleMatch,
    matchKind: 'managed_title',
  }
}

export const renameManagedReaderPageIfNeededV1 = async ({
  page,
  expectedPageName,
  logPrefix = '[Readwise Sync]',
}: {
  page: PageEntity
  expectedPageName: string
  logPrefix?: string
}): Promise<{
  page: PageEntity
  renamed: boolean
  previousPageName: string | null
}> => {
  const aliases = collectPageAliases(page)
  if (aliases.includes(expectedPageName)) {
    return {
      page,
      renamed: false,
      previousPageName: getPreferredPageName(page),
    }
  }

  const currentPageName = getPreferredPageName(page)
  if (!currentPageName) {
    throw new Error(`Failed to resolve current page name before rename to ${expectedPageName}`)
  }

  const conflictingPage = await logseq.Editor.getPage(expectedPageName)
  if (conflictingPage && conflictingPage.uuid !== page.uuid) {
    throw new Error(
      `Cannot rename ${currentPageName} to ${expectedPageName}: target page already exists`,
    )
  }

  await logseq.Editor.renamePage(currentPageName, expectedPageName)
  await delay(500)

  const renamedPage = await logseq.Editor.getPage(expectedPageName)
  if (!renamedPage) {
    throw new Error(`Failed to resolve renamed page ${expectedPageName}`)
  }

  logReadwiseInfo(logPrefix, 'renamed managed page to tracked title', {
    previousPageName: currentPageName,
    nextPageName: expectedPageName,
  })

  return {
    page: renamedPage,
    renamed: true,
    previousPageName: currentPageName,
  }
}
