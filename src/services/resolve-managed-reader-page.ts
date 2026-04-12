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
  ].filter(
    (value, index, values) =>
      value.length > 0 && values.indexOf(value) === index,
  )

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

const extractManagedId = (
  page: PageEntity,
  propertyKeys: string[],
): string | null => {
  const rawValue = findPropertyValue(page.properties, propertyKeys)
  const [firstValue] = extractStringValues(rawValue)
  return firstValue ?? null
}

const matchesNamespaceRoot = (page: PageEntity, namespaceRoot: string) =>
  collectPageAliases(page).some(
    (alias) => alias === namespaceRoot || alias.startsWith(`${namespaceRoot}/`),
  )

type GenericManagedPageMatchKind =
  | 'managed_id'
  | 'managed_title'
  | 'disambiguated_title'
  | 'none'

interface GenericManagedPageResolutionV1 {
  page: PageEntity | null
  matchKind: GenericManagedPageMatchKind
  resolvedPageName: string
}

const getPreferredPageName = (page: PageEntity): string | null =>
  collectPageAliases(page)[0] ?? null

const buildManagedPageIdentityConflictError = ({
  conflictingPageName,
  managedIdLabel,
  existingManagedId,
  incomingManagedId,
  managedPages,
  propertyKeys,
}: {
  conflictingPageName: string
  managedIdLabel: string
  existingManagedId: string
  incomingManagedId: string
  managedPages: PageEntity[]
  propertyKeys: string[]
}) => {
  const existingManagedIdPages = managedPages
    .filter(
      (page) => extractManagedId(page, propertyKeys) === existingManagedId,
    )
    .map((page) => getPreferredPageName(page))
    .filter(
      (value): value is string => typeof value === 'string' && value.length > 0,
    )
  const incomingManagedIdPages = managedPages
    .filter(
      (page) => extractManagedId(page, propertyKeys) === incomingManagedId,
    )
    .map((page) => getPreferredPageName(page))
    .filter(
      (value): value is string => typeof value === 'string' && value.length > 0,
    )

  return new Error(
    `Managed page identity conflict for ${conflictingPageName}: existing ${managedIdLabel}=${existingManagedId}, incoming ${managedIdLabel}=${incomingManagedId}; existing-id pages=[${existingManagedIdPages.join(', ')}]; incoming-id pages=[${incomingManagedIdPages.join(', ')}]`,
  )
}

const resolveManagedPageV1 = async ({
  managedId,
  propertyKeys,
  managedIdLabel,
  preferredPageName,
  disambiguatedPageName,
  namespaceRoot,
}: {
  managedId: string
  propertyKeys: string[]
  managedIdLabel: string
  preferredPageName: string
  disambiguatedPageName: string
  namespaceRoot: string
}): Promise<GenericManagedPageResolutionV1> => {
  const allPages = (await logseq.Editor.getAllPages()) ?? []
  const managedPages = allPages.filter((page) =>
    matchesNamespaceRoot(page, namespaceRoot),
  )
  const managedIdMatches = managedPages.filter(
    (page) => extractManagedId(page, propertyKeys) === managedId,
  )

  if (managedIdMatches.length > 1) {
    const duplicateTitles = managedIdMatches
      .flatMap((page) => collectPageAliases(page))
      .filter((value, index, values) => values.indexOf(value) === index)

    throw new Error(
      `Multiple managed pages share ${managedIdLabel}=${managedId}: ${duplicateTitles.join(', ')}`,
    )
  }

  const preferredTitleMatch =
    managedPages.find((page) =>
      collectPageAliases(page).includes(preferredPageName),
    ) ?? null
  const disambiguatedTitleMatch =
    disambiguatedPageName !== preferredPageName
      ? (managedPages.find((page) =>
          collectPageAliases(page).includes(disambiguatedPageName),
        ) ?? null)
      : preferredTitleMatch

  if (managedIdMatches.length === 1) {
    const conflictingPreferredMatch =
      preferredTitleMatch &&
      preferredTitleMatch.uuid !== managedIdMatches[0]!.uuid &&
      extractManagedId(preferredTitleMatch, propertyKeys) !== managedId
        ? preferredTitleMatch
        : null

    return {
      page: managedIdMatches[0]!,
      matchKind: 'managed_id',
      resolvedPageName:
        conflictingPreferredMatch != null
          ? disambiguatedPageName
          : preferredPageName,
    }
  }

  if (preferredTitleMatch != null) {
    const preferredManagedId = extractManagedId(
      preferredTitleMatch,
      propertyKeys,
    )

    if (
      preferredManagedId == null ||
      preferredManagedId.length === 0 ||
      preferredManagedId === managedId
    ) {
      return {
        page: preferredTitleMatch,
        matchKind: 'managed_title',
        resolvedPageName: preferredPageName,
      }
    }

    if (disambiguatedTitleMatch != null) {
      const disambiguatedManagedId = extractManagedId(
        disambiguatedTitleMatch,
        propertyKeys,
      )

      if (
        disambiguatedManagedId == null ||
        disambiguatedManagedId.length === 0 ||
        disambiguatedManagedId === managedId
      ) {
        return {
          page: disambiguatedTitleMatch,
          matchKind: 'disambiguated_title',
          resolvedPageName: disambiguatedPageName,
        }
      }

      throw buildManagedPageIdentityConflictError({
        conflictingPageName: disambiguatedPageName,
        managedIdLabel,
        existingManagedId: disambiguatedManagedId,
        incomingManagedId: managedId,
        managedPages,
        propertyKeys,
      })
    }

    return {
      page: null,
      matchKind: 'none',
      resolvedPageName: disambiguatedPageName,
    }
  }

  if (disambiguatedTitleMatch != null) {
    const disambiguatedManagedId = extractManagedId(
      disambiguatedTitleMatch,
      propertyKeys,
    )

    if (
      disambiguatedManagedId == null ||
      disambiguatedManagedId.length === 0 ||
      disambiguatedManagedId === managedId
    ) {
      return {
        page: disambiguatedTitleMatch,
        matchKind: 'disambiguated_title',
        resolvedPageName: disambiguatedPageName,
      }
    }

    throw buildManagedPageIdentityConflictError({
      conflictingPageName: disambiguatedPageName,
      managedIdLabel,
      existingManagedId: disambiguatedManagedId,
      incomingManagedId: managedId,
      managedPages,
      propertyKeys,
    })
  }

  return {
    page: null,
    matchKind: 'none',
    resolvedPageName: preferredPageName,
  }
}

export interface ManagedReaderPageResolutionV1 {
  page: PageEntity | null
  matchKind: 'rw-reader-id' | 'managed_title' | 'disambiguated_title' | 'none'
  resolvedPageName: string
}

export const resolveManagedReaderPageV1 = async ({
  readerDocumentId,
  preferredPageName,
  disambiguatedPageName,
  namespaceRoot,
}: {
  readerDocumentId: string
  preferredPageName: string
  disambiguatedPageName: string
  namespaceRoot: string
}): Promise<ManagedReaderPageResolutionV1> => {
  const resolved = await resolveManagedPageV1({
    managedId: readerDocumentId,
    propertyKeys: ['rw-reader-id'],
    managedIdLabel: 'rw-reader-id',
    preferredPageName,
    disambiguatedPageName,
    namespaceRoot,
  })

  return {
    page: resolved.page,
    matchKind:
      resolved.matchKind === 'managed_id' ? 'rw-reader-id' : resolved.matchKind,
    resolvedPageName: resolved.resolvedPageName,
  }
}

export interface ManagedReadwisePageResolutionV1 {
  page: PageEntity | null
  matchKind: 'rw-id' | 'managed_title' | 'disambiguated_title' | 'none'
  resolvedPageName: string
}

export const resolveManagedReadwisePageV1 = async ({
  readwiseBookId,
  preferredPageName,
  disambiguatedPageName,
  namespaceRoot,
}: {
  readwiseBookId: number
  preferredPageName: string
  disambiguatedPageName: string
  namespaceRoot: string
}): Promise<ManagedReadwisePageResolutionV1> => {
  const resolved = await resolveManagedPageV1({
    managedId: String(readwiseBookId),
    propertyKeys: ['rw-id'],
    managedIdLabel: 'rw-id',
    preferredPageName,
    disambiguatedPageName,
    namespaceRoot,
  })

  return {
    page: resolved.page,
    matchKind:
      resolved.matchKind === 'managed_id' ? 'rw-id' : resolved.matchKind,
    resolvedPageName: resolved.resolvedPageName,
  }
}

export const renameManagedPageIfNeededV1 = async ({
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
    throw new Error(
      `Failed to resolve current page name before rename to ${expectedPageName}`,
    )
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

export const renameManagedReaderPageIfNeededV1 = renameManagedPageIfNeededV1
