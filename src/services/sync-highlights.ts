import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

import type { ExportedBook } from '../types'
import { appendHighlights, upsertBookProperties } from '.'
import { buildManagedPageNamePlanV1 } from './readwise-page-names'

export interface SyncBookOptions {
  namespacePrefix?: string | null
}

const toPageTitle = (page: PageEntity): string =>
  (typeof page.originalName === 'string' && page.originalName) ||
  (typeof page.title === 'string' && page.title) ||
  (typeof page.name === 'string' && page.name) ||
  ''

const matchesNamespacePrefix = (
  page: PageEntity,
  namespacePrefix: string | null | undefined,
): boolean => {
  if (!namespacePrefix) return true

  const pageTitle = toPageTitle(page)
  return pageTitle.startsWith(`${namespacePrefix}/`)
}

const buildTargetPageName = (
  book: ExportedBook,
  namespacePrefix: string | null | undefined,
) =>
  buildManagedPageNamePlanV1({
    pageTitle: book.title,
    namespacePrefix,
    managedId: book.user_book_id,
    format: 'org',
  })

const readBookIdFromPageProperties = (page: PageEntity): number | null => {
  const properties = page.properties as Record<string, unknown> | undefined
  if (!properties) return null

  const candidates = [properties['rw-id'], properties.rwid]
  for (const value of candidates) {
    if (typeof value === 'number' && Number.isFinite(value)) {
      return value
    }

    if (typeof value === 'string' && value.trim().length > 0) {
      const parsed = Number.parseInt(value, 10)
      if (Number.isFinite(parsed)) return parsed
    }
  }

  return null
}

export const buildBookIdToPageMap = async (
  options: SyncBookOptions = {},
): Promise<Map<number, string>> => {
  const map = new Map<number, string>()
  const pages = (await logseq.Editor.getAllPages()) ?? []

  for (const page of pages as PageEntity[]) {
    if (!page?.uuid) continue
    if (!matchesNamespacePrefix(page, options.namespacePrefix)) continue

    let rwId = readBookIdFromPageProperties(page)

    if (rwId == null) {
      try {
        const rawValue = (await logseq.Editor.getBlockProperty(
          page.uuid,
          'rw-id',
        )) as unknown
        if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
          rwId = rawValue
        } else if (typeof rawValue === 'string' && rawValue.trim().length > 0) {
          const parsed = Number.parseInt(rawValue, 10)
          if (Number.isFinite(parsed)) rwId = parsed
        }
      } catch (error) {
        console.warn(
          `[Readwise Sync] Failed to inspect rw-id on page ${page.uuid}; skipping page mapping candidate.`,
          error,
        )
      }
    }

    if (rwId != null) {
      map.set(rwId, page.uuid)
    }
  }

  return map
}

export const syncBook = async (
  book: ExportedBook,
  bookIdToPage: Map<number, string>,
  options: SyncBookOptions = {},
) => {
  const existingPageUuid = bookIdToPage.get(book.user_book_id)

  if (existingPageUuid) {
    await appendHighlights(existingPageUuid, book.highlights)
  } else {
    const pageNamePlan = buildTargetPageName(book, options.namespacePrefix)
    const preferredPage = await logseq.Editor.getPage(
      pageNamePlan.preferredPageName,
    )
    const conflictingPreferredPage =
      preferredPage &&
      readBookIdFromPageProperties(preferredPage) !== book.user_book_id
        ? preferredPage
        : null
    const targetPageName =
      conflictingPreferredPage != null
        ? pageNamePlan.disambiguatedPageName
        : pageNamePlan.preferredPageName
    const page = await logseq.Editor.createPage(
      targetPageName,
      {},
      { redirect: false },
    )
    if (!page) return

    try {
      await logseq.Editor.addBlockTag(page.uuid, 'Readwise')
    } catch (error) {
      console.warn(
        '[Readwise Sync] addBlockTag is not available in this Logseq version; continuing without page tag binding.',
        error,
      )
    }
    await upsertBookProperties(page.uuid, book)
    await appendHighlights(page.uuid, book.highlights)
  }
}
