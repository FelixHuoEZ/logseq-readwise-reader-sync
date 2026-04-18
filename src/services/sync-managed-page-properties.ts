import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

import { logReadwiseDebug } from '../logging'

const normalizePropertyKey = (value: string): string =>
  value.toLowerCase().replace(/[^a-z0-9]/g, '')

const MANAGED_PROPERTY_KEY_NORMALIZATIONS = new Set([
  'rwid',
  'rwreaderid',
  'rwfirstsyncedat',
  'rwlastsyncedat',
  'author',
  'categories',
  'link',
  'tags',
  'date',
  'published',
  'saved',
  'summary',
  'rwauthor',
  'rwreadabletitle',
  'rwcategory',
  'rwsource',
  'rwcoverimage',
  'rwuniqueurl',
  'rwreadwiseurl',
  'rwsourceurl',
  'rwexternalid',
  'rwasin',
  'rwdocumentnote',
  'rwsummary',
])

const resolveManagedPagePropertyKeys = (
  properties: Record<string, unknown> | null,
) =>
  Object.keys(properties ?? {}).filter((key) =>
    MANAGED_PROPERTY_KEY_NORMALIZATIONS.has(normalizePropertyKey(key)),
  )

const readExistingPageProperties = async (page: PageEntity) => {
  const refreshedPage =
    (typeof page.name === 'string' && page.name.length > 0
      ? await logseq.Editor.getPage(page.name)
      : null) ?? page

  return (refreshedPage.properties as Record<string, unknown> | undefined) ?? null
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

const readPropertyValue = (
  properties: Record<string, unknown> | null,
  expectedKeys: string[],
) => {
  if (!properties) return null

  const normalizedExpected = new Set(expectedKeys.map(normalizePropertyKey))

  for (const [key, value] of Object.entries(properties)) {
    if (normalizedExpected.has(normalizePropertyKey(key))) {
      return value
    }
  }

  return null
}

export const readManagedPagePropertyValueV1 = async (
  page: PageEntity,
  expectedKeys: string[],
): Promise<string | null> => {
  const properties = await readExistingPageProperties(page)
  return extractStringValue(readPropertyValue(properties, expectedKeys))
}

export const syncManagedPagePropertiesV1 = async (
  page: PageEntity,
  pageProperties: Array<{ key: string; value: string | null }>,
  logPrefix = '[Readwise Sync]',
) => {
  const existingPageProperties = await readExistingPageProperties(page)
  const removablePropertyKeys = resolveManagedPagePropertyKeys(
    existingPageProperties,
  )

  for (const key of removablePropertyKeys) {
    await logseq.Editor.removeBlockProperty(page.uuid, key)
  }

  for (const entry of pageProperties) {
    await logseq.Editor.upsertBlockProperty(
      page.uuid,
      entry.key,
      entry.value ?? '',
    )
  }

  logReadwiseDebug(logPrefix, 'synced managed page properties', {
    pageName: page.name,
    removedKeys: removablePropertyKeys,
    nextKeys: pageProperties.map((entry) => entry.key),
  })
}
