import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

import { logReadwiseDebug } from '../logging'

const normalizePropertyKey = (value: string): string =>
  value.toLowerCase().replace(/[^a-z0-9]/g, '')

const MANAGED_PROPERTY_KEY_NORMALIZATIONS = new Set([
  'rwid',
  'rwreaderid',
  'author',
  'categories',
  'link',
  'tags',
  'date',
  'published',
  'saved',
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

export const syncManagedPagePropertiesV1 = async (
  page: PageEntity,
  pageProperties: Array<{ key: string; value: string | null }>,
  logPrefix = '[Readwise Sync]',
) => {
  const existingPageProperties = await logseq.Editor.getPageProperties(
    page.uuid,
  )
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
