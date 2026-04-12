import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

import { readManagedPagePropertyValueV1 } from './sync-managed-page-properties'

export const MANAGED_SYNC_TIMESTAMP_PROPERTY_KEYS_V1 = {
  firstSyncedAt: 'rw-first-synced-at',
  lastSyncedAt: 'rw-last-synced-at',
} as const

const mergePageProperties = (
  pageProperties: Array<{ key: string; value: string | null }>,
  additionalProperties: Array<{ key: string; value: string | null }>,
) => {
  const normalizedAdditionalKeys = new Set(
    additionalProperties.map((entry) => entry.key.toLowerCase()),
  )

  return [
    ...pageProperties.filter(
      (entry) => !normalizedAdditionalKeys.has(entry.key.toLowerCase()),
    ),
    ...additionalProperties,
  ]
}

export const withManagedSyncTimestampPagePropertiesV1 = async ({
  page,
  pageProperties,
  syncDate,
  fallbackFirstSyncedAt,
}: {
  page: PageEntity | null
  pageProperties: Array<{ key: string; value: string | null }>
  syncDate: string
  fallbackFirstSyncedAt?: string | null
}) => {
  const preservedFirstSyncedAt =
    page == null
      ? null
      : await readManagedPagePropertyValueV1(page, [
          MANAGED_SYNC_TIMESTAMP_PROPERTY_KEYS_V1.firstSyncedAt,
        ])

  return mergePageProperties(pageProperties, [
    {
      key: MANAGED_SYNC_TIMESTAMP_PROPERTY_KEYS_V1.firstSyncedAt,
      value: preservedFirstSyncedAt ?? fallbackFirstSyncedAt ?? syncDate,
    },
    {
      key: MANAGED_SYNC_TIMESTAMP_PROPERTY_KEYS_V1.lastSyncedAt,
      value: syncDate,
    },
  ])
}
