import type { SettingSchemaDesc } from '@logseq/libs/dist/LSPlugin'

export const settingsSchema: SettingSchemaDesc[] = [
  {
    key: 'apiToken',
    type: 'string',
    title: 'Readwise API Token',
    description:
      'Your Readwise access token. Find it at https://readwise.io/access_token',
    default: '',
  },
  {
    key: 'lastSyncTimestamp',
    type: 'string',
    title: 'Last Sync Timestamp',
    description: 'Internal — managed by the plugin. Do not edit manually.',
    default: '',
  },
  {
    key: 'propsConfigured',
    type: 'boolean',
    title: 'Properties Configured',
    description: 'Internal — managed by the plugin. Do not edit manually.',
    default: false,
  },
  {
    key: 'debugSyncMaxBooks',
    type: 'number',
    title: 'Debug Sync Max Books',
    description:
      'Temporary debug limit for real sync. Set 0 to disable the limit and allow full sync.',
    default: 5,
  },
  {
    key: 'readerFullScanTargetDocuments',
    type: 'number',
    title: 'Reader Full Scan Target Documents',
    description:
      'How many managed Reader documents formal sync should write. Default 20.',
    default: 20,
  },
  {
    key: 'readerFullScanDebugHighlightPageLimit',
    type: 'number',
    title: 'Reader Full Scan Debug Highlight Page Limit',
    description:
      'Debug-only cap for Reader highlight pages scanned before grouping by parent_id. Set 0 to disable.',
    default: 0,
  },
]
