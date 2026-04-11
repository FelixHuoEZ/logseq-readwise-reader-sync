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
      'Debug-only cap for Reader highlight pages scanned before grouping by parent_id. Roughly 100 highlights per page. Set 0 to disable.',
    default: 0,
  },
]
