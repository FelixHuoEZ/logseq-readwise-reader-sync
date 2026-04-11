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
    key: 'debugSection',
    type: 'heading',
    title: 'Debug',
    description: 'Default behavior: do not modify these settings during normal use.',
    default: null,
  },
  {
    key: 'logLevel',
    type: 'enum',
    title: 'Log Level',
    description:
      'Default warn. Normally leave unchanged.',
    default: 'warn',
    enumChoices: ['error', 'warn', 'info', 'debug'],
    enumPicker: 'select',
  },
  {
    key: 'readerFullScanTargetDocuments',
    type: 'number',
    title: 'Reader Full Scan Target Documents',
    description:
      'How many managed Reader documents formal sync should write. Default 20. Normally leave unchanged.',
    default: 20,
  },
  {
    key: 'readerFullScanDebugHighlightPageLimit',
    type: 'number',
    title: 'Reader Full Scan Debug Highlight Page Limit',
    description:
      'Debug-only cap for Reader highlight pages scanned before grouping by parent_id. Roughly 100 highlights per page. Keep 0 for normal use.',
    default: 0,
  },
]
