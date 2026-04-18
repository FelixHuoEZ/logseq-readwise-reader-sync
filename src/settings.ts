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
    key: 'automationSection',
    type: 'heading',
    title: 'Automation',
    description:
      'Auto Sync only becomes active after one successful manual Incremental Sync or Full Refresh establishes a saved cursor.',
    default: null,
  },
  {
    key: 'autoSyncEnabled',
    type: 'boolean',
    title: 'Enable Auto Sync',
    description:
      'Default on. The plugin checks whether Incremental Sync should run automatically, but only after a valid saved cursor exists.',
    default: true,
  },
  {
    key: 'syncIntervalMinutes',
    type: 'number',
    title: 'Auto Sync Interval (minutes)',
    description:
      'How often the plugin checks whether an automatic Incremental Sync should run. Default 15.',
    default: 15,
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
      'How many managed Reader documents Full Refresh should write. Default 20. Set 0 to sync all matched documents. Normally leave unchanged.',
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
