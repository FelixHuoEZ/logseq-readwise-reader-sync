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
    key: 'pluginStateJson',
    type: 'string',
    title: 'Plugin State JSON',
    description: 'Internal — managed by the plugin. Do not edit manually.',
    default: '',
  },
]
