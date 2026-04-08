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
    key: 'autoSyncEnabled',
    type: 'boolean',
    title: 'Enable Auto Sync',
    description: 'Whether the plugin should run automatic syncs.',
    default: false,
  },
  {
    key: 'syncIntervalMinutes',
    type: 'number',
    title: 'Sync Interval (minutes)',
    description: 'Used when auto sync is enabled.',
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
