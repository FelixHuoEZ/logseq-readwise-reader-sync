import type { PluginStateV1 } from './types'

const PLUGIN_STATE_JSON_KEY = 'pluginStateJson'

export const savePluginStateV1 = async (
  state: PluginStateV1,
): Promise<void> => {
  await logseq.updateSettings({
    [PLUGIN_STATE_JSON_KEY]: JSON.stringify(state),
  })
}
