import '@logseq/libs'

import { createRoot } from 'react-dom/client'

import { ReadwiseContainer } from './components'
import { setupProps } from './services'
import { settingsSchema } from './settings'
import { runSyncPlanPreviewV1 } from './sync'

const main = async () => {
  logseq.UI.showMsg('logseq-readwise-plugin loaded')

  const el = document.getElementById('app')
  if (!el) return
  const root = createRoot(el)
  root.render(<ReadwiseContainer />)

  logseq.App.registerUIItem('toolbar', {
    key: 'logseq-readwise-plugin',
    template: `<a class="button" data-on-click="syncHighlights"><i class="ti ti-letter-r"></i></a>`,
  })

  logseq.provideModel({
    syncHighlights() {
      logseq.showMainUI()
    },
  })

  logseq.App.registerCommandPalette(
    {
      key: 'readwise:setup-props',
      label: 'Readwise: Setup properties',
    },
    async () => await setupProps(),
  )

  logseq.App.registerCommandPalette(
    {
      key: 'readwise:sync',
      label: 'Readwise: Reset sync timestamp',
    },
    () =>
      logseq.updateSettings({
        lastSyncTimestamp: '',
      }),
  )

  logseq.App.registerCommandPalette(
    {
      key: 'readwise:v1-preview-sync-plan',
      label: 'Readwise: Preview V1 sync plan',
    },
    async () => {
      try {
        await runSyncPlanPreviewV1()
      } catch (error) {
        console.error('[Readwise V1 Preview] failed', error)
      }
    },
  )
}

logseq.useSettingsSchema(settingsSchema).ready(main).catch(console.error)
