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

  logseq.App.registerUIItem('toolbar', {
    key: 'logseq-readwise-plugin-v1-preview',
    template:
      '<a class="button" data-on-click="previewV1SyncPlan" title="Readwise V1 Preview"><i class="ti ti-eye"></i></a>',
  })

  logseq.provideModel({
    syncHighlights() {
      logseq.showMainUI()
    },
    async previewV1SyncPlan() {
      logseq.UI.showMsg('Starting Readwise V1 preview...')
      console.info('[Readwise V1 Preview] toolbar click')
      try {
        await runSyncPlanPreviewV1()
      } catch (error) {
        console.error('[Readwise V1 Preview] failed', error)
      }
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
      key: 'readwise:v1-preview-sync-plan',
      label: 'Readwise: Preview V1 sync plan',
    },
    async () => {
      logseq.UI.showMsg('Starting Readwise V1 preview...')
      console.info('[Readwise V1 Preview] command palette trigger')
      try {
        await runSyncPlanPreviewV1()
      } catch (error) {
        console.error('[Readwise V1 Preview] failed', error)
      }
    },
  )
}

logseq.useSettingsSchema(settingsSchema).ready(main).catch(console.error)
