import { ReadwisePageProp } from '../types'

export interface SetupPropsResult {
  success: boolean
  compatibilityMode: boolean
}

const readwisePageProps: ReadwisePageProp[] = [
  { key: 'rw-id', schema: { type: 'number' } },
  { key: 'rw-reader-id', schema: { type: 'default' } },
  { key: 'rw-author', schema: { type: 'node', cardinality: 'many' } },
  { key: 'rw-readable-title', schema: { type: 'default' } },
  { key: 'rw-category', schema: { type: 'node' } },
  { key: 'rw-source', schema: { type: 'default' } },
  { key: 'rw-cover-image', schema: { type: 'url' } },
  { key: 'rw-unique-url', schema: { type: 'url' } },
  { key: 'rw-readwise-url', schema: { type: 'url' } },
  { key: 'rw-source-url', schema: { type: 'url' } },
  { key: 'rw-external-id', schema: { type: 'default' } },
  { key: 'rw-asin', schema: { type: 'default' } },
  { key: 'rw-document-note', schema: { type: 'default' } },
  { key: 'rw-summary', schema: { type: 'default' } },
]

const ensureReadwiseTagPage = async () => {
  const existing = await logseq.Editor.getPage('Readwise')
  if (existing) return existing

  return logseq.Editor.createPage('Readwise', {}, { redirect: false })
}

const tryGetAllProperties = async () => {
  try {
    return {
      props: await logseq.Editor.getAllProperties(),
      supported: true,
    }
  } catch (error) {
    console.warn(
      '[Readwise Sync] getAllProperties is not available in this Logseq version; skipping property introspection.',
      error,
    )
    return {
      props: [],
      supported: false,
    }
  }
}

const tryUpsertProperty = async (
  key: string,
  schema: ReadwisePageProp['schema'],
) => {
  try {
    await logseq.Editor.upsertProperty(key, schema, { name: key })
    return true
  } catch (error) {
    console.warn(
      `[Readwise Sync] upsertProperty is not available for "${key}" in this Logseq version; skipping property schema creation.`,
      error,
    )
    return false
  }
}

const tryAddTagProperty = async (tagName: string, key: string) => {
  try {
    await logseq.Editor.addTagProperty(tagName, key)
    return true
  } catch (error) {
    console.warn(
      `[Readwise Sync] addTagProperty is not available for "${key}" in this Logseq version; skipping tag schema binding.`,
      error,
    )
    return false
  }
}

export const setupProps = async (): Promise<SetupPropsResult> => {
  try {
    const loadingMsg = await logseq.UI.showMsg(
      'Setting up schema. Please wait...',
      'warning',
      { timeout: 0 },
    )

    await ensureReadwiseTagPage()

    const { props: allPropsInLs, supported: canInspectProperties } =
      await tryGetAllProperties()
    const existingIdentifiers = new Set(allPropsInLs?.map((prop) => prop.ident))
    let compatibilityMode = !canInspectProperties

    const pluginName = 'logseq-readwise-plugin'
    const propsToCreate = readwisePageProps.filter(({ key }) => {
      const fullIdentifier = `:plugin.property.${pluginName}/${key}`
      return !existingIdentifiers.has(fullIdentifier)
    })

    for (const { key, schema } of propsToCreate) {
      const created = await tryUpsertProperty(key, schema)
      if (!created) compatibilityMode = true
    }

    for (const { key } of readwisePageProps) {
      const bound = await tryAddTagProperty('Readwise', key)
      if (!bound) compatibilityMode = true
    }

    await logseq.updateSettings({ propsConfigured: true })

    logseq.UI.closeMsg(loadingMsg)
    await logseq.UI.showMsg(
      compatibilityMode
        ? 'Readwise setup completed in compatibility mode. You can start sync now.'
        : 'Readwise schema setup completed.',
      'success',
    )
    return { success: true, compatibilityMode }
  } catch (err) {
    console.error('Failed to setup properties:', err)
    await logseq.UI.showMsg(`Failed to setup properties: ${err}`, 'error')
    return { success: false, compatibilityMode: true }
  }
}
