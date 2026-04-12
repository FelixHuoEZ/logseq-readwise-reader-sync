import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

import { logReadwiseInfo } from '../logging'

const uniqueStrings = (values: string[]) =>
  values.filter(
    (value, index, array) => value.length > 0 && array.indexOf(value) === index,
  )

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

export const collectManagedPageAliasesV1 = (page: PageEntity): string[] =>
  uniqueStrings([
    typeof page.originalName === 'string' ? page.originalName : '',
    typeof page.title === 'string' ? page.title : '',
    typeof page.name === 'string' ? page.name : '',
  ])

export const detectLegacyManagedPageRepairSignaturesV1 = (rootContent: string) => {
  const signatures: string[] = []
  const propertiesBlocks = (rootContent.match(/^:PROPERTIES:$/gm) ?? []).length
  const noteBlocks = (rootContent.match(/^#\+BEGIN_NOTE$/gm) ?? []).length
  const syncHeaders = (
    rootContent.match(/^\* Highlights (?:first synced|refreshed) by \[\[Readwise\]\]/gm) ??
    []
  ).length

  if (propertiesBlocks > 1) {
    signatures.push('duplicate properties block')
  }

  if (noteBlocks > 1) {
    signatures.push('duplicate note block')
  }

  if (syncHeaders > 1) {
    signatures.push('duplicate sync header')
  }

  return signatures
}

const extractLegacyFirstSyncedOnV1 = (rootContent: string): string | null => {
  const match = rootContent.match(
    /^\* Highlights first synced by \[\[Readwise\]\] \[\[([0-9]{4}-[0-9]{2}-[0-9]{2})\]\]$/m,
  )

  return match?.[1] ?? null
}

export const inspectManagedPageIntegrityV1 = async (page: PageEntity) => {
  const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
  const rootContent = pageBlocksTree?.[0]?.content ?? ''

  return {
    rootContent,
    signatures: detectLegacyManagedPageRepairSignaturesV1(rootContent),
    legacyFirstSyncedOn: extractLegacyFirstSyncedOnV1(rootContent),
  }
}

export const rebuildManagedPageIfDamagedV1 = async ({
  page,
  expectedPageName,
  logPrefix = '[Readwise Sync]',
}: {
  page: PageEntity
  expectedPageName: string
  logPrefix?: string
}): Promise<{
  page: PageEntity | null
  rebuilt: boolean
  signatures: string[]
  legacyFirstSyncedOn: string | null
}> => {
  const inspection = await inspectManagedPageIntegrityV1(page)

  if (inspection.signatures.length === 0) {
    return {
      page,
      rebuilt: false,
      signatures: [],
      legacyFirstSyncedOn: inspection.legacyFirstSyncedOn,
    }
  }

  const aliases = uniqueStrings([
    expectedPageName,
    ...collectManagedPageAliasesV1(page),
  ])
  let deleted = false

  for (const alias of aliases) {
    try {
      await logseq.Editor.deletePage(alias)
      deleted = true
      break
    } catch {
      // Keep trying aliases until one is accepted by the current runtime.
    }
  }

  if (!deleted) {
    throw new Error(
      `Failed to delete damaged managed page before rebuild: ${aliases.join(', ')}`,
    )
  }

  await delay(500)

  logReadwiseInfo(logPrefix, 'deleted damaged managed page before rebuild', {
    expectedPageName,
    aliases,
    signatures: inspection.signatures,
  })

  return {
    page: null,
    rebuilt: true,
    signatures: inspection.signatures,
    legacyFirstSyncedOn: inspection.legacyFirstSyncedOn,
  }
}
