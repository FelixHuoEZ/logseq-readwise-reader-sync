import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'

import { logReadwiseInfo } from '../logging'
import { computeCompatibleHighlightUuid } from '../uuid-compat'

const uniqueStrings = (values: string[]) =>
  values.filter(
    (value, index, array) => value.length > 0 && array.indexOf(value) === index,
  )

const ACCEPTED_BLOCK_UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const READWISE_HIGHLIGHT_URL_IN_BLOCK_PATTERN =
  /\[\[(https:\/\/read\.readwise\.io\/read\/[0-9a-z]+)\]\[View Highlight\]\]/i

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

const waitForDeletedPageAliasesV1 = async (aliases: string[]) => {
  const deadline = Date.now() + 4000

  while (Date.now() < deadline) {
    const survivors = (
      await Promise.all(
        aliases.map(async (alias) => {
          const page = await logseq.Editor.getPage(alias)
          return page ? alias : null
        }),
      )
    ).filter((alias): alias is string => typeof alias === 'string')

    if (survivors.length === 0) {
      return
    }

    await delay(250)
  }

  await delay(500)
}

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
  const invalidLegacyBlockIds = [
    ...rootContent.matchAll(
      /^:id:\s+([0-9a-f]{8}-[0-9a-f]{4}-([0-9a-f])[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12})$/gim,
    ),
  ]
    .map((match) => ({
      uuid: match[1] ?? '',
      versionChar: (match[2] ?? '').toLowerCase(),
    }))
    .filter(
      ({ uuid, versionChar }) =>
        uuid.length > 0 && !/^[1-5]$/.test(versionChar),
    )

  if (propertiesBlocks > 1) {
    signatures.push('duplicate properties block')
  }

  if (noteBlocks > 1) {
    signatures.push('duplicate note block')
  }

  if (syncHeaders > 1) {
    signatures.push('duplicate sync header')
  }

  if (invalidLegacyBlockIds.length > 0) {
    signatures.push('legacy invalid block id')
  }

  return signatures
}

const collectLegacyInvalidBlockIdsFromTreeV1 = (
  blocks: Array<BlockEntity | [unknown, string]>,
): string[] => {
  const invalid: string[] = []

  const visit = (node: BlockEntity | [unknown, string]) => {
    if (Array.isArray(node)) {
      const uuid = typeof node[1] === 'string' ? node[1] : ''
      if (uuid.length > 0 && !ACCEPTED_BLOCK_UUID_PATTERN.test(uuid)) {
        invalid.push(uuid)
      }
      return
    }

    if (typeof node.uuid === 'string' && !ACCEPTED_BLOCK_UUID_PATTERN.test(node.uuid)) {
      invalid.push(node.uuid)
    }

    for (const child of node.children ?? []) {
      visit(child as BlockEntity | [unknown, string])
    }
  }

  for (const block of blocks) {
    visit(block)
  }

  return uniqueStrings(invalid)
}

const collectLegacyDriftedBlockIdsFromTreeV1 = (
  blocks: Array<BlockEntity | [unknown, string]>,
): Array<{
  currentUuid: string
  expectedUuid: string
  highlightUrl: string
}> => {
  const mismatches = new Map<
    string,
    {
      currentUuid: string
      expectedUuid: string
      highlightUrl: string
    }
  >()

  const visit = (node: BlockEntity | [unknown, string]) => {
    if (Array.isArray(node)) return

    const highlightUrl =
      typeof node.content === 'string'
        ? (node.content.match(READWISE_HIGHLIGHT_URL_IN_BLOCK_PATTERN)?.[1] ?? null)
        : null

    if (
      highlightUrl &&
      typeof node.uuid === 'string' &&
      node.uuid.length > 0
    ) {
      const expectedUuid = computeCompatibleHighlightUuid(highlightUrl)

      if (node.uuid !== expectedUuid) {
        mismatches.set(node.uuid, {
          currentUuid: node.uuid,
          expectedUuid,
          highlightUrl,
        })
      }
    }

    for (const child of node.children ?? []) {
      visit(child as BlockEntity | [unknown, string])
    }
  }

  for (const block of blocks) {
    visit(block)
  }

  return [...mismatches.values()]
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
  const invalidTreeBlockIds = collectLegacyInvalidBlockIdsFromTreeV1(
    pageBlocksTree ?? [],
  )
  const driftedTreeBlockIds = collectLegacyDriftedBlockIdsFromTreeV1(
    pageBlocksTree ?? [],
  )
  const signatures = detectLegacyManagedPageRepairSignaturesV1(rootContent)

  if (invalidTreeBlockIds.length > 0) {
    signatures.push('legacy invalid block id')
  }

  if (driftedTreeBlockIds.length > 0) {
    signatures.push('legacy drifted block id')
  }

  return {
    rootContent,
    signatures: uniqueStrings(signatures),
    invalidTreeBlockIds,
    driftedTreeBlockIds,
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
  invalidTreeBlockIds: string[]
  driftedTreeBlockIds: Array<{
    currentUuid: string
    expectedUuid: string
    highlightUrl: string
  }>
  legacyFirstSyncedOn: string | null
}> => {
  const inspection = await inspectManagedPageIntegrityV1(page)

  if (inspection.signatures.length === 0) {
    return {
      page,
      rebuilt: false,
      signatures: [],
      invalidTreeBlockIds: [],
      driftedTreeBlockIds: [],
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

  await waitForDeletedPageAliasesV1(aliases)

  logReadwiseInfo(logPrefix, 'deleted damaged managed page before rebuild', {
    expectedPageName,
    aliases,
    signatures: inspection.signatures,
    invalidTreeBlockIds: inspection.invalidTreeBlockIds,
    driftedTreeBlockIds: inspection.driftedTreeBlockIds,
  })

  return {
    page: null,
    rebuilt: true,
    signatures: inspection.signatures,
    invalidTreeBlockIds: inspection.invalidTreeBlockIds,
    driftedTreeBlockIds: inspection.driftedTreeBlockIds,
    legacyFirstSyncedOn: inspection.legacyFirstSyncedOn,
  }
}
