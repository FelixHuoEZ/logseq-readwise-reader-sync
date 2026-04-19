import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'

import { logReadwiseInfo } from '../logging'
import { computeCompatibleHighlightUuid } from '../uuid-compat'

const uniqueStrings = (values: string[]) =>
  values.filter(
    (value, index, array) => value.length > 0 && array.indexOf(value) === index,
  )

const ACCEPTED_BLOCK_UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
const READWISE_HIGHLIGHT_URL_IN_BLOCK_PATTERN =
  /\[\[(https:\/\/read\.readwise\.io\/read\/[0-9a-z]+)\]\[View Highlight\]\]/i
const READWISE_HIGHLIGHT_URL_GLOBAL_PATTERN =
  /\[\[(https:\/\/read\.readwise\.io\/read\/[0-9a-z]+)\]\[View Highlight\]\]/gi
const READWISE_SYNC_HEADER_PATTERN =
  /^\* Highlights (?:first synced|refreshed) by \[\[Readwise\]\]/m
const READWISE_HIGHLIGHT_BLOCK_START_PATTERN = /^\*\* /m

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

const trimTrailingSeparators = (value: string) => value.replace(/[\\/]+$/, '')

const joinAbsolutePath = (basePath: string, relativePath: string) =>
  `${trimTrailingSeparators(basePath)}/${relativePath}`

const escapeDatalogString = (value: string) =>
  value.replaceAll('\\', '\\\\').replaceAll('"', '\\"')

const tryReadGraphFileFromDisk = async (
  graphPath: string | null | undefined,
  relativePath: string,
) => {
  if (typeof graphPath !== 'string' || graphPath.length === 0) {
    throw new Error('Current graph path is unavailable.')
  }

  const runtimeRequire = (
    window as unknown as { require?: ((id: string) => unknown) | undefined }
  ).require

  if (typeof runtimeRequire !== 'function') {
    throw new Error('window.require is unavailable in the current Logseq runtime.')
  }

  const fsPromises = runtimeRequire('node:fs/promises') as {
    readFile: (path: string, encoding: string) => Promise<string>
  }
  const absolutePath = joinAbsolutePath(graphPath, relativePath)
  const content = await fsPromises.readFile(absolutePath, 'utf8')

  return {
    absolutePath,
    content,
  }
}

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

export const resolveManagedPageFilePathV1 = async (
  page: PageEntity,
): Promise<string | null> => {
  const aliases = collectManagedPageAliasesV1(page)
  return (
    (page.file &&
    typeof page.file === 'object' &&
    'path' in page.file &&
    typeof page.file.path === 'string' &&
    page.file.path.length > 0
      ? page.file.path
      : null) ?? (await resolveManagedPageFilePathViaQueryV1(page, aliases))
  )
}

const resolveManagedPageFilePathViaQueryV1 = async (
  page: PageEntity,
  aliases: string[],
): Promise<string | null> => {
  if (typeof page.id === 'number') {
    try {
      const byId = await logseq.DB.q<string>(
        `[:find ?path . :where [${page.id} :block/file ?f] [?f :file/path ?path]]`,
      )
      if (typeof byId === 'string' && byId.length > 0) {
        return byId
      }
    } catch {
      // Fall through to alias-based lookup.
    }
  }

  for (const alias of aliases) {
    try {
      const normalizedAlias = escapeDatalogString(alias.toLowerCase())
      const byAlias = await logseq.DB.q<string>(
        `[:find ?path . :where [?p :block/name "${normalizedAlias}"] [?p :block/file ?f] [?f :file/path ?path]]`,
      )
      if (typeof byAlias === 'string' && byAlias.length > 0) {
        return byAlias
      }
    } catch {
      // Continue trying aliases.
    }
  }

  return null
}

const loadManagedPageSourceContentV1 = async (
  page: PageEntity,
): Promise<string | null> => {
  const relativeFilePath = await resolveManagedPageFilePathV1(page)

  if (!relativeFilePath) return null

  try {
    const dbContent = await logseq.DB.getFileContent(relativeFilePath)
    if (typeof dbContent === 'string' && dbContent.length > 0) {
      return dbContent
    }
  } catch {
    // Fall through to disk-read fallback.
  }

  try {
    const graph = await logseq.App.getCurrentGraph()
    const diskRead = await tryReadGraphFileFromDisk(graph?.path ?? null, relativeFilePath)
    return diskRead.content
  } catch {
    return null
  }
}

const extractPageLevelPreludeV1 = (rootContent: string) => {
  const lines = rootContent.split('\n')
  const prelude: string[] = []

  for (const line of lines) {
    if (
      READWISE_SYNC_HEADER_PATTERN.test(line) ||
      READWISE_HIGHLIGHT_BLOCK_START_PATTERN.test(line)
    ) {
      break
    }
    prelude.push(line)
  }

  return prelude.join('\n')
}

const inspectFirstPropertyDrawerV1 = (pagePrelude: string) => {
  const keyCounts = new Map<string, number>()
  let hasHighlightTagsProperty = false
  let inProperties = false

  for (const line of pagePrelude.split('\n')) {
    if (!inProperties) {
      if (line === ':PROPERTIES:') {
        inProperties = true
      }
      continue
    }

    if (line === ':END:') {
      break
    }

    if (/^:tags:\s+\[\[ReadwiseHighlights\]\]/.test(line)) {
      hasHighlightTagsProperty = true
    }

    const keyMatch = line.match(/^:([^:]+):/)
    const normalizedKey = (keyMatch?.[1] ?? '').trim().toLowerCase()
    if (normalizedKey.length === 0) continue

    keyCounts.set(normalizedKey, (keyCounts.get(normalizedKey) ?? 0) + 1)
  }

  return {
    keyCounts,
    hasHighlightTagsProperty,
  }
}

export const detectLegacyManagedPageRepairSignaturesV1 = (rootContent: string) => {
  const signatures: string[] = []
  const pagePrelude = extractPageLevelPreludeV1(rootContent)
  const propertiesBlocks = (pagePrelude.match(/^:PROPERTIES:$/gm) ?? []).length
  const noteBlocks = (pagePrelude.match(/^#\+BEGIN_NOTE$/gm) ?? []).length
  const firstPropertyDrawer = inspectFirstPropertyDrawerV1(pagePrelude)
  const syncHeaders = (
    rootContent.match(/^\* Highlights (?:first synced|refreshed) by \[\[Readwise\]\]/gm) ??
    []
  ).length
  const invalidLegacyBlockIds = [
    ...rootContent.matchAll(
      /^:id:\s+([0-9a-f]{8}-[0-9a-f]{4}-([0-9a-f])[0-9a-f]{3}-([0-9a-f])[0-9a-f]{3}-[0-9a-f]{12})$/gim,
    ),
  ]
    .map((match) => ({
      uuid: match[1] ?? '',
      versionChar: (match[2] ?? '').toLowerCase(),
      variantChar: (match[3] ?? '').toLowerCase(),
    }))
    .filter(
      ({ uuid, versionChar, variantChar }) =>
        uuid.length > 0 &&
        (!/^[1-5]$/.test(versionChar) || !/^[89ab]$/.test(variantChar)),
    )

  if (propertiesBlocks > 1) {
    signatures.push('duplicate properties block')
  }

  if (noteBlocks > 1) {
    signatures.push('duplicate note block')
  }

  const createdProperties = firstPropertyDrawer.keyCounts.get('created') ?? 0
  const idProperties = firstPropertyDrawer.keyCounts.get('id') ?? 0
  const tagProperties = firstPropertyDrawer.keyCounts.get('tags') ?? 0
  if (
    createdProperties > 0 &&
    (idProperties > 1 ||
      tagProperties > 1 ||
      firstPropertyDrawer.hasHighlightTagsProperty)
  ) {
    signatures.push('highlight properties leaked into page properties')
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

    const highlightMatches =
      typeof node.content === 'string'
        ? [...node.content.matchAll(READWISE_HIGHLIGHT_URL_GLOBAL_PATTERN)]
        : []
    const highlightUrl =
      highlightMatches.length === 1 &&
      typeof node.content === 'string' &&
      !node.content.includes('#+BEGIN_NOTE') &&
      !node.content.includes(':PROPERTIES:') &&
      !node.content.startsWith(':PROPERTIES:') &&
      !/\n\*\* /.test(node.content)
        ? (highlightMatches[0]?.[1] ?? null)
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

const collectLegacyDriftedBlockIdsFromSerializedContentV1 = (
  rootContent: string,
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
  const lines = rootContent.split('\n')

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? ''
    const highlightUrl =
      line.match(READWISE_HIGHLIGHT_URL_IN_BLOCK_PATTERN)?.[1] ?? null

    if (!highlightUrl) {
      continue
    }

    let currentUuid: string | null = null

    for (let nextIndex = index + 1; nextIndex < lines.length; nextIndex += 1) {
      const nextLine = lines[nextIndex] ?? ''

      if (/^\*\* /.test(nextLine)) {
        break
      }

      const idMatch = nextLine.match(
        /^:id:\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i,
      )

      if (idMatch) {
        currentUuid = idMatch[1] ?? null
        break
      }
    }

    if (!currentUuid) {
      continue
    }

    const expectedUuid = computeCompatibleHighlightUuid(highlightUrl)

    if (currentUuid !== expectedUuid) {
      mismatches.set(`${highlightUrl}:${currentUuid}`, {
        currentUuid,
        expectedUuid,
        highlightUrl,
      })
    }
  }

  return [...mismatches.values()]
}

const collectSearchableTreeContentV1 = (
  blocks: Array<BlockEntity | [unknown, string]>,
) => {
  const parts: string[] = []

  const visit = (node: BlockEntity | [unknown, string]) => {
    if (Array.isArray(node)) return

    if (typeof node.content === 'string' && node.content.length > 0) {
      parts.push(node.content)
    }

    for (const child of node.children ?? []) {
      visit(child as BlockEntity | [unknown, string])
    }
  }

  for (const block of blocks) {
    visit(block)
  }

  return parts.join('\n')
}

const extractLegacyFirstSyncedOnV1 = (rootContent: string): string | null => {
  const match = rootContent.match(
    /^\* Highlights first synced by \[\[Readwise\]\] \[\[([0-9]{4}-[0-9]{2}-[0-9]{2})\]\]$/m,
  )

  return match?.[1] ?? null
}

export const inspectManagedPageIntegrityV1 = async (page: PageEntity) => {
  const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
  const treeContent = collectSearchableTreeContentV1(pageBlocksTree ?? [])
  const sourceContent =
    (await loadManagedPageSourceContentV1(page)) ??
    pageBlocksTree?.[0]?.content ??
    treeContent
  const searchableContent =
    sourceContent.length >= treeContent.length ? sourceContent : treeContent
  const invalidTreeBlockIds = collectLegacyInvalidBlockIdsFromTreeV1(
    pageBlocksTree ?? [],
  )
  const driftedTreeBlockIds = uniqueStrings(
    [
      ...collectLegacyDriftedBlockIdsFromTreeV1(pageBlocksTree ?? []),
      ...collectLegacyDriftedBlockIdsFromSerializedContentV1(sourceContent),
    ].map(
      (entry) =>
        `${entry.currentUuid}::${entry.expectedUuid}::${entry.highlightUrl}`,
    ),
  ).map((encoded) => {
    const [currentUuid = '', expectedUuid = '', highlightUrl = ''] =
      encoded.split('::')

    return {
      currentUuid,
      expectedUuid,
      highlightUrl,
    }
  })
  const signatures = detectLegacyManagedPageRepairSignaturesV1(sourceContent)

  if (invalidTreeBlockIds.length > 0) {
    signatures.push('legacy invalid block id')
  }

  if (driftedTreeBlockIds.length > 0) {
    signatures.push('legacy drifted block id')
  }

  return {
    rootContent: sourceContent,
    sourceContent,
    searchableContent,
    signatures: uniqueStrings(signatures),
    invalidTreeBlockIds,
    driftedTreeBlockIds,
    legacyFirstSyncedOn: extractLegacyFirstSyncedOnV1(sourceContent),
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
