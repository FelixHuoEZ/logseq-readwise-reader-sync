import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'

import { logReadwiseInfo } from '../logging'
import {
  computeCompatibleHighlightUuid,
  computeLegacyHighlightUuid,
} from '../uuid-compat'

const READWISE_HIGHLIGHT_URL_IN_BLOCK_PATTERN =
  /\[\[(https:\/\/read\.readwise\.io\/read\/[0-9a-z]+)\]\[View Highlight\]\]/i
const BLOCK_REF_PATTERN = /\(\(([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\)\)/gi
const BLOCK_ID_LINE_PATTERN =
  /^:id:\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i
const REFDOCK_ITEM_ID_PATTERN =
  /^(:refdock-item-id:\s+)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\s*)$/gim

const trimTrailingSeparators = (value: string) => value.replace(/[\\/]+$/, '')

const joinAbsolutePath = (basePath: string, relativePath: string) =>
  `${trimTrailingSeparators(basePath)}/${relativePath}`

const getRuntimeFsPromises = () => {
  const runtimeRequire = (
    window as unknown as { require?: ((id: string) => unknown) | undefined }
  ).require

  if (typeof runtimeRequire !== 'function') {
    throw new Error('window.require is unavailable in the current Logseq runtime.')
  }

  return runtimeRequire('node:fs/promises') as {
    readFile: (path: string, encoding: string) => Promise<string>
    writeFile: (path: string, content: string, encoding: string) => Promise<void>
  }
}

const buildPageFileStem = (pageName: string) => pageName.replaceAll('/', '___')

const buildCurrentPageCandidateRelativePaths = (aliases: string[]) =>
  uniqueStrings(
    aliases.flatMap((alias) => [
      `pages/${buildPageFileStem(alias)}.org`,
      `pages/${buildPageFileStem(alias)}.md`,
      `whiteboards/${alias}.edn`,
    ]),
  )

const readGraphFile = async (
  graphPath: string | null | undefined,
  relativeFilePath: string,
) => {
  try {
    const fsPromises = getRuntimeFsPromises()
    const absolutePath = joinAbsolutePath(graphPath ?? '', relativeFilePath)
    const content = await fsPromises.readFile(absolutePath, 'utf8')
    return {
      content,
      absolutePath,
      readFrom: 'disk' as const,
    }
  } catch {
    const content = await logseq.DB.getFileContent(relativeFilePath)
    if (typeof content !== 'string') {
      throw new Error(`Failed to read graph file ${relativeFilePath}`)
    }
    return {
      content,
      absolutePath: null,
      readFrom: 'db' as const,
    }
  }
}

const writeGraphFile = async ({
  graphPath,
  relativeFilePath,
  content,
}: {
  graphPath: string | null | undefined
  relativeFilePath: string
  content: string
}) => {
  try {
    await logseq.DB.setFileContent(relativeFilePath, content)
  } catch {
    const fsPromises = getRuntimeFsPromises()
    await fsPromises.writeFile(
      joinAbsolutePath(graphPath ?? '', relativeFilePath),
      content,
      'utf8',
    )
  }

  const confirmed = await readGraphFile(graphPath, relativeFilePath)
  if (confirmed.content !== content) {
    throw new Error(`Failed to persist graph file ${relativeFilePath}`)
  }
}

const uniqueStrings = (values: string[]) =>
  values.filter(
    (value, index, array) => value.length > 0 && array.indexOf(value) === index,
  )

const collectPageAliases = (page: PageEntity): string[] =>
  uniqueStrings([
    typeof page.originalName === 'string' ? page.originalName : '',
    typeof page.title === 'string' ? page.title : '',
    typeof page.name === 'string' ? page.name : '',
  ])

const flattenPageBlocksTreeContent = (
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

const collectContentBlocks = (
  blocks: Array<BlockEntity | [unknown, string]>,
): Array<BlockEntity> => {
  const collected: BlockEntity[] = []

  const visit = (node: BlockEntity | [unknown, string]) => {
    if (Array.isArray(node)) return

    if (typeof node.content === 'string') {
      collected.push(node)
    }

    for (const child of node.children ?? []) {
      visit(child as BlockEntity | [unknown, string])
    }
  }

  for (const block of blocks) {
    visit(block)
  }

  return collected
}

const extractLegacyToCurrentUuidPairsFromContent = (content: string) => {
  const pairs = new Map<string, string>()
  const lines = content.split('\n')

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

      const idMatch = nextLine.match(BLOCK_ID_LINE_PATTERN)
      if (idMatch) {
        currentUuid = idMatch[1] ?? null
        break
      }
    }

    if (!currentUuid) {
      continue
    }

    const legacyUuid = computeLegacyHighlightUuid(highlightUrl)
    const canonicalUuid = computeCompatibleHighlightUuid(highlightUrl)

    if (legacyUuid !== canonicalUuid) {
      pairs.set(legacyUuid, canonicalUuid)
    }

    if (currentUuid !== canonicalUuid) {
      pairs.set(currentUuid, canonicalUuid)
    }
  }

  return pairs
}

export interface LegacyBlockRefMappingSummaryV1 {
  managedPagesScanned: number
  mappedLegacyUuids: number
}

export const buildLegacyBlockRefMappingV1 = async ({
  namespaceRoot,
  onProgress,
}: {
  namespaceRoot: string
  onProgress?: (progress: {
    total: number
    completed: number
    pageName: string
  }) => void
}): Promise<{
  mapping: Map<string, string>
  summary: LegacyBlockRefMappingSummaryV1
}> => {
  const allPages = (await logseq.Editor.getAllPages()) ?? []
  const managedPages = allPages.filter((page) =>
    collectPageAliases(page as PageEntity).some((alias) =>
      alias === namespaceRoot || alias.startsWith(`${namespaceRoot}/`),
    ),
  ) as PageEntity[]
  const mapping = new Map<string, string>()

  for (let index = 0; index < managedPages.length; index += 1) {
    const page = managedPages[index]!
    const pageName =
      collectPageAliases(page)[0] ?? page.originalName ?? page.name ?? page.title ?? ''

    onProgress?.({
      total: managedPages.length,
      completed: index + 1,
      pageName,
    })

    const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
    const content = flattenPageBlocksTreeContent(pageBlocksTree ?? [])
    const pairs = extractLegacyToCurrentUuidPairsFromContent(content)

    for (const [legacyUuid, currentUuid] of pairs) {
      mapping.set(legacyUuid, currentUuid)
    }
  }

  return {
    mapping,
    summary: {
      managedPagesScanned: managedPages.length,
      mappedLegacyUuids: mapping.size,
    },
  }
}

export interface MigrateLegacyBlockRefsSummaryV1 {
  graphPagesScanned: number
  graphPagesUpdated: number
  blocksUpdated: number
  refsRewritten: number
}

export interface LegacyBlockRefPreviewEntryV1 {
  pageName: string
  blockUuid: string
  refs: Array<{
    from: string
    to: string
  }>
}

export interface PreviewLegacyBlockRefsSummaryV1 {
  graphPagesScanned: number
  graphPagesAffected: number
  blocksAffected: number
  refsPlanned: number
}

export interface CurrentPageLegacyIdRewriteEntryV1 {
  lineNumber: number
  kind: 'block-ref' | 'refdock-item-id'
  from: string
  to: string
  beforeLine: string
  afterLine: string
}

export interface PreviewCurrentPageLegacyIdsSummaryV1 {
  pageName: string
  relativeFilePath: string
  fileKind: 'page' | 'whiteboard'
  rewritesPlanned: number
}

export interface MigrateCurrentPageLegacyIdsSummaryV1 {
  pageName: string
  relativeFilePath: string
  fileKind: 'page' | 'whiteboard'
  rewritesApplied: number
}

interface CurrentPageLegacyIdMigrationTargetV1 {
  pageName: string
  relativeFilePath: string
  fileKind: 'page' | 'whiteboard'
  content: string
}

const escapeDatalogString = (value: string) =>
  value.replaceAll('\\', '\\\\').replaceAll('"', '\\"')

const resolvePageFilePathViaQuery = async (
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
      const byName = await logseq.DB.q<string>(
        `[:find ?path . :where [?p :block/name "${normalizedAlias}"] [?p :block/file ?f] [?f :file/path ?path]]`,
      )
      if (typeof byName === 'string' && byName.length > 0) {
        return byName
      }
    } catch {
      // Continue trying more aliases.
    }
  }

  return null
}

const resolveCurrentPageLegacyIdMigrationTargetV1 =
  async (): Promise<CurrentPageLegacyIdMigrationTargetV1> => {
    const currentPage = (await logseq.Editor.getCurrentPage()) as PageEntity | null
    if (!currentPage) {
      throw new Error('No current page is open.')
    }

    const aliases = collectPageAliases(currentPage)
    const pageName = aliases[0] ?? null
    if (!pageName) {
      throw new Error('Failed to resolve the current page name.')
    }

    const graph = await logseq.App.getCurrentGraph()
    const directRelativePath =
      currentPage.file &&
      typeof currentPage.file === 'object' &&
      'path' in currentPage.file &&
      typeof currentPage.file.path === 'string' &&
      currentPage.file.path.length > 0
        ? currentPage.file.path
        : null

    const candidatePaths = uniqueStrings([
      directRelativePath ?? '',
      ...buildCurrentPageCandidateRelativePaths(aliases),
    ])

    for (const relativeFilePath of candidatePaths) {
      try {
        const resolved = await readGraphFile(graph?.path ?? null, relativeFilePath)
        return {
          pageName,
          relativeFilePath,
          fileKind: relativeFilePath.startsWith('whiteboards/')
            ? 'whiteboard'
            : 'page',
          content: resolved.content,
        }
      } catch {
        // Continue trying more candidates.
      }
    }

    const assets = await logseq.Assets.listFilesOfCurrentGraph(['org', 'md', 'edn'])
    for (const candidatePath of candidatePaths) {
      const asset = assets.find((entry) => entry.path === candidatePath)
      if (!asset) continue

      const resolved = await readGraphFile(graph?.path ?? null, asset.path)
      return {
        pageName,
        relativeFilePath: asset.path,
        fileKind: asset.path.startsWith('whiteboards/') ? 'whiteboard' : 'page',
        content: resolved.content,
      }
    }

    const queryRelativePath = await resolvePageFilePathViaQuery(currentPage, aliases)
    if (queryRelativePath) {
      const resolved = await readGraphFile(graph?.path ?? null, queryRelativePath)
      return {
        pageName,
        relativeFilePath: queryRelativePath,
        fileKind: queryRelativePath.startsWith('whiteboards/') ? 'whiteboard' : 'page',
        content: resolved.content,
      }
    }

    throw new Error(`Failed to resolve the current page file for "${pageName}".`)
  }

const countLineNumberAtIndex = (content: string, index: number) =>
  content.slice(0, index).split('\n').length

const getLineAtIndex = (content: string, index: number) => {
  const before = content.slice(0, index)
  const lineStart = before.lastIndexOf('\n') + 1
  const nextNewline = content.indexOf('\n', index)
  const lineEnd = nextNewline === -1 ? content.length : nextNewline
  return content.slice(lineStart, lineEnd)
}

const collectCurrentPageLegacyIdRewrites = (
  content: string,
  mapping: Map<string, string>,
): CurrentPageLegacyIdRewriteEntryV1[] => {
  const rewrites: CurrentPageLegacyIdRewriteEntryV1[] = []

  for (const match of content.matchAll(BLOCK_REF_PATTERN)) {
    const from = match[1] ?? ''
    const to = mapping.get(from)
    const index = match.index ?? -1
    if (!to || to === from || index < 0) continue

    const beforeLine = getLineAtIndex(content, index)
    rewrites.push({
      lineNumber: countLineNumberAtIndex(content, index),
      kind: 'block-ref',
      from,
      to,
      beforeLine,
      afterLine: beforeLine.replaceAll(`((${from}))`, `((${to}))`),
    })
  }

  for (const match of content.matchAll(REFDOCK_ITEM_ID_PATTERN)) {
    const prefix = match[1] ?? ''
    const from = match[2] ?? ''
    const suffix = match[3] ?? ''
    const to = mapping.get(from)
    const index = match.index ?? -1
    if (!to || to === from || index < 0) continue

    const beforeLine = getLineAtIndex(content, index)
    rewrites.push({
      lineNumber: countLineNumberAtIndex(content, index),
      kind: 'refdock-item-id',
      from,
      to,
      beforeLine,
      afterLine: `${prefix}${to}${suffix}`,
    })
  }

  return rewrites.sort((left, right) => left.lineNumber - right.lineNumber)
}

const rewriteCurrentPageLegacyIdsInContent = (
  content: string,
  mapping: Map<string, string>,
) => {
  let rewritesApplied = 0
  let nextContent = content.replace(BLOCK_REF_PATTERN, (match, uuid: string) => {
    const nextUuid = mapping.get(uuid)
    if (!nextUuid || nextUuid === uuid) {
      return match
    }

    rewritesApplied += 1
    return `((${nextUuid}))`
  })

  nextContent = nextContent.replace(
    REFDOCK_ITEM_ID_PATTERN,
    (match, prefix: string, uuid: string, suffix: string) => {
      const nextUuid = mapping.get(uuid)
      if (!nextUuid || nextUuid === uuid) {
        return match
      }

      rewritesApplied += 1
      return `${prefix}${nextUuid}${suffix}`
    },
  )

  return {
    nextContent,
    rewritesApplied,
  }
}

export const previewLegacyBlockRefsV1 = async ({
  mapping,
  onProgress,
}: {
  mapping: Map<string, string>
  onProgress?: (progress: {
    total: number
    completed: number
    pageName: string
    affectedPages: number
    refsPlanned: number
  }) => void
}): Promise<{
  entries: LegacyBlockRefPreviewEntryV1[]
  summary: PreviewLegacyBlockRefsSummaryV1
}> => {
  if (mapping.size === 0) {
    return {
      entries: [],
      summary: {
        graphPagesScanned: 0,
        graphPagesAffected: 0,
        blocksAffected: 0,
        refsPlanned: 0,
      },
    }
  }

  const allPages = ((await logseq.Editor.getAllPages()) ?? []) as PageEntity[]
  const entries: LegacyBlockRefPreviewEntryV1[] = []
  let graphPagesAffected = 0
  let refsPlanned = 0

  for (let index = 0; index < allPages.length; index += 1) {
    const page = allPages[index]!
    const pageName =
      collectPageAliases(page)[0] ?? page.originalName ?? page.name ?? page.title ?? ''
    const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
    const contentBlocks = collectContentBlocks(pageBlocksTree ?? [])
    let pageAffected = false

    for (const block of contentBlocks) {
      const originalContent = block.content ?? ''
      const refs = [...originalContent.matchAll(BLOCK_REF_PATTERN)]
        .map((match) => match[1] ?? '')
        .map((uuid) => {
          const nextUuid = mapping.get(uuid)
          return nextUuid && nextUuid !== uuid
            ? {
                from: uuid,
                to: nextUuid,
              }
            : null
        })
        .filter(
          (pair): pair is { from: string; to: string } =>
            pair != null,
        )

      if (refs.length === 0) {
        continue
      }

      entries.push({
        pageName,
        blockUuid: block.uuid,
        refs,
      })
      pageAffected = true
      refsPlanned += refs.length
    }

    if (pageAffected) {
      graphPagesAffected += 1
    }

    onProgress?.({
      total: allPages.length,
      completed: index + 1,
      pageName,
      affectedPages: graphPagesAffected,
      refsPlanned,
    })
  }

  return {
    entries,
    summary: {
      graphPagesScanned: allPages.length,
      graphPagesAffected,
      blocksAffected: entries.length,
      refsPlanned,
    },
  }
}

export const migrateLegacyBlockRefsV1 = async ({
  mapping,
  logPrefix = '[Readwise Sync]',
  onProgress,
}: {
  mapping: Map<string, string>
  logPrefix?: string
  onProgress?: (progress: {
    total: number
    completed: number
    pageName: string
    updatedPages: number
    refsRewritten: number
  }) => void
}): Promise<MigrateLegacyBlockRefsSummaryV1> => {
  if (mapping.size === 0) {
    return {
      graphPagesScanned: 0,
      graphPagesUpdated: 0,
      blocksUpdated: 0,
      refsRewritten: 0,
    }
  }

  const allPages = ((await logseq.Editor.getAllPages()) ?? []) as PageEntity[]
  let graphPagesUpdated = 0
  let blocksUpdated = 0
  let refsRewritten = 0

  for (let index = 0; index < allPages.length; index += 1) {
    const page = allPages[index]!
    const pageName =
      collectPageAliases(page)[0] ?? page.originalName ?? page.name ?? page.title ?? ''
    const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
    const contentBlocks = collectContentBlocks(pageBlocksTree ?? [])
    let pageUpdated = false

    for (const block of contentBlocks) {
      const originalContent = block.content ?? ''
      let replacedInBlock = 0
      const nextContent = originalContent.replace(
        BLOCK_REF_PATTERN,
        (match, uuid: string) => {
          const nextUuid = mapping.get(uuid)
          if (!nextUuid || nextUuid === uuid) {
            return match
          }

          replacedInBlock += 1
          return `((${nextUuid}))`
        },
      )

      if (replacedInBlock === 0 || nextContent === originalContent) {
        continue
      }

      await logseq.Editor.updateBlock(block.uuid, nextContent)
      pageUpdated = true
      blocksUpdated += 1
      refsRewritten += replacedInBlock
    }

    if (pageUpdated) {
      graphPagesUpdated += 1
    }

    onProgress?.({
      total: allPages.length,
      completed: index + 1,
      pageName,
      updatedPages: graphPagesUpdated,
      refsRewritten,
    })
  }

  logReadwiseInfo(logPrefix, 'migrated legacy block refs', {
    mappingSize: mapping.size,
    graphPagesScanned: allPages.length,
    graphPagesUpdated,
    blocksUpdated,
    refsRewritten,
  })

  return {
    graphPagesScanned: allPages.length,
    graphPagesUpdated,
    blocksUpdated,
    refsRewritten,
  }
}

export const previewCurrentPageLegacyIdsV1 = async ({
  mapping,
}: {
  mapping: Map<string, string>
}): Promise<{
  target: PreviewCurrentPageLegacyIdsSummaryV1
  rewrites: CurrentPageLegacyIdRewriteEntryV1[]
}> => {
  const target = await resolveCurrentPageLegacyIdMigrationTargetV1()
  const rewrites = collectCurrentPageLegacyIdRewrites(target.content, mapping)

  return {
    target: {
      pageName: target.pageName,
      relativeFilePath: target.relativeFilePath,
      fileKind: target.fileKind,
      rewritesPlanned: rewrites.length,
    },
    rewrites,
  }
}

export const migrateCurrentPageLegacyIdsV1 = async ({
  mapping,
  expectedRelativeFilePath,
  logPrefix = '[Readwise Sync]',
}: {
  mapping: Map<string, string>
  expectedRelativeFilePath?: string | null
  logPrefix?: string
}): Promise<MigrateCurrentPageLegacyIdsSummaryV1> => {
  const target = await resolveCurrentPageLegacyIdMigrationTargetV1()
  if (
    expectedRelativeFilePath &&
    target.relativeFilePath !== expectedRelativeFilePath
  ) {
    throw new Error(
      `Current page file changed from ${expectedRelativeFilePath} to ${target.relativeFilePath}. Re-run preview before applying.`,
    )
  }

  const { nextContent, rewritesApplied } = rewriteCurrentPageLegacyIdsInContent(
    target.content,
    mapping,
  )

  if (rewritesApplied > 0 && nextContent !== target.content) {
    const graph = await logseq.App.getCurrentGraph()
    await writeGraphFile({
      graphPath: graph?.path ?? null,
      relativeFilePath: target.relativeFilePath,
      content: nextContent,
    })
  }

  logReadwiseInfo(logPrefix, 'migrated current-page legacy ids', {
    pageName: target.pageName,
    relativeFilePath: target.relativeFilePath,
    fileKind: target.fileKind,
    rewritesApplied,
  })

  return {
    pageName: target.pageName,
    relativeFilePath: target.relativeFilePath,
    fileKind: target.fileKind,
    rewritesApplied,
  }
}
