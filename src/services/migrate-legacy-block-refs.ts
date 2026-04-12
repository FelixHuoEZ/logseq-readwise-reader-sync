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
