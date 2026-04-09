import type { IBatchBlock, PageEntity } from '@logseq/libs/dist/LSPlugin'
import { format } from 'date-fns'

import { normalizeBookExport } from '../normalizer'
import { buildPageRenderContext, renderPage } from '../renderer'
import type { ExportedBook } from '../types'
import { upsertBookProperties } from './upsert-book-props'

const buildDebugPageName = (
  bookTitle: string,
  namespacePrefix: string,
): string => `${namespacePrefix}/${bookTitle}`

const ensureDebugPage = async (
  pageName: string,
): Promise<{ page: PageEntity; created: boolean } | null> => {
  const existingPage = await logseq.Editor.getPage(pageName)
  if (existingPage) {
    return {
      page: existingPage,
      created: false,
    }
  }

  const createdPage = await logseq.Editor.createPage(
    pageName,
    {},
    {
      redirect: false,
      createFirstBlock: false,
      format: 'org',
    },
  )

  if (!createdPage) return null

  return {
    page: createdPage,
    created: true,
  }
}

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

const toBatchChildren = (
  children: Array<{ text: string; children?: Array<{ text: string }> }> | undefined,
): IBatchBlock[] | undefined => {
  if (!children || children.length === 0) return undefined

  return children.map((child) => ({
    content: child.text,
    children: toBatchChildren(child.children),
  }))
}

const buildRenderedBatchBlocks = (
  renderedPage: ReturnType<typeof renderPage>,
): IBatchBlock[] => {
  const blocks: IBatchBlock[] = []

  if (renderedPage.emitResult.metadataText.length > 0) {
    blocks.push({ content: renderedPage.emitResult.metadataText })
  }

  const highlightChildren = renderedPage.emitResult.highlightBlocks.map((block) => ({
    content: block.text,
    children: toBatchChildren(block.children),
  }))

  if (renderedPage.emitResult.syncHeaderText) {
    blocks.push({
      content: renderedPage.emitResult.syncHeaderText,
      children: highlightChildren.length > 0 ? highlightChildren : undefined,
    })
  } else {
    blocks.push(...highlightChildren)
  }

  return blocks
}

const createDebugHighlightUuid = (): string => {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }

  const seed = `${Date.now()}-${Math.random()}`
  const hex = Array.from(seed)
    .map((char) => char.charCodeAt(0).toString(16))
    .join('')
    .padEnd(32, '0')
    .slice(0, 32)

  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`
}

const removeAllTopLevelBlocks = async (pageUuid: string): Promise<number> => {
  const existingBlocks = await logseq.Editor.getPageBlocksTree(pageUuid)
  if (!existingBlocks || existingBlocks.length === 0) return 0

  for (const block of [...existingBlocks].reverse()) {
    await logseq.Editor.removeBlock(block.uuid)
  }

  return existingBlocks.length
}

const applyRenderedBlocksToDebugPage = async (
  page: PageEntity,
  batchBlocks: IBatchBlock[],
) => {
  if (batchBlocks.length === 0) return

  const removedBlocks = await removeAllTopLevelBlocks(page.uuid)
  await delay(500)

  const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
  const [firstBatchBlock, ...restBatchBlocks] = batchBlocks
  if (!firstBatchBlock) return

  let rootBlockUuid: string | null = null

  if (pageBlocksTree !== null && pageBlocksTree.length === 0) {
    const firstBlock = await logseq.Editor.insertBlock(
      page.uuid,
      firstBatchBlock.content,
      {
        before: false,
        isPageBlock: true,
        properties: firstBatchBlock.properties ?? {},
      } as never,
    )

    if (!firstBlock) {
      throw new Error(`Failed to insert first debug block for page "${page.name}"`)
    }

    rootBlockUuid = firstBlock.uuid
  } else if (pageBlocksTree !== null && pageBlocksTree.length === 1) {
    const implicitFirst = pageBlocksTree[0]
    if (!implicitFirst) {
      throw new Error(`Failed to resolve implicit first debug block for page "${page.name}"`)
    }

    await logseq.Editor.updateBlock(
      implicitFirst.uuid,
      `${implicitFirst.content}\n${firstBatchBlock.content}`,
      {
        properties: firstBatchBlock.properties ?? {},
      },
    )

    rootBlockUuid = implicitFirst.uuid
  } else {
    throw new Error(
      `Unexpected debug page state for "${page.name}": ${pageBlocksTree?.length ?? 'null'} top-level blocks`,
    )
  }

  if (rootBlockUuid && restBatchBlocks.length > 0) {
    await logseq.Editor.insertBatchBlock(rootBlockUuid, restBatchBlocks, {
      sibling: false,
      before: true,
      keepUUID: true,
    })
  }

  console.info('[Readwise Debug Sync] applied page blocks', {
    pageName: page.originalName ?? page.name,
    removedBlocks,
    insertedBlocks: batchBlocks.length,
    postCreateTreeLength: pageBlocksTree?.length ?? null,
  })
}

export const syncRenderedDebugPage = async (
  book: ExportedBook,
  namespacePrefix = 'ReadwiseDebug',
) => {
  const normalizedBook = normalizeBookExport(book)
  const pageName = buildDebugPageName(book.title, namespacePrefix)
  const pageState = await ensureDebugPage(pageName)
  if (!pageState) return

  const { page, created } = pageState
  const startedAt = new Date()
  const renderRuntime = {
    format: 'org' as const,
    syncDate: format(startedAt, 'yyyy-MM-dd'),
    syncTime: format(startedAt, 'HH:mm'),
    isNewPage: created,
    hasNewHighlights: !created,
  }
  const renderedPage = renderPage(
    buildPageRenderContext(normalizedBook, renderRuntime),
    createDebugHighlightUuid,
  )

  await upsertBookProperties(page.uuid, book)

  const batchBlocks = buildRenderedBatchBlocks(renderedPage)
  await applyRenderedBlocksToDebugPage(page, batchBlocks)
}
