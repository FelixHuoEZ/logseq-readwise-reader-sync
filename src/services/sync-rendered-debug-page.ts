import type { PageEntity } from '@logseq/libs/dist/LSPlugin'
import { format } from 'date-fns'

import { normalizeBookExport } from '../normalizer'
import { buildPageRenderContext, renderPage } from '../renderer'
import type { ExportedBook } from '../types'

const buildDebugPageName = (
  bookTitle: string,
  userBookId: number,
  namespacePrefix: string,
  mode: 'flat' | 'namespace' = 'flat',
): string =>
  mode === 'namespace'
    ? `${namespacePrefix}/${bookTitle
        .replaceAll('\\', '＼')
        .replaceAll('/', '／')
        .trim()}`
    : `${namespacePrefix}-book-${userBookId}`

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

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

const createPageWithRenderedContent = async (
  pageName: string,
  content: string,
): Promise<PageEntity | false> => {
  const page = await logseq.Editor.createPage(
    pageName,
    {},
    {
      createFirstBlock: false,
      redirect: false,
      format: 'org',
    },
  )

  if (!page) return false

  await delay(500)
  const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
  if (!content) return page

  console.info('[Readwise Debug Sync] createPage state', {
    pageName,
    topLevelBlockCount: pageBlocksTree?.length ?? null,
    firstExistingBlockContent: pageBlocksTree?.[0]?.content ?? null,
  })

  if (pageBlocksTree !== null && pageBlocksTree.length === 0) {
    console.info('[Readwise Debug Sync] createPage branch', {
      pageName,
      branch: 'insert-first-block',
    })
    const insertedFirstBlock = await logseq.Editor.insertBlock(
      page.uuid,
      content,
      {
        before: false,
        isPageBlock: true,
      } as never,
    )

    if (!insertedFirstBlock) {
      throw new Error(`Failed to insert first debug block for page "${page.name}"`)
    }

    return page
  }

  if (pageBlocksTree !== null && pageBlocksTree.length === 1) {
    const implicitFirst = pageBlocksTree[0]
    if (!implicitFirst) {
      throw new Error(`Failed to resolve implicit first debug block for page "${page.name}"`)
    }

    console.info('[Readwise Debug Sync] createPage branch', {
      pageName,
      branch: 'implicit-first-block',
      implicitFirstContent: implicitFirst.content,
    })

    await logseq.Editor.updateBlock(
      implicitFirst.uuid,
      `${implicitFirst.content}\n${content}`,
    )

    return page
  }

  throw new Error(
    `Unexpected debug page state for "${page.name}": ${pageBlocksTree?.length ?? 'null'} top-level blocks`,
  )
}

export const syncRenderedDebugPage = async (
  book: ExportedBook,
  namespacePrefix = 'ReadwiseDebug',
  pageNameMode: 'flat' | 'namespace' = 'flat',
) => {
  const normalizedBook = normalizeBookExport(book)
  const pageName = buildDebugPageName(
    book.title,
    book.user_book_id,
    namespacePrefix,
    pageNameMode,
  )
  const startedAt = new Date()
  const renderRuntime = {
    format: 'org' as const,
    syncDate: format(startedAt, 'yyyy-MM-dd'),
    syncTime: format(startedAt, 'HH:mm'),
    isNewPage: true,
    hasNewHighlights: false,
  }
  const renderedPage = renderPage(
    buildPageRenderContext(normalizedBook, renderRuntime),
    createDebugHighlightUuid,
  )
  const content = renderedPage.emitResult.outputText
  const createdPage = await createPageWithRenderedContent(pageName, content)
  if (!createdPage) return

  console.info('[Readwise Debug Sync] created page with official-style blocks', {
    pageName,
    blockCount: 1,
  })
}
