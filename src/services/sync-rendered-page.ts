import { format } from 'date-fns'

import { normalizeBookExport } from '../normalizer'
import { buildPageRenderContext, renderPage } from '../renderer'
import type { ExportedBook } from '../types'
import { computeCompatibleHighlightUuid } from '../uuid-compat'
import { buildFormalManagedPageName } from './readwise-page-names'
import { createManagedPageV1, writeSingleRootPageContentV1 } from './single-root-page-content'

const logRenderedContentDiagnostics = (pageName: string, content: string) => {
  const lines = content.split('\n')
  const endNoteIndex = lines.indexOf('#+END_NOTE')
  const transitionLines =
    endNoteIndex >= 0 ? lines.slice(endNoteIndex, endNoteIndex + 4) : []
  const hasBlankLineAfterEndNote =
    endNoteIndex >= 0 && lines[endNoteIndex + 1] === ''
  const firstHighlightLine =
    endNoteIndex >= 0
      ? lines.slice(endNoteIndex + 1).find((line) => line.startsWith('* '))
      : null

  console.info('[Readwise Sync] rendered content diagnostics', {
    pageName,
    previewHead: lines.slice(0, 18),
    transitionLines,
    transitionText: transitionLines.join('\\n'),
    hasBlankLineAfterEndNote,
    firstHighlightLine,
  })
}

export const syncRenderedPage = async (
  book: ExportedBook,
  namespacePrefix = 'ReadwiseHighlights',
) => {
  if (book.is_deleted) {
    console.info('[Readwise Sync] skipping deleted book until delete handling is implemented', {
      userBookId: book.user_book_id,
      title: book.title,
    })
    return
  }

  const pageName = buildFormalManagedPageName(book.title, namespacePrefix)
  const existingPage = await logseq.Editor.getPage(pageName)
  const normalizedBook = normalizeBookExport(book)
  const startedAt = new Date()
  const renderRuntime = {
    format: 'org' as const,
    syncDate: format(startedAt, 'yyyy-MM-dd'),
    syncTime: format(startedAt, 'HH:mm'),
    isNewPage: existingPage == null,
    hasNewHighlights: existingPage != null,
  }
  const renderedPage = renderPage(
    buildPageRenderContext(normalizedBook, renderRuntime),
    computeCompatibleHighlightUuid,
  )
  const content = renderedPage.emitResult.outputText
  logRenderedContentDiagnostics(pageName, content)
  const page = existingPage ?? (await createManagedPageV1(pageName))
  const result = await writeSingleRootPageContentV1(page, pageName, content)

  console.info('[Readwise Sync] synced rendered page', {
    userBookId: book.user_book_id,
    pageName,
    result,
    renderHash: renderedPage.renderHash,
    isNewPage: renderRuntime.isNewPage,
  })
}
