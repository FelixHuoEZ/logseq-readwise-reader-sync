import { format } from 'date-fns'

import { normalizeBookExport } from '../normalizer'
import { buildPageRenderContext, renderPage } from '../renderer'
import type { ExportedBook } from '../types'
import { computeCompatibleHighlightUuid } from '../uuid-compat'
import { rebuildManagedPageIfDamagedV1 } from './managed-page-integrity'
import { withManagedSyncTimestampPagePropertiesV1 } from './managed-page-sync-timestamps'
import { buildFormalManagedPageName } from './readwise-page-names'
import {
  createManagedPageV1,
  writeSingleRootPageContentV1,
} from './single-root-page-content'
import { syncManagedPagePropertiesV1 } from './sync-managed-page-properties'

const logRenderedContentDiagnostics = (
  pageName: string,
  content: string,
  logPrefix: string,
) => {
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

  console.info(`${logPrefix} rendered content diagnostics`, {
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
  logPrefix = '[Readwise Sync]',
  readerDocumentUrl: string | null = null,
) => {
  if (book.is_deleted) {
    console.info(
      `${logPrefix} skipping deleted book until delete handling is implemented`,
      {
        userBookId: book.user_book_id,
        title: book.title,
      },
    )
    return
  }

  const pageName = buildFormalManagedPageName(book.title, namespacePrefix)
  const pageBeforeRepair = await logseq.Editor.getPage(pageName)
  const repairedPage =
    pageBeforeRepair == null
      ? {
          page: null,
          rebuilt: false,
          signatures: [] as string[],
          legacyFirstSyncedOn: null,
        }
      : await rebuildManagedPageIfDamagedV1({
          page: pageBeforeRepair,
          expectedPageName: pageName,
          logPrefix,
        })
  const normalizedBook = normalizeBookExport(book, { readerDocumentUrl })
  const startedAt = new Date()
  const renderRuntime = {
    format: 'org' as const,
    syncDate: format(startedAt, 'yyyy-MM-dd'),
    syncTime: format(startedAt, 'HH:mm'),
    isNewPage: pageBeforeRepair == null,
    hasNewHighlights: pageBeforeRepair != null,
  }
  const renderedPage = renderPage(
    buildPageRenderContext(normalizedBook, renderRuntime),
    computeCompatibleHighlightUuid,
  )
  const content = renderedPage.emitResult.pageContentText
  logRenderedContentDiagnostics(pageName, content, logPrefix)
  const existingPage = repairedPage.page
  const page = existingPage ?? (await createManagedPageV1(pageName, logPrefix))
  const pageProperties = await withManagedSyncTimestampPagePropertiesV1({
    page: pageBeforeRepair,
    pageProperties: renderedPage.emitResult.pageProperties,
    syncDate: renderRuntime.syncDate,
    fallbackFirstSyncedAt: repairedPage.legacyFirstSyncedOn,
  })
  await syncManagedPagePropertiesV1(
    page,
    pageProperties,
    logPrefix,
  )
  const writeResult = await writeSingleRootPageContentV1(
    page,
    pageName,
    content,
    logPrefix,
  )
  const result =
    repairedPage.rebuilt && writeResult === 'created' ? 'updated' : writeResult

  console.info(`${logPrefix} synced rendered page`, {
    userBookId: book.user_book_id,
    pageName,
    result,
    renderHash: renderedPage.renderHash,
    isNewPage: pageBeforeRepair == null,
    repairedBeforeWrite: repairedPage.rebuilt,
    repairSignatures: repairedPage.signatures,
  })
}
