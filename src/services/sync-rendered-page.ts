import { format } from 'date-fns'

import { normalizeBookExport } from '../normalizer'
import { buildPageRenderContext, renderPage } from '../renderer'
import type { ExportedBook } from '../types'
import { computeCompatibleHighlightUuid } from '../uuid-compat'
import { rebuildManagedPageIfDamagedV1 } from './managed-page-integrity'
import { withManagedSyncTimestampPagePropertiesV1 } from './managed-page-sync-timestamps'
import { invalidateLegacyBlockRefMappingCacheV1 } from './migrate-legacy-block-refs'
import { buildManagedPageNamePlanV1 } from './readwise-page-names'
import { resolveAvailableManagedPageNameV1 } from './resolve-available-managed-page-name'
import {
  renameManagedPageIfNeededV1,
  resolveManagedReadwisePageV1,
} from './resolve-managed-reader-page'
import {
  createManagedPageV1,
  ensureManagedPageFileContentV1,
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

  const pageNamePlan = buildManagedPageNamePlanV1({
    pageTitle: book.title,
    namespacePrefix,
    managedId: book.user_book_id,
    format: 'org',
  })
  const pageResolution = await resolveManagedReadwisePageV1({
    readwiseBookId: book.user_book_id,
    preferredPageName: pageNamePlan.preferredPageName,
    disambiguatedPageName: pageNamePlan.disambiguatedPageName,
    namespaceRoot: namespacePrefix,
  })
  const pageBeforeRepair = pageResolution.page
  const pageName = (
    await resolveAvailableManagedPageNameV1({
      pageTitle: book.title,
      namespacePrefix,
      managedId: book.user_book_id,
      format: 'org',
      currentPageUuid: pageBeforeRepair?.uuid ?? null,
    })
  ).pageName
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
  const resolvedExistingPage = repairedPage.page
    ? await renameManagedPageIfNeededV1({
        page: repairedPage.page,
        expectedPageName: pageName,
        logPrefix,
      })
    : {
        page: null,
        renamed: false,
        previousPageName: null,
      }
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
  const content = renderedPage.emitResult.outputText
  logRenderedContentDiagnostics(pageName, content, logPrefix)
  const existingPage = resolvedExistingPage.page
  const page = existingPage ?? (await createManagedPageV1(pageName, logPrefix))
  const pageProperties = await withManagedSyncTimestampPagePropertiesV1({
    page: pageBeforeRepair,
    pageProperties: renderedPage.emitResult.pageProperties,
    syncDate: renderRuntime.syncDate,
    fallbackFirstSyncedAt: repairedPage.legacyFirstSyncedOn,
  })
  await syncManagedPagePropertiesV1(page, pageProperties, logPrefix)
  const writeResult = await writeSingleRootPageContentV1(
    page,
    pageName,
    content,
    logPrefix,
  )
  if (repairedPage.rebuilt && writeResult !== 'unchanged') {
    await ensureManagedPageFileContentV1(page, pageName, content, logPrefix)
  }
  const result =
    repairedPage.rebuilt && writeResult === 'created' ? 'updated' : writeResult

  if (result !== 'unchanged') {
    await invalidateLegacyBlockRefMappingCacheV1(namespacePrefix)
  }

  console.info(`${logPrefix} synced rendered page`, {
    userBookId: book.user_book_id,
    pageName,
    pageMatchKind: pageResolution.matchKind,
    pageRenamed: resolvedExistingPage.renamed,
    previousPageName: resolvedExistingPage.previousPageName,
    result,
    renderHash: renderedPage.renderHash,
    isNewPage: pageBeforeRepair == null,
    repairedBeforeWrite: repairedPage.rebuilt,
    repairSignatures: repairedPage.signatures,
  })
}
