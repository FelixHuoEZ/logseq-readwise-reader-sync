import { computeCompatibleHighlightUuid } from '../uuid-compat'
import { buildPlannerInputItemV1, buildSyncPlanV1 } from '../planner'
import { normalizeBookExport } from '../normalizer'
import { buildPageRenderContext, renderPage } from '../renderer'

import {
  buildRenderRuntimeContextV1,
} from './build-render-runtime-context'
import { buildSyncRunContextV1 } from './build-sync-run-context'
import type {
  PreparedSyncItemV1,
  PreparedSyncPlanV1,
  PrepareSyncPlanParamsV1,
} from './types'

const assertSupportedDocumentFormat = (
  documentFormat: PrepareSyncPlanParamsV1['graphState']['documentFormat'],
) => {
  if (documentFormat !== 'org') {
    throw new Error(
      `Document format "${documentFormat}" is not implemented in the V1 preview pipeline yet.`,
    )
  }
}

export const prepareSyncPlanV1 = ({
  rawBooks,
  graphState,
  graphSnapshot,
  startedAt = new Date().toISOString(),
  runId,
}: PrepareSyncPlanParamsV1): PreparedSyncPlanV1 => {
  assertSupportedDocumentFormat(graphState.documentFormat)

  const items: PreparedSyncItemV1[] = rawBooks.map((rawBook) => {
    try {
      const book = normalizeBookExport(rawBook)
      const existingPageIndexEntry =
        graphState.pageIndex[String(book.userBookId)] ?? null
      const runtime = buildRenderRuntimeContextV1(
        book,
        existingPageIndexEntry,
        graphState.documentFormat,
        startedAt,
      )
      const renderedPage = renderPage(
        buildPageRenderContext(book, runtime),
        computeCompatibleHighlightUuid,
      )
      const plannerInput = buildPlannerInputItemV1(
        book,
        renderedPage,
        graphState,
        graphSnapshot,
      )

      return {
        rawBook,
        book,
        runtime,
        renderedPage,
        plannerInput,
      }
    } catch (error) {
      const title =
        typeof rawBook.title === 'string' && rawBook.title.length > 0
          ? rawBook.title
          : '<untitled>'
      const message = error instanceof Error ? error.message : String(error)

      throw new Error(
        `Failed to prepare preview item for userBookId=${rawBook.user_book_id}, title=${title}: ${message}`,
      )
    }
  })

  const runContext = buildSyncRunContextV1(
    graphState,
    graphSnapshot,
    startedAt,
    runId,
  )
  const plan = buildSyncPlanV1(
    runContext,
    items.map((item) => item.plannerInput),
    graphState,
  )

  return {
    graphState,
    graphSnapshot,
    runContext,
    items,
    plan,
  }
}
