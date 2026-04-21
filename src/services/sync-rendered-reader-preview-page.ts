import { format, isValid, parseISO } from 'date-fns'

import type { ReaderPreviewBook } from '../api'
import { tryEnrichReaderDocumentHighlightsViaMcp } from '../api/reader-document-highlights'
import { logReadwiseDebug, logReadwiseInfo } from '../logging'
import {
  extractReaderHighlightContentSegments,
  normalizeReaderImageUrl,
} from '../reader/extract-reader-highlight-content-segments'
import {
  buildRenderHashInput,
  computeRenderHash,
  emitOrgPage,
  type SemanticHighlight,
  type SemanticMetadataEntry,
  type SemanticPage,
} from '../renderer'
import type { ReaderDocument } from '../types'
import { computeCompatibleHighlightUuid } from '../uuid-compat'
import { rebuildManagedPageIfDamagedV1 } from './managed-page-integrity'
import { withManagedSyncTimestampPagePropertiesV1 } from './managed-page-sync-timestamps'
import { invalidateLegacyBlockRefMappingCacheV1 } from './migrate-legacy-block-refs'
import { buildManagedPageNamePlanV1 } from './readwise-page-names'
import { resolveAvailableManagedPageNameV1 } from './resolve-available-managed-page-name'
import {
  renameManagedPageIfNeededV1,
  resolveManagedReaderPageV1,
} from './resolve-managed-reader-page'
import {
  createManagedPageV1,
  ensureManagedPageFileContentV1,
  writeSingleRootPageContentV1,
} from './single-root-page-content'
import { syncManagedPagePropertiesV1 } from './sync-managed-page-properties'

const toYmd = (value: string | null | undefined) => {
  if (!value) return null

  const parsed = parseISO(value)
  if (!isValid(parsed)) return null

  return format(parsed, 'yyyy-MM-dd')
}

const normalizeTagNames = (value: Record<string, unknown> | null | undefined) =>
  Object.keys(value ?? {}).filter((name) => name.trim().length > 0)

const normalizeOptionalText = (value: string | null | undefined) => {
  if (typeof value !== 'string') return null

  const normalized = value.trim()
  return normalized.length > 0 ? normalized : null
}

const deriveHighlightTextAndSegments = (highlight: ReaderDocument) => {
  const explicitPrimaryText = normalizeOptionalText(highlight.content) ?? ''
  const extractedSegments = extractReaderHighlightContentSegments({
    richContent: highlight.render_content ?? highlight.content,
    imageUrl: highlight.image_url,
    htmlContent: highlight.html_content,
    primaryText: explicitPrimaryText,
  })
  const contentSegments = extractedSegments.map((segment) => ({ ...segment }))

  if (explicitPrimaryText.length > 0) {
    return {
      text: explicitPrimaryText,
      contentSegments,
    }
  }

  const firstTextIndex = contentSegments.findIndex(
    (segment) => segment.kind === 'text',
  )

  if (firstTextIndex >= 0) {
    const firstTextSegment = contentSegments[firstTextIndex]
    if (firstTextSegment?.kind === 'text') {
      const [firstLine = '', ...restLines] = firstTextSegment.value.split('\n')
      const fallbackText = normalizeOptionalText(firstLine)
      const remainingText = normalizeOptionalText(restLines.join('\n'))

      if (fallbackText) {
        if (remainingText) {
          contentSegments[firstTextIndex] = {
            kind: 'text',
            value: remainingText,
          }
        } else {
          contentSegments.splice(firstTextIndex, 1)
        }

        return {
          text: fallbackText,
          contentSegments,
        }
      }
    }
  }

  if (contentSegments.some((segment) => segment.kind === 'image')) {
    return {
      text: 'Media highlight',
      contentSegments,
    }
  }

  return {
    text: 'View Highlight',
    contentSegments,
  }
}

const buildMetadataEntries = (
  previewBook: ReaderPreviewBook,
  syncDate: string,
): SemanticMetadataEntry[] => {
  const { document } = previewBook
  const documentTags = normalizeTagNames(document.tags)

  return [
    { key: 'rw-reader-id', value: document.id },
    { key: 'AUTHOR', value: document.author ?? null },
    { key: 'CATEGORIES', value: document.category ?? null },
    { key: 'LINK', value: document.source_url ?? document.url ?? null },
    {
      key: 'TAGS',
      value:
        documentTags.length > 0 ? ` ${documentTags.join('  ,  ')}  ,  ` : null,
    },
    { key: 'summary', value: document.summary },
    { key: 'DATE', value: syncDate },
    { key: 'PUBLISHED', value: toYmd(document.published_date) },
    { key: 'SAVED', value: toYmd(document.saved_at) },
  ]
}

const buildSemanticHighlights = (
  previewBook: ReaderPreviewBook,
): SemanticHighlight[] =>
  previewBook.highlights.map((highlight) => {
    const { text, contentSegments } = deriveHighlightTextAndSegments(highlight)

    return {
      highlightId: highlight.id,
      uuid: computeCompatibleHighlightUuid(highlight.url),
      text,
      imageUrl: null,
      contentSegments,
      locationLabel: highlight.url ? 'View Highlight' : null,
      locationUrl: highlight.url ?? null,
      createdDate: toYmd(highlight.created_at) ?? '',
      tags: normalizeTagNames(highlight.tags),
      note: normalizeOptionalText(highlight.notes),
    }
  })

const buildReaderPreviewSemanticPage = (
  previewBook: ReaderPreviewBook,
  syncDate: string,
  syncHeaderText: string,
): SemanticPage => ({
  format: 'org',
  pageTitle: previewBook.document.title?.trim().length
    ? previewBook.document.title
    : previewBook.document.id,
  metadata: buildMetadataEntries(previewBook, syncDate),
  pageNote:
    normalizeOptionalText(previewBook.document.notes) != null ||
    normalizeReaderImageUrl(previewBook.document.image_url) != null
      ? {
          imageUrl: normalizeReaderImageUrl(previewBook.document.image_url),
          text: normalizeOptionalText(previewBook.document.notes),
        }
      : null,
  syncHeader: {
    kind: 'first_sync',
    text: syncHeaderText,
  },
  highlights: buildSemanticHighlights(previewBook),
})

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

  logReadwiseDebug(logPrefix, 'rendered content diagnostics', {
    pageName,
    previewHead: lines.slice(0, 18),
    transitionLines,
    transitionText: transitionLines.join('\\n'),
    hasBlankLineAfterEndNote,
    firstHighlightLine,
  })
}

export interface SyncRenderedReaderPageResult {
  readerDocumentId: string
  pageName: string
  pageMatchKind:
    | 'rw-reader-id'
    | 'managed_title'
    | 'disambiguated_title'
    | 'none'
  pageRenamed: boolean
  previousPageName: string | null
  result: 'created' | 'updated' | 'unchanged'
  renderHash: string
  highlightCount: number
  highlightCoverage: ReaderPreviewBook['highlightCoverage']
  isNewPage: boolean
}

export const syncRenderedReaderPreviewPage = async (
  previewBook: ReaderPreviewBook,
  namespacePrefix = 'ReadwiseReaderPreview',
  logPrefix = '[Readwise Reader Preview]',
  options: {
    syncHeaderText?: string
    pageResolveMode?: 'title_only' | 'reader_id_then_title'
    identityNamespaceRoot?: string
    readerAuthToken?: string | null
  } = {},
) => {
  const enrichedPreviewBook =
    options.readerAuthToken != null
      ? {
          ...previewBook,
          highlights: (
            await tryEnrichReaderDocumentHighlightsViaMcp({
              token: options.readerAuthToken,
              document: previewBook.document,
              highlights: previewBook.highlights,
              logPrefix,
            })
          ).highlights,
        }
      : previewBook
  const sourcePageTitle = enrichedPreviewBook.document.title?.trim().length
    ? enrichedPreviewBook.document.title
    : enrichedPreviewBook.document.id
  const pageNamePlan = buildManagedPageNamePlanV1({
    pageTitle: sourcePageTitle,
    namespacePrefix,
    managedId: enrichedPreviewBook.document.id,
    format: 'org',
  })
  const preferredPageName = pageNamePlan.preferredPageName
  const disambiguatedPageName = pageNamePlan.disambiguatedPageName
  logReadwiseDebug(logPrefix, 'preparing rendered Reader page', {
    readerDocumentId: enrichedPreviewBook.document.id,
    preferredPageName,
    disambiguatedPageName,
    namespacePrefix,
    pageResolveMode: options.pageResolveMode ?? 'title_only',
    highlightCount: enrichedPreviewBook.highlights.length,
  })
  const syncDate = format(new Date(), 'yyyy-MM-dd')
  const preferredPage =
    options.pageResolveMode === 'title_only'
      ? await logseq.Editor.getPage(preferredPageName)
      : null
  const disambiguatedPage =
    options.pageResolveMode === 'title_only' &&
    disambiguatedPageName !== preferredPageName
      ? await logseq.Editor.getPage(disambiguatedPageName)
      : null
  const pageResolution =
    options.pageResolveMode === 'reader_id_then_title'
      ? await resolveManagedReaderPageV1({
          readerDocumentId: enrichedPreviewBook.document.id,
          preferredPageName,
          disambiguatedPageName,
          namespaceRoot: options.identityNamespaceRoot ?? namespacePrefix,
        })
      : {
          page: preferredPage ?? disambiguatedPage,
          matchKind:
            preferredPage == null && disambiguatedPage != null
              ? ('disambiguated_title' as const)
              : ('managed_title' as const),
          resolvedPageName:
            preferredPage == null && disambiguatedPage != null
              ? disambiguatedPageName
              : preferredPageName,
        }
  const pageBeforeRepair = pageResolution.page
  const pageName = (
    await resolveAvailableManagedPageNameV1({
      pageTitle: sourcePageTitle,
      namespacePrefix,
      managedId: enrichedPreviewBook.document.id,
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
        page: repairedPage.page,
        renamed: false,
        previousPageName: null,
      }
  const existingPage = resolvedExistingPage.page
  const syncHeaderText = options.syncHeaderText ?? ''
  const semanticPage = buildReaderPreviewSemanticPage(
    enrichedPreviewBook,
    syncDate,
    syncHeaderText,
  )
  const emitResult = emitOrgPage(semanticPage)
  const renderHashInput = buildRenderHashInput(semanticPage)
  const renderHash = computeRenderHash(renderHashInput)
  const content = emitResult.outputText

  logRenderedContentDiagnostics(pageName, content, logPrefix)

  const page = existingPage ?? (await createManagedPageV1(pageName, logPrefix))
  const pageProperties = await withManagedSyncTimestampPagePropertiesV1({
    page: pageBeforeRepair,
    pageProperties: emitResult.pageProperties,
    syncDate,
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
    await invalidateLegacyBlockRefMappingCacheV1(
      options.identityNamespaceRoot ?? namespacePrefix,
    )
  }

  const summary: SyncRenderedReaderPageResult = {
    readerDocumentId: enrichedPreviewBook.document.id,
    pageName,
    pageMatchKind: pageResolution.matchKind,
    pageRenamed: resolvedExistingPage.renamed,
    previousPageName: resolvedExistingPage.previousPageName,
    result,
    renderHash,
    highlightCount: enrichedPreviewBook.highlights.length,
    highlightCoverage: enrichedPreviewBook.highlightCoverage,
    isNewPage: pageBeforeRepair == null,
  }

  logReadwiseInfo(logPrefix, 'synced rendered page', {
    ...summary,
    repairedBeforeWrite: repairedPage.rebuilt,
    repairSignatures: repairedPage.signatures,
  })
  return summary
}
