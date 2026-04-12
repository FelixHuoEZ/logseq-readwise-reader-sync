import { format, isValid, parseISO } from 'date-fns'

import type { ReaderPreviewBook } from '../api'
import { logReadwiseDebug, logReadwiseInfo } from '../logging'
import {
  buildRenderHashInput,
  computeRenderHash,
  emitOrgPage,
  type SemanticHighlight,
  type SemanticMetadataEntry,
  type SemanticPage,
} from '../renderer'
import { computeCompatibleHighlightUuid } from '../uuid-compat'
import { buildManagedPageNamePlanV1 } from './readwise-page-names'
import {
  renameManagedPageIfNeededV1,
  resolveManagedReaderPageV1,
} from './resolve-managed-reader-page'
import {
  createManagedPageV1,
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
    { key: 'DATE', value: syncDate },
    { key: 'PUBLISHED', value: toYmd(document.published_date) },
    { key: 'SAVED', value: toYmd(document.saved_at) },
  ]
}

const buildSemanticHighlights = (
  previewBook: ReaderPreviewBook,
): SemanticHighlight[] =>
  previewBook.highlights.map((highlight) => ({
    highlightId: highlight.id,
    uuid: computeCompatibleHighlightUuid(highlight.url),
    text: highlight.content?.trim() ?? '',
    locationLabel: highlight.url ? 'View Highlight' : null,
    locationUrl: highlight.url ?? null,
    createdDate: toYmd(highlight.created_at) ?? '',
    tags: normalizeTagNames(highlight.tags),
    note:
      typeof highlight.notes === 'string' && highlight.notes.trim().length > 0
        ? highlight.notes.trim()
        : null,
  }))

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
  pageNote: {
    imageUrl: previewBook.document.image_url ?? null,
    summary: previewBook.document.summary ?? null,
  },
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
  } = {},
) => {
  const sourcePageTitle = previewBook.document.title?.trim().length
    ? previewBook.document.title
    : previewBook.document.id
  const pageNamePlan = buildManagedPageNamePlanV1({
    pageTitle: sourcePageTitle,
    namespacePrefix,
    managedId: previewBook.document.id,
    format: 'org',
  })
  const preferredPageName = pageNamePlan.preferredPageName
  const disambiguatedPageName = pageNamePlan.disambiguatedPageName
  logReadwiseDebug(logPrefix, 'preparing rendered Reader page', {
    readerDocumentId: previewBook.document.id,
    preferredPageName,
    disambiguatedPageName,
    namespacePrefix,
    pageResolveMode: options.pageResolveMode ?? 'title_only',
    highlightCount: previewBook.highlights.length,
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
          readerDocumentId: previewBook.document.id,
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
  const resolvedExistingPage = pageResolution.page
    ? await renameManagedPageIfNeededV1({
        page: pageResolution.page,
        expectedPageName: pageResolution.resolvedPageName,
        logPrefix,
      })
    : {
        page: pageResolution.page,
        renamed: false,
        previousPageName: null,
      }
  const existingPage = resolvedExistingPage.page
  const pageName = pageResolution.resolvedPageName
  const syncHeaderText =
    options.syncHeaderText ??
    (existingPage == null
      ? `Highlights first synced by [[Readwise]] [[${syncDate}]]`
      : `Highlights refreshed by [[Readwise]] [[${syncDate}]]`)
  const semanticPage = buildReaderPreviewSemanticPage(
    previewBook,
    syncDate,
    syncHeaderText,
  )
  const emitResult = emitOrgPage(semanticPage)
  const renderHashInput = buildRenderHashInput(semanticPage)
  const renderHash = computeRenderHash(renderHashInput)
  const content = emitResult.pageContentText

  logRenderedContentDiagnostics(pageName, content, logPrefix)

  const page = existingPage ?? (await createManagedPageV1(pageName, logPrefix))
  await syncManagedPagePropertiesV1(page, emitResult.pageProperties, logPrefix)
  const result = await writeSingleRootPageContentV1(
    page,
    pageName,
    content,
    logPrefix,
  )

  const summary: SyncRenderedReaderPageResult = {
    readerDocumentId: previewBook.document.id,
    pageName,
    pageMatchKind: pageResolution.matchKind,
    pageRenamed: resolvedExistingPage.renamed,
    previousPageName: resolvedExistingPage.previousPageName,
    result,
    renderHash,
    highlightCount: previewBook.highlights.length,
    highlightCoverage: previewBook.highlightCoverage,
    isNewPage: existingPage == null,
  }

  logReadwiseInfo(logPrefix, 'synced rendered page', summary)
  return summary
}
