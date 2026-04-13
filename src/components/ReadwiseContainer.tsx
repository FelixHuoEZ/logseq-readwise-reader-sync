import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'
import './ReadwiseContainer.css'

import { useEffect, useRef, useState } from 'react'
import { format } from 'date-fns'

import {
  createReadwiseClient,
  loadReaderPreviewBooks,
  loadReaderPreviewBooksByParentIds,
  isReaderPreviewLoadResumeError,
  type ReaderPreviewLoadMode,
  type ReaderPreviewLoadResumeState,
  type ReaderPreviewLoadStats,
} from '../api'
import {
  type GraphLastFormalSyncSummaryV1,
  type GraphCheckpointSourceV1,
  type GraphReaderSyncStateV1,
  loadGraphCheckpointStateV1,
  loadCurrentGraphContextV1,
  loadGraphReaderRetryFallbackEntriesV1,
  loadGraphReaderSyncStateV1,
  normalizeComparableUrlV1,
  removeGraphReaderRetryFallbackEntriesV1,
  saveGraphLastFormalSyncSummaryV1,
  saveGraphCheckpointStateV1,
  saveGraphReaderRetryFallbackEntriesV1,
  saveGraphReaderSyncStateV1,
} from '../graph'
import {
  createGraphReaderSyncCacheV1,
  type ReaderSyncCacheSummaryV1,
  type ReaderSyncRetryPageEntryV1,
} from '../cache'
import {
  assertManagedPageFileNameWithinLimits,
  auditManagedReaderPagesV1,
  backupFormalTestPages,
  buildLegacyBlockRefMappingV1,
  buildManagedPageNamePlanV1,
  captureCurrentPageFileSnapshotV1,
  clearManagedPagesByNamespacePrefix,
  clearManagedPagesBySessionNamespaceRoot,
  clearFormalTestPages,
  type CurrentPageDiffResult,
  diffCurrentPageFileSnapshotV1,
  inspectManagedPageIntegrityV1,
  listManagedPagesByNamespacePrefix,
  listManagedPagesBySessionNamespaceRoot,
  loadActiveFormalTestSessionManifestV1,
  migrateLegacyBlockRefsV1,
  migrateCurrentPageLegacyIdsV1,
  previewLegacyBlockRefsV1,
  previewCurrentPageLegacyIdsV1,
  rotateActiveFormalTestSessionNamespaceV1,
  restoreLatestFormalTestPageBackup,
  saveFormalTestSessionManifestV1,
  setupProps,
  syncRenderedDebugPage,
  syncRenderedReaderPreviewPage,
  syncRenderedPage,
  type CurrentPageLegacyIdRewriteEntryV1,
  type LegacyBlockRefPreviewEntryV1,
  type ManagedReaderPageAuditEntryV1,
} from '../services'
import { deriveNextUpdatedAfterV1 } from '../sync'
import type {
  ExportedBook,
  ExportedBookIdentity,
  ExportParams,
  ExportResponse,
  ReaderDocument,
  SyncStatus,
} from '../types'
import {
  describeUnknownError,
  logReadwiseDebug,
  logReadwiseError,
  logReadwiseInfo,
  logReadwiseWarn,
} from '../logging'
import {
  buildRunIssuesBundle,
  diagnoseRunIssue,
  formatRunIssueCategoryLabel,
  shouldRunIssueBlockReaderSyncCursor,
  type RunIssue,
  type RunIssueBundleContext,
  summarizeRunIssueCategories,
} from './run-issues'

type ReaderSyncEtaPhase = 'fetch-highlights' | 'fetch-documents' | 'write-pages'
type ReaderSyncMode = ReaderPreviewLoadMode

interface ReaderSyncEtaSample {
  msPerUnit: number
  observedAt: number
  units: number
}

interface ReaderSyncEtaSnapshot {
  phase: ReaderSyncEtaPhase
  label: string
  etaMs: number | null
  observedAt: number
}

interface ManagedPageRepairCandidate {
  pageName: string
  readerDocumentId: string
  signatures: string[]
  rootContent: string
}

interface ManagedPageRepairIdentityRetryCandidate {
  pageName: string
  rootContent: string
  signatures: string[]
}

interface ReaderDocumentInferenceResult {
  readerDocumentId: string | null
  shouldRetryAfterScan: boolean
  failedHighlightIds: string[]
}

interface ManagedPageRepairSignals {
  pageTitle: string
  normalizedPageTitle: string | null
  linkUrl: string | null
  normalizedLinkUrl: string | null
  author: string | null
  normalizedAuthor: string | null
  category: string | null
  normalizedCategory: string | null
}

interface ReaderDocumentRepairMatch {
  document: ReaderDocument
  score: number
  reasons: string[]
  identityKey: string | null
}

interface ReaderParentReplacementLookupResult {
  document: ReaderDocument | null
  shouldRetry: boolean
}

interface PendingLegacyBlockRefMigrationPlan {
  mapping: Map<string, string>
  entries: LegacyBlockRefPreviewEntryV1[]
}

interface PendingCurrentPageLegacyIdMigrationPlan {
  mapping: Map<string, string>
  pageName: string
  relativeFilePath: string
  fileKind: 'page' | 'whiteboard'
  rewrites: CurrentPageLegacyIdRewriteEntryV1[]
}

interface CurrentPageLegacyIdPreviewResult {
  pageName: string
  relativeFilePath: string
  fileKind: 'page' | 'whiteboard'
  rewrites: CurrentPageLegacyIdRewriteEntryV1[]
  managedPagesScanned: number
}

interface CurrentPageLegacyIdApplyResult {
  pageName: string
  relativeFilePath: string
  fileKind: 'page' | 'whiteboard'
  rewritesApplied: number
}

interface ManagedPageRepairScanEntryResult {
  pageName: string
  candidate?: ManagedPageRepairCandidate
  deferredRetry?: ManagedPageRepairIdentityRetryCandidate
  issue?: RunIssue
}

type ReaderSyncHelpPanelId =
  | 'managed-pages'
  | 'highlight-scan'
  | 'global-sync'
  | 'current-page'
  | 'maintenance-tools'

interface ReaderSyncHelpPopoverState {
  id: ReaderSyncHelpPanelId
  title: string
  notes: string[]
  anchorRect: {
    left: number
    top: number
    right: number
    bottom: number
    width: number
    height: number
  }
}

export const ReadwiseContainer = () => {
  const defaultReaderFullScanTargetDocuments = 20
  const defaultReaderFullScanDebugHighlightPageLimit = 0
  const managedPageRepairScanConcurrency = 4
  const maxAutomaticResumeRetries = 10
  const debugSyncMaxBooksLimit = 5
  const debugNamespaceRoot = 'ReadwiseDebug'
  const formalNamespaceRoot = 'ReadwiseHighlights'
  const readerPreviewNamespaceRoot = 'ReadwiseReaderPreview'
  const formalSyncLogPrefix = '[Readwise Sync]'
  const sessionTestLogPrefix = '[Readwise Session Test]'
  const backupLogPrefix = '[Readwise Backup]'
  const restoreLogPrefix = '[Readwise Restore]'
  const debugLogPrefix = '[Readwise Debug Sync]'
  const readerPreviewLogPrefix = '[Readwise Reader Preview]'
  const showAdvancedFormalTestActions = false
  const cancelledRef = useRef(false)
  const retryActionRef = useRef<{
    kind: 'formal' | 'preview'
    label: string
    run: () => Promise<void>
  } | null>(null)
  const etaEstimatorRef = useRef<{
    phase: ReaderSyncEtaPhase | null
    label: string
    lastCompleted: number
    lastTimestamp: number | null
    samples: ReaderSyncEtaSample[]
  }>({
    phase: null,
    label: '',
    lastCompleted: 0,
    lastTimestamp: null,
    samples: [],
  })
  const [propsReady, setPropsReady] = useState(
    () => !!logseq.settings?.propsConfigured,
  )
  const [status, setStatus] = useState<SyncStatus>('idle')
  const [current, setCurrent] = useState(0)
  const [total, setTotal] = useState(0)
  const [currentBook, setCurrentBook] = useState('')
  const [statusMessage, setStatusMessage] = useState('')
  const [errors, setErrors] = useState<RunIssue[]>([])
  const [showWarningIssues, setShowWarningIssues] = useState(false)
  const [runIssueContext, setRunIssueContext] =
    useState<RunIssueBundleContext | null>(null)
  const [pendingLegacyBlockRefMigration, setPendingLegacyBlockRefMigration] =
    useState<PendingLegacyBlockRefMigrationPlan | null>(null)
  const [pendingCurrentPageLegacyIdMigration, setPendingCurrentPageLegacyIdMigration] =
    useState<PendingCurrentPageLegacyIdMigrationPlan | null>(null)
  const [currentPageLegacyIdPreviewResult, setCurrentPageLegacyIdPreviewResult] =
    useState<CurrentPageLegacyIdPreviewResult | null>(null)
  const [currentPageLegacyIdApplyResult, setCurrentPageLegacyIdApplyResult] =
    useState<CurrentPageLegacyIdApplyResult | null>(null)
  const [pageDiffResult, setPageDiffResult] =
    useState<CurrentPageDiffResult | null>(null)
  const [cacheSummaryResult, setCacheSummaryResult] =
    useState<ReaderSyncCacheSummaryV1 | null>(null)
  const [etaTick, setEtaTick] = useState(0)
  const [etaSnapshot, setEtaSnapshot] = useState<ReaderSyncEtaSnapshot | null>(null)
  const [showMaintenanceTools, setShowMaintenanceTools] = useState(false)
  const [activeFormalTestSessionCount, setActiveFormalTestSessionCount] =
    useState<number | null>(null)
  const [readerSyncState, setReaderSyncState] =
    useState<GraphReaderSyncStateV1 | null>(null)
  const [hoveredHelpPopover, setHoveredHelpPopover] =
    useState<ReaderSyncHelpPopoverState | null>(null)
  const [pinnedHelpPopover, setPinnedHelpPopover] =
    useState<ReaderSyncHelpPopoverState | null>(null)
  const helpHideTimeoutRef = useRef<number | null>(null)
  const liveRunIssueMetricsRef = useRef<{
    processedItems: number | null
    stats: NonNullable<RunIssueBundleContext['stats']>
  }>({
    processedItems: null,
    stats: {},
  })

  useEffect(() => {
    if (pinnedHelpPopover == null) {
      return undefined
    }

    const handlePointerDown = (event: PointerEvent) => {
      if (!(event.target instanceof Element)) {
        setPinnedHelpPopover(null)
        return
      }

      if (
        !event.target.closest('.rw-help-anchor') &&
        !event.target.closest('.rw-help-popover')
      ) {
        setPinnedHelpPopover(null)
      }
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setPinnedHelpPopover(null)
      }
    }

    document.addEventListener('pointerdown', handlePointerDown)
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('pointerdown', handlePointerDown)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [pinnedHelpPopover])

  useEffect(() => {
    return () => {
      if (helpHideTimeoutRef.current != null) {
        window.clearTimeout(helpHideTimeoutRef.current)
      }
    }
  }, [])

  const refreshReaderSyncState = async () => {
    setReaderSyncState(await loadGraphReaderSyncStateV1())
  }

  const refreshActiveFormalTestSessionCount = async () => {
    const activeFormalTestSession = await loadActiveFormalTestSessionManifestV1()
    setActiveFormalTestSessionCount(activeFormalTestSession?.books.length ?? null)
  }

  useEffect(() => {
    void refreshActiveFormalTestSessionCount()
    void refreshReaderSyncState()
  }, [])

  useEffect(() => {
    if (
      (status !== 'fetching' && status !== 'syncing') ||
      etaSnapshot == null ||
      etaSnapshot.etaMs == null
    ) {
      return
    }

    const timer = window.setInterval(() => {
      setEtaTick(Date.now())
    }, 1000)

    return () => {
      window.clearInterval(timer)
    }
  }, [status, current, total])

  const resetUiState = () => {
    cancelledRef.current = false
    retryActionRef.current = null
    etaEstimatorRef.current = {
      phase: null,
      label: '',
      lastCompleted: 0,
      lastTimestamp: null,
      samples: [],
    }
    setEtaSnapshot(null)
    setStatus('idle')
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatusMessage('')
    setErrors([])
    setRunIssueContext(null)
    setPendingLegacyBlockRefMigration(null)
    setPendingCurrentPageLegacyIdMigration(null)
    setCurrentPageLegacyIdPreviewResult(null)
    setCurrentPageLegacyIdApplyResult(null)
    setPageDiffResult(null)
    setCacheSummaryResult(null)
  }

  const beginReaderSyncEtaPhase = (
    phase: ReaderSyncEtaPhase,
    label: string,
  ) => {
    etaEstimatorRef.current = {
      phase,
      label,
      lastCompleted: 0,
      lastTimestamp: Date.now(),
      samples: [],
    }
    setEtaSnapshot({
      phase,
      label,
      etaMs: null,
      observedAt: Date.now(),
    })
  }

  const getReaderSyncEtaWindowConfig = (
    phase: ReaderSyncEtaPhase,
    label: string,
    rawEtaMs: number | null,
  ) => {
    if (phase === 'fetch-highlights' && label === 'repair scan') {
      const adaptiveHorizonMs =
        rawEtaMs == null
          ? 12_000
          : Math.max(3_000, Math.min(30_000, rawEtaMs))
      return {
        maxSamples: 30,
        horizonMs: adaptiveHorizonMs,
      }
    }

    if (phase === 'fetch-highlights') {
      const adaptiveHorizonMs =
        rawEtaMs == null
          ? 10_000
          : Math.max(3_000, Math.min(30_000, rawEtaMs))
      return {
        maxSamples: 16,
        horizonMs: adaptiveHorizonMs,
      }
    }

    if (phase === 'fetch-documents') {
      const adaptiveHorizonMs =
        rawEtaMs == null
          ? 8_000
          : Math.max(3_000, Math.min(30_000, rawEtaMs))
      return {
        maxSamples: 10,
        horizonMs: adaptiveHorizonMs,
      }
    }

    return {
      maxSamples: 8,
      horizonMs: 12_000,
    }
  }

  const updateReaderSyncEta = (
    phase: ReaderSyncEtaPhase,
    label: string,
    completed: number,
    totalUnits: number,
  ) => {
    const now = Date.now()
    const estimator = etaEstimatorRef.current

    if (estimator.phase !== phase) {
      etaEstimatorRef.current = {
        phase,
        label,
        lastCompleted: completed,
        lastTimestamp: now,
        samples: [],
      }
      setEtaSnapshot({
        phase,
        label,
        etaMs: null,
        observedAt: now,
      })
      return
    }

    if (
      estimator.lastTimestamp != null &&
      completed > estimator.lastCompleted
    ) {
      const deltaUnits = completed - estimator.lastCompleted
      const sampleMs = (now - estimator.lastTimestamp) / deltaUnits
      const remainingUnits = Math.max(0, totalUnits - completed)
      const rawEtaMs = remainingUnits > 0 ? sampleMs * remainingUnits : 0
      const { maxSamples, horizonMs } = getReaderSyncEtaWindowConfig(
        phase,
        label,
        rawEtaMs,
      )
      const nextSamples = [
        ...estimator.samples,
        {
          msPerUnit: sampleMs,
          observedAt: now,
          units: deltaUnits,
        },
      ]
        .filter(sample => sample.observedAt >= now - horizonMs)
        .slice(-maxSamples)
      etaEstimatorRef.current = {
        phase,
        label,
        lastCompleted: completed,
        lastTimestamp: now,
        samples: nextSamples,
      }

      const totalSampleUnits = nextSamples.reduce(
        (sum, sample) => sum + sample.units,
        0,
      )
      const rollingAverageMs =
        totalSampleUnits > 0
          ? nextSamples.reduce(
              (sum, sample) => sum + sample.msPerUnit * sample.units,
              0,
            ) / totalSampleUnits
          : sampleMs
      setEtaSnapshot({
        phase,
        label,
        etaMs: remainingUnits > 0 ? rollingAverageMs * remainingUnits : 0,
        observedAt: now,
      })
      return
    }

    setEtaSnapshot((previous) =>
      previous == null
        ? {
            phase,
            label,
            etaMs: null,
            observedAt: now,
          }
        : previous,
    )
  }

  const copyText = async (text: string, label: string) => {
    try {
      await navigator.clipboard.writeText(text)
      await logseq.UI.showMsg(`${label} copied to clipboard.`, 'success')
    } catch (err: unknown) {
      logReadwiseError(formalSyncLogPrefix, 'failed to copy text', err)
      await logseq.UI.showMsg(
        `Copy failed: ${err instanceof Error ? err.message : String(err)}`,
        'error',
      )
    }
  }

  const formatTimestampForUi = (value: string | null | undefined) => {
    if (!value) return 'Not available'

    const parsed = new Date(value)
    if (Number.isNaN(parsed.getTime())) return value

    return format(parsed, 'yyyy-MM-dd HH:mm:ss')
  }

  const buildCacheSummaryBundle = (summary: ReaderSyncCacheSummaryV1) => {
    const state = summary.state

    return [
      'Reader Sync Cache Summary',
      `Database: ${summary.databaseName}`,
      `Graph ID: ${summary.graphId}`,
      `Parent documents cached: ${summary.parentDocumentCount}`,
      `Highlights cached: ${summary.highlightCount}`,
      `cache_state present: ${state ? 'yes' : 'no'}`,
      `State latestHighlightUpdatedAt: ${state?.latestHighlightUpdatedAt ?? 'null'}`,
      `State cachedAt: ${state?.cachedAt ?? 'null'}`,
      `State hasFullLibrarySnapshot: ${state?.hasFullLibrarySnapshot ?? 'null'}`,
      `State staleDeletionRisk: ${state?.staleDeletionRisk ?? 'null'}`,
      `State highlightCount: ${state?.highlightCount ?? 'null'}`,
    ].join('\n')
  }

  const uniqueStrings = (values: string[]) =>
    values.filter((value, index, array) => value.length > 0 && array.indexOf(value) === index)

  const normalizePropertyKey = (value: string): string =>
    value.toLowerCase().replace(/[^a-z0-9]/g, '')

  const readPropertyValue = (
    properties: Record<string, unknown> | undefined,
    expectedKey: string,
  ): unknown => {
    if (!properties) return null

    const normalizedExpected = normalizePropertyKey(expectedKey)

    for (const [key, value] of Object.entries(properties)) {
      if (normalizePropertyKey(key) === normalizedExpected) {
        return value
      }
    }

    return null
  }

  const extractStringValue = (value: unknown): string | null => {
    if (typeof value === 'string') {
      const trimmed = value.trim()
      return trimmed.length > 0 ? trimmed : null
    }

    if (typeof value === 'number' && Number.isFinite(value)) {
      return String(value)
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        const extracted = extractStringValue(item)
        if (extracted) return extracted
      }
    }

    return null
  }

  const sortReaderDocumentsByCreatedAtAscending = (
    left: { created_at: string },
    right: { created_at: string },
  ) => left.created_at.localeCompare(right.created_at)

  const collectPageAliases = (page: PageEntity): string[] =>
    uniqueStrings([
      typeof page.originalName === 'string' ? page.originalName : '',
      typeof page.title === 'string' ? page.title : '',
      typeof page.name === 'string' ? page.name : '',
    ])

  const resolvePreferredPageName = (page: PageEntity): string | null =>
    collectPageAliases(page)[0] ?? null

  const isManagedFormalPageName = (pageName: string) =>
    pageName === formalNamespaceRoot || pageName.startsWith(`${formalNamespaceRoot}/`)

  const extractReaderDocumentIdFromPage = (page: PageEntity): string | null =>
    extractStringValue(
      readPropertyValue(
        page.properties as Record<string, unknown> | undefined,
        'rw-reader-id',
      ),
    )

  const extractReaderDocumentIdsFromRootContent = (rootContent: string): string[] => {
    const readerDocumentIds: string[] = []

    for (const line of rootContent.split('\n')) {
      const match = line.match(/^:([^:\n]+):\s*(.*?)\s*$/)
      if (!match) continue

      const [, rawKey = '', rawValue = ''] = match
      if (normalizePropertyKey(rawKey) !== 'rwreaderid') continue

      const readerDocumentId = extractStringValue(rawValue)
      if (readerDocumentId) {
        readerDocumentIds.push(readerDocumentId)
      }
    }

    return uniqueStrings(readerDocumentIds)
  }

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

  const loadReaderDocumentIdFromPage = async (
    page: PageEntity,
  ): Promise<string | null> => {
    const directValue = extractReaderDocumentIdFromPage(page)
    if (directValue) return directValue
    if (!page.uuid) return null

    try {
      const blockPropertyValue = extractStringValue(
        await logseq.Editor.getBlockProperty(page.uuid, 'rw-reader-id'),
      )
      if (blockPropertyValue) return blockPropertyValue
    } catch {
      // Fall through to the legacy content drawer fallback.
    }

    try {
      const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
      const searchableContent = flattenPageBlocksTreeContent(pageBlocksTree ?? [])
      const readerDocumentIds =
        extractReaderDocumentIdsFromRootContent(searchableContent)

      return readerDocumentIds.length === 1 ? readerDocumentIds[0] ?? null : null
    } catch {
      return null
    }
  }

  const extractReaderHighlightIdsFromRootContent = (rootContent: string): string[] =>
    uniqueStrings(
      [...rootContent.matchAll(/\[\[https:\/\/read\.readwise\.io\/read\/([0-9a-z]+)\]\[View Highlight\]\]/gi)]
        .map((match) => match[1] ?? '')
        .filter((value) => value.length > 0),
    )

  const normalizeComparableText = (value: string | null | undefined) => {
    if (typeof value !== 'string') return null

    const normalized = value.replace(/\s+/g, ' ').trim().toLowerCase()
    return normalized.length > 0 ? normalized : null
  }

  const normalizeReaderCategory = (value: string | null | undefined) => {
    const normalized = normalizeComparableText(value)
    if (!normalized) return null

    const singular = normalized.endsWith('s') ? normalized.slice(0, -1) : normalized
    if (singular === 'article') return 'article'
    if (singular === 'book') return 'book'
    if (singular === 'tweet') return 'tweet'
    if (singular === 'video') return 'video'
    if (singular === 'podcast') return 'podcast'
    if (singular === 'supplemental') return 'supplemental'

    return singular
  }

  const unwrapOrgPropertyValue = (value: string | null | undefined) => {
    if (typeof value !== 'string') return null

    const trimmed = value.trim()
    if (trimmed.length === 0) return null

    const orgLinkMatch = trimmed.match(/^\[\[([^[\]]+)\]\[[^[\]]+\]\]$/)
    if (orgLinkMatch) return orgLinkMatch[1]?.trim() ?? null

    const simpleLinkMatch = trimmed.match(/^\[\[([^[\]]+)\]\]$/)
    if (simpleLinkMatch) return simpleLinkMatch[1]?.trim() ?? null

    return trimmed
  }

  const extractRootPropertyValue = (rootContent: string, key: string) => {
    const pattern = new RegExp(`^:${key}:\\s*(.+?)\\s*$`, 'im')
    const match = rootContent.match(pattern)
    return unwrapOrgPropertyValue(match?.[1] ?? null)
  }

  const extractManagedPageRepairSignals = ({
    pageName,
    rootContent,
  }: {
    pageName: string
    rootContent: string
  }): ManagedPageRepairSignals => {
    const pageTitle = pageName.startsWith(`${formalNamespaceRoot}/`)
      ? pageName.slice(formalNamespaceRoot.length + 1)
      : pageName
    const linkUrl = extractRootPropertyValue(rootContent, 'LINK')
    const author = extractRootPropertyValue(rootContent, 'AUTHOR')
    const category = extractRootPropertyValue(rootContent, 'CATEGORIES')

    return {
      pageTitle,
      normalizedPageTitle: normalizeComparableText(pageTitle),
      linkUrl,
      normalizedLinkUrl: normalizeComparableUrlV1(linkUrl),
      author,
      normalizedAuthor: normalizeComparableText(author),
      category,
      normalizedCategory: normalizeReaderCategory(category),
    }
  }

  const buildReaderDocumentRepairIdentityKey = (document: ReaderDocument) => {
    const normalizedUrl =
      normalizeComparableUrlV1(document.source_url) ??
      normalizeComparableUrlV1(document.url)
    if (normalizedUrl) {
      return `url:${normalizedUrl}`
    }

    const normalizedTitle = normalizeComparableText(document.title)
    if (!normalizedTitle) return null

    return [
      'meta',
      normalizedTitle,
      normalizeComparableText(document.author) ?? '',
      normalizeReaderCategory(document.category) ?? '',
    ].join('|')
  }

  const scoreReaderDocumentForRepairSignals = ({
    document,
    signals,
  }: {
    document: ReaderDocument
    signals: ManagedPageRepairSignals
  }): ReaderDocumentRepairMatch | null => {
    const reasons: string[] = []
    let score = 0

    const normalizedDocumentUrl =
      normalizeComparableUrlV1(document.source_url) ??
      normalizeComparableUrlV1(document.url)
    const normalizedDocumentTitle = normalizeComparableText(document.title)
    const normalizedDocumentAuthor = normalizeComparableText(document.author)
    const normalizedDocumentCategory = normalizeReaderCategory(document.category)

    if (
      signals.normalizedLinkUrl &&
      normalizedDocumentUrl &&
      signals.normalizedLinkUrl === normalizedDocumentUrl
    ) {
      score += 1000
      reasons.push('exact-url')
    }

    if (
      signals.normalizedPageTitle &&
      normalizedDocumentTitle &&
      signals.normalizedPageTitle === normalizedDocumentTitle
    ) {
      score += 300
      reasons.push('exact-title')
    }

    if (
      signals.normalizedAuthor &&
      normalizedDocumentAuthor &&
      signals.normalizedAuthor === normalizedDocumentAuthor
    ) {
      score += 90
      reasons.push('exact-author')
    }

    if (
      signals.normalizedCategory &&
      normalizedDocumentCategory &&
      signals.normalizedCategory === normalizedDocumentCategory
    ) {
      score += 40
      reasons.push('exact-category')
    }

    if (score === 0) return null

    return {
      document,
      score,
      reasons,
      identityKey: buildReaderDocumentRepairIdentityKey(document),
    }
  }

  const selectCanonicalReaderDocument = (documents: ReaderDocument[]) =>
    [...documents].sort((left, right) => {
      if (left.updated_at !== right.updated_at) {
        return right.updated_at.localeCompare(left.updated_at)
      }
      if (left.created_at !== right.created_at) {
        return right.created_at.localeCompare(left.created_at)
      }
      return left.id.localeCompare(right.id)
    })[0] ?? null

  const chooseStrongReaderDocumentMatch = (
    matches: ReaderDocumentRepairMatch[],
  ): ReaderDocument | null => {
    if (matches.length === 0) return null

    const exactUrlMatches = matches.filter((match) =>
      match.reasons.includes('exact-url'),
    )
    if (exactUrlMatches.length === 1) {
      return exactUrlMatches[0]?.document ?? null
    }
    if (exactUrlMatches.length > 1) {
      const uniqueIdentityKeys = uniqueStrings(
        exactUrlMatches
          .map((match) => match.identityKey ?? '')
          .filter((value) => value.length > 0),
      )
      if (uniqueIdentityKeys.length === 1) {
        return selectCanonicalReaderDocument(
          exactUrlMatches.map((match) => match.document),
        )
      }
      return null
    }

    const exactTitleMatches = matches.filter((match) =>
      match.reasons.includes('exact-title'),
    )
    const highConfidenceTitleMatches = exactTitleMatches.filter(
      (match) =>
        match.reasons.includes('exact-author') ||
        match.reasons.includes('exact-category'),
    )
    if (highConfidenceTitleMatches.length === 1) {
      return highConfidenceTitleMatches[0]?.document ?? null
    }
    if (highConfidenceTitleMatches.length > 1) {
      const uniqueIdentityKeys = uniqueStrings(
        highConfidenceTitleMatches
          .map((match) => match.identityKey ?? '')
          .filter((value) => value.length > 0),
      )
      if (uniqueIdentityKeys.length === 1) {
        return selectCanonicalReaderDocument(
          highConfidenceTitleMatches.map((match) => match.document),
        )
      }
    }

    return null
  }

  const hasLegacyTweetOnlyLinks = (rootContent: string) =>
    /\[\[[^\]]+\]\[View Tweet\]\]/i.test(rootContent) &&
    !/\[\[[^\]]+\]\[View Highlight\]\]/i.test(rootContent)

  const hasReaderHighlightLinks = (rootContent: string) =>
    /\[\[[^\]]+\]\[View Highlight\]\]/i.test(rootContent)

  const listReaderDocumentsWithRetry = async (
    client: ReturnType<typeof createReadwiseClient>,
    params: Parameters<ReturnType<typeof createReadwiseClient>['listReaderDocuments']>[0],
    logPrefix: string,
    context: {
      stage: 'repair-parent-scan' | 'repair-replacement-scan'
      pageName?: string
      documentId?: string
      pageCursor?: string | null
    },
  ) => {
    let lastError: unknown = null
    const totalAttempts = 4

    for (let attempt = 0; attempt < totalAttempts; attempt += 1) {
      try {
        return await client.listReaderDocuments(params)
      } catch (error) {
        lastError = error

        if (!isRetriableReaderListError(error) || attempt === totalAttempts - 1) {
          break
        }

        logReadwiseWarn(logPrefix, 'Reader API list request failed; retrying', {
          stage: context.stage,
          pageName: context.pageName ?? null,
          documentId: context.documentId ?? null,
          pageCursor: context.pageCursor ?? null,
          attempt: attempt + 1,
          totalAttempts,
          formattedError: describeUnknownError(error),
        })

        await sleep(1000 * 2 ** attempt)
      }
    }

    throw new Error(
      `Reader list request failed after ${totalAttempts} attempt(s). ${describeUnknownError(lastError)}`,
    )
  }

  const loadReaderParentDocumentsByIdsAuthoritative = async ({
    client,
    parentIds,
    previewCache,
    pageName,
    logPrefix,
  }: {
    client: ReturnType<typeof createReadwiseClient>
    parentIds: readonly string[]
    previewCache?: ReturnType<typeof createGraphReaderSyncCacheV1>
    pageName: string
    logPrefix: string
  }) => {
    const resolvedDocuments = await mapWithConcurrency(
      uniqueStrings(parentIds.filter((value) => value.length > 0)),
      3,
      async (parentId) => {
        throwIfCancelled()

        try {
          return await loadReaderParentDocumentByIdWithRetry(
            client,
            parentId,
            logPrefix,
          )
        } catch (error) {
          logReadwiseWarn(logPrefix, 'repair parent metadata lookup failed', {
            pageName,
            parentId,
            formattedError: describeUnknownError(error),
          })
          return null
        }
      },
    )

    const documents = resolvedDocuments.filter(
      (document): document is ReaderDocument => !!document,
    )

    if (documents.length > 0) {
      try {
        await previewCache?.putParentDocuments(documents)
      } catch {
        // Cache writes are best-effort during repair scan.
      }
    }

    return documents
  }

  const findReplacementReaderParentDocumentByApi = async ({
    client,
    pageName,
    rootContent,
    previewCache,
    logPrefix,
  }: {
    client: ReturnType<typeof createReadwiseClient>
    pageName: string
    rootContent: string
    previewCache?: ReturnType<typeof createGraphReaderSyncCacheV1>
    logPrefix: string
  }): Promise<ReaderParentReplacementLookupResult> => {
    const signals = extractManagedPageRepairSignals({
      pageName,
      rootContent,
    })
    if (!signals.normalizedLinkUrl && !signals.normalizedPageTitle) {
      logReadwiseDebug(logPrefix, 'repair replacement lookup skipped: insufficient page signals', {
        pageName,
      })
      return {
        document: null,
        shouldRetry: false,
      }
    }

    try {
      const matches: ReaderDocumentRepairMatch[] = []
      let pageCursor: string | null = null
      let scannedDocuments = 0

      do {
        throwIfCancelled()

        const response = await listReaderDocumentsWithRetry(
          client,
          {
            limit: 100,
            pageCursor: pageCursor ?? undefined,
            category: signals.normalizedCategory ?? undefined,
          },
          logPrefix,
          {
            stage: 'repair-replacement-scan',
            pageName,
            pageCursor,
          },
        )

        for (const document of response.results) {
          if (document.parent_id) continue
          const match = scoreReaderDocumentForRepairSignals({
            document,
            signals,
          })
          if (match) {
            matches.push(match)
          }
        }

        scannedDocuments += response.results.length
        pageCursor = response.nextPageCursor
      } while (pageCursor)

      const replacement = chooseStrongReaderDocumentMatch(matches)

      logReadwiseDebug(logPrefix, 'repair replacement lookup completed', {
        pageName,
        scannedDocuments,
        matchCount: matches.length,
        replacementReaderDocumentId: replacement?.id ?? null,
        replacementReason:
          replacement == null
            ? null
            : matches
                .find((match) => match.document.id === replacement.id)
                ?.reasons.join(', ') ?? null,
      })

      if (replacement) {
        try {
          await previewCache?.putParentDocuments([replacement])
        } catch {
          // Cache writes are best-effort during repair scan.
        }
      }

      return {
        document: replacement,
        shouldRetry: false,
      }
    } catch (error) {
      if (isRunCancelledError(error)) {
        throw error
      }

      logReadwiseWarn(logPrefix, 'repair replacement lookup failed', {
        pageName,
        formattedError: describeUnknownError(error),
      })

      return {
        document: null,
        shouldRetry: isRetriableReaderListError(error),
      }
    }
  }

  const inferReaderDocumentIdFromHighlights = async ({
    rootContent,
    pageName,
    previewCache,
    client,
    logPrefix,
  }: {
    rootContent: string
    pageName: string
    previewCache?: ReturnType<typeof createGraphReaderSyncCacheV1>
    client?: ReturnType<typeof createReadwiseClient>
    logPrefix?: string
  }): Promise<ReaderDocumentInferenceResult> => {
    throwIfCancelled()
    const resolvedLogPrefix = logPrefix ?? formalSyncLogPrefix
    const highlightIds = extractReaderHighlightIdsFromRootContent(rootContent)
    const signals = extractManagedPageRepairSignals({
      pageName,
      rootContent,
    })
    if (highlightIds.length === 0) {
      logReadwiseDebug(
        resolvedLogPrefix,
        'repair infer skipped: no highlight links found',
        { pageName },
      )
      return {
        readerDocumentId: null,
        shouldRetryAfterScan: false,
        failedHighlightIds: [],
      }
    }

    logReadwiseDebug(
      resolvedLogPrefix,
      'repair infer: extracted highlight ids from page',
      {
        pageName,
        highlightIds,
        highlightCount: highlightIds.length,
      },
    )

    const resolveUniqueParentId = (highlights: ReaderDocument[]) => {
      const parentIds = uniqueStrings(
        highlights
          .map((highlight) => highlight.parent_id ?? '')
          .filter((value) => value.length > 0),
      )

      return parentIds.length === 1 ? parentIds[0] ?? null : null
    }

    if (!client) {
      if (previewCache) {
        throwIfCancelled()
        try {
          const cachedHighlights = await previewCache.getCachedHighlightsByIds(
            highlightIds,
          )
          const cachedHighlightsList = [...cachedHighlights.values()]
          const cachedParentId = resolveUniqueParentId(cachedHighlightsList)

          if (cachedParentId) {
            logReadwiseDebug(
              resolvedLogPrefix,
              'repair infer: resolved parent from cached highlights without API client',
              {
                pageName,
                cachedParentId,
                cachedHighlightCount: cachedHighlightsList.length,
              },
            )
            return {
              readerDocumentId: cachedParentId,
              shouldRetryAfterScan: false,
              failedHighlightIds: [],
            }
          }
        } catch {
          // Fall through to null.
        }
      }

      logReadwiseDebug(
        resolvedLogPrefix,
        'repair infer failed: no Readwise client available for remote lookup',
        { pageName },
      )
      return {
        readerDocumentId: null,
        shouldRetryAfterScan: false,
        failedHighlightIds: [],
      }
    }

    const fetchedHighlights: ReaderDocument[] = []
    const failedHighlightIds: string[] = []
    let sawRetriableNetworkFailure = false
    const attemptedHighlightIds = highlightIds.slice(0, 12)

    const fetchedHighlightResults = await mapWithConcurrency(
      attemptedHighlightIds,
      3,
      async (highlightId) => {
        throwIfCancelled()
        try {
          return await loadReaderDocumentByIdWithRetry(
            client,
            highlightId,
            resolvedLogPrefix,
          )
        } catch (error) {
          failedHighlightIds.push(highlightId)
          if (isRetriableReaderListError(error)) {
            sawRetriableNetworkFailure = true
          }
          logReadwiseWarn(
            resolvedLogPrefix,
            'repair infer: highlight lookup failed; trying remaining highlight links',
            {
              pageName,
              highlightId,
              formattedError: describeUnknownError(error),
            },
          )
          return null
        }
      },
    )

    for (const highlight of fetchedHighlightResults) {
      if (highlight) {
        fetchedHighlights.push(highlight)
      }
    }

    throwIfCancelled()

    try {
      await previewCache?.putHighlights(fetchedHighlights)
    } catch {
      // Cache writes are best-effort during repair scan.
    }

    const fetchedParentId = resolveUniqueParentId(fetchedHighlights)
    if (fetchedParentId) {
      logReadwiseDebug(
        resolvedLogPrefix,
        'repair infer: resolved parent from remote highlights',
        {
          pageName,
          fetchedParentId,
          fetchedHighlightCount: fetchedHighlights.length,
          failedHighlightIds,
        },
      )

      return {
        readerDocumentId: fetchedParentId,
        shouldRetryAfterScan: false,
        failedHighlightIds,
      }
    }

    const fetchedParentIds = uniqueStrings(
      fetchedHighlights
        .map((highlight) => highlight.parent_id ?? '')
        .filter((value) => value.length > 0),
    )

    if (fetchedParentIds.length > 1) {
      const fetchedParentDocuments = await loadReaderParentDocumentsByIdsAuthoritative({
        client,
        parentIds: fetchedParentIds,
        previewCache,
        pageName,
        logPrefix: resolvedLogPrefix,
      })
      const parentMatches = fetchedParentDocuments
        .map((document) =>
          scoreReaderDocumentForRepairSignals({
            document,
            signals,
          }),
        )
        .filter((match): match is ReaderDocumentRepairMatch => !!match)
      const chosenParentDocument = chooseStrongReaderDocumentMatch(parentMatches)

      if (chosenParentDocument) {
        logReadwiseInfo(
          resolvedLogPrefix,
          'repair infer: resolved parent from multiple highlight parents via API metadata match',
          {
            pageName,
            resolvedReaderDocumentId: chosenParentDocument.id,
            candidateParentIds: fetchedParentIds,
            resolutionReasons:
              parentMatches
                .find((match) => match.document.id === chosenParentDocument.id)
                ?.reasons ?? [],
          },
        )

        return {
          readerDocumentId: chosenParentDocument.id,
          shouldRetryAfterScan: false,
          failedHighlightIds,
        }
      }
    }

    const replacementLookup = await findReplacementReaderParentDocumentByApi({
      client,
      pageName,
      rootContent,
      previewCache,
      logPrefix: resolvedLogPrefix,
    })
    const replacementDocument = replacementLookup.document

    if (replacementDocument) {
      logReadwiseInfo(
        resolvedLogPrefix,
        'repair infer: resolved parent from API replacement lookup',
        {
          pageName,
          resolvedReaderDocumentId: replacementDocument.id,
          fetchedParentIds,
          failedHighlightIds,
        },
      )

      return {
        readerDocumentId: replacementDocument.id,
        shouldRetryAfterScan: false,
        failedHighlightIds,
      }
    }

    logReadwiseDebug(
      resolvedLogPrefix,
      'repair infer failed: API relookup did not yield a repairable parent',
      {
        pageName,
        attemptedHighlightIds,
        fetchedHighlightIds: fetchedHighlights.map((highlight) => highlight.id),
        fetchedParentIds,
        failedHighlightIds,
      },
    )

    return {
      readerDocumentId: null,
      shouldRetryAfterScan:
        replacementLookup.shouldRetry ||
        (sawRetriableNetworkFailure && fetchedHighlights.length === 0),
      failedHighlightIds,
    }
  }

  const isManagedPageTitleOverlong = (pageTitle: string) => {
    try {
      assertManagedPageFileNameWithinLimits(pageTitle, 'org')
      return false
    } catch {
      return true
    }
  }

  const deleteManagedPageAuditEntry = async (
    entry: ManagedReaderPageAuditEntryV1,
  ) => {
    const aliases = uniqueStrings([entry.pageTitle, ...entry.aliases]).sort(
      (left, right) => right.length - left.length,
    )

    for (const alias of aliases) {
      try {
        await logseq.Editor.deletePage(alias)
        await sleep(500)
        return true
      } catch {
        // Keep trying aliases until the runtime accepts one.
      }
    }

    return false
  }

  const resolveSafeDuplicateReaderIdPages = async ({
    client,
    previewCache,
  }: {
    client: ReturnType<typeof createReadwiseClient>
    previewCache: ReturnType<typeof createGraphReaderSyncCacheV1>
  }): Promise<{
    resolvedGroups: number
    removedPages: number
  }> => {
    const auditResult = await auditManagedReaderPagesV1([formalNamespaceRoot])
    let resolvedGroups = 0
    let removedPages = 0

    for (const duplicateGroup of auditResult.duplicateReaderIds) {
      throwIfCancelled()
      const uniquePages = [
        ...new Map(
          duplicateGroup.pages.map((page) => [page.pageUuid, page] as const),
        ).values(),
      ]

      if (uniquePages.length < 2) {
        continue
      }

      let document =
        (
          await previewCache.getCachedParentDocuments([
            duplicateGroup.readerDocumentId,
          ])
        ).get(duplicateGroup.readerDocumentId) ?? null

      if (!document) {
        throwIfCancelled()
        document = await loadReaderParentDocumentByIdWithRetry(
          client,
          duplicateGroup.readerDocumentId,
          formalSyncLogPrefix,
        )

        if (document) {
          await previewCache.putParentDocuments([document])
        }
      }

      if (!document) {
        continue
      }

      const sourcePageTitle = document.title?.trim().length
        ? document.title
        : document.id
      const pageNamePlan = buildManagedPageNamePlanV1({
        pageTitle: sourcePageTitle,
        namespacePrefix: formalNamespaceRoot,
        managedId: duplicateGroup.readerDocumentId,
        format: 'org',
      })
      const canonicalNames = uniqueStrings([
        pageNamePlan.preferredPageName,
        pageNamePlan.disambiguatedPageName,
      ])
      const canonicalPages = uniquePages.filter((page) =>
        page.aliases.some((alias) => canonicalNames.includes(alias)) ||
        canonicalNames.includes(page.pageTitle),
      )
      const legacyPages = uniquePages.filter(
        (page) => !canonicalPages.some((candidate) => candidate.pageUuid === page.pageUuid),
      )

      if (canonicalPages.length !== 1 || legacyPages.length === 0) {
        continue
      }

      if (!legacyPages.every((page) => isManagedPageTitleOverlong(page.pageTitle))) {
        continue
      }

      let deletedAllLegacyPages = true

      for (const legacyPage of legacyPages) {
        throwIfCancelled()
        const deleted = await deleteManagedPageAuditEntry(legacyPage)
        if (!deleted) {
          deletedAllLegacyPages = false
          break
        }
        removedPages += 1
      }

      if (!deletedAllLegacyPages) {
        continue
      }

      resolvedGroups += 1
      logReadwiseInfo(
        formalSyncLogPrefix,
        'auto-resolved duplicate rw-reader-id pages',
        {
          readerDocumentId: duplicateGroup.readerDocumentId,
          keptPage: canonicalPages[0]?.pageTitle ?? null,
          removedPages: legacyPages.map((page) => page.pageTitle),
        },
      )
    }

    return {
      resolvedGroups,
      removedPages,
    }
  }

  const scanManagedPagesForRepairCandidates = async (options?: {
    onProgress?: (progress: {
      total: number
      completed: number
      pageName: string
    }) => void
    previewCache?: ReturnType<typeof createGraphReaderSyncCacheV1>
    client?: ReturnType<typeof createReadwiseClient>
  }): Promise<{
    scannedPages: number
    candidates: ManagedPageRepairCandidate[]
    issues: RunIssue[]
  }> => {
    const allPages = (await logseq.Editor.getAllPages()) ?? []
    const managedPages = allPages.filter((page) =>
      collectPageAliases(page as PageEntity).some((alias) =>
        isManagedFormalPageName(alias),
      ),
    ) as PageEntity[]
    const candidates: ManagedPageRepairCandidate[] = []
    const deferredIdentityRetries: ManagedPageRepairIdentityRetryCandidate[] = []
    const issues: RunIssue[] = []

    const scanResults = await mapWithConcurrency(
      managedPages,
      managedPageRepairScanConcurrency,
      async (page): Promise<ManagedPageRepairScanEntryResult> => {
        throwIfCancelled()
        const pageName =
          resolvePreferredPageName(page) ??
          page.originalName ??
          page.name ??
          page.title ??
          ''

        const inspection = await inspectManagedPageIntegrityV1(page)
        const signatures = inspection.signatures

        if (signatures.length === 0) {
          return { pageName }
        }

        throwIfCancelled()
        const pageReaderDocumentId = await loadReaderDocumentIdFromPage(page)
        const inferredReaderDocument = pageReaderDocumentId
          ? null
          : await inferReaderDocumentIdFromHighlights({
              pageName,
              rootContent: inspection.searchableContent,
              previewCache: options?.previewCache,
              client: options?.client,
              logPrefix: formalSyncLogPrefix,
            })
        const readerDocumentId =
          pageReaderDocumentId ?? inferredReaderDocument?.readerDocumentId ?? null

        if (!readerDocumentId) {
          if (inferredReaderDocument?.shouldRetryAfterScan) {
            return {
              pageName,
              deferredRetry: {
                pageName,
                rootContent: inspection.searchableContent,
                signatures,
              },
            }
          }

          const warningOnly =
            hasLegacyTweetOnlyLinks(inspection.searchableContent) ||
            !hasReaderHighlightLinks(inspection.searchableContent)
          return {
            pageName,
            issue: {
              book: pageName || 'Managed page',
              message: `Repair skipped because rw-reader-id is missing for ${pageName}.`,
              category: warningOnly ? 'warning' : undefined,
              summary:
                warningOnly
                  ? 'This legacy page has no Reader highlight links, so the plugin cannot infer a Reader document id for automatic repair.'
                  : 'This page matches the legacy corruption signature, but the plugin cannot map it back to a Reader document.',
              suggestedAction:
                warningOnly
                  ? 'Skip automatic repair for this page, or rebuild it later after binding a Reader document id.'
                  : 'Repair rw-reader-id first, or run Full Refresh after restoring the page identity.',
              debugFacts: [`detectedSignatures=${signatures.join(', ')}`],
              namespacePrefix: formalNamespaceRoot,
              pageName: pageName || null,
            },
          }
        }

        return {
          pageName,
          candidate: {
            pageName,
            readerDocumentId,
            signatures,
            rootContent: inspection.searchableContent,
          },
        }
      },
      (result, _index, completed) => {
        options?.onProgress?.({
          total: managedPages.length,
          completed,
          pageName: result.pageName,
        })
      },
    )

    for (const result of scanResults) {
      if (result.candidate) {
        candidates.push(result.candidate)
      }
      if (result.deferredRetry) {
        deferredIdentityRetries.push(result.deferredRetry)
      }
      if (result.issue) {
        issues.push(result.issue)
      }
    }

    if (deferredIdentityRetries.length > 0) {
      logReadwiseInfo(
        formalSyncLogPrefix,
        'retrying deferred managed page identity lookups after repair scan',
        {
          deferredPages: deferredIdentityRetries.length,
        },
      )

      await sleep(5000)

      const deferredResults = await mapWithConcurrency(
        deferredIdentityRetries,
        managedPageRepairScanConcurrency,
        async (
          deferredPage,
        ): Promise<ManagedPageRepairScanEntryResult> => {
          throwIfCancelled()

          const deferredInference = await inferReaderDocumentIdFromHighlights({
            pageName: deferredPage.pageName,
            rootContent: deferredPage.rootContent,
            previewCache: options?.previewCache,
            client: options?.client,
            logPrefix: formalSyncLogPrefix,
          })

          if (deferredInference.readerDocumentId) {
            return {
              pageName: deferredPage.pageName,
              candidate: {
                pageName: deferredPage.pageName,
                readerDocumentId: deferredInference.readerDocumentId,
                signatures: deferredPage.signatures,
                rootContent: deferredPage.rootContent,
              },
            }
          }

          return {
            pageName: deferredPage.pageName,
            issue: {
              book: deferredPage.pageName || 'Managed page',
              message: `Repair skipped because rw-reader-id is missing for ${deferredPage.pageName}.`,
              summary:
                'Automatic repair retried this page after a transient Reader lookup failure, but still could not map it back to a Reader document.',
              suggestedAction:
                'Retry repair later after the network stabilizes, or run Full Refresh after restoring the page identity.',
              debugFacts: [
                `detectedSignatures=${deferredPage.signatures.join(', ')}`,
                `deferredIdentityRetry=yes`,
                ...(deferredInference.failedHighlightIds.length > 0
                  ? [
                      `failedHighlightIds=${deferredInference.failedHighlightIds.join(', ')}`,
                    ]
                  : []),
              ],
              namespacePrefix: formalNamespaceRoot,
              pageName: deferredPage.pageName || null,
            },
          }
        },
      )

      for (const result of deferredResults) {
        if (result.candidate) {
          candidates.push(result.candidate)
        }
        if (result.issue) {
          issues.push(result.issue)
        }
      }
    }

    return {
      scannedPages: managedPages.length,
      candidates,
      issues,
    }
  }

  const resolveCurrentManagedReaderPage = async (): Promise<{
    page: PageEntity
    pageName: string
    readerDocumentId: string
  }> => {
    const currentPage = await logseq.Editor.getCurrentPage()
    if (!currentPage) {
      throw new Error('No current page is open.')
    }

    const pageName = uniqueStrings([
      typeof currentPage.originalName === 'string' ? currentPage.originalName : '',
      typeof currentPage.title === 'string' ? currentPage.title : '',
      typeof currentPage.name === 'string' ? currentPage.name : '',
    ])[0]

    if (!pageName) {
      throw new Error('Failed to resolve the current page name.')
    }

    if (!isManagedFormalPageName(pageName)) {
      throw new Error(`Current page is not a managed ${formalNamespaceRoot} page.`)
    }

    const readerDocumentId = await loadReaderDocumentIdFromPage(
      currentPage as PageEntity,
    )

    if (!readerDocumentId) {
      throw new Error('Current page does not contain rw-reader-id.')
    }

    return {
      page: currentPage as PageEntity,
      pageName,
      readerDocumentId,
    }
  }

  const resolveCurrentPageNameForExternalDiff = async () => {
    const currentPage = await logseq.Editor.getCurrentPage()
    if (!currentPage) {
      throw new Error('No current page is open.')
    }

    const pageName = uniqueStrings([
      typeof currentPage.originalName === 'string' ? currentPage.originalName : '',
      typeof currentPage.title === 'string' ? currentPage.title : '',
      typeof currentPage.name === 'string' ? currentPage.name : '',
    ])[0]

    if (!pageName) {
      throw new Error('Failed to resolve the current page name.')
    }

    return pageName
  }

  const resolvePluginRepoRootForExternalDiff = () => {
    try {
      const pathname = decodeURIComponent(new URL(window.location.href).pathname)
      if (pathname.endsWith('/dist/index.html')) {
        return pathname.slice(0, -'/dist/index.html'.length)
      }
      if (pathname.endsWith('/index.html')) {
        return pathname.slice(0, -'/index.html'.length)
      }
    } catch {
      // Fall through and let the copied command omit the cwd prefix.
    }

    return null
  }

  const buildExternalRawSnapshotCommand = async (mode: 'capture' | 'diff') => {
    const pageName = await resolveCurrentPageNameForExternalDiff()
    const graph = await logseq.App.getCurrentGraph()
    const graphPath = graph?.path
    const repoRoot = resolvePluginRepoRootForExternalDiff()
    const command = [
      'npm run diag:page-snapshot --',
      mode,
      '--page-name',
      JSON.stringify(pageName),
      ...(graphPath ? ['--graph-path', JSON.stringify(graphPath)] : []),
    ].join(' ')

    return repoRoot ? `cd ${JSON.stringify(repoRoot)} && ${command}` : command
  }

  const handleCopyExternalRawSnapshotCommand = async (mode: 'capture' | 'diff') => {
    try {
      const command = await buildExternalRawSnapshotCommand(mode)
      await copyText(
        command,
        mode === 'capture' ? 'Raw capture command' : 'Raw diff command',
      )
    } catch (err: unknown) {
      logReadwiseError(
        formalSyncLogPrefix,
        'failed to build external raw snapshot command',
        err,
      )
      setStatusMessage(
        `Command build failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const handleCopyExternalRawSnapshotWorkflow = async () => {
    try {
      const captureCommand = await buildExternalRawSnapshotCommand('capture')
      const diffCommand = await buildExternalRawSnapshotCommand('diff')
      await copyText(
        [captureCommand, '# do copy/embed in Logseq', diffCommand].join('\n'),
        'Raw snapshot workflow',
      )
    } catch (err: unknown) {
      logReadwiseError(
        formalSyncLogPrefix,
        'failed to build external raw snapshot workflow',
        err,
      )
      setStatusMessage(
        `Workflow build failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const buildPageDiffBundle = (result: CurrentPageDiffResult) =>
    [
      `${result.pageName} @ line ${result.firstDiffLine ?? '?'}`,
      `Source: ${result.source}${result.relativeFilePath ? ` (${result.relativeFilePath})` : ''}`,
      '',
      'Before Excerpt:',
      result.beforeExcerpt,
      '',
      'After Excerpt:',
      result.afterExcerpt,
      '',
      'Before Full Page:',
      result.beforeFullText,
      '',
      'After Full Page:',
      result.afterFullText,
    ].join('\n')

  const resetLiveRunIssueMetrics = () => {
    liveRunIssueMetricsRef.current = {
      processedItems: null,
      stats: {},
    }
  }

  const updateLiveRunIssueMetrics = ({
    processedItems,
    stats,
  }: {
    processedItems?: number | null
    stats?: Partial<NonNullable<RunIssueBundleContext['stats']>>
  }) => {
    liveRunIssueMetricsRef.current = {
      processedItems:
        processedItems === undefined
          ? liveRunIssueMetricsRef.current.processedItems
          : processedItems,
      stats: {
        ...liveRunIssueMetricsRef.current.stats,
        ...(stats ?? {}),
      },
    }
  }

  const buildLiveRunIssueContext = (): RunIssueBundleContext => {
    const isActiveRun = status === 'fetching' || status === 'syncing'

    return {
      modeLabel: runIssueContext?.modeLabel ?? 'Readwise sync',
      namespacePrefix: runIssueContext?.namespacePrefix ?? null,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage,
      startedAt: runIssueContext?.startedAt ?? null,
      completedAt: runIssueContext?.completedAt ?? null,
      targetDocuments:
        runIssueContext?.targetDocuments ??
        resolveConfiguredReaderFullScanTargetDocuments(),
      debugHighlightPageLimit:
        runIssueContext?.debugHighlightPageLimit ??
        resolveConfiguredReaderDebugHighlightPageLimit() ??
        0,
      processedItems: isActiveRun
        ? liveRunIssueMetricsRef.current.processedItems ?? current
        : runIssueContext?.processedItems ?? current,
      issuesCount: errors.length,
      stats: isActiveRun
        ? {
            ...(runIssueContext?.stats ?? {}),
            ...liveRunIssueMetricsRef.current.stats,
          }
        : runIssueContext?.stats,
    }
  }

  const clearRunIssues = (options?: {
    preservePendingLegacyBlockRefMigration?: boolean
    preservePendingCurrentPageLegacyIdMigration?: boolean
  }) => {
    setErrors([])
    setShowWarningIssues(false)
    setRunIssueContext(null)
    setCurrentPageLegacyIdPreviewResult(null)
    setCurrentPageLegacyIdApplyResult(null)
    if (!options?.preservePendingLegacyBlockRefMigration) {
      setPendingLegacyBlockRefMigration(null)
    }
    if (!options?.preservePendingCurrentPageLegacyIdMigration) {
      setPendingCurrentPageLegacyIdMigration(null)
    }
    resetLiveRunIssueMetrics()
  }

  const replaceRunIssues = (issues: RunIssue[]) => {
    setErrors(issues.map((issue) => diagnoseRunIssue(issue)))
  }

  const appendRunIssue = (issue: RunIssue) => {
    const diagnosedIssue = diagnoseRunIssue(issue)

    setErrors((previous) => {
      const alreadyPresent = previous.some(
        (existingIssue) =>
          existingIssue.book === diagnosedIssue.book &&
          existingIssue.message === diagnosedIssue.message,
      )

      return alreadyPresent ? previous : [...previous, diagnosedIssue]
    })
  }

  const handleCopyRunIssueBundle = async () => {
    if (errors.length === 0) return

    await copyText(
      buildRunIssuesBundle({
        issues: errors,
        context: buildLiveRunIssueContext(),
      }),
      'Run issue bundle',
    )
  }

  const handleCopyRunIssueBundleWithoutWarnings = async () => {
    if (errors.length === 0) return

    await copyText(
      buildRunIssuesBundle({
        issues: errors,
        context: buildLiveRunIssueContext(),
        includeWarnings: false,
      }),
      'Run issue bundle (errors only)',
    )
  }

  const handleCopyCurrentPageLegacyIdPreviewBundle = async () => {
    if (!currentPageLegacyIdPreviewResult) return

    await copyText(
      buildRunIssuesBundle({
        issues: buildCurrentPageLegacyIdPreviewIssues({
          pageName: currentPageLegacyIdPreviewResult.pageName,
          relativeFilePath: currentPageLegacyIdPreviewResult.relativeFilePath,
          fileKind: currentPageLegacyIdPreviewResult.fileKind,
          rewrites: currentPageLegacyIdPreviewResult.rewrites,
        }),
        context: buildLiveRunIssueContext(),
      }),
      'Current-page legacy id preview',
    )
  }

  const handleSetupProps = async () => {
    const result = await setupProps()
    if (!result.success) return

    setPropsReady(true)
    setStatusMessage(
      result.compatibilityMode
        ? 'Setup completed in compatibility mode. Incremental Sync is available now.'
        : 'Schema setup completed. Incremental Sync is available now.',
    )
  }

  const handleCancel = () => {
    cancelledRef.current = true
    retryActionRef.current = null
    setStatus('idle')
    setStatusMessage('Sync cancelled.')
    setCurrentBook('')
  }

  const createRunCancelledError = () => {
    const error = new Error('Sync cancelled.')
    error.name = 'ReadwiseRunCancelledError'
    return error
  }

  const isRunCancelledError = (error: unknown): error is Error =>
    error instanceof Error && error.name === 'ReadwiseRunCancelledError'

  const throwIfCancelled = () => {
    if (cancelledRef.current) {
      throw createRunCancelledError()
    }
  }

  const mapWithConcurrency = async <TItem, TResult>(
    items: TItem[],
    concurrency: number,
    mapper: (item: TItem, index: number) => Promise<TResult>,
    onSettled?: (result: TResult, index: number, completed: number) => void,
  ) => {
    const results = new Array<TResult>(items.length)
    let nextIndex = 0
    let completed = 0

    const worker = async () => {
      while (true) {
        throwIfCancelled()
        const currentIndex = nextIndex
        nextIndex += 1

        if (currentIndex >= items.length) {
          return
        }

        const result = await mapper(items[currentIndex]!, currentIndex)
        results[currentIndex] = result
        completed += 1
        onSettled?.(result, currentIndex, completed)
      }
    }

    await Promise.all(
      Array.from(
        { length: Math.max(1, Math.min(concurrency, items.length)) },
        () => worker(),
      ),
    )

    return results
  }

  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      window.setTimeout(resolve, ms)
    })

  const isRetriableReaderListError = (error: unknown) => {
    const message = describeUnknownError(error)

    return (
      error instanceof TypeError ||
      /Failed to fetch|NetworkError|ERR_CONNECTION_CLOSED|ERR_CONNECTION_RESET|ERR_NETWORK_CHANGED|ERR_INTERNET_DISCONNECTED|ERR_TUNNEL_CONNECTION_FAILED|ERR_TUNNEL_CONNECTION_RESET|fetch/i.test(
        message,
      )
    )
  }

  const loadReaderDocumentByIdWithRetry = async (
    client: ReturnType<typeof createReadwiseClient>,
    documentId: string,
    logPrefix: string,
  ) => {
    let lastError: unknown = null
    const totalAttempts = 4

    for (let attempt = 0; attempt < totalAttempts; attempt += 1) {
      try {
        const response = await client.listReaderDocuments({
          id: documentId,
          limit: 1,
        })
        return response.results[0] ?? null
      } catch (error) {
        lastError = error

        if (!isRetriableReaderListError(error) || attempt === totalAttempts - 1) {
          break
        }

        logReadwiseWarn(
          logPrefix,
          'Reader document fetch failed; retrying',
          {
            documentId,
            attempt: attempt + 1,
            totalAttempts,
            formattedError: describeUnknownError(error),
          },
        )

        await sleep(1000 * 2 ** attempt)
      }
    }

    throw new Error(
      `Reader document fetch for ${documentId} failed after ${totalAttempts} attempt(s). ${describeUnknownError(lastError)}`,
    )
  }

  const loadReaderParentDocumentByIdWithRetry = async (
    client: ReturnType<typeof createReadwiseClient>,
    parentId: string,
    logPrefix: string,
  ) => loadReaderDocumentByIdWithRetry(client, parentId, logPrefix)

  const formatDuration = (milliseconds: number) => {
    const totalSeconds = Math.max(0, Math.round(milliseconds / 1000))
    const hours = Math.floor(totalSeconds / 3600)
    const minutes = Math.floor((totalSeconds % 3600) / 60)
    const seconds = totalSeconds % 60

    if (hours > 0) {
      return `${String(hours).padStart(2, '0')}h ${String(minutes).padStart(2, '0')}m`
    }

    return `${String(minutes).padStart(2, '0')}m ${String(seconds).padStart(2, '0')}s`
  }

  const buildFormalRunKind = (
    mode: ReaderSyncMode,
  ): GraphLastFormalSyncSummaryV1['runKind'] => {
    if (mode === 'incremental-window') return 'reader_incremental'
    if (mode === 'cached-full-rebuild') return 'reader_cached_rebuild'
    if (mode === 'snapshot-only-refresh') return 'reader_snapshot_refresh'
    return 'reader_full_scan'
  }

  const buildReaderSyncUpdatedAfterSummary = (
    updatedAfter: string | null | undefined,
  ) => (updatedAfter ? `updated after ${updatedAfter}` : 'full library')

  const shouldAdvanceReaderSyncCursor = ({
    mode,
    blockingSyncErrorsForRun,
    debugHighlightPageLimit,
    targetDocuments,
    loadStats,
  }: {
    mode: ReaderSyncMode
    blockingSyncErrorsForRun: Array<{ book: string; message: string }>
    debugHighlightPageLimit: number | null
    targetDocuments: number | null
    loadStats: ReaderPreviewLoadStats
  }) => {
    if (mode === 'snapshot-only-refresh') return false
    if (mode === 'cached-full-rebuild') return false
    if (blockingSyncErrorsForRun.length > 0) return false
    if (debugHighlightPageLimit != null) return false
    if (mode === 'incremental-window') return true
    if (targetDocuments == null) return true

    return targetDocuments >= loadStats.parentDocumentsIdentified
  }

  const exportHighlightsWithRetry = async (
    client: ReturnType<typeof createReadwiseClient>,
    params: ExportParams,
    context: 'formal-test-books' | 'sync',
  ) => {
    let lastError: unknown = null

    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        return await client.exportHighlights(params)
      } catch (error) {
        lastError = error
        const message = error instanceof Error ? error.message : String(error)
        const isRetriable =
          error instanceof TypeError ||
          /Failed to fetch|NetworkError|ERR_CONNECTION_CLOSED/i.test(message)

        if (!isRetriable || attempt === 2) {
          throw error
        }

        logReadwiseWarn(formalSyncLogPrefix, 'transient export fetch failed; retrying', {
          context,
          attempt: attempt + 1,
          params,
          message,
        })
        setStatusMessage(
          `Readwise request failed (${message}). Retrying ${attempt + 1}/2...`,
        )
        await sleep(1000 * (attempt + 1))
      }
    }

    throw lastError instanceof Error ? lastError : new Error(String(lastError))
  }

  const resolveConfiguredSyncMaxBooks = () => {
    const rawDebugSyncMaxBooks = Number(logseq.settings?.debugSyncMaxBooks ?? 20)
    return Number.isFinite(rawDebugSyncMaxBooks) && rawDebugSyncMaxBooks > 0
      ? Math.floor(rawDebugSyncMaxBooks)
      : null
  }

  const resolveConfiguredReaderFullScanTargetDocuments = () => {
    const rawSetting = logseq.settings?.readerFullScanTargetDocuments
    if (rawSetting == null || rawSetting === '') {
      return defaultReaderFullScanTargetDocuments
    }

    const rawTargetDocuments = Number(rawSetting)
    if (!Number.isFinite(rawTargetDocuments)) {
      return defaultReaderFullScanTargetDocuments
    }

    if (rawTargetDocuments === 0) {
      return null
    }

    return rawTargetDocuments > 0
      ? Math.floor(rawTargetDocuments)
      : defaultReaderFullScanTargetDocuments
  }

  const resolveConfiguredReaderDebugHighlightPageLimit = () => {
    const rawHighlightPageLimit = Number(
      logseq.settings?.readerFullScanDebugHighlightPageLimit ??
        defaultReaderFullScanDebugHighlightPageLimit,
    )
    return Number.isFinite(rawHighlightPageLimit) && rawHighlightPageLimit > 0
      ? Math.floor(rawHighlightPageLimit)
      : null
  }

  const loadFormalTestBooks = async () => {
    const token = logseq.settings?.apiToken as string
    if (!token) {
      throw new Error('No API token configured. Set it in plugin settings.')
    }

    const activeFormalTestSession = await loadActiveFormalTestSessionManifestV1()
    if (activeFormalTestSession && activeFormalTestSession.books.length > 0) {
      const frozenBooks: ExportedBookIdentity[] = activeFormalTestSession.books.map(
        (book) => ({
          user_book_id: book.userBookId,
          title: book.title,
        }),
      )

      setTotal(frozenBooks.length)
      setStatusMessage(
        `Using active formal test session (${frozenBooks.length} frozen book(s)).`,
      )
      logReadwiseInfo(sessionTestLogPrefix, 'using active formal test session for formal page selection', {
        sessionId: activeFormalTestSession.sessionId,
        namespacePrefix: formalNamespaceRoot,
        updatedAfter: activeFormalTestSession.updatedAfter,
        maxBooks: activeFormalTestSession.maxBooks,
        frozenBooks: frozenBooks.length,
      })

      return {
        books: frozenBooks,
        checkpointBeforeRun: await loadGraphCheckpointStateV1(),
        updatedAfter: activeFormalTestSession.updatedAfter,
        maxBooks: activeFormalTestSession.maxBooks,
      }
    }

    const checkpointBeforeRun = await loadGraphCheckpointStateV1()
    const updatedAfter = checkpointBeforeRun?.updatedAfter ?? undefined
    const maxBooks = resolveConfiguredSyncMaxBooks()
    const client = createReadwiseClient(token)
    const books: ExportedBookIdentity[] = []
    let cursor: string | null = null

    logReadwiseInfo(sessionTestLogPrefix, 'loading formal test books', {
      namespacePrefix: formalNamespaceRoot,
      updatedAfter: updatedAfter ?? null,
      maxBooks,
    })

    do {
      const params: ExportParams = {}
      if (updatedAfter) params.updatedAfter = updatedAfter
      if (cursor) params.pageCursor = cursor

      const page: ExportResponse = await exportHighlightsWithRetry(
        client,
        params,
        'formal-test-books',
      )
      books.push(
        ...page.results.map((book) => ({
          user_book_id: book.user_book_id,
          title: book.title,
        })),
      )

      if (maxBooks != null && books.length > maxBooks) {
        books.length = maxBooks
      }

      setTotal(books.length)
      setStatusMessage(
        maxBooks != null
          ? `Fetched ${books.length} / ${maxBooks} formal test book(s) so far...`
          : `Fetched ${books.length} formal test book(s) so far...`,
      )

      if (maxBooks != null && books.length >= maxBooks) {
        cursor = null
        break
      }

      cursor = page.nextPageCursor
    } while (cursor)

    return {
      books,
      checkpointBeforeRun,
      updatedAfter: updatedAfter ?? null,
      maxBooks,
    }
  }

  const handleBackupFormalTestPages = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage('Resolving formal test pages to back up...')

    try {
      const { books, updatedAfter, maxBooks } = await loadFormalTestBooks()

      if (books.length === 0) {
        setStatus('completed')
        setStatusMessage('No formal test pages matched the current sync window.')
        return
      }

      setStatus('syncing')
      setCurrent(0)
      setTotal(books.length)
      setStatusMessage(
        `Backing up ${books.length} formal test page(s) to plugin storage, then deleting originals...`,
      )

      const sessionId = format(new Date(), 'yyyyMMdd-HHmmss')
      const formalTestNamespacePrefix = `${formalNamespaceRoot}/${sessionId}`
      const backupStoragePrefix = `formal-page-backups/${sessionId}`
      await saveFormalTestSessionManifestV1({
        schemaVersion: 1,
        sessionId,
        createdAt: new Date().toISOString(),
        graphName: (await logseq.App.getCurrentGraph())?.name ?? null,
        graphPath: (await logseq.App.getCurrentGraph())?.path ?? null,
        namespacePrefix: formalTestNamespacePrefix,
        updatedAfter,
        maxBooks,
        books: books.map((book) => ({
          userBookId: book.user_book_id,
          title: book.title,
        })),
        backupStoragePrefix,
      })

      const result = await backupFormalTestPages(books, formalNamespaceRoot, {
        storagePrefix: backupStoragePrefix,
        onProgress: ({ phase, total, completed, pageTitle }) => {
          if (phase === 'start') {
            setStatus('syncing')
            setCurrent(0)
            setTotal(total)
            setCurrentBook('')
            setStatusMessage(
              `Backing up ${total} formal test page(s) to plugin storage, then deleting originals...`,
            )
            return
          }

          setStatus('syncing')
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageTitle ?? '')
          setStatusMessage(`Backing up ${completed} / ${total} formal test page(s)...`)
        },
      })
      await refreshActiveFormalTestSessionCount()
      setCurrent(books.length)

      if (result.skippedPages.length > 0) {
        replaceRunIssues(
          result.skippedPages.map((pageTitle) => ({
            book: pageTitle,
            message:
              'Skipped because the source file path could not be resolved or page deletion failed.',
          })),
        )
      }

      setStatus('completed')
      setStatusMessage(
        result.touchedPages > 0
          ? `Backed up and removed ${result.touchedPages} formal test page(s) to ${result.backupDirectory}. Formal test session is now active in ${formalTestNamespacePrefix}.`
          : 'No formal test pages were backed up.',
      )
      logReadwiseInfo(backupLogPrefix, 'backed up formal test pages', {
        sessionId,
        formalTestNamespacePrefix,
        targetedBooks: result.targetedBooks,
        matchedPages: result.matchedPages,
        backedUpPages: result.touchedPages,
        skippedPages: result.skippedPages,
        backupDirectory: result.backupDirectory,
        namespacePrefix: formalNamespaceRoot,
        updatedAfter,
        maxBooks,
      })
    } catch (err: unknown) {
      logReadwiseError(backupLogPrefix, 'failed to back up formal test pages', err)
      setStatus('error')
      setStatusMessage(
        `Backup failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const handleClearFormalTestPages = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage('Resolving formal test pages to delete...')

    try {
      const { books, updatedAfter, maxBooks } = await loadFormalTestBooks()

      if (books.length === 0) {
        setStatus('completed')
        setStatusMessage('No formal test pages matched the current sync window.')
        return
      }

      setStatus('syncing')
      setCurrent(0)
      setTotal(books.length)
      setStatusMessage(`Deleting ${books.length} formal test page(s)...`)

      const result = await clearFormalTestPages(books, formalNamespaceRoot, {
        onProgress: ({ phase, total, completed, pageTitle }) => {
          if (phase === 'start') {
            setStatus('syncing')
            setCurrent(0)
            setTotal(total)
            setCurrentBook('')
            setStatusMessage(`Deleting ${total} formal test page(s)...`)
            return
          }

          setStatus('syncing')
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageTitle ?? '')
          setStatusMessage(`Deleting ${completed} / ${total} formal test page(s)...`)
        },
      })

      if (result.skippedPages.length > 0) {
        replaceRunIssues(
          result.skippedPages.map((pageTitle) => ({
            book: pageTitle,
            message: 'Delete failed for this formal test page.',
          })),
        )
      }

      setStatus('completed')
      setStatusMessage(
        result.touchedPages > 0
          ? `Deleted ${result.touchedPages} formal test page(s).`
          : 'No formal test pages were deleted.',
      )
      logReadwiseInfo(sessionTestLogPrefix, 'cleared formal test pages', {
        targetedBooks: result.targetedBooks,
        matchedPages: result.matchedPages,
        deletedPages: result.touchedPages,
        skippedPages: result.skippedPages,
        namespacePrefix: formalNamespaceRoot,
        updatedAfter,
        maxBooks,
      })
    } catch (err: unknown) {
      logReadwiseError(sessionTestLogPrefix, 'failed to clear formal test pages', err)
      setStatus('error')
      setStatusMessage(
        `Clear failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const handleRestoreTestPages = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage('Restoring the latest formal test page backup...')

    try {
      const result = await restoreLatestFormalTestPageBackup({
        onProgress: ({ phase, total, completed, pageTitle }) => {
          if (phase === 'start') {
            setStatus('syncing')
            setCurrent(0)
            setTotal(total)
            setCurrentBook('')
            setStatusMessage(
              total > 0
                ? `Restoring ${total} formal test page(s)...`
                : 'Restoring the latest formal test page backup...',
            )
            return
          }

          setStatus('syncing')
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageTitle ?? '')
          setStatusMessage(
            total > 0
              ? `Restoring ${completed} / ${total} formal test page(s)...`
              : 'Restoring the latest formal test page backup...',
          )
        },
      })
      await refreshActiveFormalTestSessionCount()

      if (result.targetedBooks === 0) {
        setStatus('completed')
        setStatusMessage('No formal test page backup was found.')
        return
      }

      if (result.skippedPages.length > 0 || result.failedPages.length > 0) {
        replaceRunIssues([
          ...result.skippedPages.map((pageTitle) => ({
            book: pageTitle,
            message: 'Restore skipped because backup content was empty.',
          })),
          ...result.failedPages.map((failedPage) => ({
            book: failedPage.pageTitle,
            message: `Restore failed after retries: ${failedPage.message}`,
          })),
        ])
      }

      setStatus('completed')
      setStatusMessage(
        result.failedPages.length > 0
          ? `Restored ${result.touchedPages} formal test page(s), but ${result.failedPages.length} page(s) failed. Active formal test session was kept for retry.`
        : result.touchedPages > 0
          ? `Restored ${result.touchedPages} formal test page(s) from ${result.backupDirectory}.`
          : 'No formal test pages were restored.',
      )
      logReadwiseInfo(restoreLogPrefix, 'restored formal test pages', {
        targetedBackups: result.targetedBooks,
        matchedPages: result.matchedPages,
        restoredPages: result.touchedPages,
        skippedPages: result.skippedPages,
        failedPages: result.failedPages,
        backupDirectory: result.backupDirectory,
      })
    } catch (err: unknown) {
      logReadwiseError(restoreLogPrefix, 'failed to restore formal test pages', err)
      setStatus('error')
      setStatusMessage(
        `Restore failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const handleClearSessionTestPages = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage('Resolving active session test pages to delete...')

    try {
      const activeFormalTestSession = await loadActiveFormalTestSessionManifestV1()

      setStatus('syncing')
      setCurrent(0)
      setTotal(0)
      setStatusMessage(
        `Deleting session test page(s) under ${formalNamespaceRoot}/<run-id>/...`,
      )

      const result = await clearManagedPagesBySessionNamespaceRoot(formalNamespaceRoot, {
        onProgress: ({ phase, total, completed, pageTitle }) => {
          if (phase === 'start') {
            setStatus('syncing')
            setCurrent(0)
            setTotal(total)
            setCurrentBook('')
            setStatusMessage(
              total > 0
                ? `Deleting ${total} session test page(s)...`
                : `Deleting session test page(s) under ${formalNamespaceRoot}/<run-id>/...`,
            )
            return
          }

          setStatus('syncing')
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageTitle ?? '')
          setStatusMessage(
            total > 0
              ? `Deleting ${completed} / ${total} session test page(s)...`
              : `Deleting session test page(s) under ${formalNamespaceRoot}/<run-id>/...`,
          )
        },
      })
      const rotatedFormalTestSession = activeFormalTestSession
        ? await rotateActiveFormalTestSessionNamespaceV1()
        : null
      await refreshActiveFormalTestSessionCount()
      setCurrent(result.touchedPages)
      setTotal(result.matchedPages)

      if (result.skippedPages.length > 0) {
        replaceRunIssues(
          result.skippedPages.map((pageTitle) => ({
            book: pageTitle,
            message: 'Delete failed for this session test page.',
          })),
        )
      }

      setStatus('completed')
      setStatusMessage(
        result.touchedPages > 0
          ? `Deleted ${result.touchedPages} session test page(s) under ${formalNamespaceRoot}/<run-id>/.${rotatedFormalTestSession ? ` Next session test run will use ${rotatedFormalTestSession.namespacePrefix}.` : ''}`
          : rotatedFormalTestSession
            ? `No session test pages were deleted. Next session test run will use ${rotatedFormalTestSession.namespacePrefix}.`
            : 'No session test pages were deleted.',
      )
      logReadwiseInfo(sessionTestLogPrefix, 'cleared session test pages', {
        sessionId: activeFormalTestSession?.sessionId ?? null,
        nextSessionId: rotatedFormalTestSession?.sessionId ?? null,
        nextNamespacePrefix: rotatedFormalTestSession?.namespacePrefix ?? null,
        targetedBooks: result.targetedBooks,
        matchedPages: result.matchedPages,
        deletedPages: result.touchedPages,
        skippedPages: result.skippedPages,
        namespaceRoot: formalNamespaceRoot,
      })
    } catch (err: unknown) {
      logReadwiseError(sessionTestLogPrefix, 'failed to clear session test pages', err)
      setStatus('error')
      setStatusMessage(
        `Clear failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const handleClearDebugPages = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage(`Deleting ${debugNamespaceRoot} pages...`)

    try {
      const result = await clearManagedPagesByNamespacePrefix(debugNamespaceRoot, {
        onProgress: ({ phase, total, completed, pageTitle }) => {
          if (phase === 'start') {
            setStatus('syncing')
            setCurrent(0)
            setTotal(total)
            setCurrentBook('')
            setStatusMessage(
              total > 0
                ? `Deleting ${total} debug page(s)...`
                : `Deleting ${debugNamespaceRoot} pages...`,
            )
            return
          }

          setStatus('syncing')
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageTitle ?? '')
          setStatusMessage(
            total > 0
              ? `Deleting ${completed} / ${total} debug page(s)...`
              : `Deleting ${debugNamespaceRoot} pages...`,
          )
        },
      })

      setStatus('completed')
      setStatusMessage(`Deleted ${result.touchedPages} debug page(s).`)
      logReadwiseInfo(debugLogPrefix, 'cleared debug pages', {
        namespacePrefix: debugNamespaceRoot,
        matchedPages: result.matchedPages,
        deletedPages: result.touchedPages,
        skippedPages: result.skippedPages,
      })
    } catch (err: unknown) {
      logReadwiseError(debugLogPrefix, 'failed to clear debug pages', err)
      setStatus('error')
      setStatusMessage(
        `Failed to clear debug pages: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const handleCaptureCurrentPageSnapshot = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCacheSummaryResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage('Capturing current page file snapshot...')

    try {
      const result = await captureCurrentPageFileSnapshotV1()
      setStatus('completed')
      setStatusMessage(
        `Captured snapshot for ${result.pageName} via ${result.source} (${result.lineCount} lines).`,
      )
    } catch (err: unknown) {
      logReadwiseError(formalSyncLogPrefix, 'failed to capture current page snapshot', err)
      setStatus('error')
      setStatusMessage(
        `Snapshot failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const handleDiffCurrentPageSnapshot = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCacheSummaryResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage('Diffing current page file against the saved snapshot...')

    try {
      const result = await diffCurrentPageFileSnapshotV1()

      if (!result.changed) {
        setStatus('completed')
        setStatusMessage(
          `No page ${result.source} changes detected for ${result.pageName}.`,
        )
        return
      }

      setPageDiffResult(result)
      setStatus('completed')
      setStatusMessage(result.summary)
    } catch (err: unknown) {
      logReadwiseError(formalSyncLogPrefix, 'failed to diff current page snapshot', err)
      setStatus('error')
      setStatusMessage(
        `Diff failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const runSync = async ({
    ignoreCheckpoint = false,
    namespacePrefix = null,
    renderedDebugPages = false,
    maxBooksOverride = null,
    pageNameMode = 'flat',
  }: {
    ignoreCheckpoint?: boolean
    namespacePrefix?: string | null
    renderedDebugPages?: boolean
    maxBooksOverride?: number | null
    pageNameMode?: 'flat' | 'namespace'
  } = {}) => {
    const token = logseq.settings?.apiToken as string
    if (!token) {
      setStatus('error')
      setStatusMessage('No API token configured. Set it in plugin settings.')
      return
    }

    cancelledRef.current = false
    retryActionRef.current = null
    clearRunIssues()
    setPageDiffResult(null)
    setCacheSummaryResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    const startedAt = new Date().toISOString()
    setStatusMessage('Fetching highlights from Readwise...')

    const client = createReadwiseClient(token)
    const allBooks: ExportedBook[] = []
    const syncErrorsForRun: RunIssue[] = []
    let cursor: string | null = null
    const checkpointBeforeRun = await loadGraphCheckpointStateV1()
    const activeFormalTestSession = renderedDebugPages
      ? null
      : await loadActiveFormalTestSessionManifestV1()
    const updatedAfter = ignoreCheckpoint
      ? undefined
      : checkpointBeforeRun?.updatedAfter ?? undefined
    const syncLimitMaxBooks =
      typeof maxBooksOverride === 'number' && maxBooksOverride > 0
        ? Math.floor(maxBooksOverride)
        : null
    const effectiveNamespacePrefix =
      activeFormalTestSession?.namespacePrefix ??
      namespacePrefix ??
      (renderedDebugPages ? debugNamespaceRoot : formalNamespaceRoot)
    const formalTestBookIds = activeFormalTestSession?.books.map(
      (book) => book.userBookId,
    ) ?? null
    const syncLogPrefix =
      renderedDebugPages
        ? debugLogPrefix
        : activeFormalTestSession || syncLimitMaxBooks != null
          ? sessionTestLogPrefix
          : formalSyncLogPrefix
    setRunIssueContext({
      modeLabel: renderedDebugPages
        ? 'Debug sync'
        : activeFormalTestSession || syncLimitMaxBooks != null
          ? 'Session test sync'
          : 'Legacy sync',
      namespacePrefix: effectiveNamespacePrefix,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage: '',
      startedAt,
      completedAt: null,
      targetDocuments: syncLimitMaxBooks,
      debugHighlightPageLimit: null,
      processedItems: 0,
      issuesCount: 0,
    })
    logReadwiseInfo(syncLogPrefix, 'starting sync')
    logReadwiseDebug(syncLogPrefix, 'checkpointBeforeRun', checkpointBeforeRun)
    logReadwiseDebug(syncLogPrefix, 'updatedAfter', updatedAfter ?? null)
    logReadwiseDebug(syncLogPrefix, 'syncLimitMaxBooks', syncLimitMaxBooks)
    logReadwiseDebug(syncLogPrefix, 'ignoreCheckpoint', ignoreCheckpoint)
    logReadwiseDebug(syncLogPrefix, 'namespacePrefix', effectiveNamespacePrefix)
    logReadwiseDebug(syncLogPrefix, 'renderedDebugPages', renderedDebugPages)
    logReadwiseDebug(syncLogPrefix, 'pageNameMode', pageNameMode)
    logReadwiseDebug(
      syncLogPrefix,
      'formalTestSessionId',
      activeFormalTestSession?.sessionId ?? null,
    )

    try {
      do {
        if (cancelledRef.current) return

        const params: ExportParams = {}
        if (formalTestBookIds && formalTestBookIds.length > 0) {
          params.ids = formalTestBookIds
        } else if (updatedAfter) {
          params.updatedAfter = updatedAfter
        }
        if (cursor) params.pageCursor = cursor

        const page: ExportResponse = await exportHighlightsWithRetry(
          client,
          params,
          'sync',
        )
        allBooks.push(...page.results)
        if (syncLimitMaxBooks != null && allBooks.length > syncLimitMaxBooks) {
          allBooks.length = syncLimitMaxBooks
        }
        setTotal(allBooks.length)
        setStatusMessage(
          activeFormalTestSession
            ? `Fetched ${allBooks.length} / ${formalTestBookIds?.length ?? allBooks.length} formal test book(s) so far...`
            : renderedDebugPages
              ? syncLimitMaxBooks != null
                ? `Fetched ${allBooks.length} / ${syncLimitMaxBooks} debug book(s) so far...`
                : `Fetched ${allBooks.length} debug book(s) so far...`
            : syncLimitMaxBooks != null
              ? `Fetched ${allBooks.length} / ${syncLimitMaxBooks} test book(s) so far...`
            : `Fetched ${allBooks.length} book(s) so far...`,
        )
        logReadwiseDebug(syncLogPrefix, 'export page', {
          pageResultCount: page.results.length,
          totalFetched: allBooks.length,
          nextPageCursor: page.nextPageCursor,
          syncLimitMaxBooks,
        })
        if (syncLimitMaxBooks != null && allBooks.length >= syncLimitMaxBooks) {
          logReadwiseInfo(syncLogPrefix, 'sync limit reached', {
            syncLimitMaxBooks,
          })
          cursor = null
          break
        }
        if (formalTestBookIds && formalTestBookIds.length > 0) {
          cursor = null
          break
        }
        cursor = page.nextPageCursor
      } while (cursor)

      if (allBooks.length === 0) {
        setStatus('completed')
        setStatusMessage('No new highlights to sync.')
        logReadwiseInfo(syncLogPrefix, 'no new highlights')
        return
      }

      setStatus('syncing')
      setTotal(allBooks.length)
      setStatusMessage(
        activeFormalTestSession
          ? `Formal test session mode: processing ${allBooks.length} frozen book(s) in ${effectiveNamespacePrefix}; checkpoint will not advance.`
          : renderedDebugPages
            ? ignoreCheckpoint
              ? `Debug sync mode: processing ${allBooks.length} book(s) from scratch into ${effectiveNamespacePrefix}; checkpoint will not advance.`
              : `Debug sync mode: processing ${allBooks.length} book(s); checkpoint will not advance.`
          : syncLimitMaxBooks != null
            ? `Limited sync mode: processing ${allBooks.length} test book(s) in ${effectiveNamespacePrefix}; checkpoint will not advance.`
          : `Syncing ${allBooks.length} book(s)...`,
      )

      for (let i = 0; i < allBooks.length; i++) {
        if (cancelledRef.current) return

        const book = allBooks[i]!
        setCurrent(i + 1)
        setCurrentBook(book.title)

        try {
          if (renderedDebugPages) {
            await syncRenderedDebugPage(
              book,
              effectiveNamespacePrefix,
              pageNameMode,
              null,
            )
          } else {
            await syncRenderedPage(
              book,
              effectiveNamespacePrefix,
              syncLogPrefix,
              null,
            )
          }
        } catch (err: unknown) {
          const msg = err instanceof Error ? err.message : String(err)
          const issue = { book: book.title, message: msg }
          syncErrorsForRun.push(issue)
          appendRunIssue(issue)
        }
      }

      const nextUpdatedAfter = deriveNextUpdatedAfterV1(
        allBooks,
        checkpointBeforeRun?.updatedAfter ?? null,
      )
      const hasSyncErrors = syncErrorsForRun.length > 0

      if (
        activeFormalTestSession ||
        syncLimitMaxBooks != null ||
        ignoreCheckpoint
      ) {
        logReadwiseInfo(syncLogPrefix, 'skipping checkpoint save', {
          formalTestSessionId: activeFormalTestSession?.sessionId ?? null,
          syncLimitMaxBooks,
          nextUpdatedAfter,
          ignoreCheckpoint,
          errorCount: syncErrorsForRun.length,
        })
      } else if (hasSyncErrors) {
        logReadwiseWarn(
          syncLogPrefix,
          'skipping checkpoint save because page sync errors occurred',
          {
            nextUpdatedAfter,
            errorCount: syncErrorsForRun.length,
            errors: syncErrorsForRun,
          },
        )
      } else if (nextUpdatedAfter != null) {
        const checkpointSource: GraphCheckpointSourceV1 =
          checkpointBeforeRun?.updatedAfter == null
            ? 'full_sync'
            : 'incremental_sync'
        const checkpointToSave = {
          schemaVersion: 1 as const,
          updatedAfter: nextUpdatedAfter,
          committedAt: new Date().toISOString(),
          source: checkpointSource,
        }
        logReadwiseInfo(syncLogPrefix, 'saving graph checkpoint', checkpointToSave)
        await saveGraphCheckpointStateV1({
          schemaVersion: 1,
          updatedAfter: nextUpdatedAfter,
          committedAt: checkpointToSave.committedAt,
          source: checkpointToSave.source,
        })
        logReadwiseInfo(syncLogPrefix, 'saved graph checkpoint', checkpointToSave)
      }

      setStatus('completed')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: allBooks.length,
              issuesCount: syncErrorsForRun.length,
            },
      )
      setStatusMessage(
        activeFormalTestSession
          ? `Formal test session complete. ${allBooks.length} frozen book(s) processed in ${effectiveNamespacePrefix}. Checkpoint was not advanced.`
          : renderedDebugPages || ignoreCheckpoint
            ? `Debug sync complete. ${allBooks.length} book(s) processed${effectiveNamespacePrefix ? ` in ${effectiveNamespacePrefix}` : ''}. Checkpoint was not advanced.`
          : syncLimitMaxBooks != null
            ? `Limited sync complete. ${allBooks.length} test book(s) processed in ${effectiveNamespacePrefix}. Checkpoint was not advanced.`
          : hasSyncErrors
            ? `Sync completed with ${syncErrorsForRun.length} error(s). Checkpoint was not advanced.`
          : `Sync complete. ${allBooks.length} book(s) processed.`,
      )
      logReadwiseInfo(syncLogPrefix, 'sync completed', {
        processedBooks: allBooks.length,
        formalTestSessionId: activeFormalTestSession?.sessionId ?? null,
        nextUpdatedAfter,
        syncLimitMaxBooks,
        ignoreCheckpoint,
        errorCount: syncErrorsForRun.length,
        namespacePrefix: effectiveNamespacePrefix,
      })
    } catch (err: unknown) {
      logReadwiseError(syncLogPrefix, 'sync failed', err)
      setStatus('error')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: allBooks.length,
              issuesCount: syncErrorsForRun.length,
            },
      )
      setStatusMessage(
        `Sync failed: ${describeUnknownError(err)}`,
      )
    }
  }

  const handleSync = async () => {
    setCacheSummaryResult(null)
    const conflicts = await detectFormalSyncConflicts()
    if (conflicts != null) {
      setShowMaintenanceTools(true)
      const summary = formatManagedPageConflictSummary(conflicts)
      logReadwiseWarn(
        formalSyncLogPrefix,
        'blocked formal sync because conflicting managed pages still exist',
        {
          conflicts: conflicts.map((conflict) => ({
            label: conflict.label,
            clearAction: conflict.clearAction,
            count: conflict.pages.length,
            sampleTitles: conflict.pages.slice(0, 5).map((page) => page.pageTitle),
          })),
        },
      )
      replaceRunIssues(
        conflicts.flatMap((conflict) =>
          conflict.pages.slice(0, 3).map((page) => ({
            book: page.pageTitle,
            message: `Incremental sync blocked until ${conflict.label} pages are cleared via ${conflict.clearAction}.`,
          })),
        ),
      )
      setRunIssueContext({
        modeLabel: 'Incremental sync',
        namespacePrefix: formalNamespaceRoot,
        logLevel: String(logseq.settings?.logLevel ?? 'warn'),
        statusMessage: '',
        startedAt: new Date().toISOString(),
        completedAt: new Date().toISOString(),
        targetDocuments: null,
        debugHighlightPageLimit:
          resolveConfiguredReaderDebugHighlightPageLimit(),
        processedItems: 0,
        issuesCount: conflicts.length,
      })
      setStatus('error')
      setCurrent(0)
      setTotal(0)
      setCurrentBook('')
      setStatusMessage(
        `Incremental sync blocked to avoid duplicate block UUIDs. ${summary}.`,
      )
      return
    }

    await runReaderManagedSync({
      namespacePrefix: formalNamespaceRoot,
      logPrefix: formalSyncLogPrefix,
      statusPrefix: 'Incremental sync',
      syncHeaderMode: 'formal',
      mode: 'incremental-window',
    })
  }

  const handleCachedFullRebuild = async () => {
    setCacheSummaryResult(null)
    const conflicts = await detectFormalSyncConflicts()
    if (conflicts != null) {
      setShowMaintenanceTools(true)
      const summary = formatManagedPageConflictSummary(conflicts)
      replaceRunIssues(
        conflicts.flatMap((conflict) =>
          conflict.pages.slice(0, 3).map((page) => ({
            book: page.pageTitle,
            message: `Cached rebuild blocked until ${conflict.label} pages are cleared via ${conflict.clearAction}.`,
          })),
        ),
      )
      setRunIssueContext({
        modeLabel: 'Cached rebuild',
        namespacePrefix: formalNamespaceRoot,
        logLevel: String(logseq.settings?.logLevel ?? 'warn'),
        statusMessage: '',
        startedAt: new Date().toISOString(),
        completedAt: new Date().toISOString(),
        targetDocuments: resolveConfiguredReaderFullScanTargetDocuments(),
        debugHighlightPageLimit: null,
        processedItems: 0,
        issuesCount: conflicts.length,
      })
      setStatus('error')
      setCurrent(0)
      setTotal(0)
      setCurrentBook('')
      setStatusMessage(
        `Cached rebuild blocked to avoid duplicate block UUIDs. ${summary}.`,
      )
      return
    }

    await runReaderManagedSync({
      namespacePrefix: formalNamespaceRoot,
      logPrefix: formalSyncLogPrefix,
      statusPrefix: 'Cached rebuild',
      syncHeaderMode: 'formal',
      mode: 'cached-full-rebuild',
    })
  }

  const handleRefreshLocalSnapshotOnly = async () => {
    setCacheSummaryResult(null)

    await runReaderManagedSync({
      namespacePrefix: formalNamespaceRoot,
      logPrefix: formalSyncLogPrefix,
      statusPrefix: 'Refresh local snapshot',
      syncHeaderMode: 'formal',
      mode: 'snapshot-only-refresh',
    })
  }

  const handleFullReconcile = async () => {
    setCacheSummaryResult(null)
    const conflicts = await detectFormalSyncConflicts()
    if (conflicts != null) {
      setShowMaintenanceTools(true)
      const summary = formatManagedPageConflictSummary(conflicts)
      replaceRunIssues(
        conflicts.flatMap((conflict) =>
          conflict.pages.slice(0, 3).map((page) => ({
            book: page.pageTitle,
            message: `Full refresh blocked until ${conflict.label} pages are cleared via ${conflict.clearAction}.`,
          })),
        ),
      )
      setRunIssueContext({
        modeLabel: 'Full refresh',
        namespacePrefix: formalNamespaceRoot,
        logLevel: String(logseq.settings?.logLevel ?? 'warn'),
        statusMessage: '',
        startedAt: new Date().toISOString(),
        completedAt: new Date().toISOString(),
        targetDocuments: resolveConfiguredReaderFullScanTargetDocuments(),
        debugHighlightPageLimit:
          resolveConfiguredReaderDebugHighlightPageLimit(),
        processedItems: 0,
        issuesCount: conflicts.length,
      })
      setStatus('error')
      setCurrent(0)
      setTotal(0)
      setCurrentBook('')
      setStatusMessage(
        `Full refresh blocked to avoid duplicate block UUIDs. ${summary}.`,
      )
      return
    }

    await runReaderManagedSync({
      namespacePrefix: formalNamespaceRoot,
      logPrefix: formalSyncLogPrefix,
      statusPrefix: 'Full refresh',
      syncHeaderMode: 'formal',
      mode: 'full-library-scan',
    })
  }

  const handleAuditManagedIds = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCacheSummaryResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    const startedAt = new Date().toISOString()
    setRunIssueContext({
      modeLabel: 'Managed page audit',
      namespacePrefix: formalNamespaceRoot,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage: '',
      startedAt,
      completedAt: null,
      targetDocuments: null,
      debugHighlightPageLimit: null,
      processedItems: 0,
      issuesCount: 0,
    })
    setStatusMessage(`Auditing managed pages under ${formalNamespaceRoot}...`)

    try {
      const auditResult = await auditManagedReaderPagesV1([formalNamespaceRoot])
      const auditIssues: RunIssue[] = [
        ...auditResult.duplicateReaderIds.map((duplicateGroup) => ({
          book: `rw-reader-id ${duplicateGroup.readerDocumentId}`,
          message: `Multiple managed pages share rw-reader-id=${duplicateGroup.readerDocumentId}: ${duplicateGroup.pages.map((page) => page.pageTitle).join(', ')}`,
          category: 'duplicate-reader-id' as const,
          summary:
            'More than one formal managed page currently points at the same Reader document id.',
          suggestedAction:
            'Inspect the listed pages, keep the canonical page, then rename, archive, or delete the extras.',
          debugFacts: [
            `namespaceRoots=${auditResult.namespaceRoots.join(', ')}`,
            `duplicatePages=${duplicateGroup.pages.map((page) => page.pageTitle).join(', ')}`,
          ],
          readerDocumentId: duplicateGroup.readerDocumentId,
          namespacePrefix: formalNamespaceRoot,
        })),
        ...auditResult.missingReaderIdPages.map((page) => ({
          book: page.pageTitle,
          message: `Managed page is missing rw-reader-id: ${page.pageTitle}`,
          summary:
            'This managed page does not currently expose rw-reader-id, so future retargeting and conflict resolution become less reliable.',
          suggestedAction:
            'Open the page, confirm whether it should remain managed, and re-sync or repair its properties if needed.',
          debugFacts: [
            `pageUuid=${page.pageUuid}`,
            `aliases=${page.aliases.join(', ')}`,
            `namespaceRoots=${auditResult.namespaceRoots.join(', ')}`,
          ],
          namespacePrefix: formalNamespaceRoot,
          pageName: page.pageTitle,
        })),
        ...auditResult.overlongFileNamePages.map((page) => ({
          book: page.pageTitle,
          message: page.diagnosticMessage,
          category: 'path-too-long' as const,
          summary:
            'This managed page title currently exceeds the filesystem file-name limit once Logseq maps it to pages/*.org.',
          suggestedAction:
            'Use a shorter stored page key or switch to an alias-based title fallback before this page needs to be recreated.',
          debugFacts: [
            `pageUuid=${page.pageUuid}`,
            `fileStem=${page.fileStem}`,
            `aliases=${page.aliases.join(', ')}`,
          ],
          namespacePrefix: formalNamespaceRoot,
          pageName: page.pageTitle,
        })),
      ]

      replaceRunIssues(auditIssues)
      setCurrent(auditResult.scannedPages)
      setTotal(auditResult.scannedPages)
      setStatus('completed')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: auditResult.scannedPages,
              issuesCount: auditIssues.length,
              stats: {
                pagesProcessed: auditResult.scannedPages,
              },
            },
      )
      setStatusMessage(
        auditIssues.length > 0
          ? `Managed page audit found ${auditIssues.length} issue(s) across ${auditResult.scannedPages} page(s).`
          : `Managed page audit found no issues across ${auditResult.scannedPages} page(s).`,
      )
    } catch (err: unknown) {
      logReadwiseError(formalSyncLogPrefix, 'managed page audit failed', err)
      setStatus('error')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              issuesCount: errors.length,
            },
      )
      setStatusMessage(
        `Managed page audit failed: ${describeUnknownError(err)}`,
      )
    }
  }

  const handleInspectCacheSummary = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCacheSummaryResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    const startedAt = new Date().toISOString()
    setRunIssueContext({
      modeLabel: 'Cache summary',
      namespacePrefix: formalNamespaceRoot,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage: '',
      startedAt,
      completedAt: null,
      targetDocuments: null,
      debugHighlightPageLimit: null,
      processedItems: 0,
      issuesCount: 0,
    })
    setStatusMessage('Inspecting Reader sync cache...')

    try {
      const graphContext = await loadCurrentGraphContextV1()
      const previewCache = createGraphReaderSyncCacheV1(graphContext.graphId)
      const summary = await previewCache.inspectCacheSummary()

      setCacheSummaryResult(summary)
      setStatus('completed')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: summary.parentDocumentCount + summary.highlightCount,
              issuesCount: 0,
              stats: {
                pagesProcessed: summary.parentDocumentCount,
                highlightsScanned: summary.highlightCount,
              },
            },
      )
      setStatusMessage(
        `Loaded cache summary for graph ${summary.graphId}. ${summary.parentDocumentCount} parent document(s) and ${summary.highlightCount} highlight(s) cached.`,
      )
    } catch (err: unknown) {
      logReadwiseError(formalSyncLogPrefix, 'cache summary failed', err)
      setStatus('error')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
            },
      )
      setStatusMessage(
        `Cache summary failed: ${describeUnknownError(err)}`,
      )
    }
  }

  const handleRepairManagedPages = async () => {
    const token = logseq.settings?.apiToken as string
    if (!token) {
      setStatus('error')
      setStatusMessage('No API token configured. Set it in plugin settings.')
      return
    }

    cancelledRef.current = false
    retryActionRef.current = null
    clearRunIssues()
    setPageDiffResult(null)
    setCacheSummaryResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    const startedAt = new Date().toISOString()
    setRunIssueContext({
      modeLabel: 'Repair managed pages',
      namespacePrefix: formalNamespaceRoot,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage: '',
      startedAt,
      completedAt: null,
      targetDocuments: null,
      debugHighlightPageLimit: null,
      processedItems: 0,
      issuesCount: 0,
    })
    resetLiveRunIssueMetrics()
    beginReaderSyncEtaPhase('fetch-highlights', 'repair scan')
    setStatusMessage(
      `Scanning managed pages under ${formalNamespaceRoot} for legacy corruption signatures...`,
    )

    let scanProgressCompleted = 0
    let scanProgressTotal = 0
    let repairProgressCompleted = 0
    let repairProgressTotal = 0
    let repairedCount = 0
    let repairIssuesCount = 0

    try {
      throwIfCancelled()
      const graphContext = await loadCurrentGraphContextV1()
      const previewCache = createGraphReaderSyncCacheV1(graphContext.graphId)
      const client = createReadwiseClient(token)
      const duplicateResolution = await resolveSafeDuplicateReaderIdPages({
        client,
        previewCache,
      })
      throwIfCancelled()

      if (duplicateResolution.resolvedGroups > 0) {
        setStatusMessage(
          `Resolved ${duplicateResolution.resolvedGroups} duplicate rw-reader-id group(s); scanning managed pages for repair candidates...`,
        )
      }

      const scanResult = await scanManagedPagesForRepairCandidates({
        previewCache,
        client,
        onProgress: ({ total, completed, pageName }) => {
          scanProgressCompleted = completed
          scanProgressTotal = total
          updateLiveRunIssueMetrics({
            processedItems: completed,
            stats: {
              pagesTargeted: total,
              pagesProcessed: completed,
            },
          })
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageName)
          updateReaderSyncEta('fetch-highlights', 'repair scan', completed, total)
          setStatusMessage(
            `Scanning ${completed} / ${total} managed page(s) for repair candidates...`,
          )
        },
      })
      throwIfCancelled()

      const repairIssues: RunIssue[] = [...scanResult.issues]
      repairIssuesCount = repairIssues.length
      replaceRunIssues(repairIssues)

      if (scanResult.candidates.length === 0) {
        setStatus('completed')
        setRunIssueContext((previous) =>
          previous == null
            ? previous
            : {
                ...previous,
                completedAt: new Date().toISOString(),
                processedItems: scanResult.scannedPages,
                issuesCount: repairIssues.length,
                stats: {
                  pagesProcessed: scanResult.scannedPages,
                },
              },
        )
        setStatusMessage(
          repairIssues.length > 0
            ? `Repair scan found ${repairIssues.length} issue(s), but no managed pages could be repaired automatically.`
            : `Repair scan found no managed pages that need this legacy fix.`,
        )
        return
      }

      const candidateIds = uniqueStrings(
        scanResult.candidates.map((candidate) => candidate.readerDocumentId),
      )
      const highlightsByParent =
        await previewCache.loadGroupedHighlightsByParent(candidateIds)
      const cachedParentDocuments = await previewCache.getCachedParentDocuments(
        candidateIds,
      )

      setStatus('syncing')
      setCurrent(0)
      setTotal(scanResult.candidates.length)
      setCurrentBook('')
      repairProgressTotal = scanResult.candidates.length
      updateLiveRunIssueMetrics({
        processedItems: 0,
        stats: {
          pagesTargeted: scanResult.candidates.length,
          pagesProcessed: 0,
        },
      })
      beginReaderSyncEtaPhase('write-pages', 'page repairs')
      setStatusMessage(
        `Repairing ${scanResult.candidates.length} managed page(s) from the cached highlight snapshot...`,
      )

      for (let index = 0; index < scanResult.candidates.length; index += 1) {
        throwIfCancelled()
        const candidate = scanResult.candidates[index]!
        setCurrentBook(candidate.pageName)

        const highlights = [
          ...(highlightsByParent.get(candidate.readerDocumentId) ?? []),
        ].sort(sortReaderDocumentsByCreatedAtAscending)

        if (highlights.length === 0) {
          const issue: RunIssue = {
            book: candidate.pageName,
            message: `Repair skipped because no cached highlights were found for rw-reader-id=${candidate.readerDocumentId}.`,
            summary:
              'Automatic repair needs the cached highlight snapshot for this page.',
            suggestedAction:
              'Run Full Refresh first, then rerun Repair Managed Pages.',
            debugFacts: [
              `detectedSignatures=${candidate.signatures.join(', ')}`,
            ],
            readerDocumentId: candidate.readerDocumentId,
            namespacePrefix: formalNamespaceRoot,
            pageName: candidate.pageName,
          }
          repairIssues.push(issue)
          repairIssuesCount = repairIssues.length
          appendRunIssue(issue)
          repairProgressCompleted = index + 1
          updateLiveRunIssueMetrics({
            processedItems: index + 1,
            stats: {
              pagesTargeted: scanResult.candidates.length,
              pagesProcessed: repairedCount,
            },
          })
          setCurrent(index + 1)
          updateReaderSyncEta(
            'write-pages',
            'page repairs',
            index + 1,
            scanResult.candidates.length,
          )
          continue
        }

        let document =
          cachedParentDocuments.get(candidate.readerDocumentId) ?? null

        if (!document) {
          throwIfCancelled()
          document = await loadReaderParentDocumentByIdWithRetry(
            client,
            candidate.readerDocumentId,
            formalSyncLogPrefix,
          )

          if (document) {
            await previewCache.putParentDocuments([document])
          }
        }

        if (!document) {
          throwIfCancelled()
          const replacementLookup = await findReplacementReaderParentDocumentByApi({
            client,
            pageName: candidate.pageName,
            rootContent: candidate.rootContent,
            previewCache,
            logPrefix: formalSyncLogPrefix,
          })
          document = replacementLookup.document
        }

        if (!document) {
          const issue: RunIssue = {
            book: candidate.pageName,
            message: `Repair skipped because Reader did not return parent metadata for rw-reader-id=${candidate.readerDocumentId}.`,
            summary:
              'Automatic repair could not rebuild this page without its parent Reader document.',
            suggestedAction:
              'Run Full Refresh or inspect the Reader document state, then retry repair.',
            debugFacts: [
              `detectedSignatures=${candidate.signatures.join(', ')}`,
            ],
            readerDocumentId: candidate.readerDocumentId,
            namespacePrefix: formalNamespaceRoot,
            pageName: candidate.pageName,
          }
          repairIssues.push(issue)
          repairIssuesCount = repairIssues.length
          appendRunIssue(issue)
          repairProgressCompleted = index + 1
          updateLiveRunIssueMetrics({
            processedItems: index + 1,
            stats: {
              pagesTargeted: scanResult.candidates.length,
              pagesProcessed: repairedCount,
            },
          })
          setCurrent(index + 1)
          updateReaderSyncEta(
            'write-pages',
            'page repairs',
            index + 1,
            scanResult.candidates.length,
          )
          continue
        }

        if (document.id !== candidate.readerDocumentId) {
          logReadwiseInfo(
            formalSyncLogPrefix,
            'repair write: retargeted page to replacement Reader document',
            {
              pageName: candidate.pageName,
              originalReaderDocumentId: candidate.readerDocumentId,
              replacementReaderDocumentId: document.id,
            },
          )
        }

        try {
          await syncRenderedReaderPreviewPage(
            {
              document,
              highlights,
              highlightCoverage: 'cached-full-rebuild',
            },
            formalNamespaceRoot,
            formalSyncLogPrefix,
            {
              pageResolveMode: 'reader_id_then_title',
              identityNamespaceRoot: formalNamespaceRoot,
            },
          )
          repairedCount += 1
        } catch (err: unknown) {
          const issue: RunIssue = {
            book: candidate.pageName,
            message: describeUnknownError(err),
            summary:
              'Automatic repair attempted to rewrite this managed page but the write failed.',
            suggestedAction:
              'Copy the issue bundle and inspect the failing page before retrying repair.',
            debugFacts: [
              `detectedSignatures=${candidate.signatures.join(', ')}`,
            ],
            readerDocumentId: candidate.readerDocumentId,
            namespacePrefix: formalNamespaceRoot,
            pageName: candidate.pageName,
          }
          repairIssues.push(issue)
          repairIssuesCount = repairIssues.length
          appendRunIssue(issue)
        }

        repairProgressCompleted = index + 1
        updateLiveRunIssueMetrics({
          processedItems: index + 1,
          stats: {
            pagesTargeted: scanResult.candidates.length,
            pagesProcessed: repairedCount,
          },
        })
        setCurrent(index + 1)
        updateReaderSyncEta(
          'write-pages',
          'page repairs',
          index + 1,
          scanResult.candidates.length,
        )
        setStatusMessage(
          `Repairing ${scanResult.candidates.length} managed page(s)... ${index + 1} / ${scanResult.candidates.length}.`,
        )
      }

      setStatus('completed')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: scanResult.scannedPages,
              issuesCount: repairIssues.length,
              stats: {
                pagesTargeted: scanResult.candidates.length,
                pagesProcessed: repairedCount,
              },
            },
      )
      setStatusMessage(
        repairIssues.length > 0
          ? `Repaired ${repairedCount} managed page(s); ${repairIssues.length} issue(s) still need attention.`
          : `Repaired ${repairedCount} managed page(s) that matched legacy corruption signatures.`,
      )
    } catch (err: unknown) {
      if (isRunCancelledError(err)) {
        const processedItems =
          repairProgressCompleted > 0 ? repairProgressCompleted : scanProgressCompleted
        const totalItems =
          repairProgressTotal > 0 ? repairProgressTotal : scanProgressTotal
        setStatus('idle')
        setCurrentBook('')
        setRunIssueContext((previous) =>
          previous == null
            ? previous
            : {
                ...previous,
                completedAt: new Date().toISOString(),
                processedItems,
                issuesCount: repairIssuesCount,
                stats:
                  totalItems > 0
                    ? {
                        pagesTargeted: totalItems,
                        pagesProcessed: repairedCount,
                      }
                    : {
                        pagesProcessed: processedItems,
                      },
              },
        )
        setStatusMessage('Repair managed pages cancelled.')
        return
      }
      logReadwiseError(formalSyncLogPrefix, 'managed page repair failed', err)
      setStatus('error')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              issuesCount: errors.length,
            },
      )
      setStatusMessage(
        `Repair managed pages failed: ${describeUnknownError(err)}`,
      )
    }
  }

  const buildLegacyBlockRefPreviewIssues = (
    entries: LegacyBlockRefPreviewEntryV1[],
  ): RunIssue[] => {
    const byPage = new Map<
      string,
      {
        refsPlanned: number
        entries: LegacyBlockRefPreviewEntryV1[]
      }
    >()

    for (const entry of entries) {
      const current = byPage.get(entry.pageName) ?? {
        refsPlanned: 0,
        entries: [],
      }
      current.refsPlanned += entry.refs.length
      current.entries.push(entry)
      byPage.set(entry.pageName, current)
    }

    return [...byPage.entries()].map(([pageName, preview]) => ({
      book: pageName,
      category: 'warning' as const,
      message: `Preview: ${preview.entries.length} block(s) and ${preview.refsPlanned} block ref(s) would be rewritten on ${pageName}.`,
      summary:
        'Preview only. This page currently contains graph block refs that point at legacy Readwise highlight UUIDs.',
      suggestedAction:
        'Review the listed UUID rewrites. If they look correct, run Apply Legacy Block Ref Migration.',
      debugFacts: [
        `previewBlockCount=${preview.entries.length}`,
        `previewRefsPlanned=${preview.refsPlanned}`,
        ...preview.entries.flatMap((entry) => [
          `blockUuid=${entry.blockUuid}`,
          ...entry.refs.map((ref) => `rewrite=${ref.from} -> ${ref.to}`),
        ]),
      ],
      namespacePrefix: formalNamespaceRoot,
      pageName,
    }))
  }

  const buildCurrentPageLegacyIdPreviewIssues = ({
    pageName,
    relativeFilePath,
    fileKind,
    rewrites,
  }: {
    pageName: string
    relativeFilePath: string
    fileKind: 'page' | 'whiteboard'
    rewrites: CurrentPageLegacyIdRewriteEntryV1[]
  }): RunIssue[] => {
    if (rewrites.length === 0) {
      return []
    }

    return [
      {
        book: pageName,
        category: 'warning' as const,
        message: `Preview: ${rewrites.length} legacy id rewrite(s) would be applied to the current ${fileKind}.`,
        summary:
          'Preview only. The current page or whiteboard contains legacy Readwise ids that can be migrated to the current canonical ids.',
        suggestedAction:
          'Review the listed rewrites. If they look correct, run Apply Current Page Legacy ID Migration.',
        debugFacts: [
          `relativeFilePath=${relativeFilePath}`,
          `fileKind=${fileKind}`,
          `rewritesPlanned=${rewrites.length}`,
          ...rewrites.map(
            (rewrite) =>
              `entry=${rewrite.entryIndex} blockUuid=${rewrite.blockUuid} kind=${rewrite.kind} rewrite=${rewrite.from} -> ${rewrite.to}`,
          ),
        ],
        namespacePrefix: formalNamespaceRoot,
        pageName,
      },
    ]
  }

  const handlePreviewLegacyBlockRefs = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCacheSummaryResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    const startedAt = new Date().toISOString()
    setRunIssueContext({
      modeLabel: 'Preview legacy block refs',
      namespacePrefix: formalNamespaceRoot,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage: '',
      startedAt,
      completedAt: null,
      targetDocuments: null,
      debugHighlightPageLimit: null,
      processedItems: 0,
      issuesCount: 0,
    })
    beginReaderSyncEtaPhase('fetch-highlights', 'block ref mapping')
    setStatusMessage(
      `Scanning ${formalNamespaceRoot} managed pages for legacy block UUID mappings...`,
    )

    try {
      const mappingResult = await buildLegacyBlockRefMappingV1({
        namespaceRoot: formalNamespaceRoot,
        onProgress: ({ total, completed, pageName }) => {
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageName)
          updateReaderSyncEta('fetch-highlights', 'block ref mapping', completed, total)
          setStatusMessage(
            `Building legacy block UUID mapping... ${completed} / ${total} managed page(s).`,
          )
        },
      })

      if (mappingResult.mapping.size === 0) {
        setStatus('completed')
        setRunIssueContext((previous) =>
          previous == null
            ? previous
            : {
                ...previous,
                completedAt: new Date().toISOString(),
                processedItems: 0,
                issuesCount: 0,
                stats: {
                  pagesProcessed: mappingResult.summary.managedPagesScanned,
                },
              },
        )
        setStatusMessage('No legacy Readwise block UUID mappings were found.')
        return
      }

      setStatus('syncing')
      setCurrent(0)
      setTotal(0)
      setCurrentBook('')
      beginReaderSyncEtaPhase('write-pages', 'block ref preview')
      setStatusMessage(
        `Previewing legacy block refs using ${mappingResult.mapping.size} UUID mapping(s)...`,
      )

      const previewResult = await previewLegacyBlockRefsV1({
        mapping: mappingResult.mapping,
        onProgress: ({ total, completed, pageName, affectedPages, refsPlanned }) => {
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageName)
          updateReaderSyncEta('write-pages', 'block ref preview', completed, total)
          setStatusMessage(
            `Previewing legacy block refs... ${completed} / ${total} page(s), ${affectedPages} affected, ${refsPlanned} ref(s) planned.`,
          )
        },
      })
      const previewIssues = buildLegacyBlockRefPreviewIssues(previewResult.entries)
      replaceRunIssues(previewIssues)
      setShowWarningIssues(true)
      setPendingLegacyBlockRefMigration({
        mapping: mappingResult.mapping,
        entries: previewResult.entries,
      })

      setStatus('completed')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: previewResult.summary.refsPlanned,
              issuesCount: previewIssues.length,
              stats: {
                pagesTargeted: previewResult.summary.graphPagesScanned,
                pagesProcessed: previewResult.summary.graphPagesAffected,
                updatedCount: previewResult.summary.blocksAffected,
              },
            },
      )
      setStatusMessage(
        previewResult.summary.refsPlanned > 0
          ? `Preview found ${previewResult.summary.refsPlanned} block ref(s) across ${previewResult.summary.graphPagesAffected} page(s). Review them before applying.`
          : 'Preview found no legacy block refs to rewrite.',
      )
    } catch (err: unknown) {
      const issue = {
        book: 'Legacy block ref migration',
        message: describeUnknownError(err),
        summary:
          'Legacy block ref migration stopped before the graph-wide rewrite completed.',
        suggestedAction:
          'Copy the issue bundle and inspect the failing page or block before retrying the migration.',
        namespacePrefix: formalNamespaceRoot,
      } satisfies RunIssue

      logReadwiseError(formalSyncLogPrefix, 'legacy block ref migration failed', err)
      replaceRunIssues([issue])
      setStatus('error')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: 0,
              issuesCount: 1,
            },
      )
      setStatusMessage(
        `Legacy block ref migration failed: ${describeUnknownError(err)}`,
      )
    }
  }

  const handlePreviewCurrentPageLegacyIds = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCacheSummaryResult(null)
    setCurrentPageLegacyIdApplyResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    const startedAt = new Date().toISOString()
    setRunIssueContext({
      modeLabel: 'Preview current-page legacy ids',
      namespacePrefix: formalNamespaceRoot,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage: '',
      startedAt,
      completedAt: null,
      targetDocuments: null,
      debugHighlightPageLimit: null,
      processedItems: 0,
      issuesCount: 0,
    })
    beginReaderSyncEtaPhase('fetch-highlights', 'block ref mapping')
    setStatusMessage(
      `Scanning ${formalNamespaceRoot} managed pages for legacy block UUID mappings...`,
    )

    try {
      const mappingResult = await buildLegacyBlockRefMappingV1({
        namespaceRoot: formalNamespaceRoot,
        onProgress: ({ total, completed, pageName }) => {
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageName)
          updateReaderSyncEta('fetch-highlights', 'block ref mapping', completed, total)
          setStatusMessage(
            `Building legacy block UUID mapping... ${completed} / ${total} managed page(s).`,
          )
        },
      })

      setStatus('syncing')
      setCurrent(0)
      setTotal(1)
      setCurrentBook('')
      beginReaderSyncEtaPhase('write-pages', 'current-page preview')
      setStatusMessage(
        `Previewing current page legacy ids using ${mappingResult.mapping.size} UUID mapping(s)...`,
      )

      const previewResult = await previewCurrentPageLegacyIdsV1({
        mapping: mappingResult.mapping,
      })
      setCurrent(1)
      setCurrentBook(previewResult.target.pageName)
      updateReaderSyncEta('write-pages', 'current-page preview', 1, 1)
      const previewIssues = buildCurrentPageLegacyIdPreviewIssues({
        pageName: previewResult.target.pageName,
        relativeFilePath: previewResult.target.relativeFilePath,
        fileKind: previewResult.target.fileKind,
        rewrites: previewResult.rewrites,
      })
      setCurrentPageLegacyIdPreviewResult({
        pageName: previewResult.target.pageName,
        relativeFilePath: previewResult.target.relativeFilePath,
        fileKind: previewResult.target.fileKind,
        rewrites: previewResult.rewrites,
        managedPagesScanned: mappingResult.summary.managedPagesScanned,
      })
      setPendingCurrentPageLegacyIdMigration(
        previewResult.rewrites.length > 0
          ? {
              mapping: mappingResult.mapping,
              pageName: previewResult.target.pageName,
              relativeFilePath: previewResult.target.relativeFilePath,
              fileKind: previewResult.target.fileKind,
              rewrites: previewResult.rewrites,
            }
          : null,
      )

      setStatus('completed')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: previewResult.rewrites.length,
              issuesCount: previewIssues.length,
              stats: {
                pagesTargeted: mappingResult.summary.managedPagesScanned,
                pagesProcessed: previewResult.rewrites.length > 0 ? 1 : 0,
                updatedCount: previewResult.rewrites.length,
              },
            },
      )
      setStatusMessage(
        previewResult.rewrites.length > 0
          ? `Preview found ${previewResult.rewrites.length} legacy id rewrite(s) in the current ${previewResult.target.fileKind}.`
          : `Preview found no legacy ids to rewrite in the current ${previewResult.target.fileKind}.`,
      )
    } catch (err: unknown) {
      const issue = {
        book: 'Current-page legacy id migration',
        message: describeUnknownError(err),
        summary:
          'Current-page legacy id preview stopped before the page or whiteboard rewrite plan was produced.',
        suggestedAction:
          'Copy the issue bundle and inspect the current page or whiteboard file before retrying the preview.',
        namespacePrefix: formalNamespaceRoot,
      } satisfies RunIssue

      logReadwiseError(
        formalSyncLogPrefix,
        'current-page legacy id preview failed',
        err,
      )
      setCurrentPageLegacyIdPreviewResult(null)
      replaceRunIssues([issue])
      setStatus('error')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: 0,
              issuesCount: 1,
            },
      )
      setStatusMessage(
        `Current-page legacy id preview failed: ${describeUnknownError(err)}`,
      )
    }
  }

  const handleApplyLegacyBlockRefMigration = async () => {
    const migrationPlan = pendingLegacyBlockRefMigration

    if (!migrationPlan) {
      setStatus('error')
      setStatusMessage(
        'No pending legacy block ref preview is available. Run Preview Legacy Block Refs first.',
      )
      return
    }

    clearRunIssues({
      preservePendingLegacyBlockRefMigration: true,
    })
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('syncing')
    const startedAt = new Date().toISOString()
    setRunIssueContext({
      modeLabel: 'Apply legacy block ref migration',
      namespacePrefix: formalNamespaceRoot,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage: '',
      startedAt,
      completedAt: null,
      targetDocuments: null,
      debugHighlightPageLimit: null,
      processedItems: 0,
      issuesCount: 0,
    })
    beginReaderSyncEtaPhase('write-pages', 'block ref rewrite')
    setStatusMessage(
      `Rewriting legacy block refs using ${migrationPlan.mapping.size} UUID mapping(s)...`,
    )

    try {
      const migrationSummary = await migrateLegacyBlockRefsV1({
        mapping: migrationPlan.mapping,
        logPrefix: formalSyncLogPrefix,
        onProgress: ({ total, completed, pageName, updatedPages, refsRewritten }) => {
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageName)
          updateReaderSyncEta('write-pages', 'block ref rewrite', completed, total)
          setStatusMessage(
            `Migrating legacy block refs... ${completed} / ${total} page(s), ${updatedPages} updated, ${refsRewritten} ref(s) rewritten.`,
          )
        },
      })

      setPendingLegacyBlockRefMigration(null)
      setStatus('completed')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: migrationSummary.refsRewritten,
              issuesCount: 0,
              stats: {
                pagesTargeted: migrationSummary.graphPagesScanned,
                pagesProcessed: migrationSummary.graphPagesUpdated,
                updatedCount: migrationSummary.blocksUpdated,
              },
            },
      )
      setStatusMessage(
        `Migrated ${migrationSummary.refsRewritten} block ref(s) across ${migrationSummary.graphPagesUpdated} page(s).`,
      )
    } catch (err: unknown) {
      const issue = {
        book: 'Legacy block ref migration',
        message: describeUnknownError(err),
        summary:
          'Legacy block ref migration stopped before the graph-wide rewrite completed.',
        suggestedAction:
          'Copy the issue bundle and inspect the failing page or block before retrying the migration.',
        namespacePrefix: formalNamespaceRoot,
      } satisfies RunIssue

      logReadwiseError(formalSyncLogPrefix, 'legacy block ref migration failed', err)
      replaceRunIssues([issue])
      setStatus('error')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: 0,
              issuesCount: 1,
            },
      )
      setStatusMessage(
        `Legacy block ref migration failed: ${describeUnknownError(err)}`,
      )
    }
  }

  const handleApplyCurrentPageLegacyIdMigration = async () => {
    const migrationPlan = pendingCurrentPageLegacyIdMigration

    if (!migrationPlan) {
      setStatus('error')
      setStatusMessage(
        'No pending current-page legacy id preview is available. Run Preview Current Page Legacy ID Migration first.',
      )
      return
    }

    clearRunIssues({
      preservePendingCurrentPageLegacyIdMigration: true,
    })
    setCurrentPageLegacyIdPreviewResult(null)
    setCurrent(0)
    setTotal(1)
    setCurrentBook('')
    setStatus('syncing')
    const startedAt = new Date().toISOString()
    setRunIssueContext({
      modeLabel: 'Apply current-page legacy ids',
      namespacePrefix: formalNamespaceRoot,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage: '',
      startedAt,
      completedAt: null,
      targetDocuments: null,
      debugHighlightPageLimit: null,
      processedItems: 0,
      issuesCount: 0,
    })
    beginReaderSyncEtaPhase('write-pages', 'current-page rewrite')
    setStatusMessage(
      `Rewriting legacy ids in the current ${migrationPlan.fileKind}...`,
    )

    try {
      const migrationSummary = await migrateCurrentPageLegacyIdsV1({
        mapping: migrationPlan.mapping,
        expectedRelativeFilePath: migrationPlan.relativeFilePath,
        logPrefix: formalSyncLogPrefix,
      })

      setCurrent(1)
      setCurrentBook(migrationSummary.pageName)
      updateReaderSyncEta('write-pages', 'current-page rewrite', 1, 1)
      setPendingCurrentPageLegacyIdMigration(null)
      setCurrentPageLegacyIdApplyResult({
        pageName: migrationSummary.pageName,
        relativeFilePath: migrationSummary.relativeFilePath,
        fileKind: migrationSummary.fileKind,
        rewritesApplied: migrationSummary.rewritesApplied,
      })
      setStatus('completed')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: migrationSummary.rewritesApplied,
              issuesCount: 0,
              stats: {
                pagesTargeted: 1,
                pagesProcessed: migrationSummary.rewritesApplied > 0 ? 1 : 0,
                updatedCount: migrationSummary.rewritesApplied,
              },
            },
      )
      setStatusMessage(
        migrationSummary.rewritesApplied > 0
          ? `Migrated ${migrationSummary.rewritesApplied} legacy id(s) in the current ${migrationSummary.fileKind}.`
          : `No legacy ids needed rewriting in the current ${migrationSummary.fileKind}.`,
      )
    } catch (err: unknown) {
      const issue = {
        book: 'Current-page legacy id migration',
        message: describeUnknownError(err),
        summary:
          'Current-page legacy id migration stopped before the page or whiteboard rewrite completed.',
        suggestedAction:
          'Copy the issue bundle and re-run Preview Current Page Legacy ID Migration before retrying the apply step.',
        namespacePrefix: formalNamespaceRoot,
      } satisfies RunIssue

      logReadwiseError(
        formalSyncLogPrefix,
        'current-page legacy id migration failed',
        err,
      )
      setCurrentPageLegacyIdApplyResult(null)
      replaceRunIssues([issue])
      setStatus('error')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: 0,
              issuesCount: 1,
            },
      )
      setStatusMessage(
        `Current-page legacy id migration failed: ${describeUnknownError(err)}`,
      )
    }
  }

  const runCurrentPageReaderAction = async ({
    action,
    statusPrefix,
  }: {
    action: 'rebuild-from-cache' | 'refresh-metadata'
    statusPrefix: string
  }) => {
    const token = logseq.settings?.apiToken as string
    if (!token) {
      setStatus('error')
      setStatusMessage('No API token configured. Set it in plugin settings.')
      return
    }

    cancelledRef.current = false
    retryActionRef.current = {
      kind: 'formal',
      label: statusPrefix,
      run: () => runCurrentPageReaderAction({ action, statusPrefix }),
    }
    clearRunIssues()
    setPageDiffResult(null)
    setCacheSummaryResult(null)
    setCurrent(0)
    setTotal(1)
    setCurrentBook('')
    setStatus('fetching')
    const startedAt = new Date().toISOString()
    setRunIssueContext({
      modeLabel: statusPrefix,
      namespacePrefix: formalNamespaceRoot,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage: '',
      startedAt,
      completedAt: null,
      targetDocuments: 1,
      debugHighlightPageLimit: null,
      processedItems: 0,
      issuesCount: 0,
    })
    setStatusMessage(`${statusPrefix}: resolving the current managed page...`)

    try {
      const graphContext = await loadCurrentGraphContextV1()
      const previewCache = createGraphReaderSyncCacheV1(graphContext.graphId)
      const client = createReadwiseClient(token)
      const currentManagedPage = await resolveCurrentManagedReaderPage()
      const { pageName, readerDocumentId } = currentManagedPage

      beginReaderSyncEtaPhase('fetch-highlights', 'cached highlight snapshot')
      setCurrentBook(pageName)
      setStatusMessage(
        `${statusPrefix}: loading cached highlights for ${pageName}...`,
      )
      const highlightsByParent = await previewCache.loadGroupedHighlightsByParent([
        readerDocumentId,
      ])
      const highlights = [
        ...(highlightsByParent.get(readerDocumentId) ?? []),
      ].sort(sortReaderDocumentsByCreatedAtAscending)
      setCurrent(1)
      updateReaderSyncEta('fetch-highlights', 'cached highlight snapshot', 1, 1)

      if (highlights.length === 0) {
        throw new Error(
          'No cached highlights were found for the current page. Run Full Refresh first.',
        )
      }

      beginReaderSyncEtaPhase('fetch-documents', 'parent document fetch')
      setCurrent(0)
      setStatusMessage(
        action === 'refresh-metadata'
          ? `${statusPrefix}: refreshing parent metadata for ${pageName}...`
          : `${statusPrefix}: resolving parent metadata for ${pageName}...`,
      )

      let document =
        action === 'rebuild-from-cache'
          ? (await previewCache.getCachedParentDocuments([readerDocumentId])).get(
              readerDocumentId,
            ) ?? null
          : null

      if (!document) {
        document = await loadReaderParentDocumentByIdWithRetry(
          client,
          readerDocumentId,
          formalSyncLogPrefix,
        )

        if (!document) {
          throw new Error(
            `Readwise did not return a parent document for rw-reader-id=${readerDocumentId}.`,
          )
        }

        await previewCache.putParentDocuments([document])
      }

      setCurrent(1)
      updateReaderSyncEta('fetch-documents', 'parent document fetch', 1, 1)

      beginReaderSyncEtaPhase('write-pages', 'page writes')
      setStatus('syncing')
      setCurrent(0)
      setStatusMessage(`${statusPrefix}: rebuilding ${pageName}...`)

      const pageSyncResult = await syncRenderedReaderPreviewPage(
        {
          document,
          highlights,
          highlightCoverage: 'cached-full-rebuild',
        },
        formalNamespaceRoot,
        formalSyncLogPrefix,
        {
          pageResolveMode: 'reader_id_then_title',
          identityNamespaceRoot: formalNamespaceRoot,
        },
      )

      setCurrent(1)
      updateReaderSyncEta('write-pages', 'page writes', 1, 1)
      setStatus('completed')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: 1,
              issuesCount: 0,
            },
      )
      setStatusMessage(
        pageSyncResult.pageRenamed
          ? `${statusPrefix}: rebuilt ${pageSyncResult.pageName} and renamed it from ${pageSyncResult.previousPageName}.`
          : `${statusPrefix}: rebuilt ${pageSyncResult.pageName}.`,
      )
    } catch (err: unknown) {
      logReadwiseError(formalSyncLogPrefix, 'current page Reader action failed', err)
      setStatus('error')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: current,
              issuesCount: errors.length,
            },
      )
      setStatusMessage(`${statusPrefix} failed: ${describeUnknownError(err)}`)
    }
  }

  const handleRebuildCurrentPageFromCache = async () => {
    await runCurrentPageReaderAction({
      action: 'rebuild-from-cache',
      statusPrefix: 'Rebuild current page from cache',
    })
  }

  const handleRefreshCurrentPageMetadata = async () => {
    await runCurrentPageReaderAction({
      action: 'refresh-metadata',
      statusPrefix: 'Refresh current page metadata',
    })
  }

  const handleLimitedSync = async () => {
    const configuredSyncMaxBooks =
      resolveConfiguredSyncMaxBooks() ?? debugSyncMaxBooksLimit

    await runSync({
      maxBooksOverride: configuredSyncMaxBooks,
    })
  }

  const handleDebugSyncFromScratch = async () => {
    const debugNamespacePrefix = `${debugNamespaceRoot}/${format(
      new Date(),
      'yyyyMMdd-HHmmss',
    )}`
    await runSync({
      ignoreCheckpoint: true,
      namespacePrefix: debugNamespacePrefix,
      renderedDebugPages: true,
      maxBooksOverride: debugSyncMaxBooksLimit,
      pageNameMode: 'namespace',
    })
  }

  const handleFlatDebugSyncFromScratch = async () => {
    const debugNamespacePrefix = `${debugNamespaceRoot}-${format(
      new Date(),
      'yyyyMMdd-HHmmss',
    )}`
    await runSync({
      ignoreCheckpoint: true,
      namespacePrefix: debugNamespacePrefix,
      renderedDebugPages: true,
      maxBooksOverride: debugSyncMaxBooksLimit,
      pageNameMode: 'flat',
    })
  }

  const runReaderManagedSync = async ({
    namespacePrefix,
    logPrefix,
    statusPrefix,
    syncHeaderMode,
    mode,
    resumeState,
    automaticResumeAttempt = 0,
    runStartedAtMs,
  }: {
    namespacePrefix: string
    logPrefix: string
    statusPrefix: string
    syncHeaderMode: 'formal' | 'preview'
    mode: ReaderSyncMode
    resumeState?: ReaderPreviewLoadResumeState
    automaticResumeAttempt?: number
    runStartedAtMs?: number
  }) => {
    const token = logseq.settings?.apiToken as string
    if (!token) {
      setStatus('error')
      setStatusMessage('No API token configured. Set it in plugin settings.')
      return
    }

    const graphContext = await loadCurrentGraphContextV1()
    const previewCache = createGraphReaderSyncCacheV1(graphContext.graphId)
    const targetDocuments =
      mode === 'full-library-scan' || mode === 'cached-full-rebuild'
        ? resolveConfiguredReaderFullScanTargetDocuments()
        : null
    const debugHighlightPageLimit =
      mode === 'cached-full-rebuild'
        ? null
        : resolveConfiguredReaderDebugHighlightPageLimit()
    let readerSyncStateBeforeRun =
      syncHeaderMode === 'formal' ? await loadGraphReaderSyncStateV1() : null
    if (
      syncHeaderMode === 'formal' &&
      mode === 'incremental-window' &&
      readerSyncStateBeforeRun?.updatedAfter == null
    ) {
      try {
        const cacheState = await previewCache.getHighlightCacheState()
        if (cacheState?.latestHighlightUpdatedAt) {
          readerSyncStateBeforeRun = {
            schemaVersion: 1,
            updatedAfter: cacheState.latestHighlightUpdatedAt,
            committedAt: cacheState.cachedAt,
            source: 'incremental_sync',
          }
          logReadwiseInfo(
            logPrefix,
            'using IndexedDB highlight cache timestamp as incremental cursor fallback',
            {
              latestHighlightUpdatedAt: cacheState.latestHighlightUpdatedAt,
              cachedAt: cacheState.cachedAt,
              staleDeletionRisk: cacheState.staleDeletionRisk,
              hasFullLibrarySnapshot: cacheState.hasFullLibrarySnapshot,
              highlightCount: cacheState.highlightCount,
            },
          )
        }
      } catch (error) {
        logReadwiseWarn(
          logPrefix,
          'failed to load IndexedDB highlight cache state for incremental cursor fallback',
          {
            formattedError: describeUnknownError(error),
          },
        )
      }
    }
    const readerSyncUpdatedAfter =
      mode === 'incremental-window'
        ? readerSyncStateBeforeRun?.updatedAfter ?? null
        : null
    const debugCapSummary =
      debugHighlightPageLimit != null
        ? `, debug cap ${debugHighlightPageLimit} highlight page(s)`
        : ''
    const targetDocumentsSummary =
      targetDocuments == null
        ? 'all matched documents'
        : `${targetDocuments} target document(s)`

    cancelledRef.current = false
    retryActionRef.current = {
      kind: syncHeaderMode,
      label: statusPrefix,
      run: () =>
        runReaderManagedSync({
          namespacePrefix,
          logPrefix,
          statusPrefix,
          syncHeaderMode,
          mode,
        }),
    }
    clearRunIssues()
    setPageDiffResult(null)
    setCurrent(0)
    setTotal(mode === 'cached-full-rebuild' ? 1 : debugHighlightPageLimit ?? 0)
    setCurrentBook('')
    setStatus('fetching')
    const startedAt = new Date().toISOString()
    setRunIssueContext({
      modeLabel: statusPrefix,
      namespacePrefix,
      logLevel: String(logseq.settings?.logLevel ?? 'warn'),
      statusMessage: '',
      startedAt,
      completedAt: null,
      targetDocuments,
      debugHighlightPageLimit,
      processedItems: 0,
      issuesCount: 0,
    })
    setStatusMessage(
      mode === 'incremental-window'
        ? `${statusPrefix}: scanning Reader highlights ${buildReaderSyncUpdatedAfterSummary(
            readerSyncUpdatedAfter,
          )} and grouping changed parent documents (${debugCapSummary || 'no debug cap'})...`
        : mode === 'cached-full-rebuild'
          ? `${statusPrefix}: loading the local Reader highlight snapshot and rebuilding cached parent groups...`
          : mode === 'snapshot-only-refresh'
            ? `${statusPrefix}: full-scanning Reader highlights and rebuilding the local snapshot only (${debugCapSummary || 'no debug cap'}). Managed pages will not be rewritten.`
          : `${statusPrefix}: full-scanning Reader highlights and grouping by parent_id (${targetDocumentsSummary}${debugCapSummary})...`,
    )

    const client = createReadwiseClient(token)
    const syncErrorsForRun: RunIssue[] = []
    const blockingSyncErrorsForRun: RunIssue[] = []
    const queuedRetryEntriesByDocumentId = new Map<
      string,
      ReaderSyncRetryPageEntryV1
    >()
    const resolvedRetryReaderDocumentIds = new Set<string>()
    const queuedRetryEntriesToUpsert = new Map<string, ReaderSyncRetryPageEntryV1>()
    let retryQueueUpdateFailed = false
    const runStartedAt = runStartedAtMs ?? Date.now()
    let loadStats: ReaderPreviewLoadStats = {
      highlightPagesScanned: 0,
      highlightsScanned: 0,
      parentDocumentsIdentified: 0,
      pagesTargeted: 0,
      pagesProcessed: 0,
      estimatedHighlightPages: null,
      estimatedHighlightResults: null,
      latestHighlightUpdatedAt: null,
      usedCachedHighlightSnapshot: false,
      staleHighlightDeletionRisk: false,
      completeHighlightSnapshotRefreshed: false,
      parentMetadataCacheHits: 0,
      parentMetadataRemoteFetches: 0,
      fetchHighlightsDurationMs: 0,
      fetchDocumentsDurationMs: 0,
    }
    let writePagesDurationMs = 0
    let createdCount = 0
    let updatedCount = 0
    let unchangedCount = 0
    let renamedCount = 0

    if (syncHeaderMode === 'formal') {
      try {
        const queuedRetryEntries = await previewCache.getQueuedRetryPages()

        for (const entry of queuedRetryEntries) {
          queuedRetryEntriesByDocumentId.set(entry.readerDocumentId, entry)
        }
      } catch (error) {
        logReadwiseWarn(logPrefix, 'failed to load queued Reader retry pages', {
          formattedError: describeUnknownError(error),
        })
      }

      try {
        const graphFallbackEntries = await loadGraphReaderRetryFallbackEntriesV1()

        for (const entry of graphFallbackEntries) {
          if (!queuedRetryEntriesByDocumentId.has(entry.readerDocumentId)) {
            queuedRetryEntriesByDocumentId.set(entry.readerDocumentId, entry)
          }
        }
      } catch (error) {
        logReadwiseWarn(
          logPrefix,
          'failed to load graph fallback Reader retry pages',
          {
            formattedError: describeUnknownError(error),
          },
        )
      }
    }

    try {
      beginReaderSyncEtaPhase(
        'fetch-highlights',
        mode === 'cached-full-rebuild' ? 'cached highlight snapshot' : 'highlight scan',
      )
      const previewLoadResult = await loadReaderPreviewBooks(client, {
        maxDocuments:
          mode === 'full-library-scan' || mode === 'cached-full-rebuild'
            ? targetDocuments
            : undefined,
        mode,
        maxHighlightPages: debugHighlightPageLimit ?? undefined,
        updatedAfter: readerSyncUpdatedAfter ?? undefined,
        resumeState,
        previewCache,
        parentMetadataMode:
          mode === 'incremental-window' ? 'cache_first' : 'always_refresh',
        logPrefix,
        onProgress: (progress) => {
          if (cancelledRef.current) return

          if (progress.phase === 'fetch-highlights') {
            const uniqueParents = progress.uniqueParents ?? 0

            if (mode === 'cached-full-rebuild') {
              setStatus('fetching')
              setCurrent(1)
              setTotal(1)
              setCurrentBook('')
              updateReaderSyncEta(
                'fetch-highlights',
                'cached highlight snapshot',
                1,
                1,
              )
              setStatusMessage(
                `${statusPrefix}: loaded ${uniqueParents} cached parent document(s) from ${progress.totalHighlights ?? 0} cached highlight(s).`,
              )
              return
            }

            const totalPages = progress.totalPages ?? progress.pageNumber ?? 0
            setStatus('fetching')
            setCurrent(progress.pageNumber ?? 0)
            setTotal(totalPages)
            setCurrentBook('')
            updateReaderSyncEta(
              'fetch-highlights',
              'highlight scan',
              progress.pageNumber ?? 0,
              totalPages,
            )
            setStatusMessage(
              mode === 'incremental-window'
                ? `${statusPrefix}: scanned ${progress.pageNumber ?? 0} / ${totalPages} highlight page(s) ${buildReaderSyncUpdatedAfterSummary(
                    readerSyncUpdatedAfter,
                  )}, identified ${uniqueParents} changed parent document(s) from ${progress.totalHighlights ?? 0} highlight(s).`
                : mode === 'snapshot-only-refresh'
                  ? `${statusPrefix}: scanned ${progress.pageNumber ?? 0} / ${totalPages} highlight page(s), identified ${uniqueParents} parent document(s), and refreshed the local snapshot from ${progress.totalHighlights ?? 0} highlight(s). No page writes will run.`
                : `${statusPrefix}: scanned ${progress.pageNumber ?? 0} / ${totalPages} highlight page(s), identified ${uniqueParents} parent document(s) from ${progress.totalHighlights ?? 0} highlight(s).`,
            )
            return
          }

          setStatus('syncing')
          setCurrent(progress.completed ?? 0)
          setTotal(progress.total ?? 0)
          setCurrentBook(progress.pageTitle ?? '')
          updateReaderSyncEta(
            'fetch-documents',
            'parent document fetch',
            progress.completed ?? 0,
            progress.total ?? 0,
          )
          setStatusMessage(
            mode === 'cached-full-rebuild'
              ? `${statusPrefix}: refreshing Reader parent metadata for cached pages... ${progress.completed ?? 0} / ${progress.total ?? 0}.`
              : `${statusPrefix}: resolving Reader parent documents... ${progress.completed ?? 0} / ${progress.total ?? 0}.`,
          )
        },
      })
      let previewBooks = previewLoadResult.books
      loadStats = {
        ...previewLoadResult.stats,
        pagesProcessed: 0,
      }
      logReadwiseInfo(logPrefix, 'fetch timing diagnostics', {
        estimatedHighlightPages: loadStats.estimatedHighlightPages,
        estimatedHighlightResults: loadStats.estimatedHighlightResults,
        highlightPagesScanned: loadStats.highlightPagesScanned,
        highlightsScanned: loadStats.highlightsScanned,
        parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
        latestHighlightUpdatedAt: loadStats.latestHighlightUpdatedAt,
        usedCachedHighlightSnapshot: loadStats.usedCachedHighlightSnapshot,
        staleHighlightDeletionRisk: loadStats.staleHighlightDeletionRisk,
        completeHighlightSnapshotRefreshed:
          loadStats.completeHighlightSnapshotRefreshed,
        parentMetadataCacheHits: loadStats.parentMetadataCacheHits,
        parentMetadataRemoteFetches: loadStats.parentMetadataRemoteFetches,
        fetchHighlightsDurationMs: loadStats.fetchHighlightsDurationMs,
        fetchDocumentsDurationMs: loadStats.fetchDocumentsDurationMs,
        averageHighlightPageDurationMs:
          loadStats.highlightPagesScanned > 0
            ? Math.round(
                loadStats.fetchHighlightsDurationMs / loadStats.highlightPagesScanned,
              )
            : null,
        averageParentDocumentDurationMs:
          loadStats.pagesTargeted > 0
            ? Math.round(
                loadStats.fetchDocumentsDurationMs / loadStats.pagesTargeted,
              )
            : null,
      })

      if (mode === 'cached-full-rebuild' && loadStats.staleHighlightDeletionRisk) {
        logReadwiseWarn(
          logPrefix,
          'cached rebuild is using a highlight snapshot that may still include deleted highlights',
          {
            graphId: graphContext.graphId,
            latestHighlightUpdatedAt: loadStats.latestHighlightUpdatedAt,
          },
        )
      }

      const queuedRetryParentIdsToReload =
        syncHeaderMode === 'formal' && mode !== 'snapshot-only-refresh'
          ? [...queuedRetryEntriesByDocumentId.keys()].filter(
              (readerDocumentId) =>
                !previewBooks.some((previewBook) => previewBook.document.id === readerDocumentId),
            )
          : []

      if (queuedRetryParentIdsToReload.length > 0) {
        try {
          setStatusMessage(
            `${statusPrefix}: reloading ${queuedRetryParentIdsToReload.length} queued retry page(s) from the local Reader cache...`,
          )

          const queuedRetryLoadResult = await loadReaderPreviewBooksByParentIds(client, {
            parentIds: queuedRetryParentIdsToReload,
            previewCache,
            parentMetadataMode: 'cache_first',
            logPrefix,
            highlightCoverage: 'cached-full-rebuild',
          })

          if (queuedRetryLoadResult.books.length > 0) {
            previewBooks = [...previewBooks, ...queuedRetryLoadResult.books]
            loadStats.pagesTargeted += queuedRetryLoadResult.books.length
            loadStats.parentMetadataCacheHits +=
              queuedRetryLoadResult.parentMetadataCacheHits
            loadStats.parentMetadataRemoteFetches +=
              queuedRetryLoadResult.parentMetadataRemoteFetches
            loadStats.fetchDocumentsDurationMs +=
              queuedRetryLoadResult.fetchDocumentsDurationMs
          }

          logReadwiseInfo(logPrefix, 'merged queued retry pages into sync target set', {
            queuedRetryEntries: queuedRetryEntriesByDocumentId.size,
            queuedRetryPagesRequested: queuedRetryParentIdsToReload.length,
            queuedRetryPagesLoaded: queuedRetryLoadResult.books.length,
            unresolvedQueuedRetryPages:
              queuedRetryLoadResult.unresolvedParentIds.length,
            unresolvedQueuedRetryPageIds:
              queuedRetryLoadResult.unresolvedParentIds,
          })
        } catch (error) {
          logReadwiseWarn(
            logPrefix,
            'failed to reload queued retry pages; continuing without them',
            {
              queuedRetryEntries: queuedRetryParentIdsToReload.length,
              formattedError: describeUnknownError(error),
            },
          )
        }
      }

      if (cancelledRef.current) return

      if (mode === 'snapshot-only-refresh') {
        if (syncHeaderMode === 'formal') {
          const summary: GraphLastFormalSyncSummaryV1 = {
            schemaVersion: 1,
            runKind: buildFormalRunKind(mode),
            status: 'success',
            completedAt: new Date().toISOString(),
            highlightPagesScanned: loadStats.highlightPagesScanned,
            highlightsScanned: loadStats.highlightsScanned,
            parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
            pagesTargeted: 0,
            pagesProcessed: 0,
            createdCount: 0,
            updatedCount: 0,
            unchangedCount: 0,
            renamedCount: 0,
            errorCount: 0,
            totalDurationMs: Date.now() - runStartedAt,
            fetchHighlightsDurationMs: loadStats.fetchHighlightsDurationMs,
            fetchDocumentsDurationMs: loadStats.fetchDocumentsDurationMs,
            writePagesDurationMs: 0,
            failureSummary: null,
          }
          await saveGraphLastFormalSyncSummaryV1(summary)
          logReadwiseInfo(logPrefix, 'saved graph formal sync summary', summary)
          logReadwiseInfo(logPrefix, 'skipped graph reader sync state update', {
            mode,
            reason: 'snapshot_refresh_does_not_advance_cursor',
            targetDocuments: null,
            parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
          })
        }

        const incompleteSnapshotSuffix = !loadStats.completeHighlightSnapshotRefreshed
          ? ' Local highlight snapshot was left unchanged because this run did not exhaust the full Reader highlight library.'
          : ''

        setStatus('completed')
        setCurrent(0)
        setTotal(0)
        setCurrentBook('')
        setRunIssueContext((previous) =>
          previous == null
            ? previous
            : {
                ...previous,
                completedAt: new Date().toISOString(),
                processedItems: loadStats.highlightsScanned,
                issuesCount: 0,
                stats: {
                  highlightPagesScanned: loadStats.highlightPagesScanned,
                  highlightsScanned: loadStats.highlightsScanned,
                  parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
                  pagesTargeted: 0,
                  pagesProcessed: 0,
                  fetchHighlightsDurationMs: loadStats.fetchHighlightsDurationMs,
                  fetchDocumentsDurationMs: loadStats.fetchDocumentsDurationMs,
                  writePagesDurationMs: 0,
                },
              },
        )
        setStatusMessage(
          `${statusPrefix}: refreshed the local full-library snapshot from ${loadStats.highlightsScanned} highlight(s) across ${loadStats.highlightPagesScanned} remote page(s). Managed pages were not rewritten.${incompleteSnapshotSuffix}`,
        )
        logReadwiseInfo(logPrefix, 'snapshot refresh completed', {
          mode,
          graphId: graphContext.graphId,
          highlightsScanned: loadStats.highlightsScanned,
          highlightPagesScanned: loadStats.highlightPagesScanned,
          parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
          completeHighlightSnapshotRefreshed:
            loadStats.completeHighlightSnapshotRefreshed,
        })
        return
      }

      if (previewBooks.length === 0) {
        if (syncHeaderMode === 'formal') {
          const summary: GraphLastFormalSyncSummaryV1 = {
            schemaVersion: 1,
            runKind: buildFormalRunKind(mode),
            status: 'success',
            completedAt: new Date().toISOString(),
            highlightPagesScanned: loadStats.highlightPagesScanned,
            highlightsScanned: loadStats.highlightsScanned,
            parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
            pagesTargeted: loadStats.pagesTargeted,
            pagesProcessed: 0,
            createdCount: 0,
            updatedCount: 0,
            unchangedCount: 0,
            renamedCount: 0,
            errorCount: 0,
            totalDurationMs: Date.now() - runStartedAt,
            fetchHighlightsDurationMs: loadStats.fetchHighlightsDurationMs,
            fetchDocumentsDurationMs: loadStats.fetchDocumentsDurationMs,
            writePagesDurationMs: 0,
            failureSummary: null,
          }
          await saveGraphLastFormalSyncSummaryV1(summary)
          logReadwiseInfo(logPrefix, 'saved graph formal sync summary', summary)

          if (
            shouldAdvanceReaderSyncCursor({
              mode,
              blockingSyncErrorsForRun,
              debugHighlightPageLimit,
              targetDocuments,
              loadStats,
            })
          ) {
            const nextReaderSyncState: GraphReaderSyncStateV1 = {
              schemaVersion: 1,
              updatedAfter:
                loadStats.latestHighlightUpdatedAt ??
                readerSyncStateBeforeRun?.updatedAfter ??
                null,
              committedAt: new Date().toISOString(),
              source:
                mode === 'incremental-window'
                  ? 'incremental_sync'
                  : 'full_reconcile',
            }
            const savedReaderSyncState =
              await saveGraphReaderSyncStateV1(nextReaderSyncState)
            setReaderSyncState(savedReaderSyncState)
            logReadwiseInfo(logPrefix, 'saved graph reader sync state', savedReaderSyncState)
          } else {
            logReadwiseInfo(logPrefix, 'skipped graph reader sync state update', {
              mode,
              reason:
                mode === 'cached-full-rebuild'
                  ? 'cached_rebuild_does_not_advance_cursor'
                  : debugHighlightPageLimit != null
                    ? 'debug_highlight_page_cap_active'
                    : 'full_reconcile_document_limit_active',
              targetDocuments,
              parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
            })
          }
        }

        setStatus('completed')
        setRunIssueContext((previous) =>
          previous == null
            ? previous
            : {
                ...previous,
                completedAt: new Date().toISOString(),
                processedItems: 0,
                issuesCount: 0,
                stats: {
                  highlightPagesScanned: loadStats.highlightPagesScanned,
                  highlightsScanned: loadStats.highlightsScanned,
                  parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
                  pagesTargeted: loadStats.pagesTargeted,
                  pagesProcessed: 0,
                  fetchHighlightsDurationMs: loadStats.fetchHighlightsDurationMs,
                  fetchDocumentsDurationMs: loadStats.fetchDocumentsDurationMs,
                },
              },
        )
        setStatusMessage(
          mode === 'incremental-window'
            ? `${statusPrefix}: no changed Reader pages were available.`
            : mode === 'cached-full-rebuild'
              ? `${statusPrefix}: no cached Reader pages were available.`
              : `${statusPrefix}: no Reader pages were available.`,
        )
        logReadwiseInfo(logPrefix, 'no pages available')
        return
      }

      setCurrent(0)
      setTotal(previewBooks.length)
      setCurrentBook('')
      beginReaderSyncEtaPhase('write-pages', 'page writes')
      setStatusMessage(
        mode === 'incremental-window'
          ? `${statusPrefix}: syncing ${previewBooks.length} changed Reader page(s) into ${namespacePrefix}...`
          : mode === 'cached-full-rebuild'
            ? `${statusPrefix}: syncing ${previewBooks.length} Reader page(s) from the cached highlight snapshot into ${namespacePrefix}...`
            : `${statusPrefix}: syncing ${previewBooks.length} Reader page(s) from full-library highlight groups into ${namespacePrefix}...`,
      )
      const writePagesStartedAt = Date.now()

      for (let index = 0; index < previewBooks.length; index += 1) {
        if (cancelledRef.current) return

        const previewBook = previewBooks[index]!
        const pageTitle = previewBook.document.title ?? previewBook.document.id
        setCurrentBook(pageTitle)

        try {
          const pageSyncResult = await syncRenderedReaderPreviewPage(
            previewBook,
            namespacePrefix,
            logPrefix,
            {
              syncHeaderText:
                syncHeaderMode === 'preview'
                  ? `Reader v3 preview synced by [[Readwise]] [[${format(new Date(), 'yyyy-MM-dd')}]]`
                  : undefined,
              pageResolveMode:
                syncHeaderMode === 'formal'
                  ? 'reader_id_then_title'
                  : 'title_only',
              identityNamespaceRoot:
                syncHeaderMode === 'formal'
                  ? formalNamespaceRoot
                  : namespacePrefix,
            },
          )
          if (pageSyncResult.result === 'created') createdCount += 1
          if (pageSyncResult.result === 'updated') updatedCount += 1
          if (pageSyncResult.result === 'unchanged') unchangedCount += 1
          if (pageSyncResult.pageRenamed) renamedCount += 1
          if (queuedRetryEntriesByDocumentId.has(previewBook.document.id)) {
            resolvedRetryReaderDocumentIds.add(previewBook.document.id)
          }
        } catch (err: unknown) {
          const message = describeUnknownError(err)
          logReadwiseError(logPrefix, 'failed to sync rendered Reader page', {
            pageTitle,
            readerDocumentId: previewBook.document.id,
            namespacePrefix,
            formattedError: message,
            error: err,
          })
          const issue = diagnoseRunIssue({
            book: pageTitle,
            message,
            readerDocumentId: previewBook.document.id,
            namespacePrefix,
            pageName: queuedRetryEntriesByDocumentId.get(previewBook.document.id)?.pageName ?? null,
          })
          syncErrorsForRun.push(issue)
          if (shouldRunIssueBlockReaderSyncCursor(issue)) {
            blockingSyncErrorsForRun.push(issue)
          } else if (syncHeaderMode === 'formal') {
            const existingRetryEntry = queuedRetryEntriesByDocumentId.get(
              previewBook.document.id,
            )
            const now = new Date().toISOString()
            queuedRetryEntriesToUpsert.set(previewBook.document.id, {
              readerDocumentId: previewBook.document.id,
              pageName:
                existingRetryEntry?.pageName ??
                pageTitle,
              category: issue.category,
              message: issue.message,
              queuedAt: existingRetryEntry?.queuedAt ?? now,
              lastSeenAt: now,
            })
          }
          appendRunIssue(issue)
        }
        loadStats.pagesProcessed += 1
        setCurrent(index + 1)
        updateReaderSyncEta(
          'write-pages',
          'page writes',
          index + 1,
          previewBooks.length,
        )
      }
      writePagesDurationMs = Date.now() - writePagesStartedAt
      logReadwiseInfo(logPrefix, 'write timing diagnostics', {
        pagesWrittenAttempted: previewBooks.length,
        pagesProcessed: loadStats.pagesProcessed,
        writePagesDurationMs,
        averagePageWriteDurationMs:
          previewBooks.length > 0
            ? Math.round(writePagesDurationMs / previewBooks.length)
            : null,
      })

      if (syncHeaderMode === 'formal') {
        if (resolvedRetryReaderDocumentIds.size > 0) {
          try {
            await previewCache.removeQueuedRetryPages([
              ...resolvedRetryReaderDocumentIds,
            ])
          } catch (error) {
            logReadwiseWarn(
              logPrefix,
              'failed to clear resolved Reader retry pages',
              {
                resolvedRetryReaderDocumentIds: [...resolvedRetryReaderDocumentIds],
                formattedError: describeUnknownError(error),
              },
            )
          }

          try {
            await removeGraphReaderRetryFallbackEntriesV1([
              ...resolvedRetryReaderDocumentIds,
            ])
          } catch (error) {
            logReadwiseWarn(
              logPrefix,
              'failed to clear resolved Reader retry pages from graph fallback page',
              {
                resolvedRetryReaderDocumentIds: [...resolvedRetryReaderDocumentIds],
                formattedError: describeUnknownError(error),
              },
            )
          }
        }

        if (queuedRetryEntriesToUpsert.size > 0) {
          try {
            await previewCache.queueRetryPages([
              ...queuedRetryEntriesToUpsert.values(),
            ])

            try {
              await removeGraphReaderRetryFallbackEntriesV1([
                ...queuedRetryEntriesToUpsert.keys(),
              ])
            } catch (error) {
              logReadwiseWarn(
                logPrefix,
                'failed to clear queued Reader retry pages from graph fallback page after IndexedDB persistence',
                {
                  queuedRetryReaderDocumentIds: [
                    ...queuedRetryEntriesToUpsert.keys(),
                  ],
                  formattedError: describeUnknownError(error),
                },
              )
            }
          } catch (error) {
            logReadwiseWarn(
              logPrefix,
              'failed to persist queued Reader retry pages in IndexedDB; falling back to graph state page',
              {
                queuedRetryReaderDocumentIds: [
                  ...queuedRetryEntriesToUpsert.keys(),
                ],
                formattedError: describeUnknownError(error),
              },
            )

            const fallbackEntriesByDocumentId = new Map<
              string,
              ReaderSyncRetryPageEntryV1
            >()

            for (const [readerDocumentId, entry] of queuedRetryEntriesByDocumentId) {
              if (!resolvedRetryReaderDocumentIds.has(readerDocumentId)) {
                fallbackEntriesByDocumentId.set(readerDocumentId, entry)
              }
            }

            for (const entry of queuedRetryEntriesToUpsert.values()) {
              fallbackEntriesByDocumentId.set(entry.readerDocumentId, entry)
            }

            try {
              await saveGraphReaderRetryFallbackEntriesV1([
                ...fallbackEntriesByDocumentId.values(),
              ])
            } catch (fallbackError) {
              retryQueueUpdateFailed = true
              logReadwiseWarn(
                logPrefix,
                'failed to persist queued Reader retry pages to graph fallback page',
                {
                  queuedRetryReaderDocumentIds: [
                    ...fallbackEntriesByDocumentId.keys(),
                  ],
                  formattedError: describeUnknownError(fallbackError),
                },
              )
            }
          }
        }
      }

      if (syncHeaderMode === 'formal') {
        const summary: GraphLastFormalSyncSummaryV1 = {
          schemaVersion: 1,
          runKind: buildFormalRunKind(mode),
          status: syncErrorsForRun.length > 0 ? 'partial_error' : 'success',
          completedAt: new Date().toISOString(),
          highlightPagesScanned: loadStats.highlightPagesScanned,
          highlightsScanned: loadStats.highlightsScanned,
          parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
          pagesTargeted: loadStats.pagesTargeted,
          pagesProcessed: loadStats.pagesProcessed,
          createdCount,
          updatedCount,
          unchangedCount,
          renamedCount,
          errorCount: syncErrorsForRun.length,
          totalDurationMs: Date.now() - runStartedAt,
          fetchHighlightsDurationMs: loadStats.fetchHighlightsDurationMs,
          fetchDocumentsDurationMs: loadStats.fetchDocumentsDurationMs,
          writePagesDurationMs,
          failureSummary:
            syncErrorsForRun.length > 0
              ? `${syncErrorsForRun.length} page(s) failed during formal Reader sync. First error: ${syncErrorsForRun[0]?.book}: ${syncErrorsForRun[0]?.message}`
              : null,
        }
        await saveGraphLastFormalSyncSummaryV1(summary)
        logReadwiseInfo(logPrefix, 'saved graph formal sync summary', summary)

        if (
          !retryQueueUpdateFailed &&
          shouldAdvanceReaderSyncCursor({
            mode,
            blockingSyncErrorsForRun,
            debugHighlightPageLimit,
            targetDocuments,
            loadStats,
          })
        ) {
          const nextReaderSyncState: GraphReaderSyncStateV1 = {
            schemaVersion: 1,
            updatedAfter:
              loadStats.latestHighlightUpdatedAt ??
              readerSyncStateBeforeRun?.updatedAfter ??
              null,
            committedAt: new Date().toISOString(),
            source:
              mode === 'incremental-window'
                ? 'incremental_sync'
                : 'full_reconcile',
          }
          const savedReaderSyncState =
            await saveGraphReaderSyncStateV1(nextReaderSyncState)
          setReaderSyncState(savedReaderSyncState)
          logReadwiseInfo(logPrefix, 'saved graph reader sync state', savedReaderSyncState)
        } else {
          logReadwiseInfo(logPrefix, 'skipped graph reader sync state update', {
            mode,
            reason:
              retryQueueUpdateFailed
                ? 'retry_queue_update_failed'
                : blockingSyncErrorsForRun.length > 0
                ? 'blocking_page_errors_present'
                : mode === 'cached-full-rebuild'
                  ? 'cached_rebuild_does_not_advance_cursor'
                  : debugHighlightPageLimit != null
                    ? 'debug_highlight_page_cap_active'
                    : 'full_reconcile_document_limit_active',
            retryQueueUpdateFailed,
            blockingPageErrorCount: blockingSyncErrorsForRun.length,
            nonBlockingPageErrorCount:
              syncErrorsForRun.length - blockingSyncErrorsForRun.length,
            targetDocuments,
            parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
          })
        }
      }

      const staleDeletionSuffix =
        mode === 'cached-full-rebuild' && loadStats.staleHighlightDeletionRisk
          ? ' Cached snapshot may still include deleted highlights until Full Refresh runs again.'
          : ''
      const incompleteSnapshotSuffix =
        mode === 'full-library-scan' && !loadStats.completeHighlightSnapshotRefreshed
          ? ' Local highlight snapshot was left unchanged because this run did not exhaust the full Reader highlight library.'
          : ''
      setStatus('completed')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: loadStats.pagesProcessed,
              issuesCount: syncErrorsForRun.length,
              stats: {
                highlightPagesScanned: loadStats.highlightPagesScanned,
                highlightsScanned: loadStats.highlightsScanned,
                parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
                pagesTargeted: loadStats.pagesTargeted,
                pagesProcessed: loadStats.pagesProcessed,
                createdCount,
                updatedCount,
                unchangedCount,
                renamedCount,
                fetchHighlightsDurationMs: loadStats.fetchHighlightsDurationMs,
                fetchDocumentsDurationMs: loadStats.fetchDocumentsDurationMs,
                writePagesDurationMs,
              },
            },
      )
      setStatusMessage(
        syncErrorsForRun.length > 0
          ? `${statusPrefix}: completed with ${syncErrorsForRun.length} error(s).`
          : `${statusPrefix}: complete. ${previewBooks.length} page(s) written to ${namespacePrefix}.${debugHighlightPageLimit != null ? ` Debug cap ${debugHighlightPageLimit} was active.` : ''}${staleDeletionSuffix}${incompleteSnapshotSuffix}`,
      )
      logReadwiseInfo(logPrefix, 'sync completed', {
        mode,
        graphId: graphContext.graphId,
        namespacePrefix,
        processedBooks: previewBooks.length,
        errorCount: syncErrorsForRun.length,
      })
    } catch (err: unknown) {
      if (isReaderPreviewLoadResumeError(err)) {
        const retryTarget = describeReaderResumeTarget(err.resumeState)

        if (automaticResumeAttempt < maxAutomaticResumeRetries) {
          const retryDelayMs = 1500 * (automaticResumeAttempt + 1)
          const automaticRetryOrdinal = automaticResumeAttempt + 1
          const retryTotal = maxAutomaticResumeRetries
          const isFetchHighlightsResume =
            err.resumeState.phase === 'fetch-highlights'
          const resumeTotal =
            err.resumeState.phase === 'fetch-highlights'
              ? Math.max(
                  err.resumeState.initialTotalPages ??
                    err.resumeState.maxHighlightPages ??
                    err.resumeState.pageNumber,
                  err.resumeState.pageNumber,
                )
              : err.resumeState.selectedParentIds.length

          retryActionRef.current = null
          setStatus(isFetchHighlightsResume ? 'fetching' : 'syncing')
          setCurrent(
            isFetchHighlightsResume
              ? err.resumeState.pageNumber
              : err.resumeState.documentIndex,
          )
          setTotal(resumeTotal)
          setCurrentBook('')
          setStatusMessage(
            `${statusPrefix}: interrupted during ${retryTarget}. Retrying automatically ${automaticRetryOrdinal} / ${retryTotal}...`,
          )
          logReadwiseWarn(logPrefix, 'resumable Reader sync step failed; retrying automatically', {
            retryTarget,
            automaticRetryOrdinal,
            retryTotal,
            retryDelayMs,
            resumePhase: err.resumeState.phase,
            formattedError: describeUnknownError(err),
          })
          await sleep(retryDelayMs)

          if (cancelledRef.current) {
            return
          }

          await runReaderManagedSync({
            namespacePrefix,
            logPrefix,
            statusPrefix,
            syncHeaderMode,
            mode,
            resumeState: err.resumeState,
            automaticResumeAttempt: automaticResumeAttempt + 1,
            runStartedAtMs: runStartedAt,
          })
          return
        }

        retryActionRef.current = {
          kind: syncHeaderMode,
          label: retryTarget,
          run: () =>
            runReaderManagedSync({
              namespacePrefix,
              logPrefix,
              statusPrefix,
              syncHeaderMode,
              mode,
              resumeState: err.resumeState,
              runStartedAtMs: runStartedAt,
            }),
        }
      }

      if (syncHeaderMode === 'formal') {
        const message = describeUnknownError(err)
        const summary: GraphLastFormalSyncSummaryV1 = {
          schemaVersion: 1,
          runKind: buildFormalRunKind(mode),
          status: 'failed',
          completedAt: new Date().toISOString(),
          highlightPagesScanned: loadStats.highlightPagesScanned,
          highlightsScanned: loadStats.highlightsScanned,
          parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
          pagesTargeted: loadStats.pagesTargeted,
          pagesProcessed: loadStats.pagesProcessed,
          createdCount,
          updatedCount,
          unchangedCount,
          renamedCount,
          errorCount: syncErrorsForRun.length + 1,
          totalDurationMs: Date.now() - runStartedAt,
          fetchHighlightsDurationMs: loadStats.fetchHighlightsDurationMs,
          fetchDocumentsDurationMs: loadStats.fetchDocumentsDurationMs,
          writePagesDurationMs,
          failureSummary: message,
        }
        await saveGraphLastFormalSyncSummaryV1(summary)
        logReadwiseInfo(logPrefix, 'saved graph formal sync summary', summary)
      }
      logReadwiseError(logPrefix, 'sync failed', err)
      setStatus('error')
      setRunIssueContext((previous) =>
        previous == null
          ? previous
          : {
              ...previous,
              completedAt: new Date().toISOString(),
              processedItems: loadStats.pagesProcessed,
              issuesCount: syncErrorsForRun.length,
              stats: {
                highlightPagesScanned: loadStats.highlightPagesScanned,
                highlightsScanned: loadStats.highlightsScanned,
                parentDocumentsIdentified: loadStats.parentDocumentsIdentified,
                pagesTargeted: loadStats.pagesTargeted,
                pagesProcessed: loadStats.pagesProcessed,
                createdCount,
                updatedCount,
                unchangedCount,
                renamedCount,
                fetchHighlightsDurationMs: loadStats.fetchHighlightsDurationMs,
                fetchDocumentsDurationMs: loadStats.fetchDocumentsDurationMs,
                writePagesDurationMs,
              },
            },
      )
      setStatusMessage(
        isReaderPreviewLoadResumeError(err)
          ? `${statusPrefix} failed: ${describeUnknownError(err)}. Retry will resume ${describeReaderResumeTarget(err.resumeState)}.`
          : `${statusPrefix} failed: ${describeUnknownError(err)}`,
      )
    }
  }

  const detectFormalSyncConflicts = async () => {
    const [readerPreviewPages, debugPages, sessionTestPages] = await Promise.all([
      listManagedPagesByNamespacePrefix(readerPreviewNamespaceRoot),
      listManagedPagesByNamespacePrefix(debugNamespaceRoot),
      listManagedPagesBySessionNamespaceRoot(formalNamespaceRoot),
    ])

    const conflicts = [
      {
        label: 'Reader preview',
        clearAction: 'Clear Reader Preview Pages',
        pages: readerPreviewPages,
      },
      {
        label: 'debug',
        clearAction: 'Clear Debug Pages',
        pages: debugPages,
      },
      {
        label: 'session test',
        clearAction: 'Clear Session Test Pages',
        pages: sessionTestPages,
      },
    ].filter((entry) => entry.pages.length > 0)

    if (conflicts.length === 0) {
      return null
    }

    return conflicts
  }

  const handleReaderPreviewSync = async () => {
    const previewNamespacePrefix = `${readerPreviewNamespaceRoot}/${format(
      new Date(),
      'yyyyMMdd-HHmmss',
    )}`

    setCacheSummaryResult(null)

    await runReaderManagedSync({
      namespacePrefix: previewNamespacePrefix,
      logPrefix: readerPreviewLogPrefix,
      statusPrefix: 'Reader v3 preview',
      syncHeaderMode: 'preview',
      mode: 'full-library-scan',
    })
  }

  const handleClearReaderPreviewPages = async () => {
    clearRunIssues()
    setPageDiffResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage(`Deleting ${readerPreviewNamespaceRoot} pages...`)

    try {
      const result = await clearManagedPagesByNamespacePrefix(readerPreviewNamespaceRoot, {
        onProgress: ({ phase, total, completed, pageTitle }) => {
          if (phase === 'start') {
            setStatus('syncing')
            setCurrent(0)
            setTotal(total)
            setCurrentBook('')
            setStatusMessage(
              total > 0
                ? `Deleting ${total} Reader preview page(s)...`
                : `Deleting ${readerPreviewNamespaceRoot} pages...`,
            )
            return
          }

          setStatus('syncing')
          setCurrent(completed)
          setTotal(total)
          setCurrentBook(pageTitle ?? '')
          setStatusMessage(
            total > 0
              ? `Deleting ${completed} / ${total} Reader preview page(s)...`
              : `Deleting ${readerPreviewNamespaceRoot} pages...`,
          )
        },
      })

      if (result.skippedPages.length > 0) {
        replaceRunIssues(
          result.skippedPages.map((pageTitle) => ({
            book: pageTitle,
            message: 'Delete failed for this Reader preview page.',
          })),
        )
      }

      setStatus('completed')
      setStatusMessage(`Deleted ${result.touchedPages} Reader preview page(s).`)
      logReadwiseInfo(readerPreviewLogPrefix, 'cleared preview pages', {
        namespacePrefix: readerPreviewNamespaceRoot,
        matchedPages: result.matchedPages,
        deletedPages: result.touchedPages,
        skippedPages: result.skippedPages,
      })
    } catch (err: unknown) {
      logReadwiseError(readerPreviewLogPrefix, 'failed to clear preview pages', err)
      setStatus('error')
      setStatusMessage(
        `Failed to clear Reader preview pages: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const progressPct = total > 0 ? Math.round((current / total) * 100) : 0
  const liveEstimatedRemainingMs =
    etaSnapshot?.etaMs != null
      ? Math.max(0, etaSnapshot.etaMs - ((etaTick || Date.now()) - etaSnapshot.observedAt))
      : null
  const isBusy = status === 'fetching' || status === 'syncing'
  const configuredSyncMaxBooks =
    resolveConfiguredSyncMaxBooks() ?? debugSyncMaxBooksLimit
  const configuredReaderTargetDocuments =
    resolveConfiguredReaderFullScanTargetDocuments()
  const configuredReaderDebugHighlightPageLimit =
    resolveConfiguredReaderDebugHighlightPageLimit() ?? 0
  const showManagedPagesSummary =
    configuredReaderTargetDocuments !== defaultReaderFullScanTargetDocuments
  const showHighlightScanSummary =
    configuredReaderDebugHighlightPageLimit !==
    defaultReaderFullScanDebugHighlightPageLimit
  const summaryCardCount =
    Number(showManagedPagesSummary) + Number(showHighlightScanSummary)
  const sessionTestSyncCount =
    activeFormalTestSessionCount ?? configuredSyncMaxBooks
  const sessionTestSyncLabel = `Incremental Sync (session test: ${sessionTestSyncCount})`
  const issueCategorySummary =
    errors.length > 0 ? summarizeRunIssueCategories(errors) : ''
  const diagnosedErrors = errors.map((issue) => diagnoseRunIssue(issue))
  const blockingIssues = diagnosedErrors.filter(
    (issue) => issue.category !== 'warning',
  )
  const warningIssues = diagnosedErrors.filter(
    (issue) => issue.category === 'warning',
  )
  const statusLabel =
    status === 'completed' && errors.length > 0
      ? 'warning'
      : status === 'idle'
        ? 'ready'
        : status
  const idleCursorSummary =
    readerSyncState?.updatedAfter != null
      ? `Updated after ${formatTimestampForUi(readerSyncState.updatedAfter)}`
      : 'No incremental cursor saved yet.'
  const idleCursorSavedAt =
    readerSyncState?.committedAt != null
      ? `Saved ${formatTimestampForUi(readerSyncState.committedAt)}`
      : ''
  const statusHeadline =
    status === 'idle'
      ? 'Last sync cursor'
      : status === 'fetching'
        ? 'Scanning Reader highlights'
        : status === 'syncing'
          ? 'Rebuilding managed pages'
          : status === 'completed' && errors.length > 0
            ? 'Completed with issues'
            : status === 'completed'
              ? 'Reader sync completed'
              : 'Reader sync stopped'
  const statusPhaseLabel =
    etaSnapshot?.label ??
    (status === 'fetching'
      ? 'highlight scan'
      : status === 'syncing'
        ? 'page writes'
        : status === 'completed'
          ? 'latest run'
          : '')
  const statusDetail =
    status === 'idle'
      ? statusMessage || idleCursorSummary
      : statusMessage || 'Ready to sync your Readwise highlights.'
  const currentOperationLabel =
    status === 'syncing'
      ? currentBook
      : status === 'fetching'
        ? 'Scanning Reader highlight pages and grouping by parent document.'
        : ''
  const activeHelpPopover = pinnedHelpPopover ?? hoveredHelpPopover
  const shortManagedPagesSummary =
    configuredReaderTargetDocuments == null
      ? 'Full Refresh targets every matched parent page.'
      : 'Full Refresh targets this many managed pages.'
  const managedPagesHelpNotes = [
    configuredReaderTargetDocuments == null
      ? 'Full Refresh writes every matched parent document in this run.'
      : 'Full Refresh targets this many managed pages per run.',
  ]
  const highlightScanHelpNotes =
    configuredReaderDebugHighlightPageLimit > 0
      ? [
          'Debug cap active for any remote Reader highlight scan.',
          'Full Refresh stays intentionally incomplete while the cap is on.',
          'A truncated run does not refresh the local cached snapshot.',
          'Roughly 100 highlights arrive per remote page.',
        ]
      : [
          'Incremental Sync scans changed Reader highlights only.',
          'Full Refresh scans the full Reader highlight library.',
        ]
  const librarySyncHelpNotes = [
    'Incremental Sync pulls changed Reader highlights, refreshes parent metadata for matched documents, and rewrites managed pages in ReadwiseHighlights/<title>.',
    'Full Refresh rescans the full Reader highlight library, refreshes parent metadata, and replaces the local full-library snapshot used for future rebuilds and deletion calibration.',
    'Full Refresh uses the Debug settings. A truncated highlight scan does not refresh the local cached snapshot.',
  ]
  const currentPageHelpNotes = [
    'Rebuild Current Page From Cache uses rw-reader-id, reads the cached highlight snapshot for that parent, and rewrites only the current managed page.',
    "Refresh Current Page Metadata re-fetches the current page's parent metadata from Reader, combines it with cached highlights, and rewrites only the current managed page.",
    'These are the common page-level recovery actions. Low-frequency migration and audit workflows live under Maintenance Tools.',
  ]
  const maintenanceToolsHelpNotes = [
    'These tools stay hidden during normal use. They are exposed automatically when formal sync detects conflicting managed pages that must be cleared first.',
    'Audit Managed IDs checks duplicate rw-reader-id bindings, missing rw-reader-id, and managed page names that would exceed Logseq file-name limits on recreate.',
    'Repair Managed Pages scans ReadwiseHighlights/* for legacy corruption signatures, re-looks up missing identities through the Reader API when needed, and rewrites only the matched pages from the cached highlight snapshot.',
    'Refresh Local Snapshot Only rescans the full Reader highlight library and refreshes the local full-library snapshot without rewriting any managed pages or advancing the incremental cursor.',
    'Preview Legacy Block Ref Migration first scans Readwise managed pages for old block UUID mappings, then lists every graph-wide ((block ref)) rewrite before you confirm the apply step.',
    'Preview Current Page Legacy ID Migration scans Readwise managed pages for UUID mappings, then previews only the current page or whiteboard rewrites before apply.',
  ]
  const highlightScanDetailLabel =
    configuredReaderDebugHighlightPageLimit > 0
      ? 'Debug cap active for remote highlight scans.'
      : 'Incremental scans changes; Full Refresh scans the full library.'
  const progressCountLabel =
    total > 0
      ? `${current} / ${total}`
      : status === 'fetching' || status === 'syncing'
        ? '0 / 0'
        : ''
  const progressPercentLabel = total > 0 ? `${progressPct}%` : ''
  const progressEtaLabel =
    liveEstimatedRemainingMs != null ? `ETA ${formatDuration(liveEstimatedRemainingMs)}` : ''
  const progressPhaseLabel = etaSnapshot?.label ?? ''
  const statusPanelClassName = [
    'rw-status-panel',
    `rw-status-panel-${status}`,
    status === 'completed' && errors.length > 0 ? 'rw-status-panel-warning' : '',
  ]
    .filter((value) => value.length > 0)
    .join(' ')
  const formatManagedPageConflictSummary = (
    conflicts: Array<{
      label: string
      clearAction: string
      pages: Array<{ pageTitle: string }>
    }>,
  ) =>
    conflicts
      .map((conflict) => {
        const sampleTitles = conflict.pages
          .slice(0, 2)
          .map((page) => page.pageTitle)
          .filter((title) => title.length > 0)
        const sampleText =
          sampleTitles.length > 0 ? ` e.g. ${sampleTitles.join(', ')}` : ''
        return `${conflict.label} ${conflict.pages.length} page(s)${sampleText}; clear via ${conflict.clearAction}`
      })
      .join(' | ')

  const describeReaderResumeTarget = (
    resumeState: ReaderPreviewLoadResumeState,
  ) => {
    if (resumeState.phase === 'fetch-highlights') {
      return `highlight scan page ${resumeState.pageNumber + 1}`
    }

    return `parent document ${resumeState.documentIndex + 1} / ${resumeState.selectedParentIds.length}`
  }

  const handleRetry = async () => {
    const retryAction = retryActionRef.current

    if (retryAction) {
      await retryAction.run()
      return
    }

    await handleSync()
  }

  const handleClose = () => {
    if (!isBusy) {
      resetUiState()
    }

    logseq.hideMainUI()
  }

  const clearHelpHideTimeout = () => {
    if (helpHideTimeoutRef.current != null) {
      window.clearTimeout(helpHideTimeoutRef.current)
      helpHideTimeoutRef.current = null
    }
  }

  const scheduleHoveredHelpClose = () => {
    if (pinnedHelpPopover != null) {
      return
    }

    clearHelpHideTimeout()
    helpHideTimeoutRef.current = window.setTimeout(() => {
      setHoveredHelpPopover(null)
      helpHideTimeoutRef.current = null
    }, 120)
  }

  const buildHelpPopoverState = (
    id: ReaderSyncHelpPanelId,
    title: string,
    notes: string[],
    target: HTMLElement,
  ): ReaderSyncHelpPopoverState => {
    const rect = target.getBoundingClientRect()

    return {
      id,
      title,
      notes,
      anchorRect: {
        left: rect.left,
        top: rect.top,
        right: rect.right,
        bottom: rect.bottom,
        width: rect.width,
        height: rect.height,
      },
    }
  }

  const openHoveredHelpPopover = (
    id: ReaderSyncHelpPanelId,
    title: string,
    notes: string[],
    target: HTMLElement,
  ) => {
    if (pinnedHelpPopover != null) {
      return
    }

    clearHelpHideTimeout()
    setHoveredHelpPopover(buildHelpPopoverState(id, title, notes, target))
  }

  const computeHelpPopoverStyle = (popover: ReaderSyncHelpPopoverState) => {
    const viewportPadding = 12
    const preferredWidth =
      popover.id === 'managed-pages' || popover.id === 'highlight-scan'
        ? 220
        : 280
    const width = Math.min(preferredWidth, window.innerWidth - viewportPadding * 2)
    const desiredLeft =
      popover.anchorRect.left + popover.anchorRect.width / 2 - width / 2
    const left = Math.min(
      Math.max(viewportPadding, desiredLeft),
      window.innerWidth - width - viewportPadding,
    )

    return {
      top: `${popover.anchorRect.bottom + 6}px`,
      left: `${left}px`,
      width: `${width}px`,
    }
  }

  const renderHelpPanel = (
    id: ReaderSyncHelpPanelId,
    title: string,
    notes: string[],
  ) => {
    const isOpen = activeHelpPopover?.id === id

    return (
      <div className="rw-help-anchor">
        <button
          type="button"
          className={`rw-help-trigger ${isOpen ? 'is-open' : ''}`}
          aria-label={`${title} help`}
          aria-expanded={isOpen}
          onMouseEnter={(event) => {
            openHoveredHelpPopover(id, title, notes, event.currentTarget)
          }}
          onMouseLeave={() => {
            scheduleHoveredHelpClose()
          }}
          onFocus={(event) => {
            openHoveredHelpPopover(id, title, notes, event.currentTarget)
          }}
          onBlur={() => {
            scheduleHoveredHelpClose()
          }}
          onClick={(event) => {
            clearHelpHideTimeout()
            setHoveredHelpPopover(null)
            setPinnedHelpPopover((currentPopover) =>
              currentPopover?.id === id
                ? null
                : buildHelpPopoverState(id, title, notes, event.currentTarget),
            )
          }}
        >
          i
        </button>
      </div>
    )
  }

  return (
    <div
      className="rw-overlay"
      onClick={(e) => {
        if (e.target === e.currentTarget) handleClose()
      }}
    >
      <div className="rw-card">
        <div className="rw-header">
          <div className="rw-header-copy">
            <div className="rw-header-title">Readwise Reader Sync</div>
          </div>
          <div className="rw-header-meta">
            <span className={`rw-badge ${statusLabel}`}>{statusLabel}</span>
          </div>
        </div>

        <div className="rw-body">
          {!propsReady && status === 'idle' && (
            <div className="rw-setup-notice">
              Properties must be set up before syncing. Click the button below
              to configure them.
            </div>
          )}

          {summaryCardCount > 0 && (
            <div
              className={`rw-summary-grid ${
                summaryCardCount === 1 ? 'rw-summary-grid-single' : ''
              }`}
            >
              {showManagedPagesSummary && (
                <div className="rw-summary-card">
                  <div className="rw-summary-heading">
                    <div className="rw-summary-label">Managed pages</div>
                    {renderHelpPanel(
                      'managed-pages',
                      'Managed Pages',
                      managedPagesHelpNotes,
                    )}
                  </div>
                  <div className="rw-summary-value">
                    {configuredReaderTargetDocuments == null
                      ? 'All matched'
                      : configuredReaderTargetDocuments}
                  </div>
                  <div className="rw-summary-note">{shortManagedPagesSummary}</div>
                </div>
              )}
              {showHighlightScanSummary && (
                <div className="rw-summary-card">
                  <div className="rw-summary-heading">
                    <div className="rw-summary-label">Highlight scan</div>
                    {renderHelpPanel(
                      'highlight-scan',
                      'Highlight Scan',
                      highlightScanHelpNotes,
                    )}
                  </div>
                  <div className="rw-summary-value">
                    {configuredReaderDebugHighlightPageLimit} page(s)
                  </div>
                  <div className="rw-summary-note">{highlightScanDetailLabel}</div>
                </div>
              )}
            </div>
          )}

          <div className={statusPanelClassName}>
            <div className="rw-status-panel-header">
              <div className="rw-status-panel-copy">
                {statusPhaseLabel && (
                  <div className="rw-status-phase">{statusPhaseLabel}</div>
                )}
                <div className="rw-status-headline">{statusHeadline}</div>
                <div className="rw-status">{statusDetail}</div>
                {status === 'idle' && idleCursorSavedAt && (
                  <div className="rw-status-meta">{idleCursorSavedAt}</div>
                )}
              </div>
              {isBusy && (
                <div className="rw-activity-indicator" aria-hidden="true">
                  <span />
                  <span />
                  <span />
                </div>
              )}
            </div>

            {currentOperationLabel && (
              <div className="rw-current-book">{currentOperationLabel}</div>
            )}

            {status !== 'idle' && (
              <>
                <div className="rw-progress-track">
                <div
                    className={`rw-progress-bar ${status}`}
                    style={{
                      width:
                        status === 'fetching' && total === 0
                          ? '8%'
                          : `${Math.max(progressPct, status === 'completed' ? 100 : 4)}%`,
                    }}
                  />
                </div>
                <div className="rw-progress-stats" aria-live="polite">
                  <span className="rw-progress-stat rw-progress-stat-count">
                    {progressCountLabel}
                  </span>
                  <span className="rw-progress-stat rw-progress-stat-percent">
                    {progressPercentLabel}
                  </span>
                  <span className="rw-progress-stat rw-progress-stat-eta">
                    {progressEtaLabel}
                  </span>
                  <span className="rw-progress-stat rw-progress-stat-phase">
                    {progressPhaseLabel}
                  </span>
                </div>
              </>
            )}
          </div>

          {currentPageLegacyIdPreviewResult && (
            <div className="rw-feedback-block rw-preview-panel">
              <div className="rw-section-header">
                <div>
                  <div className="rw-section-title">Current-Page Preview</div>
                  <div className="rw-section-meta">
                    {currentPageLegacyIdPreviewResult.rewrites.length} rewrite(s) in the
                    current {currentPageLegacyIdPreviewResult.fileKind}
                  </div>
                </div>
                <div className="rw-section-actions">
                  <button
                    className="rw-btn rw-btn-small"
                    onClick={() => void handleCopyCurrentPageLegacyIdPreviewBundle()}
                  >
                    Copy Preview Bundle
                  </button>
                </div>
              </div>
              <div className="rw-preview-summary">
                <div className="rw-preview-summary-item">
                  <span className="rw-preview-summary-key">Target</span>
                  <strong>{currentPageLegacyIdPreviewResult.pageName}</strong>
                </div>
                <div className="rw-preview-summary-item">
                  <span className="rw-preview-summary-key">Path</span>
                  <strong>{currentPageLegacyIdPreviewResult.relativeFilePath}</strong>
                </div>
                <div className="rw-preview-summary-item">
                  <span className="rw-preview-summary-key">Kind</span>
                  <strong>{currentPageLegacyIdPreviewResult.fileKind}</strong>
                </div>
                <div className="rw-preview-summary-item">
                  <span className="rw-preview-summary-key">Managed pages scanned</span>
                  <strong>{currentPageLegacyIdPreviewResult.managedPagesScanned}</strong>
                </div>
              </div>
              <div className="rw-preview-note">
                Preview only. Review these UUID rewrites, then run Apply Current Page
                Legacy ID Migration if they look correct.
              </div>
              <div className="rw-preview-list">
                {currentPageLegacyIdPreviewResult.rewrites.map((rewrite) => (
                  <div
                    key={`${rewrite.entryIndex}:${rewrite.blockUuid}:${rewrite.from}:${rewrite.to}`}
                    className="rw-preview-item"
                  >
                    <div className="rw-preview-item-head">
                      <span className="rw-preview-entry">Entry {rewrite.entryIndex}</span>
                      <span className="rw-preview-kind">{rewrite.kind}</span>
                    </div>
                    <div className="rw-preview-block">blockUuid={rewrite.blockUuid}</div>
                    <div className="rw-preview-rewrite">
                      <code>{rewrite.from}</code>
                      <span aria-hidden="true">→</span>
                      <code>{rewrite.to}</code>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {currentPageLegacyIdApplyResult && (
            <div className="rw-feedback-block rw-preview-panel rw-preview-panel-applied">
              <div className="rw-section-header">
                <div>
                  <div className="rw-section-title">Current-Page Migration</div>
                  <div className="rw-section-meta">
                    Applied {currentPageLegacyIdApplyResult.rewritesApplied} rewrite(s) to
                    the current {currentPageLegacyIdApplyResult.fileKind}
                  </div>
                </div>
              </div>
              <div className="rw-preview-summary">
                <div className="rw-preview-summary-item">
                  <span className="rw-preview-summary-key">Target</span>
                  <strong>{currentPageLegacyIdApplyResult.pageName}</strong>
                </div>
                <div className="rw-preview-summary-item">
                  <span className="rw-preview-summary-key">Path</span>
                  <strong>{currentPageLegacyIdApplyResult.relativeFilePath}</strong>
                </div>
                <div className="rw-preview-summary-item">
                  <span className="rw-preview-summary-key">Kind</span>
                  <strong>{currentPageLegacyIdApplyResult.fileKind}</strong>
                </div>
                <div className="rw-preview-summary-item">
                  <span className="rw-preview-summary-key">Rewrites applied</span>
                  <strong>{currentPageLegacyIdApplyResult.rewritesApplied}</strong>
                </div>
              </div>
            </div>
          )}

          {errors.length > 0 && (
            <div className="rw-feedback-block">
              <div className="rw-section-header">
                <div>
                  <div className="rw-section-title">Run issues</div>
                  <div className="rw-section-meta">
                    {errors.length} item(s)
                    {issueCategorySummary ? ` · ${issueCategorySummary}` : ''}
                  </div>
                </div>
                <div className="rw-section-actions">
                  <button
                    className="rw-btn rw-btn-small"
                    onClick={() => void handleCopyRunIssueBundleWithoutWarnings()}
                  >
                    Copy Errors Only
                  </button>
                  <button
                    className="rw-btn rw-btn-small"
                    onClick={() => void handleCopyRunIssueBundle()}
                  >
                    Copy Full Bundle
                  </button>
                </div>
              </div>
              {blockingIssues.length > 0 && (
                <div className="rw-errors">
                  {blockingIssues.map((issue, index) => (
                    <div
                      key={`${issue.book}:${issue.message}:${index}`}
                      className="rw-error-item"
                    >
                      <strong>{issue.book}</strong>
                      <div className="rw-error-label">
                        {formatRunIssueCategoryLabel(issue.category)}
                      </div>
                      <div className="rw-error-summary">{issue.summary}</div>
                      <div className="rw-error-message">{issue.message}</div>
                      {issue.suggestedAction && (
                        <div className="rw-error-action">
                          Next: {issue.suggestedAction}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
              {warningIssues.length > 0 && (
                <div className="rw-warning-section">
                  <div
                    className={`rw-warning-summary ${
                      showWarningIssues ? 'is-expanded' : ''
                    }`}
                  >
                    <div>
                      <div className="rw-warning-title">
                        Warnings are collapsed by default
                      </div>
                      <div className="rw-warning-meta">
                        {warningIssues.length} warning item(s)
                      </div>
                    </div>
                    <button
                      className="rw-btn rw-btn-small"
                      onClick={() => setShowWarningIssues((previous) => !previous)}
                    >
                      {showWarningIssues
                        ? 'Hide Warnings'
                        : `Show Warnings (${warningIssues.length})`}
                    </button>
                  </div>
                  {showWarningIssues && (
                    <div className="rw-errors rw-errors-warning">
                      {warningIssues.map((issue, index) => (
                        <div
                          key={`${issue.book}:${issue.message}:${index}`}
                          className="rw-error-item rw-error-item-warning"
                        >
                          <strong>{issue.book}</strong>
                          <div className="rw-error-label">
                            {formatRunIssueCategoryLabel(issue.category)}
                          </div>
                          <div className="rw-error-summary">{issue.summary}</div>
                          <div className="rw-error-message">{issue.message}</div>
                          {issue.suggestedAction && (
                            <div className="rw-error-action">
                              Next: {issue.suggestedAction}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {cacheSummaryResult && (
            <div className="rw-feedback-block rw-cache-panel">
              <div className="rw-section-header">
                <div>
                  <div className="rw-section-title">Cache Summary</div>
                  <div className="rw-section-meta">
                    {cacheSummaryResult.databaseName}
                  </div>
                </div>
                <button
                  className="rw-btn rw-btn-small"
                  onClick={() =>
                    void copyText(
                      buildCacheSummaryBundle(cacheSummaryResult),
                      'Cache summary',
                    )
                  }
                >
                  Copy
                </button>
              </div>
              <div className="rw-cache-grid">
                <div className="rw-cache-item">
                  <span className="rw-cache-key">Graph ID</span>
                  <strong>{cacheSummaryResult.graphId}</strong>
                </div>
                <div className="rw-cache-item">
                  <span className="rw-cache-key">Parent documents</span>
                  <strong>{cacheSummaryResult.parentDocumentCount}</strong>
                </div>
                <div className="rw-cache-item">
                  <span className="rw-cache-key">Highlights</span>
                  <strong>{cacheSummaryResult.highlightCount}</strong>
                </div>
                <div className="rw-cache-item">
                  <span className="rw-cache-key">cache_state</span>
                  <strong>{cacheSummaryResult.state ? 'Present' : 'Missing'}</strong>
                </div>
                <div className="rw-cache-item">
                  <span className="rw-cache-key">Latest updatedAt</span>
                  <strong>
                    {formatTimestampForUi(
                      cacheSummaryResult.state?.latestHighlightUpdatedAt,
                    )}
                  </strong>
                </div>
                <div className="rw-cache-item">
                  <span className="rw-cache-key">Cached at</span>
                  <strong>
                    {formatTimestampForUi(cacheSummaryResult.state?.cachedAt)}
                  </strong>
                </div>
                <div className="rw-cache-item">
                  <span className="rw-cache-key">Full library snapshot</span>
                  <strong>
                    {cacheSummaryResult.state == null
                      ? 'Unknown'
                      : cacheSummaryResult.state.hasFullLibrarySnapshot
                        ? 'Yes'
                        : 'No'}
                  </strong>
                </div>
                <div className="rw-cache-item">
                  <span className="rw-cache-key">Stale deletion risk</span>
                  <strong>
                    {cacheSummaryResult.state == null
                      ? 'Unknown'
                      : cacheSummaryResult.state.staleDeletionRisk
                        ? 'Yes'
                        : 'No'}
                  </strong>
                </div>
              </div>
            </div>
          )}

          {pageDiffResult && (
            <div className="rw-feedback-block rw-diff-panel">
              <div className="rw-diff-header">
                <div className="rw-diff-header-text">
                  <div className="rw-section-title">Current page diff</div>
                  <strong>
                    {pageDiffResult.pageName} @ line {pageDiffResult.firstDiffLine ?? '?'}
                  </strong>
                  <span className="rw-diff-meta">
                    {pageDiffResult.source}
                    {pageDiffResult.relativeFilePath
                      ? ` · ${pageDiffResult.relativeFilePath}`
                      : ''}
                  </span>
                </div>
                <button
                  className="rw-btn rw-btn-small"
                  onClick={() =>
                    void copyText(buildPageDiffBundle(pageDiffResult), 'Full diff bundle')
                  }
                >
                  Copy All
                </button>
              </div>

              <div className="rw-diff-section">
                <div className="rw-diff-section-header">
                  <span>Before Excerpt</span>
                  <button
                    className="rw-btn rw-btn-small"
                    onClick={() =>
                      void copyText(pageDiffResult.beforeExcerpt, 'Before excerpt')
                    }
                  >
                    Copy
                  </button>
                </div>
                <pre className="rw-diff-content">{pageDiffResult.beforeExcerpt}</pre>
              </div>

              <div className="rw-diff-section">
                <div className="rw-diff-section-header">
                  <span>After Excerpt</span>
                  <button
                    className="rw-btn rw-btn-small"
                    onClick={() =>
                      void copyText(pageDiffResult.afterExcerpt, 'After excerpt')
                    }
                  >
                    Copy
                  </button>
                </div>
                <pre className="rw-diff-content">{pageDiffResult.afterExcerpt}</pre>
              </div>

              <div className="rw-diff-section">
                <div className="rw-diff-section-header">
                  <span>Before Full Page</span>
                  <button
                    className="rw-btn rw-btn-small"
                    onClick={() =>
                      void copyText(pageDiffResult.beforeFullText, 'Before full page')
                    }
                  >
                    Copy
                  </button>
                </div>
                <pre className="rw-diff-content rw-diff-content-full">
                  {pageDiffResult.beforeFullText}
                </pre>
              </div>

              <div className="rw-diff-section">
                <div className="rw-diff-section-header">
                  <span>After Full Page</span>
                  <button
                    className="rw-btn rw-btn-small"
                    onClick={() =>
                      void copyText(pageDiffResult.afterFullText, 'After full page')
                    }
                  >
                    Copy
                  </button>
                </div>
                <pre className="rw-diff-content rw-diff-content-full">
                  {pageDiffResult.afterFullText}
                </pre>
              </div>
            </div>
          )}
        </div>

        <div className="rw-actions">
          {!propsReady && status === 'idle' && (
            <button
              className="rw-btn rw-btn-primary"
              onClick={handleSetupProps}
            >
              Setup Properties
            </button>
          )}
          {propsReady && !isBusy && (
            <div className="rw-action-groups">
              <div className="rw-action-group">
                <div className="rw-action-group-heading">
                  <div className="rw-action-group-label">
                    Library Sync
                  </div>
                  {renderHelpPanel(
                    'global-sync',
                    'Library Sync',
                    librarySyncHelpNotes,
                  )}
                </div>
                <div className="rw-action-row">
                  <button className="rw-btn rw-btn-primary" onClick={handleSync}>
                    Incremental Sync
                  </button>
                  <button className="rw-btn" onClick={handleFullReconcile}>
                    Full Refresh
                  </button>
                  <button className="rw-btn" onClick={handleClose}>
                    Close
                  </button>
                </div>
              </div>

              <div className="rw-action-group">
                <div className="rw-action-group-heading">
                  <div className="rw-action-group-label">
                    Current Page
                  </div>
                  {renderHelpPanel(
                    'current-page',
                    'Current Page',
                    currentPageHelpNotes,
                  )}
                </div>
                <div className="rw-action-row">
                  <button className="rw-btn" onClick={handleRebuildCurrentPageFromCache}>
                    Rebuild Current Page From Cache
                  </button>
                  <button className="rw-btn" onClick={handleRefreshCurrentPageMetadata}>
                    Refresh Current Page Metadata
                  </button>
                </div>
              </div>

              <div className="rw-action-group">
                <div className="rw-action-group-heading">
                  <div className="rw-action-group-label">
                    Maintenance Tools
                  </div>
                  <div className="rw-action-group-heading-actions">
                    {renderHelpPanel(
                      'maintenance-tools',
                      'Maintenance Tools',
                      maintenanceToolsHelpNotes,
                    )}
                    <button
                      type="button"
                      className="rw-btn rw-btn-small rw-toggle-btn"
                      onClick={() =>
                        setShowMaintenanceTools((currentValue) => !currentValue)
                      }
                    >
                      {showMaintenanceTools ? 'Hide Tools' : 'Show Tools'}
                    </button>
                  </div>
                </div>
                {!showMaintenanceTools && (
                  <div className="rw-summary-note">
                    Low-frequency audit, migration, snapshot, and debug tools for managed
                    pages.
                  </div>
                )}
                {showMaintenanceTools && (
                  <>
                    <div className="rw-maintenance-sections">
                      <div className="rw-maintenance-section">
                        <div className="rw-maintenance-section-header">
                          <div className="rw-maintenance-section-title">
                            Audit & Repair
                          </div>
                          <div className="rw-maintenance-section-note">
                            Inspect managed-page identity and rebuild corrupted pages.
                          </div>
                        </div>
                        <div className="rw-action-row">
                          <button className="rw-btn" onClick={handleInspectCacheSummary}>
                            Inspect Cache Summary
                          </button>
                          <button className="rw-btn" onClick={handleAuditManagedIds}>
                            Audit Managed IDs
                          </button>
                          <button className="rw-btn" onClick={handleRepairManagedPages}>
                            Repair Managed Pages
                          </button>
                        </div>
                      </div>

                      <div className="rw-maintenance-section">
                        <div className="rw-maintenance-section-header">
                          <div className="rw-maintenance-section-title">Migration</div>
                          <div className="rw-maintenance-section-note">
                            Preview and apply low-frequency UUID rewrite workflows.
                          </div>
                        </div>
                        <div className="rw-action-row">
                          <button className="rw-btn" onClick={handlePreviewCurrentPageLegacyIds}>
                            Preview Current Page Legacy ID Migration
                          </button>
                          {pendingCurrentPageLegacyIdMigration && (
                            <button
                              className="rw-btn"
                              onClick={handleApplyCurrentPageLegacyIdMigration}
                            >
                              Apply Current Page Legacy ID Migration
                            </button>
                          )}
                        </div>
                        <div className="rw-action-row">
                          <button className="rw-btn" onClick={handlePreviewLegacyBlockRefs}>
                            Preview Legacy Block Ref Migration
                          </button>
                          {pendingLegacyBlockRefMigration && (
                            <button
                              className="rw-btn"
                              onClick={handleApplyLegacyBlockRefMigration}
                            >
                              Apply Legacy Block Ref Migration
                            </button>
                          )}
                        </div>
                      </div>

                      <div className="rw-maintenance-section">
                        <div className="rw-maintenance-section-header">
                          <div className="rw-maintenance-section-title">Snapshots</div>
                          <div className="rw-maintenance-section-note">
                            Refresh the local highlight snapshot or capture raw page state for
                            diff-based debugging.
                          </div>
                        </div>
                        <div className="rw-action-row">
                          <button className="rw-btn" onClick={handleRefreshLocalSnapshotOnly}>
                            Refresh Local Snapshot Only
                          </button>
                        </div>
                        <div className="rw-action-row">
                          <button className="rw-btn" onClick={handleCaptureCurrentPageSnapshot}>
                            Capture Page Snapshot
                          </button>
                          <button className="rw-btn" onClick={handleDiffCurrentPageSnapshot}>
                            Diff Page Snapshot
                          </button>
                        </div>
                        <div className="rw-action-row">
                          <button
                            className="rw-btn"
                            onClick={() => void handleCopyExternalRawSnapshotCommand('capture')}
                          >
                            Copy Raw Capture Cmd
                          </button>
                          <button
                            className="rw-btn"
                            onClick={() => void handleCopyExternalRawSnapshotCommand('diff')}
                          >
                            Copy Raw Diff Cmd
                          </button>
                          <button
                            className="rw-btn"
                            onClick={() => void handleCopyExternalRawSnapshotWorkflow()}
                          >
                            Copy Raw Workflow
                          </button>
                        </div>
                      </div>

                      <div className="rw-maintenance-section">
                        <div className="rw-maintenance-section-header">
                          <div className="rw-maintenance-section-title">Test & Preview</div>
                          <div className="rw-maintenance-section-note">
                            Session-scoped fixtures and preview-page utilities.
                          </div>
                        </div>
                        <div className="rw-action-row">
                          <button className="rw-btn" onClick={handleLimitedSync}>
                            {sessionTestSyncLabel}
                          </button>
                          <button className="rw-btn" onClick={handleBackupFormalTestPages}>
                            Backup Test Pages
                          </button>
                          <button className="rw-btn" onClick={handleRestoreTestPages}>
                            Restore Test Pages
                          </button>
                          <button className="rw-btn" onClick={handleClearSessionTestPages}>
                            Clear Session Test Pages
                          </button>
                          {showAdvancedFormalTestActions && (
                            <button className="rw-btn" onClick={handleClearFormalTestPages}>
                              Clear Formal Test Pages
                            </button>
                          )}
                        </div>
                        <div className="rw-action-row">
                          <button className="rw-btn" onClick={handleReaderPreviewSync}>
                            Start Reader Preview (20, full scan)
                          </button>
                          <button className="rw-btn" onClick={handleClearReaderPreviewPages}>
                            Clear Reader Preview Pages
                          </button>
                        </div>
                      </div>

                      <div className="rw-maintenance-section">
                        <div className="rw-maintenance-section-header">
                          <div className="rw-maintenance-section-title">Debug</div>
                          <div className="rw-maintenance-section-note">
                            Short-lived debug pages and troubleshooting runs.
                          </div>
                        </div>
                        <div className="rw-action-row">
                          <button className="rw-btn" onClick={handleDebugSyncFromScratch}>
                            Start Debug Sync (5)
                          </button>
                          <button className="rw-btn" onClick={handleClearDebugPages}>
                            Clear Debug Pages
                          </button>
                        </div>
                      </div>
                    </div>
                  </>
                )}
              </div>
            </div>
          )}
          {isBusy && (
            <button className="rw-btn" onClick={handleCancel}>
              Cancel
            </button>
          )}
          {propsReady && status === 'completed' && (
            <button className="rw-btn rw-btn-primary" onClick={handleSync}>
              Sync Again
            </button>
          )}
          {status === 'error' && (
            <button className="rw-btn rw-btn-primary" onClick={handleRetry}>
              Retry
            </button>
          )}
          {status !== 'idle' && (
            <button className="rw-btn" onClick={handleClose}>
              Close
            </button>
          )}
        </div>
      </div>
      {activeHelpPopover && (
        <div
          className="rw-help-popover"
          role="dialog"
          aria-label={activeHelpPopover.title}
          style={computeHelpPopoverStyle(activeHelpPopover)}
          onMouseEnter={() => {
            clearHelpHideTimeout()
          }}
          onMouseLeave={() => {
            scheduleHoveredHelpClose()
          }}
        >
          <div className="rw-help-popover-title">{activeHelpPopover.title}</div>
          <div className="rw-help-popover-body">
            {activeHelpPopover.notes.map((note) => (
              <p key={note}>{note}</p>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
