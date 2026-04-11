import './ReadwiseContainer.css'

import { useEffect, useRef, useState } from 'react'
import { format } from 'date-fns'

import {
  createReadwiseClient,
  loadReaderPreviewBooks,
  type ReaderPreviewLoadStats,
} from '../api'
import {
  type GraphLastFormalSyncSummaryV1,
  type GraphCheckpointSourceV1,
  loadGraphCheckpointStateV1,
  saveGraphLastFormalSyncSummaryV1,
  saveGraphCheckpointStateV1,
} from '../graph'
import {
  backupFormalTestPages,
    captureCurrentPageFileSnapshotV1,
    clearManagedPagesByNamespacePrefix,
    clearManagedPagesBySessionNamespaceRoot,
  clearFormalTestPages,
  type CurrentPageDiffResult,
  diffCurrentPageFileSnapshotV1,
  listManagedPagesByNamespacePrefix,
  listManagedPagesBySessionNamespaceRoot,
  loadActiveFormalTestSessionManifestV1,
  rotateActiveFormalTestSessionNamespaceV1,
  restoreLatestFormalTestPageBackup,
  saveFormalTestSessionManifestV1,
  setupProps,
  syncRenderedDebugPage,
  syncRenderedReaderPreviewPage,
  syncRenderedPage,
} from '../services'
import { deriveNextUpdatedAfterV1 } from '../sync'
import type {
  ExportedBook,
  ExportedBookIdentity,
  ExportParams,
  ExportResponse,
  SyncStatus,
} from '../types'
import {
  describeUnknownError,
  logReadwiseDebug,
  logReadwiseError,
  logReadwiseInfo,
  logReadwiseWarn,
} from '../logging'

type ReaderSyncEtaPhase = 'fetch-highlights' | 'fetch-documents' | 'write-pages'

interface ReaderSyncEtaSnapshot {
  phase: ReaderSyncEtaPhase
  label: string
  etaMs: number | null
  observedAt: number
}

export const ReadwiseContainer = () => {
  const defaultReaderFullScanTargetDocuments = 20
  const defaultReaderFullScanDebugHighlightPageLimit = 0
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
  const etaEstimatorRef = useRef<{
    phase: ReaderSyncEtaPhase | null
    label: string
    lastCompleted: number
    lastTimestamp: number | null
    samplesMs: number[]
  }>({
    phase: null,
    label: '',
    lastCompleted: 0,
    lastTimestamp: null,
    samplesMs: [],
  })
  const [propsReady, setPropsReady] = useState(
    () => !!logseq.settings?.propsConfigured,
  )
  const [status, setStatus] = useState<SyncStatus>('idle')
  const [current, setCurrent] = useState(0)
  const [total, setTotal] = useState(0)
  const [currentBook, setCurrentBook] = useState('')
  const [statusMessage, setStatusMessage] = useState('')
  const [errors, setErrors] = useState<{ book: string; message: string }[]>([])
  const [pageDiffResult, setPageDiffResult] =
    useState<CurrentPageDiffResult | null>(null)
  const [etaTick, setEtaTick] = useState(0)
  const [etaSnapshot, setEtaSnapshot] = useState<ReaderSyncEtaSnapshot | null>(null)
  const [showMaintenanceTools, setShowMaintenanceTools] = useState(false)
  const [activeFormalTestSessionCount, setActiveFormalTestSessionCount] =
    useState<number | null>(null)

  const refreshActiveFormalTestSessionCount = async () => {
    const activeFormalTestSession = await loadActiveFormalTestSessionManifestV1()
    setActiveFormalTestSessionCount(activeFormalTestSession?.books.length ?? null)
  }

  useEffect(() => {
    void refreshActiveFormalTestSessionCount()
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
    etaEstimatorRef.current = {
      phase: null,
      label: '',
      lastCompleted: 0,
      lastTimestamp: null,
      samplesMs: [],
    }
    setEtaSnapshot(null)
    setStatus('idle')
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatusMessage('')
    setErrors([])
    setPageDiffResult(null)
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
      samplesMs: [],
    }
    setEtaSnapshot({
      phase,
      label,
      etaMs: null,
      observedAt: Date.now(),
    })
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
        samplesMs: [],
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
      const nextSamples = [...estimator.samplesMs, sampleMs].slice(-5)
      etaEstimatorRef.current = {
        phase,
        label,
        lastCompleted: completed,
        lastTimestamp: now,
        samplesMs: nextSamples,
      }

      const rollingAverageMs =
        nextSamples.reduce((sum, value) => sum + value, 0) / nextSamples.length
      const remainingUnits = Math.max(0, totalUnits - completed)
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
      setStatusMessage(`${label} copied to clipboard.`)
    } catch (err: unknown) {
      logReadwiseError(formalSyncLogPrefix, 'failed to copy text', err)
      setStatusMessage(
        `Copy failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const uniqueStrings = (values: string[]) =>
    values.filter((value, index, array) => value.length > 0 && array.indexOf(value) === index)

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

  const handleSetupProps = async () => {
    const result = await setupProps()
    if (!result.success) return

    setPropsReady(true)
    setStatusMessage(
      result.compatibilityMode
        ? 'Setup completed in compatibility mode. Start Sync is available now.'
        : 'Schema setup completed. Start Sync is available now.',
    )
  }

  const handleCancel = () => {
    cancelledRef.current = true
    setStatus('idle')
    setStatusMessage('Sync cancelled.')
    setCurrentBook('')
  }

  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      window.setTimeout(resolve, ms)
    })

  const formatDuration = (milliseconds: number) => {
    const totalSeconds = Math.max(0, Math.round(milliseconds / 1000))
    const hours = Math.floor(totalSeconds / 3600)
    const minutes = Math.floor((totalSeconds % 3600) / 60)
    const seconds = totalSeconds % 60

    if (hours > 0) {
      return `${hours}h ${minutes}m`
    }

    if (minutes > 0) {
      return `${minutes}m ${seconds}s`
    }

    return `${seconds}s`
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
    const rawTargetDocuments = Number(
      logseq.settings?.readerFullScanTargetDocuments ??
        defaultReaderFullScanTargetDocuments,
    )
    return Number.isFinite(rawTargetDocuments) && rawTargetDocuments > 0
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
    setErrors([])
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
        setErrors(
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
    setErrors([])
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
        setErrors(
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
    setErrors([])
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
        setErrors([
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
    setErrors([])
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
        setErrors(
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
    setErrors([])
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
    setErrors([])
    setPageDiffResult(null)
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
    setErrors([])
    setPageDiffResult(null)
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
    setErrors([])
    setPageDiffResult(null)
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage('Fetching highlights from Readwise...')

    const client = createReadwiseClient(token)
    const allBooks: ExportedBook[] = []
    const syncErrorsForRun: Array<{ book: string; message: string }> = []
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
          syncErrorsForRun.push({ book: book.title, message: msg })
          setErrors((prev) => [...prev, { book: book.title, message: msg }])
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
      setStatusMessage(
        `Sync failed: ${describeUnknownError(err)}`,
      )
    }
  }

  const handleSync = async () => {
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
      setErrors(
        conflicts.flatMap((conflict) =>
          conflict.pages.slice(0, 3).map((page) => ({
            book: page.pageTitle,
            message: `Formal sync blocked until ${conflict.label} pages are cleared via ${conflict.clearAction}.`,
          })),
        ),
      )
      setStatus('error')
      setCurrent(0)
      setTotal(0)
      setCurrentBook('')
      setStatusMessage(
        `Formal sync blocked to avoid duplicate block UUIDs. ${summary}.`,
      )
      return
    }

    await runReaderFullScanSync({
      namespacePrefix: formalNamespaceRoot,
      logPrefix: formalSyncLogPrefix,
      statusPrefix: 'Formal sync',
      syncHeaderMode: 'formal',
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

  const runReaderFullScanSync = async ({
    namespacePrefix,
    logPrefix,
    statusPrefix,
    syncHeaderMode,
  }: {
    namespacePrefix: string
    logPrefix: string
    statusPrefix: string
    syncHeaderMode: 'formal' | 'preview'
  }) => {
    const token = logseq.settings?.apiToken as string
    if (!token) {
      setStatus('error')
      setStatusMessage('No API token configured. Set it in plugin settings.')
      return
    }

    const targetDocuments = resolveConfiguredReaderFullScanTargetDocuments()
    const debugHighlightPageLimit =
      resolveConfiguredReaderDebugHighlightPageLimit()
    const debugCapSummary =
      debugHighlightPageLimit != null
        ? `, debug cap ${debugHighlightPageLimit} highlight page(s)`
        : ''

    cancelledRef.current = false
    setErrors([])
    setPageDiffResult(null)
    setCurrent(0)
    setTotal(debugHighlightPageLimit ?? 0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage(
      `${statusPrefix}: full-scanning Reader highlights and grouping by parent_id (${targetDocuments} target document(s)${debugCapSummary})...`,
    )

    const client = createReadwiseClient(token)
    const syncErrorsForRun: Array<{ book: string; message: string }> = []
    const runStartedAt = Date.now()
    let loadStats: ReaderPreviewLoadStats = {
      highlightPagesScanned: 0,
      highlightsScanned: 0,
      parentDocumentsIdentified: 0,
      pagesTargeted: 0,
      pagesProcessed: 0,
      estimatedHighlightPages: null,
      estimatedHighlightResults: null,
      fetchHighlightsDurationMs: 0,
      fetchDocumentsDurationMs: 0,
    }
    let writePagesDurationMs = 0
    let createdCount = 0
    let updatedCount = 0
    let unchangedCount = 0
    let renamedCount = 0

    try {
      beginReaderSyncEtaPhase('fetch-highlights', 'highlight scan')
      const previewLoadResult = await loadReaderPreviewBooks(client, {
        maxDocuments: targetDocuments,
        mode: 'full-library-scan',
        maxHighlightPages: debugHighlightPageLimit ?? undefined,
        logPrefix,
        onProgress: (progress) => {
          if (cancelledRef.current) return

          if (progress.phase === 'fetch-highlights') {
            const uniqueParents = progress.uniqueParents ?? 0
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
              `${statusPrefix}: scanned ${progress.pageNumber ?? 0} / ${totalPages} highlight page(s), identified ${uniqueParents} parent document(s) from ${progress.totalHighlights ?? 0} highlight(s).`,
            )
            return
          }

          setStatus('syncing')
          setCurrent(progress.completed ?? 0)
          setTotal(progress.total ?? targetDocuments)
          setCurrentBook(progress.pageTitle ?? '')
          updateReaderSyncEta(
            'fetch-documents',
            'parent document fetch',
            progress.completed ?? 0,
            progress.total ?? targetDocuments,
          )
          setStatusMessage(
            `${statusPrefix}: resolving Reader parent documents... ${progress.completed ?? 0} / ${progress.total ?? targetDocuments}.`,
          )
        },
      })
      const previewBooks = previewLoadResult.books
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

      if (cancelledRef.current) return

      if (previewBooks.length === 0) {
        setStatus('completed')
        setStatusMessage(`${statusPrefix}: no Reader pages were available.`)
        logReadwiseInfo(logPrefix, 'no pages available')
        return
      }

      setCurrent(0)
      setTotal(previewBooks.length)
      setCurrentBook('')
      beginReaderSyncEtaPhase('write-pages', 'page writes')
      setStatusMessage(
        `${statusPrefix}: syncing ${previewBooks.length} Reader page(s) from full-library highlight groups into ${namespacePrefix}...`,
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
        } catch (err: unknown) {
          const message = describeUnknownError(err)
          logReadwiseError(logPrefix, 'failed to sync rendered Reader page', {
            pageTitle,
            readerDocumentId: previewBook.document.id,
            namespacePrefix,
            formattedError: message,
            error: err,
          })
          syncErrorsForRun.push({ book: pageTitle, message })
          setErrors((prev) => [...prev, { book: pageTitle, message }])
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
        const summary: GraphLastFormalSyncSummaryV1 = {
          schemaVersion: 1,
          runKind: 'reader_full_scan',
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
      }

      setStatus('completed')
      setStatusMessage(
        syncErrorsForRun.length > 0
          ? `${statusPrefix}: completed with ${syncErrorsForRun.length} error(s).`
          : `${statusPrefix}: complete. ${previewBooks.length} page(s) written to ${namespacePrefix}.${debugHighlightPageLimit != null ? ` Debug cap ${debugHighlightPageLimit} was active.` : ''}`,
      )
      logReadwiseInfo(logPrefix, 'sync completed', {
        namespacePrefix,
        processedBooks: previewBooks.length,
        errorCount: syncErrorsForRun.length,
      })
    } catch (err: unknown) {
      if (syncHeaderMode === 'formal') {
        const message = describeUnknownError(err)
        const summary: GraphLastFormalSyncSummaryV1 = {
          schemaVersion: 1,
          runKind: 'reader_full_scan',
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
      setStatusMessage(
        `${statusPrefix} failed: ${describeUnknownError(err)}`,
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

    await runReaderFullScanSync({
      namespacePrefix: previewNamespacePrefix,
      logPrefix: readerPreviewLogPrefix,
      statusPrefix: 'Reader v3 preview',
      syncHeaderMode: 'preview',
    })
  }

  const handleClearReaderPreviewPages = async () => {
    setErrors([])
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
        setErrors(
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
  const etaSuffix =
    (status === 'fetching' || status === 'syncing') &&
    liveEstimatedRemainingMs != null &&
    etaSnapshot != null
      ? ` · ETA ${formatDuration(liveEstimatedRemainingMs)} (${etaSnapshot.label})`
      : ''
  const isBusy = status === 'fetching' || status === 'syncing'
  const configuredSyncMaxBooks =
    resolveConfiguredSyncMaxBooks() ?? debugSyncMaxBooksLimit
  const sessionTestSyncCount =
    activeFormalTestSessionCount ?? configuredSyncMaxBooks
  const sessionTestSyncLabel = `Start Sync (session test: ${sessionTestSyncCount})`
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
  const handleClose = () => {
    if (!isBusy) {
      resetUiState()
    }

    logseq.hideMainUI()
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
          <h2>Readwise Sync</h2>
          <span className={`rw-badge ${status}`}>{status}</span>
        </div>

        <div className="rw-body">
          {!propsReady && status === 'idle' && (
            <div className="rw-setup-notice">
              Properties must be set up before syncing. Click the button below
              to configure them.
            </div>
          )}

          <div className="rw-status">
            {statusMessage || 'Ready to sync your Readwise highlights.'}
          </div>

          {status !== 'idle' && (
            <>
              <div className="rw-current-book">
                {status === 'syncing' && currentBook}
                {status === 'fetching' &&
                  statusMessage === 'Fetching highlights from Readwise...' &&
                  'Fetching data from Readwise API...'}
              </div>
              <div className="rw-progress-track">
                <div
                  className={`rw-progress-bar ${status}`}
                  style={{
                    width:
                      status === 'fetching' && total === 0
                        ? '100%'
                        : `${progressPct}%`,
                    opacity: status === 'fetching' ? 0.4 : 1,
                  }}
                />
              </div>
              <div className="rw-progress-label">
                {status === 'fetching'
                  ? `${current} / ${total} (${progressPct}%)${etaSuffix}`
                  : `${current} / ${total} (${progressPct}%)${etaSuffix}`}
              </div>
            </>
          )}

          {errors.length > 0 && (
            <div className="rw-errors">
              {errors.map((err, i) => (
                <div key={i} className="rw-error-item">
                  <strong>{err.book}</strong>
                  {err.message}
                </div>
              ))}
            </div>
          )}

          {pageDiffResult && (
            <div className="rw-diff-panel">
              <div className="rw-diff-header">
                <div className="rw-diff-header-text">
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
          {propsReady && status === 'idle' && (
            <div className="rw-action-groups">
              <div className="rw-action-group">
                <div className="rw-action-group-label">
                  Formal Sync
                </div>
                <div className="rw-action-row">
                  <button className="rw-btn rw-btn-primary" onClick={handleSync}>
                    Start Sync
                  </button>
                  <button className="rw-btn" onClick={handleClose}>
                    Close
                  </button>
                </div>
                <div className="rw-action-note">
                  Uses Reader v3 full-library highlight scan, groups by
                  parent_id, then rewrites managed pages in
                  `ReadwiseHighlights/&lt;title&gt;`.
                </div>
                <div className="rw-action-note">
                  For short debug runs, lower "Reader Full Scan Target
                  Documents" and set "Reader Full Scan Debug Highlight Page
                  Limit" in plugin settings. Set the debug page limit back to 0
                  for a real full scan.
                </div>
              </div>

              {showMaintenanceTools && (
                <>
                  <div className="rw-action-group">
                    <div className="rw-action-group-label">
                      Maintenance Tools
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
                      <button className="rw-btn" onClick={handleDebugSyncFromScratch}>
                        Start Debug Sync (5)
                      </button>
                      <button className="rw-btn" onClick={handleClearDebugPages}>
                        Clear Debug Pages
                      </button>
                      <button className="rw-btn" onClick={handleReaderPreviewSync}>
                        Start Reader Preview (20, full scan)
                      </button>
                      <button className="rw-btn" onClick={handleClearReaderPreviewPages}>
                        Clear Reader Preview Pages
                      </button>
                    </div>
                    <div className="rw-action-row">
                      <button className="rw-btn" onClick={handleCaptureCurrentPageSnapshot}>
                        Capture Page Snapshot
                      </button>
                      <button className="rw-btn" onClick={handleDiffCurrentPageSnapshot}>
                        Diff Page Snapshot
                      </button>
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
                    <div className="rw-action-row">
                      <button
                        className="rw-btn"
                        onClick={() => setShowMaintenanceTools(false)}
                      >
                        Hide Maintenance Tools
                      </button>
                    </div>
                    <div className="rw-action-note">
                      These tools stay hidden during normal use. They are
                      exposed automatically when formal sync detects conflicting
                      managed pages that must be cleared first.
                    </div>
                  </div>
                </>
              )}
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
            <button className="rw-btn rw-btn-primary" onClick={handleSync}>
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
    </div>
  )
}
