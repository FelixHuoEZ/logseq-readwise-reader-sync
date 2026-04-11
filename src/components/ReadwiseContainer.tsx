import './ReadwiseContainer.css'

import { useEffect, useRef, useState } from 'react'
import { format } from 'date-fns'

import { createReadwiseClient } from '../api'
import {
  type GraphCheckpointSourceV1,
  loadGraphCheckpointStateV1,
  saveGraphCheckpointStateV1,
} from '../graph'
import {
  backupFormalTestPages,
  captureCurrentPageFileSnapshotV1,
  clearManagedPagesBySessionNamespaceRoot,
  clearFormalTestPages,
  type CurrentPageDiffResult,
  diffCurrentPageFileSnapshotV1,
  loadActiveFormalTestSessionManifestV1,
  rotateActiveFormalTestSessionNamespaceV1,
  restoreLatestFormalTestPageBackup,
  saveFormalTestSessionManifestV1,
  setupProps,
  syncRenderedDebugPage,
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

export const ReadwiseContainer = () => {
  const debugSyncMaxBooksLimit = 5
  const debugNamespaceRoot = 'ReadwiseDebug'
  const formalNamespaceRoot = 'ReadwiseHighlights'
  const showAdvancedFormalTestActions = false
  const isDebugPageTitle = (pageTitle: string) =>
    pageTitle === debugNamespaceRoot ||
    pageTitle.startsWith(`${debugNamespaceRoot}/`) ||
    pageTitle.startsWith(`${debugNamespaceRoot}___`) ||
    pageTitle.startsWith(`${debugNamespaceRoot}-`)
  const cancelledRef = useRef(false)
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
  const [activeFormalTestSessionCount, setActiveFormalTestSessionCount] =
    useState<number | null>(null)

  const refreshActiveFormalTestSessionCount = async () => {
    const activeFormalTestSession = await loadActiveFormalTestSessionManifestV1()
    setActiveFormalTestSessionCount(activeFormalTestSession?.books.length ?? null)
  }

  useEffect(() => {
    void refreshActiveFormalTestSessionCount()
  }, [])

  const resetUiState = () => {
    cancelledRef.current = false
    setStatus('idle')
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatusMessage('')
    setErrors([])
    setPageDiffResult(null)
  }

  const copyText = async (text: string, label: string) => {
    try {
      await navigator.clipboard.writeText(text)
      setStatusMessage(`${label} copied to clipboard.`)
    } catch (err: unknown) {
      console.error('[Readwise Sync] failed to copy text', err)
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
      console.error('[Readwise Sync] failed to build external raw snapshot command', err)
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
      console.error('[Readwise Sync] failed to build external raw snapshot workflow', err)
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

        console.warn('[Readwise Sync] transient export fetch failed; retrying', {
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
      console.info('[Readwise Sync] using active formal test session for formal page selection', {
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

    console.info('[Readwise Sync] loading formal test books', {
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
      console.info('[Readwise Sync] backed up formal test pages', {
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
      console.error('[Readwise Sync] failed to back up formal test pages', err)
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

      const result = await clearFormalTestPages(books, formalNamespaceRoot)
      setCurrent(books.length)

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
      console.info('[Readwise Sync] cleared formal test pages', {
        targetedBooks: result.targetedBooks,
        matchedPages: result.matchedPages,
        deletedPages: result.touchedPages,
        skippedPages: result.skippedPages,
        namespacePrefix: formalNamespaceRoot,
        updatedAfter,
        maxBooks,
      })
    } catch (err: unknown) {
      console.error('[Readwise Sync] failed to clear formal test pages', err)
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
      const result = await restoreLatestFormalTestPageBackup()
      await refreshActiveFormalTestSessionCount()

      if (result.targetedBooks === 0) {
        setStatus('completed')
        setStatusMessage('No formal test page backup was found.')
        return
      }

      if (result.skippedPages.length > 0) {
        setErrors(
          result.skippedPages.map((pageTitle) => ({
            book: pageTitle,
            message: 'Restore skipped because backup content was empty.',
          })),
        )
      }

      setStatus('completed')
      setStatusMessage(
        result.touchedPages > 0
          ? `Restored ${result.touchedPages} formal test page(s) from ${result.backupDirectory}.`
          : 'No formal test pages were restored.',
      )
      console.info('[Readwise Sync] restored formal test pages', {
        targetedBackups: result.targetedBooks,
        matchedPages: result.matchedPages,
        restoredPages: result.touchedPages,
        skippedPages: result.skippedPages,
        backupDirectory: result.backupDirectory,
      })
    } catch (err: unknown) {
      console.error('[Readwise Sync] failed to restore formal test pages', err)
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
      setTotal(activeFormalTestSession?.books.length ?? 0)
      setStatusMessage(
        `Deleting session test page(s) under ${formalNamespaceRoot}/<run-id>/...`,
      )

      const result = await clearManagedPagesBySessionNamespaceRoot(
        formalNamespaceRoot,
      )
      const rotatedFormalTestSession = activeFormalTestSession
        ? await rotateActiveFormalTestSessionNamespaceV1()
        : null
      await refreshActiveFormalTestSessionCount()
      setCurrent(activeFormalTestSession?.books.length ?? 0)

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
      console.info('[Readwise Sync] cleared session test pages', {
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
      console.error('[Readwise Sync] failed to clear session test pages', err)
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
      const pages = (await logseq.Editor.getAllPages()) ?? []

      const pageNames = new Set<string>()
      const deleteTargets: string[] = []

      for (const page of pages) {
        const candidates = [
          typeof page.originalName === 'string' ? page.originalName : '',
          typeof page.name === 'string' ? page.name : '',
          typeof page.title === 'string' ? page.title : '',
        ].filter((value, index, array) => value.length > 0 && array.indexOf(value) === index)

        if (candidates.some((candidate) => isDebugPageTitle(candidate))) {
          for (const candidate of candidates) {
            if (isDebugPageTitle(candidate)) {
              pageNames.add(candidate)
            }
          }
          deleteTargets.push(...candidates)
        }
      }

      const uniqueDeleteTargets = deleteTargets.filter(
        (value, index, array) => array.indexOf(value) === index,
      )

      let deletedPages = 0

      for (const pageName of uniqueDeleteTargets) {
        try {
          await logseq.Editor.deletePage(pageName)
          deletedPages += 1
        } catch {
          // Try other aliases from getAllPages(); one of them is usually the
          // actual page identity accepted by deletePage().
        }
      }

      setStatus('completed')
      setStatusMessage(`Deleted ${deletedPages} debug page(s).`)
      console.info('[Readwise Sync] cleared debug pages', {
        namespacePrefix: debugNamespaceRoot,
        matchedPages: pageNames.size,
        deletedPages,
      })
    } catch (err: unknown) {
      console.error('[Readwise Sync] failed to clear debug pages', err)
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
      console.error('[Readwise Sync] failed to capture current page snapshot', err)
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
      console.error('[Readwise Sync] failed to diff current page snapshot', err)
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
    console.info('[Readwise Sync] starting sync')
    console.info('[Readwise Sync] checkpointBeforeRun', checkpointBeforeRun)
    console.info('[Readwise Sync] updatedAfter', updatedAfter ?? null)
    console.info('[Readwise Sync] syncLimitMaxBooks', syncLimitMaxBooks)
    console.info('[Readwise Sync] ignoreCheckpoint', ignoreCheckpoint)
    console.info('[Readwise Sync] namespacePrefix', effectiveNamespacePrefix)
    console.info('[Readwise Sync] renderedDebugPages', renderedDebugPages)
    console.info('[Readwise Sync] pageNameMode', pageNameMode)
    console.info('[Readwise Sync] formalTestSessionId', activeFormalTestSession?.sessionId ?? null)

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
        console.info('[Readwise Sync] export page', {
          pageResultCount: page.results.length,
          totalFetched: allBooks.length,
          nextPageCursor: page.nextPageCursor,
          syncLimitMaxBooks,
        })
        if (syncLimitMaxBooks != null && allBooks.length >= syncLimitMaxBooks) {
          console.info('[Readwise Sync] sync limit reached', {
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
        console.info('[Readwise Sync] no new highlights')
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
            )
          } else {
            await syncRenderedPage(book, effectiveNamespacePrefix)
          }
        } catch (err: unknown) {
          const msg = err instanceof Error ? err.message : String(err)
          setErrors((prev) => [...prev, { book: book.title, message: msg }])
        }
      }

      const nextUpdatedAfter = deriveNextUpdatedAfterV1(
        allBooks,
        checkpointBeforeRun?.updatedAfter ?? null,
      )

      if (
        activeFormalTestSession ||
        syncLimitMaxBooks != null ||
        ignoreCheckpoint
      ) {
        console.info('[Readwise Sync] skipping checkpoint save in debug mode', {
          formalTestSessionId: activeFormalTestSession?.sessionId ?? null,
          syncLimitMaxBooks,
          nextUpdatedAfter,
          ignoreCheckpoint,
        })
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
        console.info('[Readwise Sync] saving graph checkpoint', checkpointToSave)
        await saveGraphCheckpointStateV1({
          schemaVersion: 1,
          updatedAfter: nextUpdatedAfter,
          committedAt: checkpointToSave.committedAt,
          source: checkpointToSave.source,
        })
        console.info('[Readwise Sync] saved graph checkpoint', checkpointToSave)
      }

      setStatus('completed')
      setStatusMessage(
        activeFormalTestSession
          ? `Formal test session complete. ${allBooks.length} frozen book(s) processed in ${effectiveNamespacePrefix}. Checkpoint was not advanced.`
          : renderedDebugPages || ignoreCheckpoint
            ? `Debug sync complete. ${allBooks.length} book(s) processed${effectiveNamespacePrefix ? ` in ${effectiveNamespacePrefix}` : ''}. Checkpoint was not advanced.`
          : syncLimitMaxBooks != null
            ? `Limited sync complete. ${allBooks.length} test book(s) processed in ${effectiveNamespacePrefix}. Checkpoint was not advanced.`
          : `Sync complete. ${allBooks.length} book(s) processed.`,
      )
      console.info('[Readwise Sync] sync completed', {
        processedBooks: allBooks.length,
        formalTestSessionId: activeFormalTestSession?.sessionId ?? null,
        nextUpdatedAfter,
        syncLimitMaxBooks,
        ignoreCheckpoint,
        namespacePrefix: effectiveNamespacePrefix,
      })
    } catch (err: unknown) {
      console.error('[Readwise Sync] sync failed', err)
      setStatus('error')
      setStatusMessage(
        `Sync failed: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const handleSync = async () => {
    await runSync()
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

  const progressPct = total > 0 ? Math.round((current / total) * 100) : 0
  const isBusy = status === 'fetching' || status === 'syncing'
  const configuredSyncMaxBooks =
    resolveConfiguredSyncMaxBooks() ?? debugSyncMaxBooksLimit
  const sessionTestSyncCount =
    activeFormalTestSessionCount ?? configuredSyncMaxBooks
  const sessionTestSyncLabel = `Start Sync (session test: ${sessionTestSyncCount})`
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
                {status === 'fetching' && 'Fetching data from Readwise API...'}
              </div>
              <div className="rw-progress-track">
                <div
                  className={`rw-progress-bar ${status}`}
                  style={{
                    width: status === 'fetching' ? '100%' : `${progressPct}%`,
                    opacity: status === 'fetching' ? 0.4 : 1,
                  }}
                />
              </div>
              <div className="rw-progress-label">
                {status === 'fetching'
                  ? `${total} book(s) fetched`
                  : `${current} / ${total} (${progressPct}%)`}
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
              </div>

              <div className="rw-action-group">
                <div className="rw-action-group-label">
                  Real UUID Test
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
              </div>

              <div className="rw-action-group">
                <div className="rw-action-group-label">
                  Fake UUID Test
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

              <div className="rw-action-group">
                <div className="rw-action-group-label">
                  Page File Diff
                </div>
                <div className="rw-action-row">
                  <button className="rw-btn" onClick={handleCaptureCurrentPageSnapshot}>
                    Capture Page Snapshot
                  </button>
                  <button className="rw-btn" onClick={handleDiffCurrentPageSnapshot}>
                    Diff Page Snapshot
                  </button>
                </div>
              </div>

              <div className="rw-action-group">
                <div className="rw-action-group-label">
                  Raw File Diff
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
                <div className="rw-action-note">
                  These buttons copy terminal commands. The Logseq plugin runtime
                  cannot execute the raw file script directly.
                </div>
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
