import './ReadwiseContainer.css'

import { useRef, useState } from 'react'
import { format } from 'date-fns'

import { createReadwiseClient } from '../api'
import {
  type GraphCheckpointSourceV1,
  loadGraphCheckpointStateV1,
  saveGraphCheckpointStateV1,
} from '../graph'
import {
  buildBookIdToPageMap,
  setupProps,
  syncBook,
  syncRenderedDebugPage,
} from '../services'
import { deriveNextUpdatedAfterV1 } from '../sync'
import type {
  ExportedBook,
  ExportParams,
  ExportResponse,
  SyncStatus,
} from '../types'

export const ReadwiseContainer = () => {
  const debugSyncMaxBooksLimit = 5
  const debugNamespaceRoot = 'ReadwiseDebug'
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

  const handleClearDebugPages = async () => {
    setErrors([])
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage(`Deleting ${debugNamespaceRoot} pages...`)

    try {
      const pagesFromNamespace =
        (await logseq.Editor.getPagesFromNamespace(debugNamespaceRoot)) ?? []
      const rootPage = await logseq.Editor.getPage(debugNamespaceRoot)

      const pageNames = new Set<string>()

      for (const page of pagesFromNamespace) {
        const pageName =
          (typeof page.originalName === 'string' && page.originalName) ||
          (typeof page.name === 'string' && page.name) ||
          (typeof page.title === 'string' && page.title) ||
          ''

        if (pageName.startsWith(`${debugNamespaceRoot}/`)) {
          pageNames.add(pageName)
        }
      }

      for (const pageName of pageNames) {
        await logseq.Editor.deletePage(pageName)
      }

      if (rootPage) {
        await logseq.Editor.deletePage(debugNamespaceRoot)
      }

      setStatus('completed')
      setStatusMessage(
        `Deleted ${pageNames.size}${rootPage ? ' + namespace root' : ''} debug page(s).`,
      )
      console.info('[Readwise Sync] cleared debug pages', {
        namespacePrefix: debugNamespaceRoot,
        deletedPages: pageNames.size,
        deletedRootPage: !!rootPage,
      })
    } catch (err: unknown) {
      console.error('[Readwise Sync] failed to clear debug pages', err)
      setStatus('error')
      setStatusMessage(
        `Failed to clear debug pages: ${err instanceof Error ? err.message : String(err)}`,
      )
    }
  }

  const runSync = async ({
    ignoreCheckpoint = false,
    namespacePrefix = null,
    renderedDebugPages = false,
    maxBooksOverride = null,
  }: {
    ignoreCheckpoint?: boolean
    namespacePrefix?: string | null
    renderedDebugPages?: boolean
    maxBooksOverride?: number | null
  } = {}) => {
    const token = logseq.settings?.apiToken as string
    if (!token) {
      setStatus('error')
      setStatusMessage('No API token configured. Set it in plugin settings.')
      return
    }

    cancelledRef.current = false
    setErrors([])
    setCurrent(0)
    setTotal(0)
    setCurrentBook('')
    setStatus('fetching')
    setStatusMessage('Fetching highlights from Readwise...')

    const client = createReadwiseClient(token)
    const allBooks: ExportedBook[] = []
    let cursor: string | null = null
    const checkpointBeforeRun = await loadGraphCheckpointStateV1()
    const updatedAfter = ignoreCheckpoint
      ? undefined
      : checkpointBeforeRun?.updatedAfter ?? undefined
    const rawDebugSyncMaxBooks = Number(logseq.settings?.debugSyncMaxBooks ?? 20)
    const configuredDebugSyncMaxBooks =
      Number.isFinite(rawDebugSyncMaxBooks) && rawDebugSyncMaxBooks > 0
        ? Math.floor(rawDebugSyncMaxBooks)
        : null
    const debugSyncMaxBooks =
      typeof maxBooksOverride === 'number' && maxBooksOverride > 0
        ? Math.floor(maxBooksOverride)
        : configuredDebugSyncMaxBooks
    console.info('[Readwise Sync] starting sync')
    console.info('[Readwise Sync] checkpointBeforeRun', checkpointBeforeRun)
    console.info('[Readwise Sync] updatedAfter', updatedAfter ?? null)
    console.info('[Readwise Sync] debugSyncMaxBooks', debugSyncMaxBooks)
    console.info('[Readwise Sync] ignoreCheckpoint', ignoreCheckpoint)
    console.info('[Readwise Sync] namespacePrefix', namespacePrefix)
    console.info('[Readwise Sync] renderedDebugPages', renderedDebugPages)

    try {
      do {
        if (cancelledRef.current) return

        const params: ExportParams = {}
        if (updatedAfter) params.updatedAfter = updatedAfter
        if (cursor) params.pageCursor = cursor

        const page: ExportResponse = await client.exportHighlights(params)
        allBooks.push(...page.results)
        if (debugSyncMaxBooks != null && allBooks.length > debugSyncMaxBooks) {
          allBooks.length = debugSyncMaxBooks
        }
        setTotal(allBooks.length)
        setStatusMessage(
          debugSyncMaxBooks != null
            ? `Fetched ${allBooks.length} / ${debugSyncMaxBooks} debug book(s) so far...`
            : `Fetched ${allBooks.length} book(s) so far...`,
        )
        console.info('[Readwise Sync] export page', {
          pageResultCount: page.results.length,
          totalFetched: allBooks.length,
          nextPageCursor: page.nextPageCursor,
          debugSyncMaxBooks,
        })
        if (debugSyncMaxBooks != null && allBooks.length >= debugSyncMaxBooks) {
          console.info('[Readwise Sync] debug limit reached', {
            debugSyncMaxBooks,
          })
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
        debugSyncMaxBooks != null
          ? ignoreCheckpoint
            ? `Debug sync mode: processing ${allBooks.length} book(s) from scratch into ${namespacePrefix ?? 'default'}; checkpoint will not advance.`
            : `Debug sync mode: processing ${allBooks.length} book(s); checkpoint will not advance.`
          : `Syncing ${allBooks.length} book(s)...`,
      )

      const bookIdToPage = renderedDebugPages
        ? null
        : await buildBookIdToPageMap({ namespacePrefix })

      for (let i = 0; i < allBooks.length; i++) {
        if (cancelledRef.current) return

        const book = allBooks[i]!
        setCurrent(i + 1)
        setCurrentBook(book.title)

        try {
          if (renderedDebugPages) {
            await syncRenderedDebugPage(book, namespacePrefix ?? 'ReadwiseDebug')
          } else {
            await syncBook(book, bookIdToPage!, { namespacePrefix })
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

      if (debugSyncMaxBooks != null || ignoreCheckpoint) {
        console.info('[Readwise Sync] skipping checkpoint save in debug mode', {
          debugSyncMaxBooks,
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
        debugSyncMaxBooks != null || ignoreCheckpoint
          ? `Debug sync complete. ${allBooks.length} book(s) processed${namespacePrefix ? ` in ${namespacePrefix}` : ''}. Checkpoint was not advanced.`
          : `Sync complete. ${allBooks.length} book(s) processed.`,
      )
      console.info('[Readwise Sync] sync completed', {
        processedBooks: allBooks.length,
        nextUpdatedAfter,
        debugSyncMaxBooks,
        ignoreCheckpoint,
        namespacePrefix,
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
    })
  }

  const progressPct = total > 0 ? Math.round((current / total) * 100) : 0
  const isBusy = status === 'fetching' || status === 'syncing'

  return (
    <div
      className="rw-overlay"
      onClick={(e) => {
        if (e.target === e.currentTarget) logseq.hideMainUI()
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
            <button className="rw-btn rw-btn-primary" onClick={handleSync}>
              Start Sync
            </button>
          )}
          {propsReady && status === 'idle' && (
            <button className="rw-btn" onClick={handleDebugSyncFromScratch}>
              Start Debug Sync (5)
            </button>
          )}
          {propsReady && status === 'idle' && (
            <button className="rw-btn" onClick={handleClearDebugPages}>
              Clear Debug Pages
            </button>
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
          <button className="rw-btn" onClick={() => logseq.hideMainUI()}>
            Close
          </button>
        </div>
      </div>
    </div>
  )
}
