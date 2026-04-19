import { logReadwiseError, logReadwiseInfo } from '../logging'
import type { ReaderDocument } from '../types'

const DATABASE_NAME_PREFIX = 'readwise-reader-sync-cache'
const DATABASE_VERSION = 2
const CACHE_LOG_PREFIX = '[Readwise Cache]'

const PARENT_DOCUMENT_STORE = 'parent_documents'
const HIGHLIGHT_STORE = 'highlights'
const STATE_STORE = 'cache_state'
const RETRY_PAGE_STORE = 'page_retry_queue'
const STATE_KEY = 'state'

export interface ReaderSyncHighlightCacheStateV1 {
  schemaVersion: 1
  graphId: string
  latestHighlightUpdatedAt: string | null
  cachedAt: string
  hasFullLibrarySnapshot: boolean
  staleDeletionRisk: boolean
  highlightCount: number
}

export interface ReaderSyncCacheSummaryV1 {
  schemaVersion: 1
  graphId: string
  databaseName: string
  parentDocumentCount: number
  highlightCount: number
  state: ReaderSyncHighlightCacheStateV1 | null
}

export interface ReaderSyncRetryPageEntryV1 {
  readerDocumentId: string
  pageName: string | null
  category: string
  message: string
  queuedAt: string
  lastSeenAt: string
}

export interface GraphReaderSyncCacheV1 {
  getCachedHighlightsByIds(
    highlightIds: readonly string[],
  ): Promise<Map<string, ReaderDocument>>
  putHighlights(highlights: ReaderDocument[]): Promise<void>
  loadAllCachedParentDocuments(): Promise<ReaderDocument[]>
  getCachedParentDocuments(
    parentIds: string[],
  ): Promise<Map<string, ReaderDocument>>
  putParentDocuments(documents: ReaderDocument[]): Promise<void>
  loadGroupedHighlightsByParent(
    parentIds?: readonly string[],
  ): Promise<Map<string, ReaderDocument[]>>
  replaceHighlightsFromFullScan(
    highlights: ReaderDocument[],
    latestHighlightUpdatedAt: string | null,
  ): Promise<void>
  upsertHighlightsFromIncremental(
    highlights: ReaderDocument[],
    latestHighlightUpdatedAt: string | null,
  ): Promise<void>
  getHighlightCacheState(): Promise<ReaderSyncHighlightCacheStateV1 | null>
  inspectCacheSummary(): Promise<ReaderSyncCacheSummaryV1>
  getQueuedRetryPages(): Promise<ReaderSyncRetryPageEntryV1[]>
  queueRetryPages(entries: ReaderSyncRetryPageEntryV1[]): Promise<void>
  removeQueuedRetryPages(readerDocumentIds: string[]): Promise<void>
}

interface ParentDocumentRecord {
  id: string
  document: ReaderDocument
}

interface HighlightRecord {
  id: string
  parentId: string | null
  updatedAt: string
  document: ReaderDocument
}

interface CacheStateRecord {
  key: string
  state: ReaderSyncHighlightCacheStateV1
}

interface RetryPageRecord {
  readerDocumentId: string
  entry: ReaderSyncRetryPageEntryV1
}

const cacheInstances = new Map<string, ReaderSyncCacheImplV1>()

function ensureIndexedDbAvailable() {
  if (typeof indexedDB === 'undefined') {
    throw new Error('IndexedDB is not available in this environment')
  }
}

function uniqueStrings(values: readonly string[]): string[] {
  return [...new Set(values)]
}

function uniqueReaderDocumentsById(
  documents: readonly ReaderDocument[],
): ReaderDocument[] {
  const seen = new Set<string>()
  const unique: ReaderDocument[] = []

  for (const document of documents) {
    if (seen.has(document.id)) continue
    seen.add(document.id)
    unique.push(document)
  }

  return unique
}

function requestToPromise<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    request.onsuccess = () => resolve(request.result)
    request.onerror = () =>
      reject(request.error ?? new Error('IndexedDB request failed'))
  })
}

function txToPromise(transaction: IDBTransaction): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    transaction.oncomplete = () => resolve()
    transaction.onerror = () =>
      reject(transaction.error ?? new Error('IndexedDB transaction failed'))
    transaction.onabort = () =>
      reject(transaction.error ?? new Error('IndexedDB transaction aborted'))
  })
}

function openDatabase(graphId: string): Promise<IDBDatabase> {
  ensureIndexedDbAvailable()

  const databaseName = `${DATABASE_NAME_PREFIX}:${graphId}`

  return new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(databaseName, DATABASE_VERSION)

    request.onupgradeneeded = () => {
      const db = request.result

      if (!db.objectStoreNames.contains(PARENT_DOCUMENT_STORE)) {
        db.createObjectStore(PARENT_DOCUMENT_STORE, { keyPath: 'id' })
      }

      if (!db.objectStoreNames.contains(HIGHLIGHT_STORE)) {
        const store = db.createObjectStore(HIGHLIGHT_STORE, { keyPath: 'id' })
        store.createIndex('byParentId', 'parentId', { unique: false })
      }

      if (!db.objectStoreNames.contains(STATE_STORE)) {
        db.createObjectStore(STATE_STORE, { keyPath: 'key' })
      }

      if (!db.objectStoreNames.contains(RETRY_PAGE_STORE)) {
        db.createObjectStore(RETRY_PAGE_STORE, { keyPath: 'readerDocumentId' })
      }
    }

    request.onsuccess = () => {
      const db = request.result
      db.onversionchange = () => db.close()
      resolve(db)
    }

    request.onerror = () =>
      reject(request.error ?? new Error('Failed to open IndexedDB database'))
  })
}

async function clearAndReplaceHighlights(
  db: IDBDatabase,
  highlights: readonly ReaderDocument[],
): Promise<void> {
  const tx = db.transaction([HIGHLIGHT_STORE], 'readwrite')
  const store = tx.objectStore(HIGHLIGHT_STORE)

  store.clear()

  for (const document of highlights) {
    const record: HighlightRecord = {
      id: document.id,
      parentId: document.parent_id,
      updatedAt: document.updated_at,
      document,
    }
    store.put(record)
  }

  await txToPromise(tx)
}

async function upsertHighlights(
  db: IDBDatabase,
  highlights: readonly ReaderDocument[],
): Promise<number> {
  const tx = db.transaction([HIGHLIGHT_STORE], 'readwrite')
  const store = tx.objectStore(HIGHLIGHT_STORE)

  for (const document of highlights) {
    const record: HighlightRecord = {
      id: document.id,
      parentId: document.parent_id,
      updatedAt: document.updated_at,
      document,
    }
    store.put(record)
  }

  const highlightCount = await requestToPromise<number>(store.count())
  await txToPromise(tx)
  return highlightCount
}

async function loadStateFromDb(
  db: IDBDatabase,
  graphId: string,
): Promise<ReaderSyncHighlightCacheStateV1 | null> {
  const tx = db.transaction([STATE_STORE], 'readonly')
  const store = tx.objectStore(STATE_STORE)
  const record = await requestToPromise<CacheStateRecord | undefined>(
    store.get(STATE_KEY),
  )
  await txToPromise(tx)

  if (!record) return null
  if (record.state.graphId !== graphId) return null
  return record.state
}

async function writeStateToDb(
  db: IDBDatabase,
  state: ReaderSyncHighlightCacheStateV1 | null,
): Promise<void> {
  const tx = db.transaction([STATE_STORE], 'readwrite')
  const store = tx.objectStore(STATE_STORE)

  if (!state) {
    store.delete(STATE_KEY)
    await txToPromise(tx)
    return
  }

  const record: CacheStateRecord = {
    key: STATE_KEY,
    state,
  }
  store.put(record)
  await txToPromise(tx)
}

async function loadRetryQueueFromDb(
  db: IDBDatabase,
): Promise<ReaderSyncRetryPageEntryV1[]> {
  const tx = db.transaction([RETRY_PAGE_STORE], 'readonly')
  const store = tx.objectStore(RETRY_PAGE_STORE)
  const records = await requestToPromise<RetryPageRecord[]>(store.getAll())
  await txToPromise(tx)

  return records
    .map((record) => record.entry)
    .sort((left, right) => left.queuedAt.localeCompare(right.queuedAt))
}

async function queueRetryPagesInDb(
  db: IDBDatabase,
  entries: readonly ReaderSyncRetryPageEntryV1[],
): Promise<void> {
  if (entries.length === 0) return

  const tx = db.transaction([RETRY_PAGE_STORE], 'readwrite')
  const store = tx.objectStore(RETRY_PAGE_STORE)

  for (const entry of entries) {
    const record: RetryPageRecord = {
      readerDocumentId: entry.readerDocumentId,
      entry,
    }
    store.put(record)
  }

  await txToPromise(tx)
}

async function removeRetryPagesFromDb(
  db: IDBDatabase,
  readerDocumentIds: readonly string[],
): Promise<void> {
  const ids = uniqueStrings(readerDocumentIds)
  if (ids.length === 0) return

  const tx = db.transaction([RETRY_PAGE_STORE], 'readwrite')
  const store = tx.objectStore(RETRY_PAGE_STORE)

  for (const readerDocumentId of ids) {
    store.delete(readerDocumentId)
  }

  await txToPromise(tx)
}

function buildNextState(
  graphId: string,
  current: ReaderSyncHighlightCacheStateV1 | null,
  updates: {
    latestHighlightUpdatedAt: string | null
    cachedAt: string
    hasFullLibrarySnapshot: boolean
    staleDeletionRisk: boolean
    highlightCount: number
  },
): ReaderSyncHighlightCacheStateV1 {
  return {
    schemaVersion: 1,
    graphId: current?.graphId ?? graphId,
    latestHighlightUpdatedAt: updates.latestHighlightUpdatedAt,
    cachedAt: updates.cachedAt,
    hasFullLibrarySnapshot: updates.hasFullLibrarySnapshot,
    staleDeletionRisk: updates.staleDeletionRisk,
    highlightCount: updates.highlightCount,
  }
}

class ReaderSyncCacheImplV1 implements GraphReaderSyncCacheV1 {
  readonly graphId: string

  private dbPromise: Promise<IDBDatabase> | null = null

  constructor(graphId: string) {
    if (!graphId) {
      throw new Error('graphId is required')
    }

    this.graphId = graphId
  }

  private getDatabase(): Promise<IDBDatabase> {
    if (!this.dbPromise) {
      this.dbPromise = openDatabase(this.graphId)
    }

    return this.dbPromise
  }

  async getCachedHighlightsByIds(
    highlightIds: readonly string[],
  ): Promise<Map<string, ReaderDocument>> {
    const ids = uniqueStrings(highlightIds)
    if (ids.length === 0) return new Map()

    const db = await this.getDatabase()
    const tx = db.transaction([HIGHLIGHT_STORE], 'readonly')
    const store = tx.objectStore(HIGHLIGHT_STORE)

    const records = await Promise.all(
      ids.map((id) =>
        requestToPromise<HighlightRecord | undefined>(store.get(id)),
      ),
    )
    await txToPromise(tx)

    const documentsById = new Map<string, ReaderDocument>()
    for (const record of records) {
      if (!record?.document?.id) continue
      documentsById.set(record.document.id, record.document)
    }

    return documentsById
  }

  async putHighlights(highlights: ReaderDocument[]): Promise<void> {
    const uniqueHighlights = uniqueReaderDocumentsById(highlights)
    if (uniqueHighlights.length === 0) return

    try {
      const db = await this.getDatabase()
      await upsertHighlights(db, uniqueHighlights)
      logReadwiseInfo(
        CACHE_LOG_PREFIX,
        'wrote highlight documents to IndexedDB',
        {
          graphId: this.graphId,
          databaseName: db.name,
          store: HIGHLIGHT_STORE,
          highlightCount: uniqueHighlights.length,
          sampleHighlightIds: uniqueHighlights
            .slice(0, 5)
            .map((document) => document.id),
        },
      )
    } catch (error: unknown) {
      logReadwiseError(
        CACHE_LOG_PREFIX,
        'failed to write highlight documents to IndexedDB',
        {
          graphId: this.graphId,
          store: HIGHLIGHT_STORE,
          highlightCount: uniqueHighlights.length,
          error,
        },
      )
      throw error
    }
  }

  async getCachedParentDocuments(
    parentIds: string[],
  ): Promise<Map<string, ReaderDocument>> {
    const ids = uniqueStrings(parentIds)
    if (!ids.length) return new Map()

    const db = await this.getDatabase()
    const tx = db.transaction([PARENT_DOCUMENT_STORE], 'readonly')
    const store = tx.objectStore(PARENT_DOCUMENT_STORE)

    const entries = await Promise.all(
      ids.map(async (id) => {
        const record = await requestToPromise<ParentDocumentRecord | undefined>(
          store.get(id),
        )
        return record ? ([id, record.document] as const) : null
      }),
    )

    await txToPromise(tx)

    const documents = new Map<string, ReaderDocument>()
    for (const entry of entries) {
      if (!entry) continue
      documents.set(entry[0], entry[1])
    }

    return documents
  }

  async loadAllCachedParentDocuments(): Promise<ReaderDocument[]> {
    const db = await this.getDatabase()
    const tx = db.transaction([PARENT_DOCUMENT_STORE], 'readonly')
    const store = tx.objectStore(PARENT_DOCUMENT_STORE)
    const records = await requestToPromise<ParentDocumentRecord[]>(
      store.getAll(),
    )
    await txToPromise(tx)

    return uniqueReaderDocumentsById(records.map((record) => record.document))
  }

  async putParentDocuments(
    documents: readonly ReaderDocument[],
  ): Promise<void> {
    const uniqueDocuments = uniqueReaderDocumentsById(documents)
    if (!uniqueDocuments.length) return

    try {
      const db = await this.getDatabase()
      const tx = db.transaction([PARENT_DOCUMENT_STORE], 'readwrite')
      const store = tx.objectStore(PARENT_DOCUMENT_STORE)

      for (const document of uniqueDocuments) {
        const record: ParentDocumentRecord = {
          id: document.id,
          document,
        }
        store.put(record)
      }

      await txToPromise(tx)
      logReadwiseInfo(CACHE_LOG_PREFIX, 'wrote parent documents to IndexedDB', {
        graphId: this.graphId,
        databaseName: db.name,
        store: PARENT_DOCUMENT_STORE,
        documentCount: uniqueDocuments.length,
        sampleParentIds: uniqueDocuments
          .slice(0, 5)
          .map((document) => document.id),
      })
    } catch (error: unknown) {
      logReadwiseError(
        CACHE_LOG_PREFIX,
        'failed to write parent documents to IndexedDB',
        {
          graphId: this.graphId,
          store: PARENT_DOCUMENT_STORE,
          documentCount: uniqueDocuments.length,
          error,
        },
      )
      throw error
    }
  }

  async loadGroupedHighlightsByParent(
    parentIds?: readonly string[],
  ): Promise<Map<string, ReaderDocument[]>> {
    const db = await this.getDatabase()
    const tx = db.transaction([HIGHLIGHT_STORE], 'readonly')
    const store = tx.objectStore(HIGHLIGHT_STORE)
    const groups = new Map<string, ReaderDocument[]>()

    if (parentIds && parentIds.length > 0) {
      const ids = uniqueStrings(parentIds)
      const index = store.index('byParentId')

      for (const parentId of ids) {
        const records = await requestToPromise<HighlightRecord[]>(
          index.getAll(parentId),
        )
        if (!records.length) continue
        groups.set(
          parentId,
          records.map((record) => record.document),
        )
      }

      await txToPromise(tx)
      return groups
    }

    const records = await requestToPromise<HighlightRecord[]>(store.getAll())
    await txToPromise(tx)

    for (const record of records) {
      if (!record.parentId) continue
      const group = groups.get(record.parentId) ?? []
      group.push(record.document)
      groups.set(record.parentId, group)
    }

    return groups
  }

  async replaceHighlightsFromFullScan(
    highlights: ReaderDocument[],
    latestHighlightUpdatedAt: string | null,
  ): Promise<void> {
    const uniqueHighlights = uniqueReaderDocumentsById(highlights)
    try {
      const db = await this.getDatabase()

      await clearAndReplaceHighlights(db, uniqueHighlights)

      const currentState = await this.readState()
      await this.writeState({
        ...buildNextState(this.graphId, currentState, {
          latestHighlightUpdatedAt,
          cachedAt: new Date().toISOString(),
          hasFullLibrarySnapshot: true,
          staleDeletionRisk: false,
          highlightCount: uniqueHighlights.length,
        }),
      })
      logReadwiseInfo(
        CACHE_LOG_PREFIX,
        'replaced highlight snapshot in IndexedDB',
        {
          graphId: this.graphId,
          databaseName: db.name,
          store: HIGHLIGHT_STORE,
          highlightCount: uniqueHighlights.length,
          latestHighlightUpdatedAt,
          hasFullLibrarySnapshot: true,
        },
      )
    } catch (error: unknown) {
      logReadwiseError(
        CACHE_LOG_PREFIX,
        'failed to replace highlight snapshot in IndexedDB',
        {
          graphId: this.graphId,
          store: HIGHLIGHT_STORE,
          highlightCount: uniqueHighlights.length,
          latestHighlightUpdatedAt,
          error,
        },
      )
      throw error
    }
  }

  async upsertHighlightsFromIncremental(
    highlights: ReaderDocument[],
    latestHighlightUpdatedAt: string | null,
  ): Promise<void> {
    const uniqueHighlights = uniqueReaderDocumentsById(highlights)
    try {
      const db = await this.getDatabase()

      const highlightCount = await upsertHighlights(db, uniqueHighlights)

      const currentState = await this.readState()
      await this.writeState({
        ...buildNextState(this.graphId, currentState, {
          latestHighlightUpdatedAt,
          cachedAt: new Date().toISOString(),
          hasFullLibrarySnapshot: currentState?.hasFullLibrarySnapshot ?? false,
          staleDeletionRisk: true,
          highlightCount,
        }),
      })
      logReadwiseInfo(
        CACHE_LOG_PREFIX,
        'upserted incremental highlights into IndexedDB',
        {
          graphId: this.graphId,
          databaseName: db.name,
          store: HIGHLIGHT_STORE,
          incomingHighlightCount: uniqueHighlights.length,
          totalHighlightCount: highlightCount,
          latestHighlightUpdatedAt,
          staleDeletionRisk: true,
        },
      )
    } catch (error: unknown) {
      logReadwiseError(
        CACHE_LOG_PREFIX,
        'failed to upsert incremental highlights into IndexedDB',
        {
          graphId: this.graphId,
          store: HIGHLIGHT_STORE,
          incomingHighlightCount: uniqueHighlights.length,
          latestHighlightUpdatedAt,
          error,
        },
      )
      throw error
    }
  }

  async getHighlightCacheState(): Promise<ReaderSyncHighlightCacheStateV1 | null> {
    const db = await this.getDatabase()
    return await loadStateFromDb(db, this.graphId)
  }

  async inspectCacheSummary(): Promise<ReaderSyncCacheSummaryV1> {
    const db = await this.getDatabase()
    const tx = db.transaction(
      [PARENT_DOCUMENT_STORE, HIGHLIGHT_STORE, STATE_STORE],
      'readonly',
    )
    const parentStore = tx.objectStore(PARENT_DOCUMENT_STORE)
    const highlightStore = tx.objectStore(HIGHLIGHT_STORE)
    const stateStore = tx.objectStore(STATE_STORE)

    const [parentDocumentCount, highlightCount, stateRecord] =
      await Promise.all([
        requestToPromise<number>(parentStore.count()),
        requestToPromise<number>(highlightStore.count()),
        requestToPromise<CacheStateRecord | undefined>(
          stateStore.get(STATE_KEY),
        ),
      ])

    await txToPromise(tx)

    return {
      schemaVersion: 1,
      graphId: this.graphId,
      databaseName: db.name,
      parentDocumentCount,
      highlightCount,
      state:
        stateRecord && stateRecord.state.graphId === this.graphId
          ? stateRecord.state
          : null,
    }
  }

  async getQueuedRetryPages(): Promise<ReaderSyncRetryPageEntryV1[]> {
    const db = await this.getDatabase()
    return await loadRetryQueueFromDb(db)
  }

  async queueRetryPages(entries: ReaderSyncRetryPageEntryV1[]): Promise<void> {
    if (entries.length === 0) return

    const db = await this.getDatabase()
    await queueRetryPagesInDb(db, entries)
    logReadwiseInfo(
      CACHE_LOG_PREFIX,
      'queued page retry entries in IndexedDB',
      {
        graphId: this.graphId,
        databaseName: db.name,
        store: RETRY_PAGE_STORE,
        entryCount: entries.length,
        readerDocumentIds: entries.map((entry) => entry.readerDocumentId),
      },
    )
  }

  async removeQueuedRetryPages(readerDocumentIds: string[]): Promise<void> {
    const ids = uniqueStrings(readerDocumentIds)
    if (ids.length === 0) return

    const db = await this.getDatabase()
    await removeRetryPagesFromDb(db, ids)
    logReadwiseInfo(
      CACHE_LOG_PREFIX,
      'removed page retry entries from IndexedDB',
      {
        graphId: this.graphId,
        databaseName: db.name,
        store: RETRY_PAGE_STORE,
        entryCount: ids.length,
        readerDocumentIds: ids,
      },
    )
  }

  private async readState(): Promise<ReaderSyncHighlightCacheStateV1 | null> {
    const db = await this.getDatabase()
    return await loadStateFromDb(db, this.graphId)
  }

  private async writeState(
    state: ReaderSyncHighlightCacheStateV1 | null,
  ): Promise<void> {
    if (state && state.graphId !== this.graphId) {
      throw new Error('state.graphId does not match this cache instance')
    }

    const db = await this.getDatabase()
    await writeStateToDb(db, state)
    logReadwiseInfo(CACHE_LOG_PREFIX, 'wrote cache_state to IndexedDB', {
      graphId: this.graphId,
      databaseName: db.name,
      store: STATE_STORE,
      state,
    })
  }
}

export function createGraphReaderSyncCacheV1(
  graphId: string,
): GraphReaderSyncCacheV1 {
  return new ReaderSyncCacheImplV1(graphId)
}

export function createReaderSyncCache(graphId: string): GraphReaderSyncCacheV1 {
  return createGraphReaderSyncCacheV1(graphId)
}

export function getReaderSyncCache(graphId: string): GraphReaderSyncCacheV1 {
  const existing = cacheInstances.get(graphId)
  if (existing) return existing

  const cache = new ReaderSyncCacheImplV1(graphId)
  cacheInstances.set(graphId, cache)
  return cache
}
