import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'
import { format } from 'date-fns'

import type { ExportedBookIdentity } from '../types'
import {
  buildFormalManagedPageName,
  buildManagedPageFileStem,
} from './readwise-page-names'
import { upsertSingleRootPageContentV1 } from './single-root-page-content'

interface FormalTestPageActionResult {
  targetedBooks: number
  matchedPages: number
  touchedPages: number
  backupDirectory: string | null
  skippedPages: string[]
  failedPages: Array<{
    pageTitle: string
    message: string
  }>
}

interface BatchPageActionProgress {
  phase: 'start' | 'item'
  total: number
  completed: number
  pageTitle: string | null
}

export interface ManagedPageMatch {
  aliases: string[]
  pageTitle: string
}

export interface FormalTestSessionManifestV1 {
  schemaVersion: 1
  sessionId: string
  createdAt: string
  graphName: string | null
  graphPath: string | null
  namespacePrefix: string
  updatedAfter: string | null
  maxBooks: number | null
  books: Array<{
    userBookId: number
    title: string
  }>
  backupStoragePrefix: string
}

interface StoredFormalPageBackupV1 {
  schemaVersion: 1
  capturedAt: string
  graphName: string | null
  graphPath: string | null
  pageName: string
  pageAliases: string[]
  relativeFilePath: string | null
  captureMode: 'raw_file' | 'page_tree'
  rawContent?: string
  pageTree?: BlockEntity[] | null
}

const ACTIVE_FORMAL_TEST_SESSION_KEY = 'formal-test-sessions/active.json'
const RESTORE_RETRY_DELAYS_MS = [250, 500, 1000] as const
const RESTORE_LOG_PREFIX = '[Readwise Restore]'
const SESSION_TEST_LOG_PREFIX = '[Readwise Session Test]'

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

const buildFormalTestSessionKey = (sessionId: string) =>
  `formal-test-sessions/${sessionId}.json`

const uniqueValues = (values: string[]): string[] =>
  values.filter((value, index, array) => value.length > 0 && array.indexOf(value) === index)

const collectPageAliases = (page: PageEntity | null): string[] => {
  if (!page) return []

  return uniqueValues([
    typeof page.originalName === 'string' ? page.originalName : '',
    typeof page.name === 'string' ? page.name : '',
    typeof page.title === 'string' ? page.title : '',
  ])
}

const findPageByExpectedName = (
  expectedPageName: string,
  pages: PageEntity[],
): PageEntity | null =>
  pages.find((page) => collectPageAliases(page).includes(expectedPageName)) ?? null

const resolvePageFilePath = async (
  pageName: string,
  expectedFormat: 'org' | 'markdown' | null,
): Promise<string | null> => {
  const assets = await logseq.Assets.listFilesOfCurrentGraph(['org', 'md'])
  const expectedStem = `pages/${buildManagedPageFileStem(pageName)}`
  const preferredExtensions =
    expectedFormat === 'markdown' ? ['md', 'org'] : ['org', 'md']

  for (const extension of preferredExtensions) {
    const exact = assets.find((asset) => asset.path === `${expectedStem}.${extension}`)
    if (exact) {
      return exact.path
    }
  }

  const fallback = assets.find((asset) => asset.path.startsWith(`${expectedStem}.`))
  return fallback?.path ?? null
}

const deletePageByAliases = async (aliases: string[]): Promise<boolean> => {
  for (const alias of aliases) {
    try {
      await logseq.Editor.deletePage(alias)
      return true
    } catch {
      // Keep trying aliases until one is accepted by the current runtime.
    }
  }

  return false
}

const uniqueStrings = (values: string[]) =>
  values.filter((value, index, array) => value.length > 0 && array.indexOf(value) === index)

const buildBackupStoragePrefix = () =>
  `formal-page-backups/${format(new Date(), 'yyyyMMdd-HHmmss')}`

const buildBackupStorageKey = (prefix: string, pageName: string): string =>
  `${prefix}/${buildManagedPageFileStem(pageName)}.json`

const isMissingFileStorageItemError = (error: unknown) => {
  const message = error instanceof Error ? error.message : String(error)
  return message.includes('file not existed')
}

const safeGetFileStorageItem = async (key: string): Promise<string | null> => {
  try {
    const value = await logseq.FileStorage.getItem(key)
    return typeof value === 'string' ? value : null
  } catch (error: unknown) {
    if (isMissingFileStorageItemError(error)) {
      return null
    }

    throw error
  }
}

const restoreFormalPageBackupWithRetry = async (
  pageName: string,
  content: string,
) => {
  let lastError: unknown = null

  for (let attempt = 0; attempt <= RESTORE_RETRY_DELAYS_MS.length; attempt += 1) {
    try {
      await upsertSingleRootPageContentV1(pageName, content, RESTORE_LOG_PREFIX)
      return
    } catch (err: unknown) {
      lastError = err
      const message = err instanceof Error ? err.message : String(err)
      console.warn(`${RESTORE_LOG_PREFIX} failed to restore formal test page attempt`, {
        pageName,
        attempt: attempt + 1,
        totalAttempts: RESTORE_RETRY_DELAYS_MS.length + 1,
        message,
      })

      if (attempt < RESTORE_RETRY_DELAYS_MS.length) {
        await delay(RESTORE_RETRY_DELAYS_MS[attempt]!)
      }
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError))
}

export const listManagedPagesByNamespacePrefix = async (
  namespacePrefix: string,
): Promise<ManagedPageMatch[]> => {
  const pages = (await logseq.Editor.getAllPages()) ?? []
  const matchedTargets: ManagedPageMatch[] = []

  for (const page of pages) {
    const aliases = collectPageAliases(page)
    const isMatch = aliases.some(
      (alias) => alias === namespacePrefix || alias.startsWith(`${namespacePrefix}/`),
    )

    if (!isMatch) continue

    matchedTargets.push({
      aliases,
      pageTitle: aliases[0] ?? page.originalName ?? page.name ?? page.title ?? '',
    })
  }

  return matchedTargets
}

export const listManagedPagesBySessionNamespaceRoot = async (
  namespaceRoot: string,
): Promise<ManagedPageMatch[]> => {
  const pages = (await logseq.Editor.getAllPages()) ?? []
  const matchedTargets: ManagedPageMatch[] = []
  const sessionNamespacePattern = new RegExp(
    `^${namespaceRoot.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/\\d{8}-\\d{6}(?:/|$)`,
  )

  for (const page of pages) {
    const aliases = collectPageAliases(page)
    const isMatch = aliases.some((alias) => sessionNamespacePattern.test(alias))

    if (!isMatch) continue

    matchedTargets.push({
      aliases,
      pageTitle: aliases[0] ?? page.originalName ?? page.name ?? page.title ?? '',
    })
  }

  return matchedTargets
}

export const saveFormalTestSessionManifestV1 = async (
  manifest: FormalTestSessionManifestV1,
) => {
  const payload = JSON.stringify(manifest, null, 2)
  await logseq.FileStorage.setItem(
    buildFormalTestSessionKey(manifest.sessionId),
    payload,
  )
  await logseq.FileStorage.setItem(ACTIVE_FORMAL_TEST_SESSION_KEY, payload)
}

export const loadActiveFormalTestSessionManifestV1 =
  async (): Promise<FormalTestSessionManifestV1 | null> => {
    const raw = await safeGetFileStorageItem(ACTIVE_FORMAL_TEST_SESSION_KEY)
    if (raw == null) return null

    const manifest = JSON.parse(raw) as FormalTestSessionManifestV1
    if (manifest.schemaVersion !== 1) return null

    const graph = await logseq.App.getCurrentGraph()
    if (manifest.graphPath && graph?.path && manifest.graphPath !== graph.path) {
      return null
    }

    return manifest
  }

export const clearActiveFormalTestSessionManifestV1 = async () => {
  const exists = await logseq.FileStorage.hasItem(ACTIVE_FORMAL_TEST_SESSION_KEY)
  if (exists) {
    await logseq.FileStorage.removeItem(ACTIVE_FORMAL_TEST_SESSION_KEY)
  }
}

export const rotateActiveFormalTestSessionNamespaceV1 =
  async (): Promise<FormalTestSessionManifestV1 | null> => {
    const activeSession = await loadActiveFormalTestSessionManifestV1()
    if (!activeSession) return null

    const namespaceRoot = activeSession.namespacePrefix.split('/')[0] || 'ReadwiseHighlights'
    const nextSessionIdCandidate = format(new Date(), 'yyyyMMdd-HHmmss')
    const nextSessionId =
      nextSessionIdCandidate === activeSession.sessionId
        ? format(new Date(Date.now() + 1000), 'yyyyMMdd-HHmmss')
        : nextSessionIdCandidate

    const nextManifest: FormalTestSessionManifestV1 = {
      ...activeSession,
      sessionId: nextSessionId,
      createdAt: new Date().toISOString(),
      namespacePrefix: `${namespaceRoot}/${nextSessionId}`,
    }

    await saveFormalTestSessionManifestV1(nextManifest)
    return nextManifest
  }

const captureFormalPageBackup = async (
  pageName: string,
  pageAliases: string[],
  relativeFilePath: string | null,
): Promise<StoredFormalPageBackupV1> => {
  const graph = await logseq.App.getCurrentGraph()
  const rawContent =
    relativeFilePath == null
      ? null
      : await logseq.DB.getFileContent(relativeFilePath).catch(() => null)

  if (typeof rawContent === 'string') {
    return {
      schemaVersion: 1,
      capturedAt: new Date().toISOString(),
      graphName: graph?.name ?? null,
      graphPath: graph?.path ?? null,
      pageName,
      pageAliases,
      relativeFilePath,
      captureMode: 'raw_file',
      rawContent,
    }
  }

  const pageTree = await logseq.Editor.getPageBlocksTree(pageName)
  return {
    schemaVersion: 1,
    capturedAt: new Date().toISOString(),
    graphName: graph?.name ?? null,
    graphPath: graph?.path ?? null,
    pageName,
    pageAliases,
    relativeFilePath,
    captureMode: 'page_tree',
    pageTree,
  }
}

export const backupFormalTestPages = async (
  books: ExportedBookIdentity[],
  namespacePrefix = 'ReadwiseHighlights',
  options: {
    storagePrefix?: string
    onProgress?: (progress: BatchPageActionProgress) => void
  } = {},
): Promise<FormalTestPageActionResult> => {
  const storagePrefix = options.storagePrefix ?? buildBackupStoragePrefix()
  const skippedPages: string[] = []
  let matchedPages = 0
  let backedUpPages = 0
  const allPages = (await logseq.Editor.getAllPages()) ?? []
  options.onProgress?.({
    phase: 'start',
    total: books.length,
    completed: 0,
    pageTitle: null,
  })

  for (const [index, book] of books.entries()) {
    const pageName = buildFormalManagedPageName(book.title, namespacePrefix)
    const page =
      (await logseq.Editor.getPage(pageName)) ??
      findPageByExpectedName(pageName, allPages)
    const aliases = collectPageAliases(page)

    if (!page || aliases.length === 0) {
      options.onProgress?.({
        phase: 'item',
        total: books.length,
        completed: index + 1,
        pageTitle: pageName,
      })
      continue
    }

    matchedPages += 1

    const relativeFilePath = await resolvePageFilePath(pageName, page.format ?? null)
    const backupPayload = await captureFormalPageBackup(
      pageName,
      aliases,
      relativeFilePath,
    )
    const backupKey = buildBackupStorageKey(storagePrefix, pageName)

    await logseq.FileStorage.setItem(
      backupKey,
      JSON.stringify(backupPayload, null, 2),
    )

    const deleted = await deletePageByAliases(aliases)
    if (!deleted) {
      skippedPages.push(pageName)
      console.warn(
        `${SESSION_TEST_LOG_PREFIX} stored formal test page backup but failed to delete page`,
        {
          pageName,
          backupKey,
        },
      )
      options.onProgress?.({
        phase: 'item',
        total: books.length,
        completed: index + 1,
        pageTitle: pageName,
      })
      continue
    }

    backedUpPages += 1
    options.onProgress?.({
      phase: 'item',
      total: books.length,
      completed: index + 1,
      pageTitle: pageName,
    })
  }

  return {
    targetedBooks: books.length,
    matchedPages,
    touchedPages: backedUpPages,
    backupDirectory: `plugin-storage://${storagePrefix}`,
    skippedPages,
    failedPages: [],
  }
}

export const clearFormalTestPages = async (
  books: ExportedBookIdentity[],
  namespacePrefix = 'ReadwiseHighlights',
  options: {
    onProgress?: (progress: BatchPageActionProgress) => void
  } = {},
): Promise<FormalTestPageActionResult> => {
  const skippedPages: string[] = []
  let matchedPages = 0
  let deletedPages = 0
  const allPages = (await logseq.Editor.getAllPages()) ?? []
  options.onProgress?.({
    phase: 'start',
    total: books.length,
    completed: 0,
    pageTitle: null,
  })

  for (const [index, book] of books.entries()) {
    const pageName = buildFormalManagedPageName(book.title, namespacePrefix)
    const page =
      (await logseq.Editor.getPage(pageName)) ??
      findPageByExpectedName(pageName, allPages)
    const aliases = collectPageAliases(page)

    if (!page || aliases.length === 0) {
      options.onProgress?.({
        phase: 'item',
        total: books.length,
        completed: index + 1,
        pageTitle: pageName,
      })
      continue
    }

    matchedPages += 1
    const deleted = await deletePageByAliases(aliases)

    if (!deleted) {
      skippedPages.push(pageName)
      options.onProgress?.({
        phase: 'item',
        total: books.length,
        completed: index + 1,
        pageTitle: pageName,
      })
      continue
    }

    deletedPages += 1
    options.onProgress?.({
      phase: 'item',
      total: books.length,
      completed: index + 1,
      pageTitle: pageName,
    })
  }

  return {
    targetedBooks: books.length,
    matchedPages,
    touchedPages: deletedPages,
    backupDirectory: null,
    skippedPages,
    failedPages: [],
  }
}

export const clearManagedPagesByNamespacePrefix = async (
  namespacePrefix: string,
  options: {
    onProgress?: (progress: BatchPageActionProgress) => void
  } = {},
): Promise<FormalTestPageActionResult> => {
  const skippedPages: string[] = []
  const matchedTargets = await listManagedPagesByNamespacePrefix(namespacePrefix)
  let deletedPages = 0

  options.onProgress?.({
    phase: 'start',
    total: matchedTargets.length,
    completed: 0,
    pageTitle: null,
  })

  for (const [index, target] of matchedTargets.entries()) {
    const deleted = await deletePageByAliases(target.aliases)
    if (!deleted) {
      skippedPages.push(target.pageTitle)
      options.onProgress?.({
        phase: 'item',
        total: matchedTargets.length,
        completed: index + 1,
        pageTitle: target.pageTitle,
      })
      continue
    }

    deletedPages += 1
    options.onProgress?.({
      phase: 'item',
      total: matchedTargets.length,
      completed: index + 1,
      pageTitle: target.pageTitle,
    })
  }

  return {
    targetedBooks: 0,
    matchedPages: matchedTargets.length,
    touchedPages: deletedPages,
    backupDirectory: null,
    skippedPages,
    failedPages: [],
  }
}

export const clearManagedPagesBySessionNamespaceRoot = async (
  namespaceRoot: string,
  options: {
    onProgress?: (progress: BatchPageActionProgress) => void
  } = {},
): Promise<FormalTestPageActionResult> => {
  const skippedPages: string[] = []
  const matchedTargets = await listManagedPagesBySessionNamespaceRoot(namespaceRoot)
  let deletedPages = 0

  options.onProgress?.({
    phase: 'start',
    total: matchedTargets.length,
    completed: 0,
    pageTitle: null,
  })

  for (const [index, target] of matchedTargets.entries()) {
    const deleted = await deletePageByAliases(target.aliases)
    if (!deleted) {
      skippedPages.push(target.pageTitle)
      options.onProgress?.({
        phase: 'item',
        total: matchedTargets.length,
        completed: index + 1,
        pageTitle: target.pageTitle,
      })
      continue
    }

    deletedPages += 1
    options.onProgress?.({
      phase: 'item',
      total: matchedTargets.length,
      completed: index + 1,
      pageTitle: target.pageTitle,
    })
  }

  return {
    targetedBooks: 0,
    matchedPages: matchedTargets.length,
    touchedPages: deletedPages,
    backupDirectory: null,
    skippedPages,
    failedPages: [],
  }
}

const flattenBlockTreeToText = (blocks: BlockEntity[] | null | undefined): string => {
  if (!Array.isArray(blocks) || blocks.length === 0) return ''

  const parts: string[] = []

  const visit = (block: BlockEntity) => {
    if (typeof block.content === 'string' && block.content.length > 0) {
      parts.push(block.content)
    }

    for (const child of block.children ?? []) {
      if (Array.isArray(child)) continue
      visit(child)
    }
  }

  for (const block of blocks) {
    visit(block)
  }

  return parts.join('\n\n')
}

export const restoreLatestFormalTestPageBackup = async (
  options: {
    onProgress?: (progress: BatchPageActionProgress) => void
  } = {},
): Promise<FormalTestPageActionResult> => {
  const activeSession = await loadActiveFormalTestSessionManifestV1()

  if (!activeSession) {
    return {
      targetedBooks: 0,
      matchedPages: 0,
      touchedPages: 0,
      backupDirectory: null,
      skippedPages: [],
      failedPages: [],
    }
  }

  const latestTimestamp =
    activeSession.backupStoragePrefix.split('/').at(-1) ?? activeSession.sessionId
  const allKeys = await logseq.FileStorage.allKeys()
  const latestKeys = allKeys.filter((key) =>
    key.startsWith(`${activeSession.backupStoragePrefix}/`),
  )
  options.onProgress?.({
    phase: 'start',
    total: latestKeys.length,
    completed: 0,
    pageTitle: null,
  })

  let matchedPages = 0
  let restoredPages = 0
  const skippedPages: string[] = []
  const failedPages: Array<{ pageTitle: string; message: string }> = []

  for (const [index, key] of latestKeys.entries()) {
    const rawBackup = await safeGetFileStorageItem(key)
    if (rawBackup == null) {
      failedPages.push({
        pageTitle: key,
        message: 'Backup file not found in plugin storage.',
      })
      options.onProgress?.({
        phase: 'item',
        total: latestKeys.length,
        completed: index + 1,
        pageTitle: key,
      })
      continue
    }

    const parsed = JSON.parse(rawBackup) as StoredFormalPageBackupV1
    matchedPages += 1

    const restoreContent =
      parsed.captureMode === 'raw_file'
        ? parsed.rawContent ?? ''
        : flattenBlockTreeToText(parsed.pageTree)

    if (restoreContent.length === 0) {
      skippedPages.push(parsed.pageName)
      options.onProgress?.({
        phase: 'item',
        total: latestKeys.length,
        completed: index + 1,
        pageTitle: parsed.pageName,
      })
      continue
    }

    try {
      await restoreFormalPageBackupWithRetry(parsed.pageName, restoreContent)
      restoredPages += 1
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err)
      failedPages.push({
        pageTitle: parsed.pageName,
        message,
      })
      console.error(`${RESTORE_LOG_PREFIX} failed to restore formal test page`, {
        pageName: parsed.pageName,
        key,
        message,
      })
    }

    options.onProgress?.({
      phase: 'item',
      total: latestKeys.length,
      completed: index + 1,
      pageTitle: parsed.pageName,
    })
  }

  if (failedPages.length === 0 && latestKeys.length > 0) {
    await clearActiveFormalTestSessionManifestV1()
  } else {
    console.warn(`${RESTORE_LOG_PREFIX} retained active formal test session after restore`, {
      sessionId: activeSession.sessionId,
      backupStoragePrefix: activeSession.backupStoragePrefix,
      targetedBackups: latestKeys.length,
      failedPages,
    })
  }

  return {
    targetedBooks: latestKeys.length,
    matchedPages,
    touchedPages: restoredPages,
    backupDirectory: `plugin-storage://formal-page-backups/${latestTimestamp}`,
    skippedPages,
    failedPages,
  }
}
