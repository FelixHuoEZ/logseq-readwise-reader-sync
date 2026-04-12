import type {
  AppGraphInfo,
  BlockEntity,
  PageEntity,
} from '@logseq/libs/dist/LSPlugin'

import { describeUnknownError, logReadwiseDebug, logReadwiseInfo } from '../logging'
import {
  computeCompatibleHighlightUuid,
  computeLegacyHighlightUuid,
} from '../uuid-compat'

const READWISE_HIGHLIGHT_URL_IN_BLOCK_PATTERN =
  /\[\[(https:\/\/read\.readwise\.io\/read\/[0-9a-z]+)\]\[View Highlight\]\]/i
const BLOCK_REF_PATTERN = /\(\(([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\)\)/gi
const BLOCK_ID_LINE_PATTERN =
  /^:id:\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i
const REFDOCK_ITEM_ID_PATTERN =
  /^(:refdock-item-id:\s+)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\s*)$/gim

const trimTrailingSeparators = (value: string) => value.replace(/[\\/]+$/, '')

const joinAbsolutePath = (basePath: string, relativePath: string) =>
  `${trimTrailingSeparators(basePath)}/${relativePath}`

const LEGACY_ID_MIGRATION_LOG_PREFIX = '[Readwise Sync]'
const LEGACY_BLOCK_REF_MAPPING_CACHE_SCHEMA_VERSION = 1
const LEGACY_BLOCK_REF_MAPPING_CACHE_TTL_MS = 1000 * 60 * 60

interface StoredLegacyBlockRefMappingCacheV1 {
  schemaVersion: 1
  namespaceRoot: string
  cachedAt: string
  expiresAt: string
  managedPagesScanned: number
  entries: Array<[string, string]>
}

const getRuntimeFsPromises = () => {
  const runtimeRequire =
    (window as unknown as { require?: ((id: string) => unknown) | undefined }).require ??
    (
      window.top as unknown as {
        require?: ((id: string) => unknown) | undefined
      } | null
    )?.require

  if (typeof runtimeRequire !== 'function') {
    throw new Error('window.require is unavailable in the current Logseq runtime.')
  }

  return runtimeRequire('node:fs/promises') as {
    readFile: (path: string, encoding: string) => Promise<string>
    writeFile: (path: string, content: string, encoding: string) => Promise<void>
  }
}

const serializeDiagnosticValue = (value: unknown): string => {
  if (value instanceof Error) {
    return JSON.stringify({
      name: value.name,
      message: value.message,
      stack: value.stack,
    })
  }

  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return JSON.stringify(value)
  }

  if (typeof value === 'undefined') {
    return 'undefined'
  }

  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

const describeRootdirApiResult = (value: unknown) => {
  if (typeof value === 'string') {
    return {
      type: 'string',
      length: value.length,
      preview: value.slice(0, 120),
    }
  }

  if (value === null) {
    return { type: 'null' }
  }

  if (typeof value === 'undefined') {
    return { type: 'undefined' }
  }

  if (Array.isArray(value)) {
    return { type: 'array', length: value.length }
  }

  if (typeof value === 'object') {
    return {
      type: 'object',
      keys: Object.keys(value as Record<string, unknown>).slice(0, 12),
    }
  }

  return {
    type: typeof value,
    value: String(value),
  }
}

const getInternalExecCallableApiAsync = () => {
  const pluginApi = logseq as unknown as {
    _execCallableAPIAsync?: (
      method: string,
      ...args: unknown[]
    ) => Promise<unknown>
  }

  return typeof pluginApi._execCallableAPIAsync === 'function'
    ? pluginApi._execCallableAPIAsync.bind(pluginApi)
    : null
}

const getHostRootdirFileApi = () => {
  try {
    const hostScope =
      (
        logseq.Experiments as unknown as {
          ensureHostScope?: () =>
            | {
                logseq?: {
                  api?: {
                    read_rootdir_file?: (
                      file: string,
                      subRoot: string,
                      rootDir: string,
                    ) => Promise<unknown>
                    write_rootdir_file?: (
                      file: string,
                      content: string,
                      subRoot: string,
                      rootDir: string,
                    ) => Promise<unknown>
                  }
                }
              }
            | undefined
        }
      ).ensureHostScope?.() ?? null

    const rootdirApi = hostScope?.logseq?.api
    return rootdirApi &&
      typeof rootdirApi.read_rootdir_file === 'function' &&
      typeof rootdirApi.write_rootdir_file === 'function'
      ? rootdirApi
      : null
  } catch {
    return null
  }
}

const readGraphFileViaRootdirApi = async (
  graphPath: string | null | undefined,
  relativeFilePath: string,
) => {
  if (!graphPath) {
    throw new Error('Current graph path is unavailable.')
  }

  const failures: string[] = []
  const execCallableApiAsync = getInternalExecCallableApiAsync()
  if (execCallableApiAsync) {
    try {
      const content = await execCallableApiAsync(
        'read_rootdir_file',
        relativeFilePath,
        '',
        graphPath,
      )
      if (typeof content === 'string') {
        return {
          content,
          absolutePath: joinAbsolutePath(graphPath, relativeFilePath),
          readFrom: 'rootdir-api' as const,
        }
      }

      failures.push(
        `internal read_rootdir_file returned ${serializeDiagnosticValue(
          describeRootdirApiResult(content),
        )}`,
      )
    } catch (error: unknown) {
      failures.push(`internal read_rootdir_file threw ${serializeDiagnosticValue(error)}`)
    }
  } else {
    failures.push('internal _execCallableAPIAsync is unavailable')
  }

  const hostRootdirApi = getHostRootdirFileApi()
  if (hostRootdirApi?.read_rootdir_file) {
    try {
      const content = await hostRootdirApi.read_rootdir_file(
        relativeFilePath,
        '',
        graphPath,
      )
      if (typeof content === 'string') {
        return {
          content,
          absolutePath: joinAbsolutePath(graphPath, relativeFilePath),
          readFrom: 'host-rootdir-api' as const,
        }
      }

      failures.push(
        `host read_rootdir_file returned ${serializeDiagnosticValue(
          describeRootdirApiResult(content),
        )}`,
      )
    } catch (error: unknown) {
      failures.push(`host read_rootdir_file threw ${serializeDiagnosticValue(error)}`)
    }
  } else {
    failures.push('host rootdir file API is unavailable')
  }

  throw new Error(
    `Rootdir file API failed for ${relativeFilePath} | ${failures.join(' | ')}`,
  )
}

const writeGraphFileViaRootdirApi = async ({
  graphPath,
  relativeFilePath,
  content,
}: {
  graphPath: string | null | undefined
  relativeFilePath: string
  content: string
}) => {
  if (!graphPath) {
    throw new Error('Current graph path is unavailable.')
  }

  const failures: string[] = []
  const execCallableApiAsync = getInternalExecCallableApiAsync()
  if (execCallableApiAsync) {
    try {
      const result = await execCallableApiAsync(
        'write_rootdir_file',
        relativeFilePath,
        content,
        '',
        graphPath,
      )
      return result
    } catch (error: unknown) {
      failures.push(`internal write_rootdir_file threw ${serializeDiagnosticValue(error)}`)
    }
  } else {
    failures.push('internal _execCallableAPIAsync is unavailable')
  }

  const hostRootdirApi = getHostRootdirFileApi()
  if (hostRootdirApi?.write_rootdir_file) {
    try {
      const result = await hostRootdirApi.write_rootdir_file(
        relativeFilePath,
        content,
        '',
        graphPath,
      )
      return result
    } catch (error: unknown) {
      failures.push(`host write_rootdir_file threw ${serializeDiagnosticValue(error)}`)
    }
  } else {
    failures.push('host rootdir file API is unavailable')
  }

  throw new Error(
    `Rootdir write API failed for ${relativeFilePath} | ${failures.join(' | ')}`,
  )
}

const buildPageFileStem = (pageName: string) => pageName.replaceAll('/', '___')

const buildCurrentPageCandidateRelativePaths = (aliases: string[]) =>
  uniqueStrings(
    aliases.flatMap((alias) => [
      `pages/${buildPageFileStem(alias)}.org`,
      `pages/${buildPageFileStem(alias)}.md`,
      `whiteboards/${alias}.edn`,
    ]),
  )

const readGraphFile = async (
  graphPath: string | null | undefined,
  relativeFilePath: string,
) => {
  const failures: string[] = []

  try {
    if (!graphPath) {
      throw new Error('Current graph path is unavailable.')
    }

    const fsPromises = getRuntimeFsPromises()
    const absolutePath = joinAbsolutePath(graphPath, relativeFilePath)
    const content = await fsPromises.readFile(absolutePath, 'utf8')
    return {
      content,
      absolutePath,
      readFrom: 'disk' as const,
    }
  } catch (error: unknown) {
    failures.push(`disk read failed ${serializeDiagnosticValue(error)}`)
  }

  try {
    const content = await logseq.DB.getFileContent(relativeFilePath)
    if (typeof content !== 'string') {
      throw new Error(
        `getFileContent returned ${serializeDiagnosticValue(
          describeRootdirApiResult(content),
        )}`,
      )
    }

    return {
      content,
      absolutePath: null,
      readFrom: 'db' as const,
    }
  } catch (error: unknown) {
    failures.push(`DB.getFileContent failed ${serializeDiagnosticValue(error)}`)
  }

  try {
    const resolved = await readGraphFileViaRootdirApi(graphPath, relativeFilePath)
    return resolved
  } catch (error: unknown) {
    failures.push(describeUnknownError(error))
  }

  throw new Error(
    `Failed to read graph file ${relativeFilePath} | ${failures.join(' | ')}`,
  )
}

const writeGraphFile = async ({
  graphPath,
  relativeFilePath,
  content,
}: {
  graphPath: string | null | undefined
  relativeFilePath: string
  content: string
}) => {
  try {
    await writeGraphFileViaRootdirApi({
      graphPath,
      relativeFilePath,
      content,
    })
  } catch {
    try {
      await logseq.DB.setFileContent(relativeFilePath, content)
    } catch {
      if (!graphPath) {
        throw new Error(
          `Failed to persist graph file ${relativeFilePath}: current graph path is unavailable.`,
        )
      }
      const fsPromises = getRuntimeFsPromises()
      await fsPromises.writeFile(
        joinAbsolutePath(graphPath, relativeFilePath),
        content,
        'utf8',
      )
    }
  }

  const confirmed = await readGraphFile(graphPath, relativeFilePath)
  if (confirmed.content !== content) {
    throw new Error(`Failed to persist graph file ${relativeFilePath}`)
  }
}

const resolveGraphRootPath = (graph: AppGraphInfo | null | undefined) => {
  if (typeof graph?.path === 'string' && graph.path.trim().length > 0) {
    return graph.path.trim()
  }

  if (typeof graph?.url === 'string' && graph.url.trim().length > 0) {
    const rawUrl = graph.url.trim()
    if (rawUrl.startsWith('file://')) {
      try {
        return decodeURIComponent(new URL(rawUrl).pathname)
      } catch {
        // Fall through to other heuristics.
      }
    }

    if (rawUrl.startsWith('/')) {
      return rawUrl
    }
  }

  return null
}

const uniqueStrings = (values: string[]) =>
  values.filter(
    (value, index, array) => value.length > 0 && array.indexOf(value) === index,
  )

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

const buildLegacyBlockRefMappingCacheKeyV1 = (namespaceRoot: string) =>
  `legacy-block-ref-mapping/v1/${encodeURIComponent(namespaceRoot)}.json`

const collectPageAliases = (page: PageEntity): string[] =>
  uniqueStrings([
    typeof page.originalName === 'string' ? page.originalName : '',
    typeof page.title === 'string' ? page.title : '',
    typeof page.name === 'string' ? page.name : '',
  ])

const isPageEntityLike = (value: unknown): value is PageEntity =>
  value != null &&
  typeof value === 'object' &&
  ('originalName' in value || 'title' in value || 'name' in value)

const getCurrentRoutePath = () => {
  try {
    const location = window.top?.location ?? window.location
    if (location.hash && location.hash !== '#') {
      return location.hash.startsWith('#') ? location.hash.slice(1) : location.hash
    }

    const path = `${location.pathname}${location.search}`
    return path || null
  } catch {
    return null
  }
}

const decodeCurrentRoutePageName = () => {
  const routePath = getCurrentRoutePath()
  if (!routePath) {
    return null
  }

  const [rawRoutePath] = routePath.split('?')
  const normalizedRoutePath = rawRoutePath ?? ''
  const candidateMatches = [
    normalizedRoutePath.match(/^\/page\/(.+)$/i),
    normalizedRoutePath.match(/^\/whiteboard\/(.+)$/i),
    normalizedRoutePath.match(/^\/whiteboards\/(.+)$/i),
  ]

  for (const pagePrefixMatch of candidateMatches) {
    if (!pagePrefixMatch) continue
    const rawPageName = pagePrefixMatch[1]?.trim() ?? ''
    if (rawPageName.length === 0) continue

    try {
      return decodeURIComponent(rawPageName)
    } catch {
      return rawPageName
    }
  }

  return null
}

const resolveCurrentOpenPageEntityV1 = async (): Promise<PageEntity> => {
  const currentPage = await logseq.Editor.getCurrentPage()
  if (isPageEntityLike(currentPage)) {
    return currentPage
  }

  const routePageName = decodeCurrentRoutePageName()
  if (routePageName) {
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const routePage = await logseq.Editor.getPage(routePageName)
      if (routePage) {
        return routePage
      }

      if (attempt < 3) {
        await new Promise((resolve) => window.setTimeout(resolve, 80 * (attempt + 1)))
      }
    }
  }

  throw new Error('No current page is open.')
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

const collectContentBlocks = (
  blocks: Array<BlockEntity | [unknown, string]>,
): Array<BlockEntity> => {
  const collected: BlockEntity[] = []

  const visit = (node: BlockEntity | [unknown, string]) => {
    if (Array.isArray(node)) return

    if (typeof node.content === 'string') {
      collected.push(node)
    }

    for (const child of node.children ?? []) {
      visit(child as BlockEntity | [unknown, string])
    }
  }

  for (const block of blocks) {
    visit(block)
  }

  return collected
}

const getBlockPropertyString = (
  block: BlockEntity,
  candidateKeys: string[],
): string | null => {
  const properties = block.properties
  if (!properties || typeof properties !== 'object') {
    return null
  }

  const normalizedKeys = uniqueStrings(
    candidateKeys.flatMap((key) => [
      key,
      key.toLowerCase(),
      key.replaceAll('-', '_'),
      key.toLowerCase().replaceAll('-', '_'),
    ]),
  )

  for (const key of normalizedKeys) {
    const value = (properties as Record<string, unknown>)[key]
    if (typeof value === 'string' && value.length > 0) {
      return value
    }
  }

  return null
}

const resolveCurrentPageBestEffortRelativeFilePath = (
  page: PageEntity,
  aliases: string[],
) => {
  const directRelativePath =
    page.file &&
    typeof page.file === 'object' &&
    'path' in page.file &&
    typeof page.file.path === 'string' &&
    page.file.path.length > 0
      ? page.file.path
      : null

  if (directRelativePath) {
    return directRelativePath
  }

  const pageName = aliases[0] ?? page.originalName ?? page.name ?? page.title ?? ''
  if (page.type === 'whiteboard') {
    return `whiteboards/${pageName}.edn`
  }

  return `pages/${buildPageFileStem(pageName)}.org`
}

const extractLegacyToCurrentUuidPairsFromContent = (content: string) => {
  const pairs = new Map<string, string>()
  const lines = content.split('\n')

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? ''
    const highlightUrl =
      line.match(READWISE_HIGHLIGHT_URL_IN_BLOCK_PATTERN)?.[1] ?? null

    if (!highlightUrl) {
      continue
    }

    let currentUuid: string | null = null

    for (let nextIndex = index + 1; nextIndex < lines.length; nextIndex += 1) {
      const nextLine = lines[nextIndex] ?? ''

      if (/^\*\* /.test(nextLine)) {
        break
      }

      const idMatch = nextLine.match(BLOCK_ID_LINE_PATTERN)
      if (idMatch) {
        currentUuid = idMatch[1] ?? null
        break
      }
    }

    if (!currentUuid) {
      continue
    }

    const legacyUuid = computeLegacyHighlightUuid(highlightUrl)
    const canonicalUuid = computeCompatibleHighlightUuid(highlightUrl)

    if (legacyUuid !== canonicalUuid) {
      pairs.set(legacyUuid, canonicalUuid)
    }

    if (currentUuid !== canonicalUuid) {
      pairs.set(currentUuid, canonicalUuid)
    }
  }

  return pairs
}

export interface LegacyBlockRefMappingSummaryV1 {
  managedPagesScanned: number
  mappedLegacyUuids: number
  cacheStatus: 'hit' | 'rebuilt'
}

const restoreLegacyBlockRefMappingCacheV1 = async ({
  namespaceRoot,
  managedPagesScanned,
}: {
  namespaceRoot: string
  managedPagesScanned: number
}): Promise<Map<string, string> | null> => {
  const key = buildLegacyBlockRefMappingCacheKeyV1(namespaceRoot)
  const raw = await safeGetFileStorageItem(key)
  if (!raw) return null

  let parsed: StoredLegacyBlockRefMappingCacheV1
  try {
    parsed = JSON.parse(raw) as StoredLegacyBlockRefMappingCacheV1
  } catch {
    return null
  }

  if (
    parsed.schemaVersion !== LEGACY_BLOCK_REF_MAPPING_CACHE_SCHEMA_VERSION ||
    parsed.namespaceRoot !== namespaceRoot
  ) {
    return null
  }

  const expiresAt = Date.parse(parsed.expiresAt)
  if (!Number.isFinite(expiresAt) || expiresAt < Date.now()) {
    return null
  }

  if (parsed.managedPagesScanned !== managedPagesScanned) {
    return null
  }

  return new Map(
    (parsed.entries ?? []).filter(
      (entry): entry is [string, string] =>
        Array.isArray(entry) &&
        typeof entry[0] === 'string' &&
        typeof entry[1] === 'string' &&
        entry[0].length > 0 &&
        entry[1].length > 0,
    ),
  )
}

const persistLegacyBlockRefMappingCacheV1 = async ({
  namespaceRoot,
  managedPagesScanned,
  mapping,
}: {
  namespaceRoot: string
  managedPagesScanned: number
  mapping: Map<string, string>
}) => {
  const now = new Date()
  const payload: StoredLegacyBlockRefMappingCacheV1 = {
    schemaVersion: LEGACY_BLOCK_REF_MAPPING_CACHE_SCHEMA_VERSION,
    namespaceRoot,
    cachedAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + LEGACY_BLOCK_REF_MAPPING_CACHE_TTL_MS).toISOString(),
    managedPagesScanned,
    entries: [...mapping.entries()],
  }

  await logseq.FileStorage.setItem(
    buildLegacyBlockRefMappingCacheKeyV1(namespaceRoot),
    JSON.stringify(payload),
  )
}

export const invalidateLegacyBlockRefMappingCacheV1 = async (
  namespaceRoot: string,
) => {
  try {
    const key = buildLegacyBlockRefMappingCacheKeyV1(namespaceRoot)
    const hasItem = await logseq.FileStorage.hasItem(key)
    if (hasItem) {
      await logseq.FileStorage.removeItem(key)
    }
  } catch (error: unknown) {
    logReadwiseDebug(LEGACY_ID_MIGRATION_LOG_PREFIX, 'failed to invalidate legacy block ref mapping cache', {
      namespaceRoot,
      error: describeUnknownError(error),
    })
  }
}

export const buildLegacyBlockRefMappingV1 = async ({
  namespaceRoot,
  onProgress,
}: {
  namespaceRoot: string
  onProgress?: (progress: {
    total: number
    completed: number
    pageName: string
  }) => void
}): Promise<{
  mapping: Map<string, string>
  summary: LegacyBlockRefMappingSummaryV1
}> => {
  const allPages = (await logseq.Editor.getAllPages()) ?? []
  const managedPages = allPages.filter((page) =>
    collectPageAliases(page as PageEntity).some((alias) =>
      alias === namespaceRoot || alias.startsWith(`${namespaceRoot}/`),
    ),
  ) as PageEntity[]

  const cachedMapping = await restoreLegacyBlockRefMappingCacheV1({
    namespaceRoot,
    managedPagesScanned: managedPages.length,
  })
  if (cachedMapping) {
    logReadwiseDebug(LEGACY_ID_MIGRATION_LOG_PREFIX, 'loaded legacy block ref mapping cache', {
      namespaceRoot,
      managedPagesScanned: managedPages.length,
      mappedLegacyUuids: cachedMapping.size,
    })
    return {
      mapping: cachedMapping,
      summary: {
        managedPagesScanned: managedPages.length,
        mappedLegacyUuids: cachedMapping.size,
        cacheStatus: 'hit' as const,
      },
    }
  }

  const mapping = new Map<string, string>()

  for (let index = 0; index < managedPages.length; index += 1) {
    const page = managedPages[index]!
    const pageName =
      collectPageAliases(page)[0] ?? page.originalName ?? page.name ?? page.title ?? ''

    onProgress?.({
      total: managedPages.length,
      completed: index + 1,
      pageName,
    })

    const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
    const content = flattenPageBlocksTreeContent(pageBlocksTree ?? [])
    const pairs = extractLegacyToCurrentUuidPairsFromContent(content)

    for (const [legacyUuid, currentUuid] of pairs) {
      mapping.set(legacyUuid, currentUuid)
    }
  }

  try {
    await persistLegacyBlockRefMappingCacheV1({
      namespaceRoot,
      managedPagesScanned: managedPages.length,
      mapping,
    })
    logReadwiseDebug(LEGACY_ID_MIGRATION_LOG_PREFIX, 'persisted legacy block ref mapping cache', {
      namespaceRoot,
      managedPagesScanned: managedPages.length,
      mappedLegacyUuids: mapping.size,
    })
  } catch (error: unknown) {
    logReadwiseDebug(LEGACY_ID_MIGRATION_LOG_PREFIX, 'failed to persist legacy block ref mapping cache', {
      namespaceRoot,
      managedPagesScanned: managedPages.length,
      mappedLegacyUuids: mapping.size,
      error: describeUnknownError(error),
    })
  }

  return {
    mapping,
    summary: {
      managedPagesScanned: managedPages.length,
      mappedLegacyUuids: mapping.size,
      cacheStatus: 'rebuilt',
    },
  }
}

export interface MigrateLegacyBlockRefsSummaryV1 {
  graphPagesScanned: number
  graphPagesUpdated: number
  blocksUpdated: number
  refsRewritten: number
}

export interface LegacyBlockRefPreviewEntryV1 {
  pageName: string
  blockUuid: string
  refs: Array<{
    from: string
    to: string
  }>
}

export interface PreviewLegacyBlockRefsSummaryV1 {
  graphPagesScanned: number
  graphPagesAffected: number
  blocksAffected: number
  refsPlanned: number
}

export interface CurrentPageLegacyIdRewriteEntryV1 {
  entryIndex: number
  blockUuid: string
  kind: 'block-ref' | 'refdock-item-id'
  from: string
  to: string
  beforeLine: string
  afterLine: string
}

export interface PreviewCurrentPageLegacyIdsSummaryV1 {
  pageName: string
  relativeFilePath: string
  fileKind: 'page' | 'whiteboard'
  rewritesPlanned: number
}

export interface MigrateCurrentPageLegacyIdsSummaryV1 {
  pageName: string
  relativeFilePath: string
  fileKind: 'page' | 'whiteboard'
  rewritesApplied: number
}

interface CurrentPageLegacyIdMigrationTargetV1 {
  page: PageEntity
  pageName: string
  relativeFilePath: string
  fileKind: 'page' | 'whiteboard'
}

const escapeDatalogString = (value: string) =>
  value.replaceAll('\\', '\\\\').replaceAll('"', '\\"')

const resolvePageFilePathViaQuery = async (
  page: PageEntity,
  aliases: string[],
): Promise<string | null> => {
  if (typeof page.id === 'number') {
    try {
      const byId = await logseq.DB.q<string>(
        `[:find ?path . :where [${page.id} :block/file ?f] [?f :file/path ?path]]`,
      )
      if (typeof byId === 'string' && byId.length > 0) {
        return byId
      }
    } catch {
      // Fall through to alias-based lookup.
    }
  }

  for (const alias of aliases) {
    try {
      const normalizedAlias = escapeDatalogString(alias.toLowerCase())
      const byName = await logseq.DB.q<string>(
        `[:find ?path . :where [?p :block/name "${normalizedAlias}"] [?p :block/file ?f] [?f :file/path ?path]]`,
      )
      if (typeof byName === 'string' && byName.length > 0) {
        return byName
      }
    } catch {
      // Continue trying more aliases.
    }
  }

  return null
}

const resolveCurrentPageLegacyIdMigrationTargetV1 =
  async (): Promise<CurrentPageLegacyIdMigrationTargetV1> => {
    const currentPage = await resolveCurrentOpenPageEntityV1()

    const aliases = collectPageAliases(currentPage)
    const pageName = aliases[0] ?? null
    if (!pageName) {
      throw new Error('Failed to resolve the current page name.')
    }

    return {
      page: currentPage,
      pageName,
      relativeFilePath: resolveCurrentPageBestEffortRelativeFilePath(currentPage, aliases),
      fileKind: currentPage.type === 'whiteboard' ? 'whiteboard' : 'page',
    }
  }

const getLineAtIndex = (content: string, index: number) => {
  const before = content.slice(0, index)
  const lineStart = before.lastIndexOf('\n') + 1
  const nextNewline = content.indexOf('\n', index)
  const lineEnd = nextNewline === -1 ? content.length : nextNewline
  return content.slice(lineStart, lineEnd)
}

const collectCurrentPageLegacyIdRewritesFromBlocks = (
  blocks: BlockEntity[],
  mapping: Map<string, string>,
): CurrentPageLegacyIdRewriteEntryV1[] => {
  const rewrites: CurrentPageLegacyIdRewriteEntryV1[] = []
  let rewriteIndex = 0

  for (const block of blocks) {
    const originalContent = block.content ?? ''

    for (const match of originalContent.matchAll(BLOCK_REF_PATTERN)) {
      const from = match[1] ?? ''
      const to = mapping.get(from)
      const index = match.index ?? -1
      if (!to || to === from || index < 0) continue

      const beforeLine = getLineAtIndex(originalContent, index)
      rewriteIndex += 1
      rewrites.push({
        entryIndex: rewriteIndex,
        blockUuid: block.uuid,
        kind: 'block-ref',
        from,
        to,
        beforeLine,
        afterLine: beforeLine.replaceAll(`((${from}))`, `((${to}))`),
      })
    }

    for (const match of originalContent.matchAll(REFDOCK_ITEM_ID_PATTERN)) {
      const prefix = match[1] ?? ''
      const from = match[2] ?? ''
      const suffix = match[3] ?? ''
      const to = mapping.get(from)
      const index = match.index ?? -1
      if (!to || to === from || index < 0) continue

      const beforeLine = getLineAtIndex(originalContent, index)
      rewriteIndex += 1
      rewrites.push({
        entryIndex: rewriteIndex,
        blockUuid: block.uuid,
        kind: 'refdock-item-id',
        from,
        to,
        beforeLine,
        afterLine: `${prefix}${to}${suffix}`,
      })
    }

    if (originalContent.match(REFDOCK_ITEM_ID_PATTERN)) {
      continue
    }

    const propertyValue = getBlockPropertyString(block, ['refdock-item-id'])
    if (!propertyValue) {
      continue
    }

    const nextUuid = mapping.get(propertyValue)
    if (!nextUuid || nextUuid === propertyValue) {
      continue
    }

    rewriteIndex += 1
    rewrites.push({
      entryIndex: rewriteIndex,
      blockUuid: block.uuid,
      kind: 'refdock-item-id',
      from: propertyValue,
      to: nextUuid,
      beforeLine: `:refdock-item-id: ${propertyValue}`,
      afterLine: `:refdock-item-id: ${nextUuid}`,
    })
  }

  return rewrites.sort((left, right) => left.entryIndex - right.entryIndex)
}

const rewriteCurrentPageLegacyIdsInBlocks = (
  blocks: BlockEntity[],
  mapping: Map<string, string>,
) => {
  let rewritesApplied = 0
  const contentUpdates: Array<{
    blockUuid: string
    nextContent: string
  }> = []
  const propertyUpdates: Array<{
    blockUuid: string
    key: string
    value: string
  }> = []

  for (const block of blocks) {
    const originalContent = block.content ?? ''
    let replacedInBlock = 0
    let nextContent = originalContent.replace(BLOCK_REF_PATTERN, (match, uuid: string) => {
      const nextUuid = mapping.get(uuid)
      if (!nextUuid || nextUuid === uuid) {
        return match
      }

      replacedInBlock += 1
      return `((${nextUuid}))`
    })

    const contentContainsRefdockProperty = Boolean(
      originalContent.match(REFDOCK_ITEM_ID_PATTERN),
    )
    nextContent = nextContent.replace(
      REFDOCK_ITEM_ID_PATTERN,
      (match, prefix: string, uuid: string, suffix: string) => {
        const nextUuid = mapping.get(uuid)
        if (!nextUuid || nextUuid === uuid) {
          return match
        }

        replacedInBlock += 1
        return `${prefix}${nextUuid}${suffix}`
      },
    )

    if (replacedInBlock > 0 && nextContent !== originalContent) {
      contentUpdates.push({
        blockUuid: block.uuid,
        nextContent,
      })
      rewritesApplied += replacedInBlock
    }

    if (contentContainsRefdockProperty) {
      continue
    }

    const propertyValue = getBlockPropertyString(block, ['refdock-item-id'])
    if (!propertyValue) {
      continue
    }

    const nextUuid = mapping.get(propertyValue)
    if (!nextUuid || nextUuid === propertyValue) {
      continue
    }

    propertyUpdates.push({
      blockUuid: block.uuid,
      key: 'refdock-item-id',
      value: nextUuid,
    })
    rewritesApplied += 1
  }

  return {
    rewritesApplied,
    contentUpdates,
    propertyUpdates,
  }
}

export const previewLegacyBlockRefsV1 = async ({
  mapping,
  onProgress,
}: {
  mapping: Map<string, string>
  onProgress?: (progress: {
    total: number
    completed: number
    pageName: string
    affectedPages: number
    refsPlanned: number
  }) => void
}): Promise<{
  entries: LegacyBlockRefPreviewEntryV1[]
  summary: PreviewLegacyBlockRefsSummaryV1
}> => {
  if (mapping.size === 0) {
    return {
      entries: [],
      summary: {
        graphPagesScanned: 0,
        graphPagesAffected: 0,
        blocksAffected: 0,
        refsPlanned: 0,
      },
    }
  }

  const allPages = ((await logseq.Editor.getAllPages()) ?? []) as PageEntity[]
  const entries: LegacyBlockRefPreviewEntryV1[] = []
  let graphPagesAffected = 0
  let refsPlanned = 0

  for (let index = 0; index < allPages.length; index += 1) {
    const page = allPages[index]!
    const pageName =
      collectPageAliases(page)[0] ?? page.originalName ?? page.name ?? page.title ?? ''
    const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
    const contentBlocks = collectContentBlocks(pageBlocksTree ?? [])
    let pageAffected = false

    for (const block of contentBlocks) {
      const originalContent = block.content ?? ''
      const refs = [...originalContent.matchAll(BLOCK_REF_PATTERN)]
        .map((match) => match[1] ?? '')
        .map((uuid) => {
          const nextUuid = mapping.get(uuid)
          return nextUuid && nextUuid !== uuid
            ? {
                from: uuid,
                to: nextUuid,
              }
            : null
        })
        .filter(
          (pair): pair is { from: string; to: string } =>
            pair != null,
        )

      if (refs.length === 0) {
        continue
      }

      entries.push({
        pageName,
        blockUuid: block.uuid,
        refs,
      })
      pageAffected = true
      refsPlanned += refs.length
    }

    if (pageAffected) {
      graphPagesAffected += 1
    }

    onProgress?.({
      total: allPages.length,
      completed: index + 1,
      pageName,
      affectedPages: graphPagesAffected,
      refsPlanned,
    })
  }

  return {
    entries,
    summary: {
      graphPagesScanned: allPages.length,
      graphPagesAffected,
      blocksAffected: entries.length,
      refsPlanned,
    },
  }
}

export const migrateLegacyBlockRefsV1 = async ({
  mapping,
  logPrefix = '[Readwise Sync]',
  onProgress,
}: {
  mapping: Map<string, string>
  logPrefix?: string
  onProgress?: (progress: {
    total: number
    completed: number
    pageName: string
    updatedPages: number
    refsRewritten: number
  }) => void
}): Promise<MigrateLegacyBlockRefsSummaryV1> => {
  if (mapping.size === 0) {
    return {
      graphPagesScanned: 0,
      graphPagesUpdated: 0,
      blocksUpdated: 0,
      refsRewritten: 0,
    }
  }

  const allPages = ((await logseq.Editor.getAllPages()) ?? []) as PageEntity[]
  let graphPagesUpdated = 0
  let blocksUpdated = 0
  let refsRewritten = 0

  for (let index = 0; index < allPages.length; index += 1) {
    const page = allPages[index]!
    const pageName =
      collectPageAliases(page)[0] ?? page.originalName ?? page.name ?? page.title ?? ''
    const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)
    const contentBlocks = collectContentBlocks(pageBlocksTree ?? [])
    let pageUpdated = false

    for (const block of contentBlocks) {
      const originalContent = block.content ?? ''
      let replacedInBlock = 0
      const nextContent = originalContent.replace(
        BLOCK_REF_PATTERN,
        (match, uuid: string) => {
          const nextUuid = mapping.get(uuid)
          if (!nextUuid || nextUuid === uuid) {
            return match
          }

          replacedInBlock += 1
          return `((${nextUuid}))`
        },
      )

      if (replacedInBlock === 0 || nextContent === originalContent) {
        continue
      }

      await logseq.Editor.updateBlock(block.uuid, nextContent)
      pageUpdated = true
      blocksUpdated += 1
      refsRewritten += replacedInBlock
    }

    if (pageUpdated) {
      graphPagesUpdated += 1
    }

    onProgress?.({
      total: allPages.length,
      completed: index + 1,
      pageName,
      updatedPages: graphPagesUpdated,
      refsRewritten,
    })
  }

  logReadwiseInfo(logPrefix, 'migrated legacy block refs', {
    mappingSize: mapping.size,
    graphPagesScanned: allPages.length,
    graphPagesUpdated,
    blocksUpdated,
    refsRewritten,
  })

  return {
    graphPagesScanned: allPages.length,
    graphPagesUpdated,
    blocksUpdated,
    refsRewritten,
  }
}

export const previewCurrentPageLegacyIdsV1 = async ({
  mapping,
}: {
  mapping: Map<string, string>
}): Promise<{
  target: PreviewCurrentPageLegacyIdsSummaryV1
  rewrites: CurrentPageLegacyIdRewriteEntryV1[]
}> => {
  const target = await resolveCurrentPageLegacyIdMigrationTargetV1()
  const pageBlocksTree = await logseq.Editor.getPageBlocksTree(target.page.name)
  const rewrites = collectCurrentPageLegacyIdRewritesFromBlocks(
    collectContentBlocks(pageBlocksTree ?? []),
    mapping,
  )

  return {
    target: {
      pageName: target.pageName,
      relativeFilePath: target.relativeFilePath,
      fileKind: target.fileKind,
      rewritesPlanned: rewrites.length,
    },
    rewrites,
  }
}

export const migrateCurrentPageLegacyIdsV1 = async ({
  mapping,
  expectedRelativeFilePath,
  logPrefix = '[Readwise Sync]',
}: {
  mapping: Map<string, string>
  expectedRelativeFilePath?: string | null
  logPrefix?: string
}): Promise<MigrateCurrentPageLegacyIdsSummaryV1> => {
  const target = await resolveCurrentPageLegacyIdMigrationTargetV1()
  if (
    expectedRelativeFilePath &&
    target.relativeFilePath !== expectedRelativeFilePath
  ) {
    throw new Error(
      `Current page file changed from ${expectedRelativeFilePath} to ${target.relativeFilePath}. Re-run preview before applying.`,
    )
  }

  const pageBlocksTree = await logseq.Editor.getPageBlocksTree(target.page.name)
  const { rewritesApplied, contentUpdates, propertyUpdates } =
    rewriteCurrentPageLegacyIdsInBlocks(
    collectContentBlocks(pageBlocksTree ?? []),
    mapping,
  )

  for (const update of contentUpdates) {
    await logseq.Editor.updateBlock(update.blockUuid, update.nextContent)
  }

  for (const propertyUpdate of propertyUpdates) {
    await logseq.Editor.upsertBlockProperty(
      propertyUpdate.blockUuid,
      propertyUpdate.key,
      propertyUpdate.value,
    )
  }

  logReadwiseInfo(logPrefix, 'migrated current-page legacy ids', {
    pageName: target.pageName,
    relativeFilePath: target.relativeFilePath,
    fileKind: target.fileKind,
    rewritesApplied,
  })

  return {
    pageName: target.pageName,
    relativeFilePath: target.relativeFilePath,
    fileKind: target.fileKind,
    rewritesApplied,
  }
}
