import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'

interface CurrentPageSnapshotV1 {
  schemaVersion: 1
  capturedAt: string
  graphName: string | null
  graphPath: string | null
  pageName: string
  pageAliases: string[]
  source: 'file' | 'page_tree'
  relativeFilePath: string | null
  content: string
}

export interface CurrentPageDiffResult {
  changed: boolean
  pageName: string
  source: 'file' | 'page_tree'
  relativeFilePath: string | null
  firstDiffLine: number | null
  beforeExcerpt: string
  afterExcerpt: string
  beforeFullText: string
  afterFullText: string
  summary: string
}

const CURRENT_PAGE_SNAPSHOT_KEY = 'page-file-diff/current-page-snapshot.json'

interface PageFileResolutionDiagnostics {
  pageName: string
  pageAliases: string[]
  pageEntity: {
    id: number | null
    originalName: string | null
    name: string | null
    title: string | null
    format: string | null
  }
  candidatePaths: string[]
  directReadAttempts: Array<{
    path: string
    ok: boolean
    error: string | null
  }>
  diskReadAttempts: Array<{
    relativePath: string
    absolutePath: string
    ok: boolean
    error: string | null
  }>
  assetsCount: number | null
  assetExactMatch: string | null
  assetStemMatch: string | null
  queryMatchById: string | null
  queryMatchByAlias: string | null
  resolvedFrom:
    | 'direct-read'
    | 'disk-read'
    | 'asset-exact'
    | 'asset-stem'
    | 'query'
    | null
  contentReadError: string | null
}

class PageFileResolutionError extends Error {
  diagnostics: PageFileResolutionDiagnostics

  constructor(message: string, diagnostics: PageFileResolutionDiagnostics) {
    super(message)
    this.name = 'PageFileResolutionError'
    this.diagnostics = diagnostics
  }
}

const safeJsonStringify = (value: unknown) => {
  try {
    return JSON.stringify(value, null, 2)
  } catch (error) {
    return JSON.stringify({
      serializationError: error instanceof Error ? error.message : String(error),
    })
  }
}

const trimTrailingSeparators = (value: string) => value.replace(/[\\/]+$/, '')

const joinAbsolutePath = (basePath: string, relativePath: string) =>
  `${trimTrailingSeparators(basePath)}/${relativePath}`

const tryReadGraphFileFromDisk = async (
  graphPath: string | null | undefined,
  relativePath: string,
) => {
  if (typeof graphPath !== 'string' || graphPath.length === 0) {
    throw new Error('Current graph path is unavailable.')
  }

  const runtimeRequire = (window as unknown as { require?: ((id: string) => unknown) | undefined })
    .require

  if (typeof runtimeRequire !== 'function') {
    throw new Error('window.require is unavailable in the current Logseq runtime.')
  }

  const fsPromises = runtimeRequire('node:fs/promises') as {
    readFile: (path: string, encoding: string) => Promise<string>
  }
  const absolutePath = joinAbsolutePath(graphPath, relativePath)
  const content = await fsPromises.readFile(absolutePath, 'utf8')

  return {
    absolutePath,
    content,
  }
}

const readResolvedGraphFileContent = async (
  relativeFilePath: string,
  graphPath: string | null | undefined,
  diagnostics?: PageFileResolutionDiagnostics | null,
) => {
  try {
    const diskRead = await tryReadGraphFileFromDisk(graphPath, relativeFilePath)
    return {
      content: diskRead.content,
      absolutePath: diskRead.absolutePath,
      readFrom: 'disk-read' as const,
    }
  } catch (error) {
    if (diagnostics) {
      diagnostics.diskReadAttempts.push({
        relativePath: relativeFilePath,
        absolutePath:
          typeof graphPath === 'string' && graphPath.length > 0
            ? joinAbsolutePath(graphPath, relativeFilePath)
            : relativeFilePath,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      })
    }
  }

  try {
    const content = await logseq.DB.getFileContent(relativeFilePath)
    if (typeof content !== 'string') {
      throw new Error('getFileContent returned non-string content')
    }

    return {
      content,
      absolutePath: null,
      readFrom: 'direct-read' as const,
    }
  } catch (error) {
    if (diagnostics) {
      diagnostics.contentReadError =
        error instanceof Error ? error.message : String(error)
    }
    throw error
  }
}

const uniqueValues = (values: string[]) =>
  values.filter((value, index, array) => value.length > 0 && array.indexOf(value) === index)

const collectAliases = (page: Partial<PageEntity> | Partial<BlockEntity> | null) => {
  if (!page) return []

  return uniqueValues([
    typeof page['originalName'] === 'string' ? page['originalName'] : '',
    typeof page['name'] === 'string' ? page['name'] : '',
    typeof page['title'] === 'string' ? page['title'] : '',
  ])
}

const buildPageFileStem = (pageName: string) => pageName.replaceAll('/', '___')

const buildCandidateRelativeFilePaths = (
  pageName: string,
  expectedFormat: 'org' | 'markdown' | null,
) => {
  const expectedStem = `pages/${buildPageFileStem(pageName)}`
  const preferredExtensions =
    expectedFormat === 'markdown' ? ['md', 'org'] : ['org', 'md']

  return preferredExtensions.map((extension) => `${expectedStem}.${extension}`)
}

const escapeDatalogString = (value: string) =>
  value.replaceAll('\\', '\\\\').replaceAll('"', '\\"')

const resolvePageFilePathViaQuery = async (
  page: PageEntity,
  aliases: string[],
): Promise<{ relativeFilePath: string | null; byId: string | null; byAlias: string | null }> => {
  let byIdPath: string | null = null

  if (typeof page.id === 'number') {
    try {
      const byId = await logseq.DB.q<string>(
        `[:find ?path . :where [${page.id} :block/file ?f] [?f :file/path ?path]]`,
      )
      if (typeof byId === 'string' && byId.length > 0) {
        return {
          relativeFilePath: byId,
          byId: byId,
          byAlias: null,
        }
      }
      byIdPath = null
    } catch {
      // Fall through to name-based query.
    }
  }

  for (const alias of aliases) {
    try {
      const normalizedAlias = escapeDatalogString(alias.toLowerCase())
      const byName = await logseq.DB.q<string>(
        `[:find ?path . :where [?p :block/name "${normalizedAlias}"] [?p :block/file ?f] [?f :file/path ?path]]`,
      )
      if (typeof byName === 'string' && byName.length > 0) {
        return {
          relativeFilePath: byName,
          byId: byIdPath,
          byAlias: byName,
        }
      }
    } catch {
      // Continue trying more aliases.
    }
  }

  return {
    relativeFilePath: null,
    byId: byIdPath,
    byAlias: null,
  }
}

const resolveCurrentPageEntity = async (): Promise<PageEntity> => {
  const currentPage = await logseq.Editor.getCurrentPage()
  if (!currentPage) {
    throw new Error('No current page is open.')
  }

  const aliases = collectAliases(currentPage)
  for (const alias of aliases) {
    const page = await logseq.Editor.getPage(alias)
    if (page) return page
  }

  throw new Error('Failed to resolve the current page entity.')
}

const resolveCurrentPageFile = async (page: PageEntity) => {
  const aliases = collectAliases(page)
  const pageName = aliases[0]
  if (!pageName) {
    throw new Error('Current page does not have a stable page name.')
  }

  const candidatePaths = buildCandidateRelativeFilePaths(pageName, page.format ?? null)
  const diagnostics: PageFileResolutionDiagnostics = {
    pageName,
    pageAliases: aliases,
    pageEntity: {
      id: typeof page.id === 'number' ? page.id : null,
      originalName: typeof page.originalName === 'string' ? page.originalName : null,
      name: typeof page.name === 'string' ? page.name : null,
      title: typeof page.title === 'string' ? page.title : null,
      format: typeof page.format === 'string' ? page.format : null,
    },
    candidatePaths,
    directReadAttempts: [],
    diskReadAttempts: [],
    assetsCount: null,
    assetExactMatch: null,
    assetStemMatch: null,
    queryMatchById: null,
    queryMatchByAlias: null,
    resolvedFrom: null,
    contentReadError: null,
  }

  for (const candidatePath of candidatePaths) {
    try {
      const content = await logseq.DB.getFileContent(candidatePath)
      if (typeof content === 'string') {
        diagnostics.directReadAttempts.push({
          path: candidatePath,
          ok: true,
          error: null,
        })
        diagnostics.resolvedFrom = 'direct-read'
        return {
          pageName,
          pageAliases: aliases,
          relativeFilePath: candidatePath,
          diagnostics,
        }
      }
      diagnostics.directReadAttempts.push({
        path: candidatePath,
        ok: false,
        error: 'getFileContent returned non-string content',
      })
    } catch {
      diagnostics.directReadAttempts.push({
        path: candidatePath,
        ok: false,
        error: 'getFileContent threw',
      })
    }
  }

  const graph = await logseq.App.getCurrentGraph()
  for (const candidatePath of candidatePaths) {
    try {
      const diskRead = await tryReadGraphFileFromDisk(graph?.path ?? null, candidatePath)
      diagnostics.diskReadAttempts.push({
        relativePath: candidatePath,
        absolutePath: diskRead.absolutePath,
        ok: true,
        error: null,
      })
      diagnostics.resolvedFrom = 'disk-read'
      return {
        pageName,
        pageAliases: aliases,
        relativeFilePath: candidatePath,
        diagnostics,
      }
    } catch (error) {
      diagnostics.diskReadAttempts.push({
        relativePath: candidatePath,
        absolutePath:
          typeof graph?.path === 'string' && graph.path.length > 0
            ? joinAbsolutePath(graph.path, candidatePath)
            : candidatePath,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      })
    }
  }

  const assets = await logseq.Assets.listFilesOfCurrentGraph(['org', 'md'])
  diagnostics.assetsCount = assets.length
  for (const candidatePath of candidatePaths) {
    const exact = assets.find((asset) => asset.path === candidatePath)
    if (exact) {
      diagnostics.assetExactMatch = exact.path
      diagnostics.resolvedFrom = 'asset-exact'
      return {
        pageName,
        pageAliases: aliases,
        relativeFilePath: candidatePath,
        diagnostics,
      }
    }
  }

  const expectedStem = `pages/${buildPageFileStem(pageName)}`
  const fallback = assets.find((asset) => asset.path.startsWith(`${expectedStem}.`))
  if (fallback) {
    diagnostics.assetStemMatch = fallback.path
    diagnostics.resolvedFrom = 'asset-stem'
    return {
      pageName,
      pageAliases: aliases,
      relativeFilePath: fallback.path,
      diagnostics,
    }
  }

  const queryResolution = await resolvePageFilePathViaQuery(page, aliases)
  diagnostics.queryMatchById = queryResolution.byId
  diagnostics.queryMatchByAlias = queryResolution.byAlias
  if (queryResolution.relativeFilePath) {
    diagnostics.resolvedFrom = 'query'
    return {
      pageName,
      pageAliases: aliases,
      relativeFilePath: queryResolution.relativeFilePath,
      diagnostics,
    }
  }

  throw new PageFileResolutionError(
    `Failed to resolve the current page file for "${pageName}".`,
    diagnostics,
  )
}

const flattenPageTreeToText = (blocks: BlockEntity[] | null | undefined): string => {
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

const resolveCurrentPageTreeContent = async (page: PageEntity) => {
  const aliases = collectAliases(page)
  const pageName = aliases[0]
  if (!pageName) {
    throw new Error('Current page does not have a stable page name.')
  }

  const pageBlocks = await logseq.Editor.getPageBlocksTree(pageName)
  const content = flattenPageTreeToText(pageBlocks)

  if (content.length === 0) {
    throw new Error(`Failed to resolve the current page tree for "${pageName}".`)
  }

  return {
    pageName,
    pageAliases: aliases,
    relativeFilePath: null,
    source: 'page_tree' as const,
    content,
  }
}

const formatExcerpt = (
  lines: string[],
  startLine: number,
  endLine: number,
) =>
  lines
    .slice(startLine, endLine)
    .map((line, index) => `${startLine + index + 1}: ${line}`)
    .join('\n')

const diffTexts = (
  before: string,
  after: string,
  pageName: string,
  source: 'file' | 'page_tree',
  relativeFilePath: string | null,
): CurrentPageDiffResult => {
  if (before === after) {
    return {
      changed: false,
      pageName,
      source,
      relativeFilePath,
      firstDiffLine: null,
      beforeExcerpt: '',
      afterExcerpt: '',
      beforeFullText: before,
      afterFullText: after,
      summary: 'No file content changes detected.',
    }
  }

  const beforeLines = before.split('\n')
  const afterLines = after.split('\n')
  let firstDiff = 0

  while (
    firstDiff < beforeLines.length &&
    firstDiff < afterLines.length &&
    beforeLines[firstDiff] === afterLines[firstDiff]
  ) {
    firstDiff += 1
  }

  let beforeEnd = beforeLines.length - 1
  let afterEnd = afterLines.length - 1

  while (
    beforeEnd >= firstDiff &&
    afterEnd >= firstDiff &&
    beforeLines[beforeEnd] === afterLines[afterEnd]
  ) {
    beforeEnd -= 1
    afterEnd -= 1
  }

  const contextStart = Math.max(0, firstDiff - 2)
  const beforeContextEnd = Math.min(beforeLines.length, beforeEnd + 3)
  const afterContextEnd = Math.min(afterLines.length, afterEnd + 3)

  return {
    changed: true,
    pageName,
    source,
    relativeFilePath,
    firstDiffLine: firstDiff + 1,
    beforeExcerpt: formatExcerpt(beforeLines, contextStart, beforeContextEnd),
    afterExcerpt: formatExcerpt(afterLines, contextStart, afterContextEnd),
    beforeFullText: before,
    afterFullText: after,
    summary: `Detected file changes starting at line ${firstDiff + 1}.`,
  }
}

export const captureCurrentPageFileSnapshotV1 = async () => {
  const page = await resolveCurrentPageEntity()
  let pageName = ''
  let pageAliases: string[] = []
  let relativeFilePath: string | null = null
  let content: string | null = null
  let source: 'file' | 'page_tree' = 'file'
  const graph = await logseq.App.getCurrentGraph()
  let resolutionDiagnostics: PageFileResolutionDiagnostics | null = null

  try {
    const resolved = await resolveCurrentPageFile(page)
    pageName = resolved.pageName
    pageAliases = resolved.pageAliases
    relativeFilePath = resolved.relativeFilePath
    resolutionDiagnostics = resolved.diagnostics
    const resolvedContent = await readResolvedGraphFileContent(
      relativeFilePath,
      graph?.path ?? null,
      resolutionDiagnostics,
    )
    content = resolvedContent.content
  } catch (error) {
    const resolved = await resolveCurrentPageTreeContent(page)
    pageName = resolved.pageName
    pageAliases = resolved.pageAliases
    relativeFilePath = resolved.relativeFilePath
    content = resolved.content
    source = 'page_tree'

    console.warn('[Readwise Sync] falling back to page_tree snapshot', {
      pageName: resolved.pageName,
      reason: error instanceof Error ? error.message : String(error),
      diagnostics:
        error instanceof PageFileResolutionError
          ? error.diagnostics
          : resolutionDiagnostics,
    })
    console.info(
      '[Readwise Sync] page_tree fallback diagnostics json',
      safeJsonStringify({
        pageName: resolved.pageName,
        reason: error instanceof Error ? error.message : String(error),
        diagnostics:
          error instanceof PageFileResolutionError
            ? error.diagnostics
            : resolutionDiagnostics,
      }),
    )
  }

  if (typeof content !== 'string') {
    throw new Error(
      relativeFilePath
        ? `Failed to read file content from "${relativeFilePath}".`
        : `Failed to read page tree content for "${pageName}".`,
    )
  }

  const snapshot: CurrentPageSnapshotV1 = {
    schemaVersion: 1,
    capturedAt: new Date().toISOString(),
    graphName: graph?.name ?? null,
    graphPath: graph?.path ?? null,
    pageName,
    pageAliases,
    source,
    relativeFilePath,
    content,
  }

  await logseq.FileStorage.setItem(
    CURRENT_PAGE_SNAPSHOT_KEY,
    JSON.stringify(snapshot, null, 2),
  )

  console.info('[Readwise Sync] captured current page file snapshot', {
    pageName,
    source,
    relativeFilePath,
    lineCount: content.split('\n').length,
  })

  return {
    pageName,
    source,
    relativeFilePath,
    lineCount: content.split('\n').length,
  }
}

export const diffCurrentPageFileSnapshotV1 = async (): Promise<CurrentPageDiffResult> => {
  const rawSnapshot = await logseq.FileStorage.getItem(CURRENT_PAGE_SNAPSHOT_KEY)
  if (typeof rawSnapshot !== 'string') {
    throw new Error('No current page snapshot was found. Capture one first.')
  }

  const snapshot = JSON.parse(rawSnapshot) as CurrentPageSnapshotV1
  if (snapshot.schemaVersion !== 1) {
    throw new Error('Unsupported current page snapshot version.')
  }

  const currentContent =
    snapshot.source === 'file'
      ? (
          await readResolvedGraphFileContent(
            snapshot.relativeFilePath ?? '',
            snapshot.graphPath,
            null,
          )
        ).content
      : (await resolveCurrentPageTreeContent(await resolveCurrentPageEntity())).content

  if (typeof currentContent !== 'string') {
    throw new Error(
      snapshot.relativeFilePath
        ? `Failed to read file content from "${snapshot.relativeFilePath}".`
        : `Failed to read page tree content for "${snapshot.pageName}".`,
    )
  }

  const diff = diffTexts(
    snapshot.content,
    currentContent,
    snapshot.pageName,
    snapshot.source,
    snapshot.relativeFilePath,
  )

  console.info('[Readwise Sync] current page file diff', diff)
  return diff
}
