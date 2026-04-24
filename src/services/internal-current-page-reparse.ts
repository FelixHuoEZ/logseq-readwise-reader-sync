import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'

import { describeUnknownError, logReadwiseDebug } from '../logging'

const INTERNAL_REPARSE_LOG_PREFIX = '[Readwise Sync]'
const INTERNAL_REPARSE_SETTLE_DELAY_MS = 240

interface NodeFsPromises {
  readFile: (path: string, encoding: string) => Promise<string>
  stat: (path: string) => Promise<{ mtimeMs?: number; mtime?: Date }>
}

interface RootdirFileApi {
  read_rootdir_file?: (
    file: string,
    subRoot: string,
    rootDir: string,
  ) => Promise<unknown>
}

interface InternalReparseBridgeV1 {
  label: string
  invoke: (payload: InternalReparseBridgePayloadV1) => Promise<unknown>
}

interface InternalFunctionCandidateV1 {
  label: string
  path?: string[]
  expression?: string
}

interface ResolvedInternalFunctionV1 {
  label: string
  fn: (...args: unknown[]) => unknown
}

interface InternalReparseBridgePayloadV1 {
  repo: string
  graphPath: string
  relativeFilePath: string
  content: string
  mtime: Date | null
}

export interface InternalCurrentPageReparseProbeResultV1 {
  pageName: string
  relativeFilePath: string
  graphPath: string
  repo: string
  contentHash: string
  contentBytes: number
  beforeTreeHash: string
  afterTreeHash: string
  bridge: string | null
  changed: boolean
  diagnostics: string[]
}

interface CurrentPageFileResolutionV1 {
  page: PageEntity
  pageName: string
  pageAliases: string[]
  pageFilePath: string | null
  candidatePaths: string[]
}

interface CurrentPageDiskFileV1 {
  absolutePath: string
  relativeFilePath: string
  content: string
  mtime: Date | null
  readFrom:
    | 'node-fs'
    | 'host-internal-load-file'
    | 'host-internal-read-file'
    | 'db-get-file-content'
    | 'rootdir-api'
    | 'host-rootdir-api'
}

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

const trimTrailingSeparators = (value: string) => value.replace(/[\\/]+$/, '')

const joinAbsolutePath = (basePath: string, relativePath: string) =>
  `${trimTrailingSeparators(basePath)}/${relativePath}`

const normalizePath = (value: string) => value.replaceAll('\\', '/')

const buildLocalRepoId = (graphPath: string) =>
  `logseq_local_${normalizePath(graphPath)}`

const hashString = (value: string) => {
  let hash = 2166136261
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }

  return (hash >>> 0).toString(16).padStart(8, '0')
}

const getUtf8ByteLength = (value: string) => new Blob([value]).size

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value != null

const uniqueValues = (values: string[]) =>
  values.filter(
    (value, index, array) => value.length > 0 && array.indexOf(value) === index,
  )

const collectAliases = (
  page: Partial<PageEntity> | Partial<BlockEntity> | null,
) => {
  if (!page) return []

  return uniqueValues([
    typeof page.originalName === 'string' ? page.originalName : '',
    typeof page.name === 'string' ? page.name : '',
    typeof page.title === 'string' ? page.title : '',
    typeof page.fullTitle === 'string' ? page.fullTitle : '',
  ])
}

const buildPageFileStem = (pageName: string) => pageName.replaceAll('/', '___')

const encodeReservedPathCharacters = (value: string) =>
  value.replace(/[#:?*"<>|%]/g, (character) =>
    encodeURIComponent(character).toUpperCase(),
  )

const buildCandidateRelativeFilePaths = (
  pageName: string,
  expectedFormat: 'org' | 'markdown' | null,
) => {
  const fileStems = uniqueValues([
    buildPageFileStem(pageName),
    encodeReservedPathCharacters(buildPageFileStem(pageName)),
  ])
  const preferredExtensions =
    expectedFormat === 'markdown' ? ['md', 'org'] : ['org', 'md']

  return fileStems.flatMap((stem) =>
    preferredExtensions.map((extension) => `pages/${stem}.${extension}`),
  )
}

const buildCandidateRelativeFilePathsForAliases = (
  aliases: string[],
  expectedFormat: 'org' | 'markdown' | null,
) =>
  uniqueValues(
    aliases.flatMap((alias) =>
      buildCandidateRelativeFilePaths(alias, expectedFormat),
    ),
  )

const describeProbeValue = (value: unknown) => {
  if (typeof value === 'function') return 'function'
  if (Array.isArray(value)) return `array(${value.length})`
  if (isRecord(value))
    return `object(${Object.keys(value).slice(0, 8).join(',')})`
  if (value == null) return String(value)
  return typeof value
}

const getRuntimeRequire = () =>
  (window as unknown as { require?: ((id: string) => unknown) | undefined })
    .require ??
  (() => {
    try {
      return (
        window.top as unknown as {
          require?: ((id: string) => unknown) | undefined
        } | null
      )?.require
    } catch {
      return undefined
    }
  })()

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

  if ('name' in currentPage && typeof currentPage.name === 'string') {
    return currentPage as PageEntity
  }

  throw new Error('Failed to resolve the current page entity.')
}

const extractPageFilePath = (page: PageEntity): string | null => {
  const record = page as unknown as Record<string, unknown>
  const file = record.file

  if (isRecord(file) && typeof file.path === 'string' && file.path.length > 0) {
    return file.path
  }

  if (
    typeof record['file/path'] === 'string' &&
    record['file/path'].length > 0
  ) {
    return record['file/path']
  }

  return null
}

const resolveCurrentPageFileCandidates = async () => {
  const page = await resolveCurrentPageEntity()
  const aliases = collectAliases(page)
  const pageName = aliases[0]
  if (!pageName) {
    throw new Error('Current page does not have a stable page name.')
  }

  const pageFilePath = extractPageFilePath(page)
  const candidatePaths = uniqueValues([
    pageFilePath ?? '',
    ...buildCandidateRelativeFilePathsForAliases(aliases, page.format ?? null),
  ])

  return {
    page,
    pageName,
    pageAliases: aliases,
    pageFilePath,
    candidatePaths,
  } satisfies CurrentPageFileResolutionV1
}

const readDiskFileViaNode = async (
  graphPath: string,
  relativeFilePath: string,
) => {
  const runtimeRequire = getRuntimeRequire()
  if (typeof runtimeRequire !== 'function') {
    throw new Error('window.require is unavailable')
  }

  const fsPromises = runtimeRequire('node:fs/promises') as NodeFsPromises
  const absolutePath = joinAbsolutePath(graphPath, relativeFilePath)
  const [content, stat] = await Promise.all([
    fsPromises.readFile(absolutePath, 'utf8'),
    fsPromises.stat(absolutePath),
  ])

  return {
    absolutePath,
    content,
    mtime: stat.mtime instanceof Date ? stat.mtime : null,
    readFrom: 'node-fs' as const,
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

const getHostRootdirFileApi = (): RootdirFileApi | null => {
  try {
    const hostScope =
      (
        logseq.Experiments as unknown as {
          ensureHostScope?: () =>
            | {
                logseq?: {
                  api?: RootdirFileApi
                }
              }
            | undefined
        }
      ).ensureHostScope?.() ?? null

    return hostScope?.logseq?.api ?? null
  } catch {
    return null
  }
}

const resolveHostScopeWindow = (scope: unknown) =>
  isRecord(scope) && isRecord(scope.window) ? scope.window : null

const getReachableWindowScope = (name: 'parent' | 'top') => {
  try {
    const candidate = name === 'parent' ? window.parent : window.top
    return typeof candidate === 'object' && candidate !== window
      ? candidate
      : null
  } catch {
    return null
  }
}

const collectHostReadScopes = () => {
  const hostScope = readHostScope()

  return [
    ['hostScope', hostScope],
    ['hostScope.window', resolveHostScopeWindow(hostScope)],
    ['window.parent', getReachableWindowScope('parent')],
    ['window.top', getReachableWindowScope('top')],
  ] as const
}

const readDiskFileViaHostInternalApi = async (
  graphPath: string,
  repo: string,
  relativeFilePath: string,
) => {
  const failures: string[] = []
  let readerCount = 0

  for (const [scopeName, scope] of collectHostReadScopes()) {
    if (!scope) {
      failures.push(`${scopeName} unavailable`)
      continue
    }

    const loadFile =
      resolveFunctionPath(scope, [
        '$APP',
        '$frontend$handler$file$load_file$$',
      ]) ?? resolveFunctionPath(scope, ['$frontend$handler$file$load_file$$'])
    if (loadFile) {
      readerCount += 1
      try {
        const content = await loadFile(repo, relativeFilePath)
        if (typeof content === 'string') {
          return {
            absolutePath: joinAbsolutePath(graphPath, relativeFilePath),
            content,
            mtime: null,
            readFrom: 'host-internal-load-file' as const,
          }
        }

        failures.push(
          `${scopeName}.load_file returned ${describeProbeValue(content)}`,
        )
      } catch (error) {
        failures.push(
          `${scopeName}.load_file threw ${describeUnknownError(error)}`,
        )
      }
    }

    const readFile =
      resolveFunctionPath(scope, [
        '$APP',
        '$frontend$fs$read_file$cljs$0core$0IFn$0_invoke$0arity$02$$',
      ]) ??
      resolveFunctionPath(scope, [
        '$frontend$fs$read_file$cljs$0core$0IFn$0_invoke$0arity$02$$',
      ])
    if (readFile) {
      readerCount += 1
      try {
        const content = await readFile(graphPath, relativeFilePath)
        if (typeof content === 'string') {
          return {
            absolutePath: joinAbsolutePath(graphPath, relativeFilePath),
            content,
            mtime: null,
            readFrom: 'host-internal-read-file' as const,
          }
        }

        failures.push(
          `${scopeName}.read_file returned ${describeProbeValue(content)}`,
        )
      } catch (error) {
        failures.push(
          `${scopeName}.read_file threw ${describeUnknownError(error)}`,
        )
      }
    }
  }

  if (readerCount === 0) {
    throw new Error(
      `No host internal file reader was found. ${failures.join(' | ')}`,
    )
  }

  throw new Error(failures.join(' | '))
}

const readFileViaLogseqDb = async (relativeFilePath: string) => {
  const content = await logseq.DB.getFileContent(relativeFilePath)
  if (typeof content !== 'string') {
    throw new Error(`DB.getFileContent returned ${describeProbeValue(content)}`)
  }

  return {
    absolutePath: relativeFilePath,
    content,
    mtime: null,
    readFrom: 'db-get-file-content' as const,
  }
}

const readDiskFileViaRootdirApi = async (
  graphPath: string,
  relativeFilePath: string,
) => {
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
          absolutePath: joinAbsolutePath(graphPath, relativeFilePath),
          content,
          mtime: null,
          readFrom: 'rootdir-api' as const,
        }
      }

      failures.push(
        `_execCallableAPIAsync read_rootdir_file returned ${describeProbeValue(
          content,
        )}`,
      )
    } catch (error) {
      failures.push(
        `_execCallableAPIAsync read_rootdir_file threw ${describeUnknownError(
          error,
        )}`,
      )
    }
  } else {
    failures.push('_execCallableAPIAsync is unavailable')
  }

  const hostRootdirApi = getHostRootdirFileApi()
  if (typeof hostRootdirApi?.read_rootdir_file === 'function') {
    try {
      const content = await hostRootdirApi.read_rootdir_file(
        relativeFilePath,
        '',
        graphPath,
      )
      if (typeof content === 'string') {
        return {
          absolutePath: joinAbsolutePath(graphPath, relativeFilePath),
          content,
          mtime: null,
          readFrom: 'host-rootdir-api' as const,
        }
      }

      failures.push(
        `host read_rootdir_file returned ${describeProbeValue(content)}`,
      )
    } catch (error) {
      failures.push(
        `host read_rootdir_file threw ${describeUnknownError(error)}`,
      )
    }
  } else {
    failures.push('host read_rootdir_file is unavailable')
  }

  throw new Error(failures.join(' | '))
}

const readCurrentPageDiskFile = async (
  graphPath: string,
  repo: string,
  relativeFilePath: string,
) => {
  const failures: string[] = []

  try {
    return await readDiskFileViaNode(graphPath, relativeFilePath)
  } catch (error) {
    failures.push(`node: ${describeUnknownError(error)}`)
  }

  try {
    return await readDiskFileViaHostInternalApi(
      graphPath,
      repo,
      relativeFilePath,
    )
  } catch (error) {
    failures.push(`host-internal: ${describeUnknownError(error)}`)
  }

  try {
    return await readFileViaLogseqDb(relativeFilePath)
  } catch (error) {
    failures.push(`db: ${describeUnknownError(error)}`)
  }

  try {
    return await readDiskFileViaRootdirApi(graphPath, relativeFilePath)
  } catch (error) {
    failures.push(`rootdir: ${describeUnknownError(error)}`)
  }

  throw new Error(
    `Failed to read current page from disk. ${failures.join('; ')}`,
  )
}

const readFirstReadableCurrentPageDiskFile = async (
  graphPath: string,
  repo: string,
  resolution: CurrentPageFileResolutionV1,
  diagnostics: string[],
): Promise<CurrentPageDiskFileV1> => {
  const failures: string[] = []

  diagnostics.push(
    `aliases=${resolution.pageAliases.join(',')}`,
    `pageFilePath=${resolution.pageFilePath ?? 'null'}`,
    `candidates=${resolution.candidatePaths.join(',')}`,
  )

  for (const candidatePath of resolution.candidatePaths) {
    try {
      const diskFile = await readCurrentPageDiskFile(
        graphPath,
        repo,
        candidatePath,
      )
      diagnostics.push(
        `file=${candidatePath}`,
        `disk=${diskFile.absolutePath}`,
        `readFrom=${diskFile.readFrom}`,
      )
      return {
        ...diskFile,
        relativeFilePath: candidatePath,
      }
    } catch (error) {
      const message = describeUnknownError(error)
      failures.push(`${candidatePath}: ${message}`)
      diagnostics.push(`readFailed=${candidatePath}: ${message}`)
    }
  }

  throw new Error(
    `Failed to read the current page file. ${diagnostics.join(' | ')} | ${failures.join(' | ')}`,
  )
}

const normalizeBlockTreeForHash = (
  blocks: BlockEntity[] | null | undefined,
): unknown[] => {
  if (!Array.isArray(blocks)) return []

  return blocks.map((block) => ({
    uuid: typeof block.uuid === 'string' ? block.uuid : null,
    content: typeof block.content === 'string' ? block.content : '',
    properties:
      isRecord(block.properties) && Object.keys(block.properties).length > 0
        ? block.properties
        : null,
    children: normalizeBlockTreeForHash(
      (block.children ?? []).filter(
        (child): child is BlockEntity => !Array.isArray(child),
      ),
    ),
  }))
}

const hashCurrentPageTree = async (pageName: string) => {
  const blocks = await logseq.Editor.getPageBlocksTree(pageName)
  return hashString(JSON.stringify(normalizeBlockTreeForHash(blocks)))
}

const readHostScope = () => {
  try {
    return (
      logseq.Experiments as unknown as {
        ensureHostScope?: () => unknown
      }
    ).ensureHostScope?.()
  } catch {
    return null
  }
}

const resolveFunctionPath = (
  root: unknown,
  path: string[],
): ((...args: unknown[]) => unknown) | null => {
  let cursor = root

  for (const key of path) {
    if (!isRecord(cursor)) return null
    try {
      cursor = cursor[key]
    } catch {
      return null
    }
  }

  return typeof cursor === 'function'
    ? (cursor as (...args: unknown[]) => unknown)
    : null
}

const resolveFunctionExpression = (
  root: unknown,
  expression: string,
): ((...args: unknown[]) => unknown) | null => {
  if (!isRecord(root)) return null

  try {
    const evaluator = root.eval
    if (typeof evaluator !== 'function') return null
    const value = evaluator.call(root, expression)
    return typeof value === 'function'
      ? (value as (...args: unknown[]) => unknown)
      : null
  } catch {
    return null
  }
}

const resolveInternalFunctionCandidate = (
  scope: unknown,
  scopeName: string,
  candidates: InternalFunctionCandidateV1[],
): ResolvedInternalFunctionV1 | null => {
  for (const candidate of candidates) {
    const fn = candidate.path
      ? resolveFunctionPath(scope, candidate.path)
      : candidate.expression
        ? resolveFunctionExpression(scope, candidate.expression)
        : null

    if (fn) {
      return {
        label: `${scopeName}.${candidate.label}`,
        fn,
      }
    }
  }

  return null
}

const collectInternalReparseBridges = (diagnostics: string[]) => {
  const bridges: InternalReparseBridgeV1[] = []
  const hostScope = readHostScope()
  const hostScopeWindow = resolveHostScopeWindow(hostScope)
  const hostWindow = getReachableWindowScope('parent')
  const hostTop = getReachableWindowScope('top')

  const scopes = [
    ['hostScope', hostScope],
    ['hostScope.window', hostScopeWindow],
    ['window.parent', hostWindow],
    ['window.top', hostTop],
  ] as const

  for (const [scopeName, scope] of scopes) {
    if (!scope) {
      diagnostics.push(`${scopeName}: unavailable`)
      continue
    }

    if (isRecord(scope)) {
      try {
        diagnostics.push(
          `${scopeName}: keys=${Object.keys(scope).slice(0, 16).join(',')}`,
        )
      } catch (error) {
        diagnostics.push(
          `${scopeName}: keys unavailable (${describeUnknownError(error)})`,
        )
      }
    } else {
      diagnostics.push(`${scopeName}: ${describeProbeValue(scope)}`)
    }

    const directBridge = resolveFunctionPath(scope, [
      '__readwiseInternalReparseCurrentPageV1',
    ])
    if (directBridge) {
      bridges.push({
        label: `${scopeName}.__readwiseInternalReparseCurrentPageV1`,
        invoke: async (payload) => await directBridge(payload),
      })
    }

    const watcherBridge = resolveInternalFunctionCandidate(scope, scopeName, [
      {
        label: 'frontend.fs.watcher_handler.handle_add_and_change_BANG_',
        path: [
          'frontend',
          'fs',
          'watcher_handler',
          'handle_add_and_change_BANG_',
        ],
      },
      {
        label:
          '$APP.$frontend$fs$watcher_handler$handle_add_and_change_BANG_$$',
        path: [
          '$APP',
          '$frontend$fs$watcher_handler$handle_add_and_change_BANG_$$',
        ],
      },
      {
        label: '$frontend$fs$watcher_handler$handle_add_and_change_BANG_$$',
        path: ['$frontend$fs$watcher_handler$handle_add_and_change_BANG_$$'],
      },
      {
        label:
          'eval($frontend$fs$watcher_handler$handle_add_and_change_BANG_$$)',
        expression:
          '$frontend$fs$watcher_handler$handle_add_and_change_BANG_$$',
      },
      {
        label:
          'eval($APP.$frontend$fs$watcher_handler$handle_add_and_change_BANG_$$)',
        expression:
          '$APP.$frontend$fs$watcher_handler$handle_add_and_change_BANG_$$',
      },
    ])
    if (watcherBridge) {
      diagnostics.push(`${scopeName}: watcherBridge=${watcherBridge.label}`)
      bridges.push({
        label: watcherBridge.label,
        invoke: async (payload) =>
          await watcherBridge.fn(
            payload.repo,
            payload.relativeFilePath,
            payload.content,
            null,
            payload.mtime,
            false,
          ),
      })
    } else {
      diagnostics.push(`${scopeName}: watcherBridge=missing`)
    }

    const pubEventBridge = resolveInternalFunctionCandidate(scope, scopeName, [
      {
        label: 'frontend.state.pub_event_BANG_',
        path: ['frontend', 'state', 'pub_event_BANG_'],
      },
      {
        label: '$APP.$frontend$state$pub_event_BANG_$$',
        path: ['$APP', '$frontend$state$pub_event_BANG_$$'],
      },
      {
        label: '$frontend$state$pub_event_BANG_$$',
        path: ['$frontend$state$pub_event_BANG_$$'],
      },
      {
        label: 'eval($frontend$state$pub_event_BANG_$$)',
        expression: '$frontend$state$pub_event_BANG_$$',
      },
      {
        label: 'eval($APP.$frontend$state$pub_event_BANG_$$)',
        expression: '$APP.$frontend$state$pub_event_BANG_$$',
      },
    ])
    if (pubEventBridge) {
      diagnostics.push(
        `${scopeName}: pubEventBridge=${pubEventBridge.label} (skipped: file/alter may write)`,
      )
    }
  }

  if (bridges.length === 0) {
    diagnostics.push(
      'No callable internal reparse bridge was found. Normal iframe plugins cannot reach Logseq compiled private handlers in this build.',
    )
  }

  return bridges
}

export const experimentalInternalReparseCurrentPageV1 =
  async (): Promise<InternalCurrentPageReparseProbeResultV1> => {
    const editingBlock = await logseq.Editor.checkEditing()
    if (editingBlock) {
      throw new Error(
        'Internal reparse probe is unavailable while a block is being edited. Exit editing mode and try again.',
      )
    }

    const graph = await logseq.App.getCurrentGraph()
    const graphPath = typeof graph?.path === 'string' ? graph.path : ''
    if (!graphPath) {
      throw new Error('Current graph path is unavailable.')
    }

    const currentPage = await resolveCurrentPageFileCandidates()
    const repo = buildLocalRepoId(graphPath)
    const diagnostics = [
      `page=${currentPage.pageName}`,
      `graphPath=${graphPath}`,
      `repo=${repo}`,
    ]
    const diskFile = await readFirstReadableCurrentPageDiskFile(
      graphPath,
      repo,
      currentPage,
      diagnostics,
    )
    const beforeTreeHash = await hashCurrentPageTree(currentPage.pageName)
    const bridges = collectInternalReparseBridges(diagnostics)
    const bridge = bridges[0] ?? null

    if (!bridge) {
      throw new Error(
        `Internal reparse bridge is unavailable. ${diagnostics.join(' | ')}`,
      )
    }

    logReadwiseDebug(
      INTERNAL_REPARSE_LOG_PREFIX,
      'running experimental internal current-page reparse',
      {
        pageName: currentPage.pageName,
        relativeFilePath: diskFile.relativeFilePath,
        bridge: bridge.label,
        contentHash: hashString(diskFile.content),
        contentBytes: getUtf8ByteLength(diskFile.content),
      },
    )

    try {
      await bridge.invoke({
        repo,
        graphPath,
        relativeFilePath: diskFile.relativeFilePath,
        content: diskFile.content,
        mtime: diskFile.mtime,
      })
    } catch (error) {
      throw new Error(
        `Internal reparse bridge "${bridge.label}" failed: ${describeUnknownError(
          error,
        )}. ${diagnostics.join(' | ')}`,
      )
    }
    await delay(INTERNAL_REPARSE_SETTLE_DELAY_MS)

    const afterTreeHash = await hashCurrentPageTree(currentPage.pageName)

    return {
      pageName: currentPage.pageName,
      relativeFilePath: diskFile.relativeFilePath,
      graphPath,
      repo,
      contentHash: hashString(diskFile.content),
      contentBytes: getUtf8ByteLength(diskFile.content),
      beforeTreeHash,
      afterTreeHash,
      bridge: bridge.label,
      changed: beforeTreeHash !== afterTreeHash,
      diagnostics,
    }
  }
