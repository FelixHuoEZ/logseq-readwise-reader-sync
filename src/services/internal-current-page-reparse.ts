import type { BlockEntity } from '@logseq/libs/dist/LSPlugin'

import { describeUnknownError, logReadwiseDebug } from '../logging'
import { loadCurrentPageFileContentV1 } from './page-file-diff'

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
    .require

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
  relativeFilePath: string,
) => {
  try {
    return await readDiskFileViaNode(graphPath, relativeFilePath)
  } catch (nodeError) {
    try {
      return await readDiskFileViaRootdirApi(graphPath, relativeFilePath)
    } catch (rootdirError) {
      throw new Error(
        `Failed to read current page from disk. node: ${describeUnknownError(
          nodeError,
        )}; rootdir: ${describeUnknownError(rootdirError)}`,
      )
    }
  }
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
    cursor = cursor[key]
  }

  return typeof cursor === 'function'
    ? (cursor as (...args: unknown[]) => unknown)
    : null
}

const collectInternalReparseBridges = (diagnostics: string[]) => {
  const bridges: InternalReparseBridgeV1[] = []
  const hostScope = readHostScope()
  const hostWindow =
    typeof window.parent === 'object' && window.parent !== window
      ? window.parent
      : null
  const hostTop =
    typeof window.top === 'object' && window.top !== window ? window.top : null

  const scopes = [
    ['hostScope', hostScope],
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

    const pubEventBridge = resolveFunctionPath(scope, [
      'frontend',
      'state',
      'pub_event_BANG_',
    ])
    if (pubEventBridge) {
      bridges.push({
        label: `${scopeName}.frontend.state.pub_event_BANG_`,
        invoke: async (payload) =>
          await pubEventBridge([
            'file/alter',
            payload.repo,
            payload.relativeFilePath,
            payload.content,
          ]),
      })
    }

    const watcherBridge = resolveFunctionPath(scope, [
      'frontend',
      'fs',
      'watcher_handler',
      'handle_add_and_change_BANG_',
    ])
    if (watcherBridge) {
      bridges.push({
        label: `${scopeName}.frontend.fs.watcher_handler.handle_add_and_change_BANG_`,
        invoke: async (payload) =>
          await watcherBridge(
            payload.repo,
            payload.relativeFilePath,
            payload.content,
            null,
            payload.mtime,
            false,
          ),
      })
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

    const currentPage = await loadCurrentPageFileContentV1()
    const graph = await logseq.App.getCurrentGraph()
    const graphPath = typeof graph?.path === 'string' ? graph.path : ''
    if (!graphPath) {
      throw new Error('Current graph path is unavailable.')
    }

    const diskFile = await readCurrentPageDiskFile(
      graphPath,
      currentPage.relativeFilePath,
    )
    const repo = buildLocalRepoId(graphPath)
    const beforeTreeHash = await hashCurrentPageTree(currentPage.pageName)
    const diagnostics = [
      `page=${currentPage.pageName}`,
      `file=${currentPage.relativeFilePath}`,
      `disk=${diskFile.absolutePath}`,
      `readFrom=${diskFile.readFrom}`,
      `repo=${repo}`,
    ]
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
        relativeFilePath: currentPage.relativeFilePath,
        bridge: bridge.label,
        contentHash: hashString(diskFile.content),
        contentBytes: getUtf8ByteLength(diskFile.content),
      },
    )

    await bridge.invoke({
      repo,
      graphPath,
      relativeFilePath: currentPage.relativeFilePath,
      content: diskFile.content,
      mtime: diskFile.mtime,
    })
    await delay(INTERNAL_REPARSE_SETTLE_DELAY_MS)

    const afterTreeHash = await hashCurrentPageTree(currentPage.pageName)

    return {
      pageName: currentPage.pageName,
      relativeFilePath: currentPage.relativeFilePath,
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
