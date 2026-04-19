import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'

import {
  collectManagedPageAliasesV1,
  resolveManagedPageFilePathV1,
} from './managed-page-integrity'

const FORCE_REPARSE_RESTORE_DELAY_MS = 120
const SOFT_REOPEN_ROUTE_TIMEOUT_MS = 2000
const SOFT_REOPEN_ROUTE_POLL_INTERVAL_MS = 50
const SOFT_REOPEN_ROUTE_SETTLE_DELAY_MS = 80
const SOFT_REOPEN_CURRENT_PAGE_EDITING_MESSAGE =
  'Soft reopen current page is unavailable while a block is being edited. Exit editing mode and try again.'

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

export const getSoftReopenCurrentPageUnavailableReasonV1 = async () => {
  const editingBlock = await logseq.Editor.checkEditing()
  if (editingBlock) {
    await logseq.Editor.exitEditingMode(true)
    await delay(SOFT_REOPEN_ROUTE_SETTLE_DELAY_MS)
  }

  const remainingEditingBlock = await logseq.Editor.checkEditing()
  return remainingEditingBlock ? SOFT_REOPEN_CURRENT_PAGE_EDITING_MESSAGE : null
}

const uniqueStrings = (values: string[]) =>
  values.filter(
    (value, index, array) => value.length > 0 && array.indexOf(value) === index,
  )

const collectCurrentPageAliasesV1 = (
  page: Partial<PageEntity> | Partial<BlockEntity> | null,
) =>
  uniqueStrings([
    typeof page?.originalName === 'string' ? page.originalName : '',
    typeof page?.title === 'string' ? page.title : '',
    typeof page?.name === 'string' ? page.name : '',
  ])

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value != null

const readRecordString = (
  value: Record<string, unknown> | null | undefined,
  key: string,
) => (value && typeof value[key] === 'string' ? value[key] : null)

type SupportedCurrentPageRouteKeyV1 = 'page' | 'page-block'

interface CurrentRouteSnapshotV1 {
  path: string | null
  routeKey: SupportedCurrentPageRouteKeyV1
  pageName: string | null
  blockRouteName: string | null
  queryParams: Record<string, unknown> | null
}

const normalizeCurrentPageRouteKeyV1 = (
  value: string | null,
): SupportedCurrentPageRouteKeyV1 =>
  value === 'page-block' || value === ':page-block' ? 'page-block' : 'page'

const readCurrentRouteSnapshotV1 = async (): Promise<CurrentRouteSnapshotV1> => {
  const routeMatch = (await logseq.App.getStateFromStore('route-match')) as unknown
  const routeRecord = isRecord(routeMatch) ? routeMatch : null
  const data = isRecord(routeRecord?.data) ? routeRecord.data : null
  const parameters = isRecord(routeRecord?.parameters)
    ? routeRecord.parameters
    : null
  const parameterPath = isRecord(parameters?.path) ? parameters.path : null
  const legacyPathParams = isRecord(routeRecord?.['path-params'])
    ? routeRecord['path-params']
    : null
  const effectivePathParams = legacyPathParams ?? parameterPath
  const queryParams = isRecord(routeRecord?.['query-params'])
    ? routeRecord['query-params']
    : null

  return {
    path: readRecordString(routeRecord, 'path'),
    routeKey: normalizeCurrentPageRouteKeyV1(readRecordString(data, 'name')),
    pageName: readRecordString(effectivePathParams, 'name'),
    blockRouteName: readRecordString(effectivePathParams, 'block-route-name'),
    queryParams,
  }
}

const waitForCurrentRouteSnapshotV1 = async ({
  routeKey,
  pageName,
  blockRouteName,
}: {
  routeKey: SupportedCurrentPageRouteKeyV1
  pageName: string
  blockRouteName: string | null
}) => {
  const deadline = Date.now() + SOFT_REOPEN_ROUTE_TIMEOUT_MS

  while (Date.now() < deadline) {
    const currentRoute = await readCurrentRouteSnapshotV1()
    if (
      currentRoute.routeKey === routeKey &&
      currentRoute.pageName === pageName &&
      (routeKey !== 'page-block' ||
        currentRoute.blockRouteName === blockRouteName)
    ) {
      return currentRoute
    }

    await delay(SOFT_REOPEN_ROUTE_POLL_INTERVAL_MS)
  }

  throw new Error(
    `Timed out while waiting for the current route to reopen "${pageName}".`,
  )
}

const replaceCurrentPageRouteV1 = async ({
  routeKey,
  pageName,
  blockRouteName,
  queryParams,
}: {
  routeKey: SupportedCurrentPageRouteKeyV1
  pageName: string
  blockRouteName: string | null
  queryParams: Record<string, unknown> | null
}) => {
  const params =
    routeKey === 'page-block' && blockRouteName
      ? {
          name: pageName,
          'block-route-name': blockRouteName,
        }
      : {
          name: pageName,
        }

  logseq.App.replaceState(
    routeKey,
    params,
    (queryParams ?? undefined) as Record<string, unknown> | undefined,
  )

  await waitForCurrentRouteSnapshotV1({
    routeKey,
    pageName,
    blockRouteName: routeKey === 'page-block' ? blockRouteName : null,
  })
  await delay(SOFT_REOPEN_ROUTE_SETTLE_DELAY_MS)
}

const resolveCurrentPageEntityV1 = async (): Promise<PageEntity> => {
  const currentPage = await logseq.Editor.getCurrentPage()
  if (!currentPage) {
    throw new Error('No current page is open.')
  }

  if (logseq.Editor.isPageBlock(currentPage as PageEntity)) {
    return currentPage as PageEntity
  }

  const aliases = collectCurrentPageAliasesV1(currentPage)
  for (const alias of aliases) {
    const page = await logseq.Editor.getPage(alias)
    if (page) return page
  }

  throw new Error('Failed to resolve the current page entity.')
}

const inferPageFormat = (
  relativeFilePath: string,
  pageFormat: string | null | undefined,
): 'org' | 'markdown' => {
  const normalizedPath = relativeFilePath.toLowerCase()
  if (normalizedPath.endsWith('.md')) return 'markdown'
  if (normalizedPath.endsWith('.org')) return 'org'
  if (pageFormat === 'markdown') return 'markdown'
  if (pageFormat === 'org') return 'org'

  throw new Error(
    `Unsupported current page file type for force reparse: "${relativeFilePath}".`,
  )
}

const buildTemporaryTouchedContent = (
  content: string,
  format: 'org' | 'markdown',
) => {
  const stamp = new Date().toISOString()
  const marker =
    format === 'markdown'
      ? `\n\n<!-- readwise-force-reparse ${stamp} -->\n`
      : `\n# readwise-force-reparse ${stamp}\n`

  return `${content}${marker}`
}

const forceReparseFileContentV1 = async ({
  relativeFilePath,
  content,
  format,
}: {
  relativeFilePath: string
  content: string
  format: 'org' | 'markdown'
}) => {
  const temporaryContent = buildTemporaryTouchedContent(content, format)
  let restoreError: unknown = null

  try {
    await logseq.DB.setFileContent(relativeFilePath, temporaryContent)
    await delay(FORCE_REPARSE_RESTORE_DELAY_MS)
  } finally {
    try {
      await logseq.DB.setFileContent(relativeFilePath, content)
    } catch (error) {
      restoreError = error
    }
  }

  if (restoreError) {
    throw new Error(
      `Temporary touch succeeded, but restoring the original content failed for "${relativeFilePath}": ${
        restoreError instanceof Error
          ? restoreError.message
          : String(restoreError)
      }`,
    )
  }

  const restoredContent = await logseq.DB.getFileContent(relativeFilePath)
  if (restoredContent !== content) {
    throw new Error(
      `Original content was not restored cleanly for "${relativeFilePath}".`,
    )
  }
}

export const softReopenCurrentPageV1 = async () => {
  const unavailableReason = await getSoftReopenCurrentPageUnavailableReasonV1()
  if (unavailableReason) {
    throw new Error(unavailableReason)
  }

  const page = await resolveCurrentPageEntityV1()
  const currentRoute = await readCurrentRouteSnapshotV1()
  const routeKey: SupportedCurrentPageRouteKeyV1 =
    currentRoute.routeKey === 'page-block' &&
    typeof currentRoute.blockRouteName === 'string' &&
    currentRoute.blockRouteName.length > 0
      ? 'page-block'
      : 'page'
  const stableRouteName =
    currentRoute.pageName ??
    (typeof page.uuid === 'string' && page.uuid.length > 0 ? page.uuid : null) ??
    collectManagedPageAliasesV1(page)[0] ??
    null
  const routeNames = uniqueStrings([
    stableRouteName ?? '',
    typeof page.uuid === 'string' ? page.uuid : '',
    ...collectManagedPageAliasesV1(page),
  ])
  const alternateRouteName =
    routeNames.find((name) => name !== stableRouteName) ?? null

  if (!stableRouteName) {
    throw new Error('Current page does not expose a stable route identity.')
  }

  if (!alternateRouteName) {
    throw new Error(
      'Soft reopen current page requires an alternate route identity for the current page.',
    )
  }

  let restoredStableRoute = false

  try {
    await replaceCurrentPageRouteV1({
      routeKey,
      pageName: alternateRouteName,
      blockRouteName: currentRoute.blockRouteName,
      queryParams: null,
    })
    await replaceCurrentPageRouteV1({
      routeKey,
      pageName: stableRouteName,
      blockRouteName: currentRoute.blockRouteName,
      queryParams: currentRoute.queryParams,
    })
    restoredStableRoute = true
  } finally {
    if (!restoredStableRoute) {
      try {
        await replaceCurrentPageRouteV1({
          routeKey,
          pageName: stableRouteName,
          blockRouteName: currentRoute.blockRouteName,
          queryParams: currentRoute.queryParams,
        })
      } catch {
        // Best effort only. The user is still on the same page identity family.
      }
    }
  }

  const reopenedPage = await resolveCurrentPageEntityV1()
  if (
    typeof page.uuid === 'string' &&
    page.uuid.length > 0 &&
    typeof reopenedPage.uuid === 'string' &&
    reopenedPage.uuid.length > 0 &&
    reopenedPage.uuid !== page.uuid
  ) {
    throw new Error(
      `Soft reopen finished on a different page (${reopenedPage.uuid}) than the page that was reopened (${page.uuid}).`,
    )
  }

  return {
    pageName:
      collectManagedPageAliasesV1(reopenedPage)[0] ??
      stableRouteName,
    routeKey,
    routeName: stableRouteName,
  }
}

interface ManagedPageForceReparseFailureV1 {
  pageName: string
  relativeFilePath: string | null
  message: string
}

interface ManagedPageForceReparseProgressV1 {
  total: number
  completed: number
  pageName: string | null
}

export interface ManagedPageForceReparseResultV1 {
  matchedPages: number
  touchedPages: number
  failedPages: ManagedPageForceReparseFailureV1[]
}

const matchesNamespacePrefix = (page: PageEntity, namespacePrefix: string) =>
  collectManagedPageAliasesV1(page).some(
    (alias) =>
      alias === namespacePrefix || alias.startsWith(`${namespacePrefix}/`),
  )

export const forceReparseManagedPagesByNamespaceV1 = async (
  namespacePrefix: string,
  options: {
    onProgress?: (progress: ManagedPageForceReparseProgressV1) => void
  } = {},
): Promise<ManagedPageForceReparseResultV1> => {
  const allPages = ((await logseq.Editor.getAllPages()) ?? []) as PageEntity[]
  const matchedPages = allPages.filter((page) =>
    matchesNamespacePrefix(page, namespacePrefix),
  )
  const failedPages: ManagedPageForceReparseFailureV1[] = []
  let touchedPages = 0

  options.onProgress?.({
    total: matchedPages.length,
    completed: 0,
    pageName: null,
  })

  for (const [index, page] of matchedPages.entries()) {
    const aliases = collectManagedPageAliasesV1(page)
    const pageName =
      aliases[0] ?? page.originalName ?? page.name ?? page.title ?? ''

    try {
      const relativeFilePath = await resolveManagedPageFilePathV1(page)
      if (!relativeFilePath) {
        throw new Error('Failed to resolve the managed page file path.')
      }

      const content = await logseq.DB.getFileContent(relativeFilePath)
      if (typeof content !== 'string') {
        throw new Error('DB.getFileContent returned non-string content.')
      }

      const format = inferPageFormat(relativeFilePath, page.format ?? null)
      await forceReparseFileContentV1({
        relativeFilePath,
        content,
        format,
      })
      touchedPages += 1
    } catch (error) {
      failedPages.push({
        pageName,
        relativeFilePath:
          typeof page.file === 'object' &&
          page.file != null &&
          'path' in page.file &&
          typeof page.file.path === 'string'
            ? page.file.path
            : null,
        message: error instanceof Error ? error.message : String(error),
      })
    }

    options.onProgress?.({
      total: matchedPages.length,
      completed: index + 1,
      pageName,
    })
  }

  return {
    matchedPages: matchedPages.length,
    touchedPages,
    failedPages,
  }
}
