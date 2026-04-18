import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

import {
  collectManagedPageAliasesV1,
  resolveManagedPageFilePathV1,
} from './managed-page-integrity'
import { loadCurrentPageFileContentV1 } from './page-file-diff'

const FORCE_REPARSE_RESTORE_DELAY_MS = 120

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

const inferPageFormat = (
  relativeFilePath: string,
  pageFormat: string | null | undefined,
): 'org' | 'markdown' => {
  if (relativeFilePath.toLowerCase().endsWith('.md')) return 'markdown'
  return pageFormat === 'markdown' ? 'markdown' : 'org'
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

export const forceReparseCurrentPageV1 = async () => {
  const currentPage = await loadCurrentPageFileContentV1()
  const format = inferPageFormat(
    currentPage.relativeFilePath,
    currentPage.page.format,
  )

  await forceReparseFileContentV1({
    relativeFilePath: currentPage.relativeFilePath,
    content: currentPage.content,
    format,
  })

  return {
    pageName: currentPage.pageName,
    relativeFilePath: currentPage.relativeFilePath,
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
