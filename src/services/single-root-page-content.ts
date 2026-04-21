import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'
import { logReadwiseDebug } from '../logging'
import { resolveManagedPageFilePathV1 } from './managed-page-integrity'
import { assertManagedPageFileNameWithinLimits } from './readwise-page-names'

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

const INSERTED_ROOT_POST_UPDATE_DELAY_MS = 500
const FORCE_REPARSE_RESTORE_DELAY_MS = 450

const removeDirectChildren = async (block: BlockEntity) => {
  for (const child of block.children ?? []) {
    const childUuid = Array.isArray(child) ? child[1] : child.uuid
    await logseq.Editor.removeBlock(childUuid)
  }
}

const stabilizeInsertedRootBlock = async (
  page: PageEntity,
  pageName: string,
  content: string,
  logPrefix = '[Readwise Sync]',
) => {
  // Best-effort rewrite only. Raw file diffs still show Logseq may later insert
  // one blank line after `#+END_NOTE` during its own canonical serialization.
  await delay(300)

  const refreshedTree = await logseq.Editor.getPageBlocksTree(page.name)
  const refreshedRoot = refreshedTree?.[0]

  if (!refreshedRoot) {
    throw new Error(`Failed to resolve inserted root block for "${pageName}"`)
  }

  await logseq.Editor.updateBlock(refreshedRoot.uuid, content)
  await delay(INSERTED_ROOT_POST_UPDATE_DELAY_MS)

  logReadwiseDebug(logPrefix, 'stabilized inserted root block', {
    pageName,
    rootBlockUuid: refreshedRoot.uuid,
    topLevelBlockCountAfterInsert: refreshedTree?.length ?? null,
    postUpdateDelayMs: INSERTED_ROOT_POST_UPDATE_DELAY_MS,
    strategy: 'best-effort-post-insert-update',
  })
}

const detectManagedPageFileFormatV1 = (
  relativeFilePath: string,
): 'org' | 'markdown' =>
  relativeFilePath.toLowerCase().endsWith('.md') ? 'markdown' : 'org'

const buildTemporaryTouchedManagedPageContentV1 = (
  content: string,
  format: 'org' | 'markdown',
) => {
  const stamp = new Date().toISOString()
  const marker =
    format === 'markdown'
      ? `\n\n<!-- readwise-managed-page-reparse ${stamp} -->\n`
      : `\n# readwise-managed-page-reparse ${stamp}\n`

  return `${content}${marker}`
}

const forceReparseManagedPageFileContentV1 = async ({
  relativeFilePath,
  content,
  format,
}: {
  relativeFilePath: string
  content: string
  format: 'org' | 'markdown'
}) => {
  const temporaryContent = buildTemporaryTouchedManagedPageContentV1(
    content,
    format,
  )
  let restoreError: unknown = null

  try {
    await logseq.DB.setFileContent(relativeFilePath, temporaryContent)
    await delay(FORCE_REPARSE_RESTORE_DELAY_MS)
  } finally {
    try {
      await logseq.DB.setFileContent(relativeFilePath, content)
      await delay(FORCE_REPARSE_RESTORE_DELAY_MS)
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

export const createManagedPageV1 = async (
  pageName: string,
  logPrefix = '[Readwise Sync]',
): Promise<PageEntity> => {
  assertManagedPageFileNameWithinLimits(pageName, 'org')

  const created = await logseq.Editor.createPage(
    pageName,
    {},
    {
      createFirstBlock: false,
      redirect: false,
      format: 'org',
    },
  )

  if (!created) {
    throw new Error(`Failed to create managed page "${pageName}"`)
  }

  await delay(500)
  return created
}

export const writeSingleRootPageContentV1 = async (
  page: PageEntity,
  pageName: string,
  content: string,
  logPrefix = '[Readwise Sync]',
): Promise<'created' | 'updated' | 'unchanged'> => {
  const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)

  logReadwiseDebug(logPrefix, 'rendered page state', {
    pageName,
    topLevelBlockCount: pageBlocksTree?.length ?? null,
    firstExistingBlockContent: pageBlocksTree?.[0]?.content ?? null,
  })

  if (pageBlocksTree == null) {
    throw new Error(`Failed to inspect page blocks for "${pageName}"`)
  }

  if (pageBlocksTree.length === 0) {
    const inserted = await logseq.Editor.insertBlock(page.uuid, content, {
      before: false,
      isPageBlock: true,
    } as never)

    if (!inserted) {
      throw new Error(`Failed to insert rendered content for "${pageName}"`)
    }

    logReadwiseDebug(logPrefix, 'rendered page branch', {
      pageName,
      branch: 'insert-first-block',
    })

    await stabilizeInsertedRootBlock(page, pageName, content, logPrefix)
    return 'created'
  }

  const rootBlock = pageBlocksTree[0]
  if (!rootBlock) {
    throw new Error(`Failed to resolve root block for "${pageName}"`)
  }

  const extraTopLevelBlocks = pageBlocksTree.slice(1)
  const hasDirectChildren = (rootBlock.children?.length ?? 0) > 0
  const needsContentUpdate = rootBlock.content !== content

  if (!needsContentUpdate && !hasDirectChildren && extraTopLevelBlocks.length === 0) {
    logReadwiseDebug(logPrefix, 'rendered page branch', {
      pageName,
      branch: 'unchanged',
    })
    return 'unchanged'
  }

  logReadwiseDebug(logPrefix, 'rendered page branch', {
    pageName,
    branch:
      extraTopLevelBlocks.length > 0 || hasDirectChildren
        ? 'rewrite-existing-tree'
        : 'update-root-block',
    extraTopLevelBlocks: extraTopLevelBlocks.length,
    directChildren: rootBlock.children?.length ?? 0,
  })

  for (const extraBlock of extraTopLevelBlocks) {
    await logseq.Editor.removeBlock(extraBlock.uuid)
  }

  if (hasDirectChildren) {
    await removeDirectChildren(rootBlock)
  }

  if (needsContentUpdate) {
    await logseq.Editor.updateBlock(rootBlock.uuid, content)
  }

  return 'updated'
}

export const ensureManagedPageFileContentV1 = async (
  page: PageEntity,
  pageName: string,
  content: string,
  logPrefix = '[Readwise Sync]',
  options?: {
    forceReparseAfterExactRewrite?: boolean
  },
) => {
  const relativeFilePath = await resolveManagedPageFilePathV1(page)
  if (!relativeFilePath) return
  const shouldForceReparse = options?.forceReparseAfterExactRewrite === true

  await delay(300)

  let currentContent: string | null = null
  try {
    const nextContent = await logseq.DB.getFileContent(relativeFilePath)
    currentContent = typeof nextContent === 'string' ? nextContent : null
  } catch {
    currentContent = null
  }

  if (currentContent === content && !shouldForceReparse) {
    return
  }

  if (currentContent !== content) {
    logReadwiseDebug(logPrefix, 'forcing exact managed page file rewrite', {
      pageName,
      relativeFilePath,
      currentLength: currentContent?.length ?? null,
      expectedLength: content.length,
    })

    await logseq.DB.setFileContent(relativeFilePath, content)
    await delay(400)

    const confirmedContent = await logseq.DB.getFileContent(relativeFilePath)
    if (confirmedContent !== content) {
      throw new Error(
        `Failed to stabilize managed page file content for "${pageName}"`,
      )
    }
  }

  if (!shouldForceReparse) {
    return
  }

  logReadwiseDebug(logPrefix, 'forcing managed page file reparse after rewrite', {
    pageName,
    relativeFilePath,
  })

  await forceReparseManagedPageFileContentV1({
    relativeFilePath,
    content,
    format: detectManagedPageFileFormatV1(relativeFilePath),
  })
}

export const upsertSingleRootPageContentV1 = async (
  pageName: string,
  content: string,
  logPrefix = '[Readwise Sync]',
): Promise<'created' | 'updated' | 'unchanged'> => {
  const page =
    (await logseq.Editor.getPage(pageName)) ??
    (await createManagedPageV1(pageName, logPrefix))
  return writeSingleRootPageContentV1(page, pageName, content, logPrefix)
}
