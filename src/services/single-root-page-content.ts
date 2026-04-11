import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'

const delay = async (ms: number) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })

const INSERTED_ROOT_POST_UPDATE_DELAY_MS = 500

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

  console.info('[Readwise Sync] stabilized inserted root block', {
    pageName,
    rootBlockUuid: refreshedRoot.uuid,
    topLevelBlockCountAfterInsert: refreshedTree?.length ?? null,
    postUpdateDelayMs: INSERTED_ROOT_POST_UPDATE_DELAY_MS,
    strategy: 'best-effort-post-insert-update',
  })
}

export const createManagedPageV1 = async (
  pageName: string,
): Promise<PageEntity> => {
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
): Promise<'created' | 'updated' | 'unchanged'> => {
  const pageBlocksTree = await logseq.Editor.getPageBlocksTree(page.name)

  console.info('[Readwise Sync] rendered page state', {
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

    console.info('[Readwise Sync] rendered page branch', {
      pageName,
      branch: 'insert-first-block',
    })

    await stabilizeInsertedRootBlock(page, pageName, content)
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
    console.info('[Readwise Sync] rendered page branch', {
      pageName,
      branch: 'unchanged',
    })
    return 'unchanged'
  }

  console.info('[Readwise Sync] rendered page branch', {
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

export const upsertSingleRootPageContentV1 = async (
  pageName: string,
  content: string,
): Promise<'created' | 'updated' | 'unchanged'> => {
  const page = (await logseq.Editor.getPage(pageName)) ?? (await createManagedPageV1(pageName))
  return writeSingleRootPageContentV1(page, pageName, content)
}
