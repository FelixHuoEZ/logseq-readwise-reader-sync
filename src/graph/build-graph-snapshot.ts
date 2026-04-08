import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

import { loadCurrentGraphContextV1 } from './load-current-graph-context'
import type {
  GraphPageCandidateV1,
  GraphPageSnapshotSourceV1,
  GraphSnapshotV1,
} from './types'

const toPageTitle = (page: GraphPageSnapshotSourceV1): string =>
  page.originalName ?? page.title ?? page.name

const toPagePath = (page: GraphPageSnapshotSourceV1): string | null => {
  if (typeof page.path === 'string' && page.path.length > 0) {
    return page.path
  }

  if (
    page.file &&
    typeof page.file === 'object' &&
    'path' in page.file &&
    typeof page.file.path === 'string' &&
    page.file.path.length > 0
  ) {
    return page.file.path
  }

  return null
}

const toPageCandidate = (
  page: GraphPageSnapshotSourceV1,
): GraphPageCandidateV1 => ({
  pageUuid: page.uuid,
  pageTitle: toPageTitle(page),
  path: toPagePath(page),
})

export const buildGraphSnapshotV1 = (
  graphId: string,
  pages: GraphPageSnapshotSourceV1[],
): GraphSnapshotV1 => {
  const pageUuidExists: Record<string, boolean> = {}
  const pagesByExactTitle: Record<string, GraphPageCandidateV1[]> = {}

  for (const page of pages) {
    if (page.uuid.length === 0) continue

    pageUuidExists[page.uuid] = true

    const pageTitle = toPageTitle(page)
    if (pageTitle.length === 0) continue

    const candidate = toPageCandidate(page)
    const existingCandidates = pagesByExactTitle[pageTitle] ?? []
    existingCandidates.push(candidate)
    pagesByExactTitle[pageTitle] = existingCandidates
  }

  return {
    graphId,
    pageUuidExists,
    pagesByExactTitle,
  }
}

export const loadCurrentGraphSnapshotV1 = async (): Promise<GraphSnapshotV1> => {
  const [graphContext, pages] = await Promise.all([
    loadCurrentGraphContextV1(),
    logseq.Editor.getAllPages(),
  ])

  return buildGraphSnapshotV1(
    graphContext.graphId,
    (pages ?? []) as PageEntity[],
  )
}
