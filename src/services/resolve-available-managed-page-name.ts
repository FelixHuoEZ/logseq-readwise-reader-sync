import type { PageEntity } from '@logseq/libs/dist/LSPlugin'

import {
  buildManagedPageFileStem,
  buildManagedPageNameAttemptV1,
  buildManagedPageNamePlanV1,
} from './readwise-page-names'

const MAX_PAGE_NAME_ATTEMPTS = 64

const collectPageAliases = (page: PageEntity): string[] =>
  [
    typeof page.originalName === 'string' ? page.originalName : '',
    typeof page.name === 'string' ? page.name : '',
    typeof page.title === 'string' ? page.title : '',
  ].filter(
    (value, index, values) =>
      value.length > 0 && values.indexOf(value) === index,
  )

const getPreferredPageName = (page: PageEntity): string | null =>
  collectPageAliases(page)[0] ?? null

const toPagePath = (page: PageEntity): string | null => {
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

const safeDecodeURIComponent = (value: string): string => {
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

const stripFileExtension = (value: string): string =>
  value.replace(/\.(md|markdown|org)$/i, '')

const getCurrentPageFileStem = (page: PageEntity): string | null => {
  const path = toPagePath(page)
  if (path?.startsWith('pages/')) {
    const fileName = path.split('/').pop() ?? path
    return stripFileExtension(safeDecodeURIComponent(fileName))
  }

  const currentPageName = getPreferredPageName(page)
  return currentPageName ? buildManagedPageFileStem(currentPageName) : null
}

export interface AvailableManagedPageNameResolutionV1 {
  pageName: string
  strategy: 'preferred' | 'disambiguated' | 'deduplicated'
  attempt: number
}

export const resolveAvailableManagedPageNameV1 = async ({
  pageTitle,
  namespacePrefix,
  managedId,
  format = 'org',
  currentPageUuid = null,
}: {
  pageTitle: string
  namespacePrefix?: string | null
  managedId: string | number
  format?: 'org' | 'md'
  currentPageUuid?: string | null
}): Promise<AvailableManagedPageNameResolutionV1> => {
  const pages = ((await logseq.Editor.getAllPages()) ?? []) as PageEntity[]
  const otherPages = pages.filter((page) => page.uuid !== currentPageUuid)
  const plan = buildManagedPageNamePlanV1({
    pageTitle,
    namespacePrefix,
    managedId,
    format,
  })
  const attemptedNames = new Set<string>()

  for (let attempt = 0; attempt < MAX_PAGE_NAME_ATTEMPTS; attempt += 1) {
    const candidateName = buildManagedPageNameAttemptV1({
      pageTitle,
      namespacePrefix,
      managedId,
      format,
      attempt,
    })

    if (attemptedNames.has(candidateName)) {
      continue
    }
    attemptedNames.add(candidateName)

    const candidateFileStem = buildManagedPageFileStem(candidateName)
    const exactNameConflict = otherPages.some((page) =>
      collectPageAliases(page).includes(candidateName),
    )
    if (exactNameConflict) {
      continue
    }

    const fileStemConflict = otherPages.some(
      (page) => getCurrentPageFileStem(page) === candidateFileStem,
    )
    if (fileStemConflict) {
      continue
    }

    return {
      pageName: candidateName,
      strategy:
        candidateName === plan.preferredPageName
          ? 'preferred'
          : candidateName === plan.disambiguatedPageName
            ? 'disambiguated'
            : 'deduplicated',
      attempt,
    }
  }

  throw new Error(
    `Unable to find a non-conflicting managed page name for "${pageTitle}" in namespace "${namespacePrefix ?? '<root>'}".`,
  )
}
