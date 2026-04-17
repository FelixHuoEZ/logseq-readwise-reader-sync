const MAX_FILE_NAME_BYTES = 255
const DEFAULT_PAGE_TITLE_SEGMENT = 'Untitled'
const MAX_INLINE_DISAMBIGUATOR_LENGTH = 12

const utf8ByteLength = (value: string) => new TextEncoder().encode(value).length

export const sanitizePageTitleSegment = (value: string): string =>
  value.replaceAll('\\', '＼').replaceAll('/', '／').trim()

const buildPageName = (
  pageTitleSegment: string,
  namespacePrefix: string | null | undefined,
) =>
  namespacePrefix ? `${namespacePrefix}/${pageTitleSegment}` : pageTitleSegment

const normalizePageTitleSegment = (value: string) => {
  const sanitized = sanitizePageTitleSegment(value)
  return sanitized.length > 0 ? sanitized : DEFAULT_PAGE_TITLE_SEGMENT
}

const normalizeDisambiguator = (value: string | number) =>
  String(value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')

const hashDisambiguator = (value: string) => {
  let hash = 0x811c9dc5

  for (const char of value) {
    hash ^= char.codePointAt(0) ?? 0
    hash = Math.imul(hash, 0x01000193) >>> 0
  }

  return hash.toString(36).padStart(7, '0').slice(-7)
}

const buildDisambiguationSuffix = (managedId: string | number) => {
  const normalized = normalizeDisambiguator(managedId)
  if (normalized.length === 0) return '~0000000'

  return normalized.length <= MAX_INLINE_DISAMBIGUATOR_LENGTH
    ? `~${normalized}`
    : `~${hashDisambiguator(normalized)}`
}

const fitsPageNameWithinFileNameLimit = (
  pageName: string,
  format: 'org' | 'md' = 'org',
) =>
  utf8ByteLength(`${buildManagedPageFileStem(pageName)}.${format}`) <=
  MAX_FILE_NAME_BYTES

const truncateUtf8ToFit = (
  value: string,
  predicate: (candidate: string) => boolean,
) => {
  const characters = Array.from(value)
  let low = 0
  let high = characters.length
  let best = ''

  while (low <= high) {
    const middle = Math.floor((low + high) / 2)
    const candidate = characters.slice(0, middle).join('').trimEnd()

    if (predicate(candidate)) {
      best = candidate
      low = middle + 1
    } else {
      high = middle - 1
    }
  }

  return best
}

const buildFittedPageTitleSegment = ({
  pageTitleSegment,
  namespacePrefix,
  suffix,
  format,
}: {
  pageTitleSegment: string
  namespacePrefix: string | null | undefined
  suffix: string
  format: 'org' | 'md'
}) => {
  const untruncatedPageName = buildPageName(
    `${pageTitleSegment}${suffix}`,
    namespacePrefix,
  )
  if (fitsPageNameWithinFileNameLimit(untruncatedPageName, format)) {
    return {
      pageTitleSegment,
      wasTruncated: false,
    }
  }

  const truncated = truncateUtf8ToFit(pageTitleSegment, (candidate) =>
    fitsPageNameWithinFileNameLimit(
      buildPageName(`${candidate}${suffix}`, namespacePrefix),
      format,
    ),
  )

  const fallbackCandidates = [
    truncated,
    truncateUtf8ToFit(DEFAULT_PAGE_TITLE_SEGMENT, (candidate) =>
      fitsPageNameWithinFileNameLimit(
        buildPageName(`${candidate}${suffix}`, namespacePrefix),
        format,
      ),
    ),
    '',
  ]

  for (const candidate of fallbackCandidates) {
    if (
      fitsPageNameWithinFileNameLimit(
        buildPageName(`${candidate}${suffix}`, namespacePrefix),
        format,
      )
    ) {
      return {
        pageTitleSegment: candidate,
        wasTruncated: true,
      }
    }
  }

  throw new Error(
    `Unable to build a managed page name within the file-name byte limit for namespace "${namespacePrefix ?? '<root>'}".`,
  )
}

export interface ManagedPageNamePlanV1 {
  preferredPageName: string
  disambiguatedPageName: string
  basePageName: string
  disambiguationSuffix: string
  wasTruncated: boolean
  usesDisambiguationInPreferredName: boolean
}

export const buildManagedPageNamePlanV1 = ({
  pageTitle,
  namespacePrefix,
  managedId,
  format = 'org',
}: {
  pageTitle: string
  namespacePrefix?: string | null
  managedId: string | number
  format?: 'org' | 'md'
}): ManagedPageNamePlanV1 => {
  const normalizedPageTitleSegment = normalizePageTitleSegment(pageTitle)
  const basePageName = buildPageName(
    normalizedPageTitleSegment,
    namespacePrefix,
  )
  const disambiguationSuffix = buildDisambiguationSuffix(managedId)
  const disambiguatedTitleSegment = buildFittedPageTitleSegment({
    pageTitleSegment: normalizedPageTitleSegment,
    namespacePrefix,
    suffix: disambiguationSuffix,
    format,
  })
  const disambiguatedPageName = buildPageName(
    `${disambiguatedTitleSegment.pageTitleSegment}${disambiguationSuffix}`,
    namespacePrefix,
  )

  if (fitsPageNameWithinFileNameLimit(basePageName, format)) {
    return {
      preferredPageName: basePageName,
      disambiguatedPageName,
      basePageName,
      disambiguationSuffix,
      wasTruncated: false,
      usesDisambiguationInPreferredName: false,
    }
  }

  return {
    preferredPageName: disambiguatedPageName,
    disambiguatedPageName,
    basePageName,
    disambiguationSuffix,
    wasTruncated: disambiguatedTitleSegment.wasTruncated,
    usesDisambiguationInPreferredName: true,
  }
}

export const buildManagedPageNameAttemptV1 = ({
  pageTitle,
  namespacePrefix,
  managedId,
  format = 'org',
  attempt = 0,
}: {
  pageTitle: string
  namespacePrefix?: string | null
  managedId: string | number
  format?: 'org' | 'md'
  attempt?: number
}): string => {
  const plan = buildManagedPageNamePlanV1({
    pageTitle,
    namespacePrefix,
    managedId,
    format,
  })

  if (attempt <= 0) {
    return plan.preferredPageName
  }

  if (attempt === 1) {
    return plan.disambiguatedPageName
  }

  const normalizedPageTitleSegment = normalizePageTitleSegment(pageTitle)
  const attemptSuffix = `${plan.disambiguationSuffix}${attempt}`
  const disambiguatedTitleSegment = buildFittedPageTitleSegment({
    pageTitleSegment: normalizedPageTitleSegment,
    namespacePrefix,
    suffix: attemptSuffix,
    format,
  })

  return buildPageName(
    `${disambiguatedTitleSegment.pageTitleSegment}${attemptSuffix}`,
    namespacePrefix,
  )
}

export const buildFormalManagedPageName = (
  bookTitle: string,
  namespacePrefix = 'ReadwiseHighlights',
): string => `${namespacePrefix}/${sanitizePageTitleSegment(bookTitle)}`

export const buildDebugManagedPageName = (
  bookTitle: string,
  userBookId: number,
  namespacePrefix: string,
  mode: 'flat' | 'namespace' = 'flat',
): string =>
  mode === 'namespace'
    ? `${namespacePrefix}/${sanitizePageTitleSegment(bookTitle)}`
    : `${namespacePrefix}-book-${userBookId}`

export const buildManagedPageFileStem = (pageName: string): string =>
  pageName.replaceAll('/', '___')

export const assertManagedPageFileNameWithinLimits = (
  pageName: string,
  format: 'org' | 'md' = 'org',
) => {
  const fileName = `${buildManagedPageFileStem(pageName)}.${format}`
  const fileNameBytes = utf8ByteLength(fileName)

  if (fileNameBytes <= MAX_FILE_NAME_BYTES) {
    return
  }

  throw new Error(
    `ENAMETOOLONG: name too long, open 'pages/${fileName}' (predicted before createPage, ${fileNameBytes} bytes)`,
  )
}
