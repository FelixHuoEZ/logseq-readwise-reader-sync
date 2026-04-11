const MAX_FILE_NAME_BYTES = 255

const utf8ByteLength = (value: string) => new TextEncoder().encode(value).length

export const sanitizePageTitleSegment = (value: string): string =>
  value.replaceAll('\\', '＼').replaceAll('/', '／').trim()

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
