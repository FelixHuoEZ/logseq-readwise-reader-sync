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
