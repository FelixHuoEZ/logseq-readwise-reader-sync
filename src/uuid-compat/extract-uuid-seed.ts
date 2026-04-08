export const extractUuidSeed = (highlightLocationUrl: string): string => {
  const segments = highlightLocationUrl.split('/')
  const lastSegment = segments[segments.length - 1] ?? ''
  return lastSegment.slice(2).toLowerCase()
}
