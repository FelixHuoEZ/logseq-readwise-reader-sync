const stripWrappingLinkSyntax = (value: string): string => {
  const trimmed = value.trim()

  const orgMatch = trimmed.match(/^\[\[([^[\]]+)\]\[[^[\]]+\]\]$/)
  if (orgMatch) return orgMatch[1] ?? trimmed

  const simpleBracketMatch = trimmed.match(/^\[\[([^[\]]+)\]\]$/)
  if (simpleBracketMatch) return simpleBracketMatch[1] ?? trimmed

  return trimmed
}

export const normalizeComparableUrlV1 = (
  value: string | null | undefined,
): string | null => {
  if (typeof value !== 'string') return null

  const stripped = stripWrappingLinkSyntax(value).trim()
  if (stripped.length === 0) return null

  return stripped.replace(/\/+$/, '')
}
