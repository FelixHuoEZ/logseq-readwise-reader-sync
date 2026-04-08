import { extractUuidSeed } from './extract-uuid-seed'
import { seedToCompatibleUuid } from './seed-to-compatible-uuid'

export const computeCompatibleHighlightUuid = (
  highlightLocationUrl: string,
): string => seedToCompatibleUuid(extractUuidSeed(highlightLocationUrl))
