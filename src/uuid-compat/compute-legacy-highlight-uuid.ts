import { extractUuidSeed } from './extract-uuid-seed'
import { seedToLegacyUuid } from './seed-to-legacy-uuid'

export const computeLegacyHighlightUuid = (
  highlightLocationUrl: string,
): string => seedToLegacyUuid(extractUuidSeed(highlightLocationUrl))
