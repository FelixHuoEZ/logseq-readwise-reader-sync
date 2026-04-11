const HEX_CHARS = '0123456789abcdef'
const BASE36_CHARS = '0123456789abcdefghijklmnopqrstuvwxyz'
const RFC4122_VARIANT_CHARS = '89ab'
const UUID_HEX_LENGTH = 32
const VERSION_NIBBLE_INDEX = 12
const VARIANT_NIBBLE_INDEX = 16

const isRfc4122VersionChar = (value: string) => /^[1-8]$/i.test(value)

const normalizeUuidHex = (uuidHex: string) => {
  const normalized = uuidHex
    .slice(0, UUID_HEX_LENGTH)
    .padEnd(UUID_HEX_LENGTH, '0')
    .split('')

  const versionChar = normalized[VERSION_NIBBLE_INDEX] ?? '0'
  if (!isRfc4122VersionChar(versionChar)) {
    normalized[VERSION_NIBBLE_INDEX] = '4'
  }

  const variantChar = normalized[VARIANT_NIBBLE_INDEX] ?? '0'
  if (!RFC4122_VARIANT_CHARS.includes(variantChar)) {
    const parsed = Number.parseInt(variantChar, 16)
    normalized[VARIANT_NIBBLE_INDEX] =
      RFC4122_VARIANT_CHARS[
        Number.isNaN(parsed) ? 0 : parsed % RFC4122_VARIANT_CHARS.length
      ] ?? '8'
  }

  return normalized.join('')
}

const toCompatibleHexChar = (value: number): string =>
  HEX_CHARS[value % HEX_CHARS.length] ?? '0'

const formatUuid = (uuidHex: string): string => {
  const normalized = normalizeUuidHex(uuidHex)

  return [
    normalized.slice(0, 8),
    normalized.slice(8, 12),
    normalized.slice(12, 16),
    normalized.slice(16, 20),
    normalized.slice(20, 32),
  ].join('-')
}

export const seedToCompatibleUuid = (seed: string): string => {
  const replacedIndices: number[] = []
  const uuidHexParts: string[] = []

  for (const [index, rawChar] of Array.from(seed.toLowerCase()).entries()) {
    if (HEX_CHARS.includes(rawChar)) {
      uuidHexParts.push(rawChar)
      continue
    }

    const base36Index = BASE36_CHARS.indexOf(rawChar)

    if (base36Index >= 0) {
      uuidHexParts.push(toCompatibleHexChar(base36Index))
    } else {
      uuidHexParts.push('0')
    }

    replacedIndices.push(index)
  }

  const paddingLength = Math.max(UUID_HEX_LENGTH - uuidHexParts.length, 0)

  for (let index = 0; index < paddingLength; index += 1) {
    const replacedIndex = replacedIndices[index]
    uuidHexParts.push(
      replacedIndex != null ? toCompatibleHexChar(replacedIndex) : '0',
    )
  }

  return formatUuid(uuidHexParts.join(''))
}
