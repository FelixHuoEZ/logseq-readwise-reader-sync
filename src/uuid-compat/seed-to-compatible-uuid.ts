const HEX_CHARS = '0123456789abcdef'
const BASE36_CHARS = '0123456789abcdefghijklmnopqrstuvwxyz'
const UUID_HEX_LENGTH = 32

const toCompatibleHexChar = (value: number): string =>
  HEX_CHARS[value % HEX_CHARS.length] ?? '0'

const formatUuid = (uuidHex: string): string => {
  const normalized = uuidHex.slice(0, UUID_HEX_LENGTH)

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
