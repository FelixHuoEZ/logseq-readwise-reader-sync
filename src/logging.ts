export type ReadwiseLogLevel = 'error' | 'warn' | 'info' | 'debug'

const LOG_LEVEL_ORDER: Record<ReadwiseLogLevel, number> = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3,
}

const normalizeLogLevel = (value: unknown): ReadwiseLogLevel => {
  if (typeof value !== 'string') return 'warn'

  const normalized = value.trim().toLowerCase()
  if (
    normalized === 'error' ||
    normalized === 'warn' ||
    normalized === 'info' ||
    normalized === 'debug'
  ) {
    return normalized
  }

  return 'warn'
}

export const getConfiguredReadwiseLogLevel = (): ReadwiseLogLevel =>
  normalizeLogLevel(logseq.settings?.logLevel)

export const shouldLogReadwiseLevel = (level: ReadwiseLogLevel) =>
  LOG_LEVEL_ORDER[level] <= LOG_LEVEL_ORDER[getConfiguredReadwiseLogLevel()]

const emit = (
  method: 'error' | 'warn' | 'info' | 'debug',
  line: string,
  payload?: unknown,
) => {
  if (payload === undefined) {
    console[method](line)
    return
  }

  console[method](line, payload)
}

const extractErrorTextParts = (value: unknown): string[] => {
  if (typeof value === 'string') {
    const trimmed = value.trim()
    return trimmed.length > 0 ? [trimmed] : []
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return [String(value)]
  }

  if (Array.isArray(value)) {
    return value.flatMap((item) => extractErrorTextParts(item))
  }

  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>
    return [
      ...extractErrorTextParts(record.message),
      ...extractErrorTextParts(record.reason),
      ...extractErrorTextParts(record.error),
      ...extractErrorTextParts(record.name),
      ...extractErrorTextParts(record.code),
    ]
  }

  return []
}

export const describeUnknownError = (error: unknown): string => {
  if (error instanceof Error) {
    const message = error.message.trim()
    return message.length > 0 ? message : error.name
  }

  const extracted = extractErrorTextParts(error)
  const uniqueExtracted = extracted.filter(
    (value, index, values) => values.indexOf(value) === index,
  )

  if (uniqueExtracted.length > 0) {
    return uniqueExtracted.join(' | ')
  }

  if (error && typeof error === 'object') {
    try {
      const serialized = JSON.stringify(error)
      if (serialized && serialized !== '{}') {
        return serialized
      }
    } catch {
      return 'Unknown error object'
    }

    return 'Unknown error object'
  }

  return String(error)
}

export const logReadwiseError = (
  prefix: string,
  message: string,
  payload?: unknown,
) => {
  if (!shouldLogReadwiseLevel('error')) return
  emit('error', `${prefix} ${message}`, payload)
}

export const logReadwiseWarn = (
  prefix: string,
  message: string,
  payload?: unknown,
) => {
  if (!shouldLogReadwiseLevel('warn')) return
  emit('warn', `${prefix} ${message}`, payload)
}

export const logReadwiseInfo = (
  prefix: string,
  message: string,
  payload?: unknown,
) => {
  if (!shouldLogReadwiseLevel('info')) return
  emit('info', `${prefix} ${message}`, payload)
}

export const logReadwiseDebug = (
  prefix: string,
  message: string,
  payload?: unknown,
) => {
  if (!shouldLogReadwiseLevel('debug')) return
  emit('info', `${prefix} ${message}`, payload)
}
