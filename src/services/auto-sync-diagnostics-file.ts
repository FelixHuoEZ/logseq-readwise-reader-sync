export interface ReadwiseAutoSyncDiagnosticRecordV1 {
  schemaVersion: 1
  timestamp: string
  message: string
  payload: unknown
}

const padTwoDigits = (value: number) => String(value).padStart(2, '0')

const buildLocalMonthKey = (date: Date) =>
  `${date.getFullYear()}-${padTwoDigits(date.getMonth() + 1)}`

const autoSyncDiagnosticFlushDelayMs = 10_000
const autoSyncDiagnosticMaxBufferedLines = 1_000

const sanitizeForJson = (value: unknown): unknown => {
  if (value instanceof Error) {
    return {
      name: value.name,
      message: value.message,
      stack: value.stack,
    }
  }

  if (typeof value === 'undefined') return null

  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return value
  }

  if (Array.isArray(value)) {
    return value.map((item) => sanitizeForJson(item))
  }

  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
        key,
        sanitizeForJson(entry),
      ]),
    )
  }

  return String(value)
}

let pendingAutoSyncDiagnosticWrite: Promise<void> = Promise.resolve()
let bufferedAutoSyncDiagnosticLines = new Map<string, string[]>()
let bufferedAutoSyncDiagnosticLineCount = 0
let autoSyncDiagnosticFlushTimer: number | null = null
let lifecycleFlushListenersRegistered = false

export const buildReadwiseAutoSyncDiagnosticLogFileName = (date = new Date()) =>
  `auto-sync-diagnostics-${buildLocalMonthKey(date)}.jsonl`

export const buildReadwiseAutoSyncDiagnosticStorageKey = (date = new Date()) =>
  `auto-sync-diagnostics/${buildReadwiseAutoSyncDiagnosticLogFileName(date)}`

const hasBufferedAutoSyncDiagnosticLines = () =>
  bufferedAutoSyncDiagnosticLineCount > 0

const clearScheduledAutoSyncDiagnosticFlush = () => {
  if (autoSyncDiagnosticFlushTimer == null) return
  window.clearTimeout(autoSyncDiagnosticFlushTimer)
  autoSyncDiagnosticFlushTimer = null
}

const scheduleAutoSyncDiagnosticFlush = () => {
  if (autoSyncDiagnosticFlushTimer != null) return

  autoSyncDiagnosticFlushTimer = window.setTimeout(() => {
    autoSyncDiagnosticFlushTimer = null
    if (hasBufferedAutoSyncDiagnosticLines()) {
      void flushReadwiseAutoSyncDiagnosticLogBuffer()
    }
  }, autoSyncDiagnosticFlushDelayMs)
}

const registerAutoSyncDiagnosticLifecycleFlush = () => {
  if (lifecycleFlushListenersRegistered) return
  lifecycleFlushListenersRegistered = true

  window.addEventListener('pagehide', () => {
    if (hasBufferedAutoSyncDiagnosticLines()) {
      void flushReadwiseAutoSyncDiagnosticLogBuffer()
    }
  })
  window.addEventListener('beforeunload', () => {
    if (hasBufferedAutoSyncDiagnosticLines()) {
      void flushReadwiseAutoSyncDiagnosticLogBuffer()
    }
  })
  document.addEventListener('visibilitychange', () => {
    if (
      document.visibilityState === 'hidden' &&
      hasBufferedAutoSyncDiagnosticLines()
    ) {
      void flushReadwiseAutoSyncDiagnosticLogBuffer()
    }
  })
}

export const flushReadwiseAutoSyncDiagnosticLogBuffer = () => {
  clearScheduledAutoSyncDiagnosticFlush()

  if (!hasBufferedAutoSyncDiagnosticLines()) {
    return pendingAutoSyncDiagnosticWrite
  }

  const linesByStorageKey = bufferedAutoSyncDiagnosticLines
  bufferedAutoSyncDiagnosticLines = new Map()
  bufferedAutoSyncDiagnosticLineCount = 0

  pendingAutoSyncDiagnosticWrite = pendingAutoSyncDiagnosticWrite
    .catch(() => undefined)
    .then(async () => {
      for (const [storageKey, lines] of linesByStorageKey) {
        let existing = ''

        try {
          const raw = await logseq.FileStorage.getItem(storageKey)
          existing = typeof raw === 'string' ? raw : ''
        } catch {
          existing = ''
        }

        await logseq.FileStorage.setItem(
          storageKey,
          `${existing}${lines.join('')}`,
        )
      }
    })
    .catch((error: unknown) => {
      console.warn(
        '[Readwise Auto Sync Debug] failed to flush persistent diagnostic log buffer',
        error,
      )
    })

  return pendingAutoSyncDiagnosticWrite
}

export const appendReadwiseAutoSyncDiagnosticLogEntry = (
  record: ReadwiseAutoSyncDiagnosticRecordV1,
) => {
  registerAutoSyncDiagnosticLifecycleFlush()

  const storageKey = buildReadwiseAutoSyncDiagnosticStorageKey(
    new Date(record.timestamp),
  )
  const line = `${JSON.stringify({
    ...record,
    payload: sanitizeForJson(record.payload),
  })}\n`
  const existingLines = bufferedAutoSyncDiagnosticLines.get(storageKey)
  if (existingLines == null) {
    bufferedAutoSyncDiagnosticLines.set(storageKey, [line])
  } else {
    existingLines.push(line)
  }
  bufferedAutoSyncDiagnosticLineCount += 1

  if (
    bufferedAutoSyncDiagnosticLineCount >= autoSyncDiagnosticMaxBufferedLines
  ) {
    return flushReadwiseAutoSyncDiagnosticLogBuffer()
  }

  scheduleAutoSyncDiagnosticFlush()
  return Promise.resolve()
}
