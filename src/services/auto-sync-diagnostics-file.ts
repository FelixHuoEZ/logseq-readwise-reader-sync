export interface ReadwiseAutoSyncDiagnosticRecordV1 {
  schemaVersion: 1
  timestamp: string
  message: string
  payload: unknown
}

const padTwoDigits = (value: number) => String(value).padStart(2, '0')

const buildLocalMonthKey = (date: Date) =>
  `${date.getFullYear()}-${padTwoDigits(date.getMonth() + 1)}`

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

export const buildReadwiseAutoSyncDiagnosticLogFileName = (date = new Date()) =>
  `auto-sync-diagnostics-${buildLocalMonthKey(date)}.jsonl`

export const buildReadwiseAutoSyncDiagnosticStorageKey = (date = new Date()) =>
  `auto-sync-diagnostics/${buildReadwiseAutoSyncDiagnosticLogFileName(date)}`

export const appendReadwiseAutoSyncDiagnosticLogEntry = (
  record: ReadwiseAutoSyncDiagnosticRecordV1,
) => {
  pendingAutoSyncDiagnosticWrite = pendingAutoSyncDiagnosticWrite
    .catch(() => undefined)
    .then(async () => {
      const storageKey = buildReadwiseAutoSyncDiagnosticStorageKey(
        new Date(record.timestamp),
      )
      const line = `${JSON.stringify({
        ...record,
        payload: sanitizeForJson(record.payload),
      })}\n`
      let existing = ''

      try {
        const raw = await logseq.FileStorage.getItem(storageKey)
        existing = typeof raw === 'string' ? raw : ''
      } catch {
        existing = ''
      }

      await logseq.FileStorage.setItem(storageKey, `${existing}${line}`)
    })

  return pendingAutoSyncDiagnosticWrite
}
