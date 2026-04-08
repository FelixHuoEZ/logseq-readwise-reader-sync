import type { UserConfigV1 } from './types'

const DEFAULT_SYNC_INTERVAL_MINUTES = 15

const toPositiveInteger = (value: unknown, fallback: number) => {
  if (typeof value === 'number' && Number.isFinite(value) && value > 0) {
    return Math.floor(value)
  }

  if (typeof value === 'string') {
    const parsed = Number.parseInt(value, 10)
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed
    }
  }

  return fallback
}

export const readUserConfig = (): UserConfigV1 => {
  const settings = (logseq.settings ?? {}) as Record<string, unknown>
  const rawToken = settings.apiToken
  const rawLastSyncTimestamp = settings.lastSyncTimestamp

  return {
    apiToken: typeof rawToken === 'string' ? rawToken.trim() : '',
    autoSyncEnabled: settings.autoSyncEnabled === true,
    syncIntervalMinutes: toPositiveInteger(
      settings.syncIntervalMinutes,
      DEFAULT_SYNC_INTERVAL_MINUTES,
    ),
    legacyLastSyncTimestamp:
      typeof rawLastSyncTimestamp === 'string' &&
      rawLastSyncTimestamp.trim().length > 0
        ? rawLastSyncTimestamp.trim()
        : null,
  }
}
