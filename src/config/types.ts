export interface UserConfigV1 {
  apiToken: string
  autoSyncEnabled: boolean
  autoSyncFileDiagnosticsEnabled: boolean
  syncIntervalMinutes: number
  legacyLastSyncTimestamp: string | null
}
