export type ReadwiseCommandActionV1 =
  | 'rebuild-current-page-from-cache'
  | 'refresh-current-page-metadata'
  | 'cached-full-rebuild'

const READWISE_COMMAND_EVENT_V1 = 'readwise-reader-sync:command'

const isReadwiseCommandActionV1 = (
  value: unknown,
): value is ReadwiseCommandActionV1 =>
  value === 'rebuild-current-page-from-cache' ||
  value === 'refresh-current-page-metadata' ||
  value === 'cached-full-rebuild'

const extractReadwiseCommandActionV1 = (detail: unknown) => {
  if (isReadwiseCommandActionV1(detail)) {
    return detail
  }

  if (
    detail &&
    typeof detail === 'object' &&
    'action' in detail &&
    isReadwiseCommandActionV1(detail.action)
  ) {
    return detail.action
  }

  return null
}

export const listenForReadwiseCommandsV1 = (
  handler: (action: ReadwiseCommandActionV1) => void,
) => {
  const handleEvent = (event: Event) => {
    const action =
      event instanceof CustomEvent
        ? extractReadwiseCommandActionV1(event.detail)
        : null

    if (!action) return
    handler(action)
  }

  window.addEventListener(READWISE_COMMAND_EVENT_V1, handleEvent as EventListener)

  return () => {
    window.removeEventListener(
      READWISE_COMMAND_EVENT_V1,
      handleEvent as EventListener,
    )
  }
}
