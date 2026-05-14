import { useEffect, useRef } from 'react'

export type SSEEvent = {
  type: string
  service: string
  timestamp: string
  payload?: Record<string, unknown>
}

type UseEventSourceOptions = {
  /** Comma-separated service names to filter by, e.g. ['mail', 's3']. Empty = all. */
  topics: string[]
  /** Called whenever a matching event arrives. Keep stable or wrap in useCallback. */
  onEvent: (event: SSEEvent) => void
  /** Set to false to disable the SSE connection (e.g. service is disabled). */
  enabled?: boolean
}

const BACKOFF_DELAYS_MS = [1_000, 2_000, 4_000, 8_000, 16_000, 30_000]

// Known service names the server publishes under `event: <service>` lines.
// When the caller does not specify `topics`, the hook listens to all of them.
const KNOWN_SERVICES = [
  'mail',
  's3',
  'gcs',
  'redis',
  'dynamodb',
  'bigquery',
  'sqs',
  'pubsub',
  'redshift',
] as const

/**
 * Opens a persistent SSE connection to /api/events and calls onEvent for each
 * incoming message. Automatically reconnects with exponential backoff when the
 * connection drops. Cleaned up on unmount or when `enabled` becomes false.
 */
export function useEventSource({ topics, onEvent, enabled = true }: UseEventSourceOptions): void {
  // Keep a stable ref to the latest callback so the reconnect loop doesn't go
  // stale when the parent component re-renders.
  const onEventRef = useRef(onEvent)
  useEffect(() => {
    onEventRef.current = onEvent
  }, [onEvent])

  // Keep the topic list as a stable string for the dependency array.
  const topicsKey = topics.slice().sort().join(',')

  useEffect(() => {
    if (!enabled) {
      return
    }

    let destroyed = false
    let es: EventSource | null = null
    let retryIndex = 0
    let retryTimeout: ReturnType<typeof setTimeout> | null = null

    function connect(): void {
      if (destroyed) {
        return
      }
      const query = topicsKey ? `?topics=${encodeURIComponent(topicsKey)}` : ''
      es = new EventSource(`/api/events${query}`)

      es.addEventListener('ready', () => {
        // Connection established – reset backoff.
        retryIndex = 0
      })

      const handleServiceEvent = (raw: MessageEvent): void => {
        try {
          const event = JSON.parse(raw.data as string) as SSEEvent
          onEventRef.current(event)
        } catch {
          // Malformed JSON – ignore silently.
        }
      }

      // The server tags each event with `event: <service>`, so the default
      // `onmessage` (which only fires for unnamed events) would never see them.
      // Register a named listener for each requested topic, or for every known
      // service when the caller did not narrow the subscription.
      const services = topicsKey ? topicsKey.split(',') : KNOWN_SERVICES
      services.forEach((service) => {
        es!.addEventListener(service, handleServiceEvent)
      })

      es.onerror = () => {
        es?.close()
        es = null
        if (destroyed) {
          return
        }
        const delayMs = BACKOFF_DELAYS_MS[Math.min(retryIndex, BACKOFF_DELAYS_MS.length - 1)]
        retryIndex += 1
        retryTimeout = setTimeout(connect, delayMs)
      }
    }

    connect()

    return () => {
      destroyed = true
      if (retryTimeout !== null) {
        clearTimeout(retryTimeout)
      }
      es?.close()
      es = null
    }
    // topicsKey is derived from topics; enabled is a primitive bool.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topicsKey, enabled])
}
