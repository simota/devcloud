import { useEffect, useRef } from 'react'
import { subscribeSSE, type SSEEvent } from '../sseClient'

export type { SSEEvent } from '../sseClient'

type UseEventSourceOptions = {
  /** Service names to filter by, e.g. ['mail', 's3']. Empty array = all. */
  topics: string[]
  /** Called whenever a matching event arrives. */
  onEvent: (event: SSEEvent) => void
  /** Set to false to opt out of the shared SSE stream. */
  enabled?: boolean
}

/**
 * Registers an event listener on the tab-wide SSE stream. All hooks (and
 * services) share a single EventSource managed by sseClient — see that file
 * for why connection multiplexing matters.
 */
export function useEventSource({ topics, onEvent, enabled = true }: UseEventSourceOptions): void {
  const onEventRef = useRef(onEvent)
  useEffect(() => {
    onEventRef.current = onEvent
  }, [onEvent])

  const topicsKey = topics.slice().sort().join(',')

  useEffect(() => {
    if (!enabled) {
      return
    }
    const requestedTopics = topicsKey ? topicsKey.split(',') : []
    const unsubscribe = subscribeSSE(requestedTopics, (event) => {
      onEventRef.current(event)
    })
    return unsubscribe
  }, [topicsKey, enabled])
}
