import { useEffect, useRef } from 'react'
import { subscribeDashboardEvents, type DashboardEvent } from '../eventClient'

export type { DashboardEvent } from '../eventClient'

type UseDashboardEventsOptions = {
  /** Service names to filter by, e.g. ['mail', 's3']. Empty array = all. */
  topics: string[]
  /** Called whenever a matching event arrives. */
  onEvent: (event: DashboardEvent) => void
  /** Refresh snapshots after reconnect or a relay queue overflow. */
  onResync?: () => void
  /** Set to false to opt out of the shared WebSocket stream. */
  enabled?: boolean
}

/**
 * Registers an event listener on the tab-wide event WebSocket. The underlying
 * connection is managed by eventClient — see that file for the rationale
 * behind multiplexing all subscribers onto a single socket.
 */
export function useDashboardEvents({
  topics,
  onEvent,
  onResync,
  enabled = true,
}: UseDashboardEventsOptions): void {
  const onEventRef = useRef(onEvent)
  const onResyncRef = useRef(onResync)
  useEffect(() => {
    onEventRef.current = onEvent
    onResyncRef.current = onResync
  }, [onEvent, onResync])

  const topicsKey = topics.slice().sort().join(',')

  useEffect(() => {
    if (!enabled) {
      return
    }
    const requestedTopics = topicsKey ? topicsKey.split(',') : []
    const unsubscribe = subscribeDashboardEvents(
      requestedTopics,
      (event) => onEventRef.current(event),
      () => onResyncRef.current?.(),
    )
    return unsubscribe
  }, [topicsKey, enabled])
}
