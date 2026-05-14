import { useEffect, useRef } from 'react'
import { subscribeDashboardEvents, type DashboardEvent } from '../eventClient'

export type { DashboardEvent } from '../eventClient'

type UseDashboardEventsOptions = {
  /** Service names to filter by, e.g. ['mail', 's3']. Empty array = all. */
  topics: string[]
  /** Called whenever a matching event arrives. */
  onEvent: (event: DashboardEvent) => void
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
  enabled = true,
}: UseDashboardEventsOptions): void {
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
    const unsubscribe = subscribeDashboardEvents(requestedTopics, (event) => {
      onEventRef.current(event)
    })
    return unsubscribe
  }, [topicsKey, enabled])
}
