import { useCallback, useEffect, useRef, useState } from 'react'
import { errorMessage, getMessage, listMessages, type MessagePage } from './api'
import { addressLabel } from './format'

export function useDebouncedValue(value: string, delay: number): string {
  const [debounced, setDebounced] = useState(value)
  useEffect(() => {
    const timer = window.setTimeout(() => setDebounced(value), delay)
    return () => window.clearTimeout(timer)
  }, [value, delay])
  return debounced
}

type RefreshOptions = { notify?: boolean; newMessageID?: string; reset?: boolean }
const EMPTY_PAGE: MessagePage = { total: 0, start: 0, items: [] }

export function useMessages(query: string) {
  const [page, setPage] = useState<MessagePage>(EMPTY_PAGE)
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string>()
  const [moreError, setMoreError] = useState<string>()
  const [highlightedIDs, setHighlightedIDs] = useState<string[]>([])
  const [announcement, setAnnouncement] = useState('')
  const pageRef = useRef(page)
  const nextStart = useRef(0)
  const version = useRef(0)
  const firstRequest = useRef<AbortController>()
  const moreRequest = useRef<AbortController>()
  const pendingNewIDs = useRef(new Set<string>())
  const pendingNotification = useRef(false)

  const refresh = useCallback(async (options: RefreshOptions = {}): Promise<MessagePage | undefined> => {
    const revision = ++version.current
    firstRequest.current?.abort()
    moreRequest.current?.abort()
    const controller = new AbortController()
    firstRequest.current = controller
    if (options.newMessageID) pendingNewIDs.current.add(options.newMessageID)
    if (options.notify) {
      pendingNotification.current = true
      setAnnouncement('')
    }
    if (options.reset) {
      pageRef.current = EMPTY_PAGE
      setPage(EMPTY_PAGE)
      nextStart.current = 0
      pendingNewIDs.current.clear()
      pendingNotification.current = false
      setHighlightedIDs([])
    }
    setLoading(true)
    setLoadingMore(false)
    setError(undefined)
    setMoreError(undefined)
    try {
      const wanted = Math.max(50, pageRef.current.items.length)
      const result = await listMessages(query, 0, controller.signal, Math.min(100, wanted))
      let fetched = result.items.length
      const ids = new Set(result.items.map((item) => item.id))
      while (fetched < Math.min(wanted, result.total)) {
        const next = await listMessages(query, fetched, controller.signal, Math.min(100, wanted - fetched))
        if (controller.signal.aborted || revision !== version.current) return undefined
        result.total = next.total
        if (next.items.length === 0) break
        fetched = next.start + next.items.length
        for (const item of next.items) {
          if (!ids.has(item.id)) {
            ids.add(item.id)
            result.items.push(item)
          }
        }
      }
      if (controller.signal.aborted || revision !== version.current) return undefined
      const previousIDs = new Set(pageRef.current.items.map((item) => item.id))
      const arrivalIDs = new Set(pendingNewIDs.current)
      const notify = pendingNotification.current
      pendingNewIDs.current.clear()
      pendingNotification.current = false
      pageRef.current = result
      nextStart.current = fetched
      setPage(result)
      setLoading(false)
      if (notify) {
        const arrivals = result.items.filter((item) =>
          arrivalIDs.has(item.id) || !previousIDs.has(item.id),
        )
        setHighlightedIDs(arrivals.map((item) => item.id))
        const newest = arrivals[0]
        const hiddenID = [...arrivalIDs].at(-1)
        setAnnouncement(newest
          ? `New message received from ${addressLabel(newest.from)}: ${newest.subject || '(No subject)'}`
          : 'New message received')
        if (!newest && hiddenID) {
          try {
            // A filtered-out arrival still needs decoded metadata for its live announcement.
            const hidden = await getMessage(hiddenID, controller.signal)
            if (!controller.signal.aborted && revision === version.current) {
              setAnnouncement(`New message received from ${addressLabel(hidden.from)}: ${hidden.subject || '(No subject)'}`)
            }
          } catch {
            // If the arrival was deleted or became unavailable, retain the generic notification.
          }
        }
      }
      return result
    } catch (failure: unknown) {
      if (!controller.signal.aborted && revision === version.current) setError(errorMessage(failure))
      return undefined
    } finally {
      if (!controller.signal.aborted && revision === version.current) setLoading(false)
    }
  }, [query])

  const loadMore = useCallback(async (): Promise<void> => {
    if (loading || loadingMore || nextStart.current >= pageRef.current.total) return
    const revision = version.current
    const controller = new AbortController()
    moreRequest.current?.abort()
    moreRequest.current = controller
    setLoadingMore(true)
    setMoreError(undefined)
    try {
      const result = await listMessages(query, nextStart.current, controller.signal)
      if (controller.signal.aborted || revision !== version.current) return
      const old = pageRef.current
      const ids = new Set(old.items.map((item) => item.id))
      const combined = { ...result, start: 0, items: [...old.items, ...result.items.filter((item) => !ids.has(item.id))] }
      pageRef.current = combined
      nextStart.current = result.start + result.items.length
      setPage(combined)
    } catch (failure: unknown) {
      if (!controller.signal.aborted && revision === version.current) setMoreError(errorMessage(failure))
    } finally {
      if (!controller.signal.aborted && revision === version.current) setLoadingMore(false)
    }
  }, [query, loading, loadingMore])

  useEffect(() => {
    void refresh({ reset: true })
    return () => {
      ++version.current
      firstRequest.current?.abort()
      moreRequest.current?.abort()
    }
  }, [refresh])

  useEffect(() => {
    if (highlightedIDs.length === 0) return
    const timer = window.setTimeout(() => setHighlightedIDs([]), 1500)
    return () => window.clearTimeout(timer)
  }, [highlightedIDs])

  return {
    ...page, loading, loadingMore, error, moreError, refresh, loadMore,
    hasMore: nextStart.current < page.total,
    highlightedIDs, announcement, setAnnouncement,
  }
}

type EventCallbacks = { onOpen: () => void; onMessage: (id?: string) => void }

export function useEventStream(callbacks: EventCallbacks): boolean {
  const handlers = useRef(callbacks)
  const [reconnecting, setReconnecting] = useState(false)
  useEffect(() => { handlers.current = callbacks }, [callbacks])
  useEffect(() => {
    const stream = new EventSource('/api/v1/events')
    stream.onopen = () => {
      setReconnecting(false)
      handlers.current.onOpen()
    }
    stream.onmessage = (event: MessageEvent<string>) => {
      let id: string | undefined
      try {
        const value: unknown = JSON.parse(event.data)
        if (typeof value === 'object' && value !== null && 'ID' in value && typeof value.ID === 'string') {
          id = value.ID
        }
      } catch {
        // Even an unrecognised event must reconcile the inbox with the decoded API.
      }
      handlers.current.onMessage(id)
    }
    stream.onerror = () => setReconnecting(true)
    return () => stream.close()
  }, [])
  return reconnecting
}
