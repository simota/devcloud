export type DashboardEvent = {
  type: string
  service: string
  timestamp: string
  payload?: Record<string, unknown>
}

type Listener = {
  topics: Set<string>
  cb: (event: DashboardEvent) => void
  onResync?: () => void
}

const BACKOFF_DELAYS_MS = [1_000, 2_000, 4_000, 8_000, 16_000, 30_000]

function buildEventsURL(): string {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${window.location.host}/api/events`
}

// All useDashboardEvents subscribers share this single WebSocket. Compared to
// the previous SSE-based implementation, a WebSocket does not consume one of
// the browser's six HTTP/1.1 connection slots and lets DevTools / page-state
// based tooling consider the page fully loaded.
class EventClient {
  private ws: WebSocket | null = null
  private listeners = new Map<number, Listener>()
  private nextID = 0
  private retryIndex = 0
  private retryTimeout: number | null = null

  subscribe(topics: string[], cb: (event: DashboardEvent) => void, onResync?: () => void): () => void {
    const id = this.nextID
    this.nextID += 1
    this.listeners.set(id, { topics: new Set(topics), cb, onResync })
    this.ensureOpen()
    let cancelled = false
    return () => {
      if (cancelled) return
      cancelled = true
      this.listeners.delete(id)
      if (this.listeners.size === 0) {
        this.close()
      }
    }
  }

  private ensureOpen(): void {
    if (this.ws || this.retryTimeout !== null) {
      return
    }
    this.connect()
  }

  private connect(): void {
    if (typeof WebSocket === 'undefined') {
      return
    }
    let ws: WebSocket
    try {
      ws = new WebSocket(buildEventsURL())
    } catch {
      this.scheduleReconnect()
      return
    }
    this.ws = ws

    ws.onopen = () => {
      if (this.ws === ws) {
        this.retryIndex = 0
      }
    }

    ws.onmessage = (raw) => {
      if (this.ws !== ws || typeof raw.data !== 'string') {
        return
      }
      try {
        // The dashboard forwards the relay's envelope verbatim. Only the
        // nested event is a service event; hello/ping/resync are control frames.
        const frame: unknown = JSON.parse(raw.data)
        if (!isRecord(frame)) return
        if (frame.type === 'event' && isDashboardEvent(frame.event)) {
          this.dispatch(frame.event)
        } else if (frame.type === 'hello') {
          // The relay has no replay buffer. Refresh snapshots after reconnect
          // and on first connect: a mutation can race the initial HTTP load
          // or happen while the first WebSocket connection is still retrying.
          this.resync()
        } else if (frame.type === 'resync') {
          this.resync()
        }
      } catch {
        /* malformed frame — ignore */
      }
    }

    const handleClose = (): void => {
      if (this.ws !== ws) {
        // A newer connection has already taken over.
        return
      }
      this.ws = null
      this.detach(ws)
      try {
        ws.close()
      } catch {
        /* a failed connection may already be closed */
      }
      if (this.listeners.size === 0) {
        return
      }
      this.scheduleReconnect()
    }
    ws.onerror = handleClose
    ws.onclose = handleClose
  }

  private scheduleReconnect(): void {
    if (this.retryTimeout !== null) {
      return
    }
    const delayMs = BACKOFF_DELAYS_MS[Math.min(this.retryIndex, BACKOFF_DELAYS_MS.length - 1)]
    this.retryIndex += 1
    this.retryTimeout = window.setTimeout(() => {
      this.retryTimeout = null
      if (this.listeners.size > 0 && !this.ws) {
        this.connect()
      }
    }, delayMs)
  }

  private dispatch(event: DashboardEvent): void {
    for (const listener of this.listeners.values()) {
      if (listener.topics.size > 0 && !listener.topics.has(event.service)) {
        continue
      }
      try {
        listener.cb(event)
      } catch {
        /* listener errors must not bring the stream down */
      }
    }
  }

  private resync(): void {
    for (const listener of this.listeners.values()) {
      try {
        listener.onResync?.()
      } catch {
        /* one failed refresh must not prevent other subscribers refreshing */
      }
    }
  }

  private detach(ws: WebSocket): void {
    ws.onopen = null
    ws.onmessage = null
    ws.onerror = null
    ws.onclose = null
  }

  private close(): void {
    if (this.retryTimeout !== null) {
      window.clearTimeout(this.retryTimeout)
      this.retryTimeout = null
    }
    if (this.ws) {
      this.detach(this.ws)
      try {
        this.ws.close(1000, 'no listeners')
      } catch {
        /* ignore */
      }
    }
    this.ws = null
    this.retryIndex = 0
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isDashboardEvent(value: unknown): value is DashboardEvent {
  return isRecord(value)
    && typeof value.type === 'string'
    && typeof value.service === 'string'
    && typeof value.timestamp === 'string'
    && (value.payload === undefined || isRecord(value.payload))
}

const eventClient = new EventClient()

export function subscribeDashboardEvents(
  topics: string[],
  cb: (event: DashboardEvent) => void,
  onResync?: () => void,
): () => void {
  return eventClient.subscribe(topics, cb, onResync)
}
