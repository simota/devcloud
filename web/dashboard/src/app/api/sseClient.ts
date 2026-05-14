export type SSEEvent = {
  type: string
  service: string
  timestamp: string
  payload?: Record<string, unknown>
}

type Listener = {
  topics: Set<string>
  cb: (event: SSEEvent) => void
}

// Service names the dashboard server publishes under `event: <service>`.
// We listen for each of them so the EventSource default `onmessage` (named
// events bypass it) actually delivers to subscribers.
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

const BACKOFF_DELAYS_MS = [1_000, 2_000, 4_000, 8_000, 16_000, 30_000]

// HTTP/1.1 browsers cap concurrent connections per origin (typically 6). If
// each dashboard panel opens its own EventSource on top of the notifications
// stream, two slots are permanently consumed and a panel that fans out 4+
// parallel REST calls can hit the cap and time out. To prevent that, every
// `useEventSource` subscriber multiplexes onto a single tab-wide stream.
class SSEClient {
  private es: EventSource | null = null
  private listeners = new Map<number, Listener>()
  private nextID = 0
  private retryIndex = 0
  private retryTimeout: number | null = null
  private destroyed = false

  subscribe(topics: string[], cb: (event: SSEEvent) => void): () => void {
    const id = this.nextID
    this.nextID += 1
    this.listeners.set(id, { topics: new Set(topics), cb })
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
    if (this.es || this.destroyed) {
      return
    }
    this.connect()
  }

  private connect(): void {
    if (typeof EventSource === 'undefined') {
      return
    }
    this.destroyed = false
    const es = new EventSource('/api/events')
    this.es = es

    es.addEventListener('ready', () => {
      this.retryIndex = 0
    })

    const handle = (raw: MessageEvent): void => {
      try {
        const event = JSON.parse(raw.data as string) as SSEEvent
        this.dispatch(event)
      } catch {
        /* malformed JSON — ignore silently */
      }
    }
    KNOWN_SERVICES.forEach((service) => {
      es.addEventListener(service, handle)
    })

    es.onerror = () => {
      es.close()
      if (this.es === es) {
        this.es = null
      }
      if (this.listeners.size === 0) {
        // Nobody is listening anymore — don't keep retrying.
        return
      }
      const delayMs = BACKOFF_DELAYS_MS[Math.min(this.retryIndex, BACKOFF_DELAYS_MS.length - 1)]
      this.retryIndex += 1
      this.retryTimeout = window.setTimeout(() => {
        this.retryTimeout = null
        this.connect()
      }, delayMs)
    }
  }

  private dispatch(event: SSEEvent): void {
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

  private close(): void {
    this.destroyed = true
    if (this.retryTimeout !== null) {
      window.clearTimeout(this.retryTimeout)
      this.retryTimeout = null
    }
    this.es?.close()
    this.es = null
    this.retryIndex = 0
  }
}

const sseClient = new SSEClient()

export function subscribeSSE(
  topics: string[],
  cb: (event: SSEEvent) => void,
): () => void {
  return sseClient.subscribe(topics, cb)
}
