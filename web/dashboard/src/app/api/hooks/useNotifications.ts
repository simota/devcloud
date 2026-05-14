import { useCallback, useState } from 'react'
import { getMailMessage } from '../../services/mail/api'
import { decodeMimeAddress, decodeMimeEncodedWord } from '../../services/mail/mimeDecoder'
import { useEventSource, type SSEEvent } from './useEventSource'

const PREFERENCE_STORAGE_KEY = 'devcloud.notifications.enabled'

export type NotificationPermissionState = 'default' | 'granted' | 'denied' | 'unsupported'

function readPermission(): NotificationPermissionState {
  if (typeof window === 'undefined' || typeof Notification === 'undefined') {
    return 'unsupported'
  }
  return Notification.permission as NotificationPermissionState
}

function readPreference(): boolean {
  if (typeof window === 'undefined') {
    return false
  }
  try {
    return window.localStorage.getItem(PREFERENCE_STORAGE_KEY) === '1'
  } catch {
    return false
  }
}

function writePreference(enabled: boolean): void {
  if (typeof window === 'undefined') {
    return
  }
  try {
    window.localStorage.setItem(PREFERENCE_STORAGE_KEY, enabled ? '1' : '0')
  } catch {
    /* storage unavailable — ignore */
  }
}

export type UseNotifications = {
  permission: NotificationPermissionState
  enabled: boolean
  setEnabled: (next: boolean) => void
  requestPermission: () => Promise<NotificationPermissionState>
}

/**
 * Tracks browser Notification permission state and a user preference toggle
 * persisted in localStorage. Both must be on (`granted` + `enabled`) before
 * useEventNotifications will actually display anything.
 */
export function useNotifications(): UseNotifications {
  const [permission, setPermission] = useState<NotificationPermissionState>(() => readPermission())
  const [enabled, setEnabledState] = useState<boolean>(() => readPreference())

  const setEnabled = useCallback((next: boolean): void => {
    setEnabledState(next)
    writePreference(next)
  }, [])

  const requestPermission = useCallback(async (): Promise<NotificationPermissionState> => {
    if (typeof Notification === 'undefined') {
      return 'unsupported'
    }
    try {
      const result = await Notification.requestPermission()
      const next = result as NotificationPermissionState
      setPermission(next)
      if (next === 'granted') {
        setEnabled(true)
      }
      return next
    } catch {
      return readPermission()
    }
  }, [setEnabled])

  return { permission, enabled, setEnabled, requestPermission }
}

type FormattedEvent = {
  title: string
  body: string
  tag: string
}

const SERVICE_LABELS: Record<string, string> = {
  mail: 'Mail',
  s3: 'S3',
  gcs: 'GCS',
  redis: 'Redis',
  dynamodb: 'DynamoDB',
  bigquery: 'BigQuery',
  sqs: 'SQS',
  pubsub: 'Pub/Sub',
  redshift: 'Redshift',
}

function serviceLabel(service: string): string {
  return SERVICE_LABELS[service] ?? service
}

function payloadValue(payload: Record<string, unknown> | undefined, key: string): string | undefined {
  const value = payload?.[key]
  if (typeof value === 'string') return value
  if (typeof value === 'number' && Number.isFinite(value)) return String(value)
  return undefined
}

function payloadStringArray(payload: Record<string, unknown> | undefined, key: string): string[] {
  const value = payload?.[key]
  if (Array.isArray(value)) {
    return value.filter((item): item is string => typeof item === 'string')
  }
  return []
}

function formatAddressList(addresses: string[]): string {
  return addresses
    .map((addr) => decodeMimeAddress(addr))
    .filter((addr) => addr.length > 0)
    .join(', ')
}

function formatBytes(raw: string | undefined): string | undefined {
  if (raw === undefined) return undefined
  const n = Number(raw)
  if (!Number.isFinite(n) || n < 0) return undefined
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`
}

const MAIL_BODY_PREVIEW_CHARS = 180

function stripHTML(html: string): string {
  return html
    .replace(/<(script|style)[\s\S]*?<\/\1>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/\s+/g, ' ')
    .trim()
}

function excerpt(text: string, max: number): string {
  const normalized = text.replace(/\r/g, '').replace(/\n{3,}/g, '\n\n').trim()
  if (normalized.length <= max) return normalized
  return `${normalized.slice(0, max).trimEnd()}…`
}

function mailReceivedFallback(event: SSEEvent): FormattedEvent {
  const rawFrom = payloadValue(event.payload, 'from') ?? '(unknown sender)'
  const rawSubject = payloadValue(event.payload, 'subject') ?? ''
  const from = decodeMimeAddress(rawFrom) || rawFrom
  const to = formatAddressList(payloadStringArray(event.payload, 'to'))
  const subject = decodeMimeEncodedWord(rawSubject).trim() || '(no subject)'
  const id = payloadValue(event.payload, 'messageID')
  const lines = [`From: ${from}`]
  if (to) lines.push(`To: ${to}`)
  return {
    title: subject,
    body: lines.join('\n'),
    tag: `mail.received:${id ?? subject}`,
  }
}

// showMailReceivedNotification fetches the full mail message to enrich the
// notification with the subject as title and a short body preview. If the
// fetch fails or the body is empty, falls back to subject + sender from the
// SSE payload alone.
async function showMailReceivedNotification(event: SSEEvent): Promise<void> {
  if (typeof Notification === 'undefined') return
  const id = payloadValue(event.payload, 'messageID')
  let formatted: FormattedEvent = mailReceivedFallback(event)
  if (id) {
    try {
      const message = await getMailMessage(id)
      const subject = decodeMimeEncodedWord(message.subject ?? '').trim() || '(no subject)'
      const rawFrom =
        message.from?.trim() || payloadValue(event.payload, 'from') || '(unknown sender)'
      const from = decodeMimeAddress(rawFrom) || rawFrom
      const to = formatAddressList(message.to ?? payloadStringArray(event.payload, 'to'))
      const rawText = message.textBody?.trim() || stripHTML(message.htmlBody ?? '')
      const preview = rawText ? excerpt(rawText, MAIL_BODY_PREVIEW_CHARS) : ''
      const header = to ? `From: ${from}\nTo: ${to}` : `From: ${from}`
      formatted = {
        title: subject,
        body: preview ? `${header}\n\n${preview}` : header,
        tag: `mail.received:${id}`,
      }
    } catch {
      /* fetch failed — keep the SSE-payload fallback */
    }
  }
  try {
    new Notification(formatted.title, { body: formatted.body, tag: formatted.tag })
  } catch {
    /* Notification constructor can throw on some browsers — ignore */
  }
}

// formatEvent maps a raw SSE event to a human-readable notification.
// Type strings and payload keys match exactly what the Go services emit
// (see internal/services/*/. for the canonical list).
function formatEvent(event: SSEEvent): FormattedEvent | null {
  const p = event.payload
  const get = (key: string): string | undefined => payloadValue(p, key)
  const label = serviceLabel(event.service)

  switch (event.type) {
    // ---- Mail ----
    // mail.received is handled separately because we asynchronously fetch
    // the full message to include subject as the title and a body preview.
    case 'mail.received':
      return mailReceivedFallback(event)
    case 'mail.deleted':
      return {
        title: 'Mail message deleted',
        body: 'A message was removed from the inbox.',
        tag: `mail.deleted:${get('messageID') ?? event.timestamp}`,
      }
    case 'mail.cleared':
      return { title: 'Mail inbox cleared', body: 'All messages were removed.', tag: 'mail.cleared' }

    // ---- S3 / GCS objects ----
    case 's3.object.put':
    case 'gcs.object.put': {
      const bucket = get('bucket') ?? '?'
      const key = get('key') ?? '?'
      const size = formatBytes(get('contentLength'))
      const body = size ? `${bucket}/${key}\n${size}` : `${bucket}/${key}`
      return { title: `${label} object uploaded`, body, tag: `${event.type}:${bucket}/${key}` }
    }
    case 's3.object.deleted':
    case 'gcs.object.deleted': {
      const bucket = get('bucket') ?? '?'
      const key = get('key') ?? '?'
      return {
        title: `${label} object deleted`,
        body: `${bucket}/${key}`,
        tag: `${event.type}:${bucket}/${key}`,
      }
    }

    // ---- S3 / GCS buckets ----
    case 's3.bucket.created':
    case 'gcs.bucket.created': {
      const bucket = get('bucket') ?? '?'
      return { title: `${label} bucket created`, body: bucket, tag: `${event.type}:${bucket}` }
    }
    case 's3.bucket.deleted':
    case 'gcs.bucket.deleted': {
      const bucket = get('bucket') ?? '?'
      return { title: `${label} bucket deleted`, body: bucket, tag: `${event.type}:${bucket}` }
    }

    // ---- DynamoDB ----
    case 'dynamodb.table.created':
    case 'dynamodb.table.deleted': {
      const table = get('table') ?? '?'
      const verb = event.type.endsWith('.created') ? 'created' : 'deleted'
      return { title: `DynamoDB table ${verb}`, body: table, tag: `${event.type}:${table}` }
    }
    case 'dynamodb.item.put':
    case 'dynamodb.item.updated':
    case 'dynamodb.item.deleted': {
      const table = get('table') ?? '?'
      const verb = event.type.split('.').pop() ?? 'changed'
      return {
        title: `DynamoDB item ${verb}`,
        body: `Table: ${table}`,
        tag: `${event.type}:${table}`,
      }
    }

    // ---- BigQuery ----
    case 'bigquery.dataset.created':
    case 'bigquery.dataset.deleted': {
      const project = get('project') ?? '?'
      const dataset = get('dataset') ?? '?'
      const verb = event.type.endsWith('.created') ? 'created' : 'deleted'
      return {
        title: `BigQuery dataset ${verb}`,
        body: `${project}.${dataset}`,
        tag: `${event.type}:${project}.${dataset}`,
      }
    }
    case 'bigquery.table.created':
    case 'bigquery.table.deleted': {
      const project = get('project') ?? '?'
      const dataset = get('dataset') ?? '?'
      const table = get('table') ?? '?'
      const verb = event.type.endsWith('.created') ? 'created' : 'deleted'
      return {
        title: `BigQuery table ${verb}`,
        body: `${project}.${dataset}.${table}`,
        tag: `${event.type}:${project}.${dataset}.${table}`,
      }
    }
    case 'bigquery.job.inserted': {
      const project = get('project') ?? '?'
      const jobType = get('jobType') ?? 'job'
      return {
        title: `BigQuery ${jobType} job started`,
        body: `Project: ${project}`,
        tag: `bigquery.job.inserted:${project}:${jobType}:${event.timestamp}`,
      }
    }

    // ---- SQS ----
    case 'sqs.message.sent':
    case 'sqs.message.deleted': {
      const queue = get('queue') ?? '?'
      const count = get('count')
      const verb = event.type.endsWith('.sent') ? 'sent' : 'deleted'
      const body = count ? `Queue: ${queue}  (${count} message${count === '1' ? '' : 's'})` : `Queue: ${queue}`
      return { title: `SQS message ${verb}`, body, tag: `${event.type}:${queue}:${event.timestamp}` }
    }

    // ---- Pub/Sub ----
    case 'pubsub.topic.created':
    case 'pubsub.topic.deleted': {
      const topic = get('topic') ?? '?'
      const verb = event.type.endsWith('.created') ? 'created' : 'deleted'
      return { title: `Pub/Sub topic ${verb}`, body: topic, tag: `${event.type}:${topic}` }
    }
    case 'pubsub.subscription.created': {
      const subscription = get('subscription') ?? '?'
      const topic = get('topic') ?? '?'
      return {
        title: 'Pub/Sub subscription created',
        body: `${subscription}\non topic ${topic}`,
        tag: `${event.type}:${subscription}`,
      }
    }
    case 'pubsub.message.published': {
      const topic = get('topic') ?? '?'
      const count = get('count') ?? '1'
      return {
        title: 'Pub/Sub message published',
        body: `${count} message${count === '1' ? '' : 's'} → ${topic}`,
        tag: `${event.type}:${topic}:${event.timestamp}`,
      }
    }
    case 'pubsub.message.pulled': {
      const subscription = get('subscription') ?? '?'
      const count = get('count') ?? '0'
      return {
        title: 'Pub/Sub messages pulled',
        body: `${count} from ${subscription}`,
        tag: `${event.type}:${subscription}:${event.timestamp}`,
      }
    }

    // ---- Redshift ----
    case 'redshift.statement.executed':
    case 'redshift.statement.batch_executed': {
      const id = get('statementID') ?? '?'
      const kind = event.type.endsWith('.batch_executed') ? 'Batch statement' : 'Statement'
      return { title: `Redshift ${kind.toLowerCase()} executed`, body: `ID: ${id}`, tag: `${event.type}:${id}` }
    }

    // ---- Redis ----
    case 'redis.command.mutation': {
      const command = get('command') ?? '?'
      const key = get('key')
      return {
        title: `Redis ${command}`,
        body: key ? `Key: ${key}` : 'Mutation executed',
        tag: `redis.command.mutation:${command}:${key ?? ''}:${event.timestamp}`,
      }
    }

    default:
      // Unknown event — produce a generic but readable notification rather
      // than swallow it. Useful while iterating on new event types.
      return {
        title: `${label} event`,
        body: event.type,
        tag: `${event.type}:${event.timestamp}`,
      }
  }
}

type UseEventNotificationsOptions = {
  enabled: boolean
  permission: NotificationPermissionState
  /** When true, suppress notifications while the dashboard tab is focused. */
  suppressWhileVisible?: boolean
}

/**
 * Subscribes globally to all SSE events and displays a browser Notification
 * for each one — but only when the user opted in and permission is granted.
 * When `suppressWhileVisible` is set, notifications are dropped while the
 * dashboard tab is focused so the user isn't double-notified.
 */
export function useEventNotifications({
  enabled,
  permission,
  suppressWhileVisible = false,
}: UseEventNotificationsOptions): void {
  const active = enabled && permission === 'granted'

  const onEvent = useCallback(
    (event: SSEEvent): void => {
      if (typeof Notification === 'undefined') {
        return
      }
      if (suppressWhileVisible && typeof document !== 'undefined' && document.visibilityState === 'visible') {
        return
      }
      // Mail receipts go through an async path so we can fetch the body and
      // build a richer subject+preview notification.
      if (event.type === 'mail.received') {
        void showMailReceivedNotification(event)
        return
      }
      const formatted = formatEvent(event)
      if (!formatted) {
        return
      }
      try {
        new Notification(formatted.title, {
          body: formatted.body,
          tag: formatted.tag,
        })
      } catch {
        /* Notification constructor can throw on some browsers — ignore */
      }
    },
    [suppressWhileVisible],
  )

  // The SSE connection is opened only when notifications are actually active,
  // so users who never grant permission don't keep an open stream.
  useEventSource({ topics: [], onEvent, enabled: active })
}
