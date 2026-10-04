export type MailAddress = { name: string; address: string }

export type MessageSummary = {
  id: string
  from: MailAddress
  to: MailAddress[]
  subject: string
  receivedAt: string
  size: number
  attachmentCount: number
  snippet: string
}

export type MessagePage = { total: number; start: number; items: MessageSummary[] }
export type Attachment = { index: number; filename: string; contentType: string; size: number }
export type MessageDetail = {
  id: string
  from: MailAddress
  to: MailAddress[]
  cc: MailAddress[]
  subject: string
  date: string
  receivedAt: string
  size: number
  headers: [string, string][]
  text: string
  hasHtml: boolean
  attachments: Attachment[]
  warnings: string[]
}

export class ApiError extends Error {
  constructor(message: string, readonly status?: number) {
    super(message)
    this.name = 'ApiError'
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

function isAddress(value: unknown): value is MailAddress {
  return isRecord(value) && typeof value.name === 'string' && typeof value.address === 'string'
}

function isSummary(value: unknown): value is MessageSummary {
  return isRecord(value) && typeof value.id === 'string' && isAddress(value.from) &&
    Array.isArray(value.to) && value.to.every(isAddress) && typeof value.subject === 'string' &&
    typeof value.receivedAt === 'string' && isCount(value.size) &&
    isCount(value.attachmentCount) && typeof value.snippet === 'string'
}

function isPage(value: unknown): value is MessagePage {
  return isRecord(value) && isCount(value.total) && isCount(value.start) &&
    Array.isArray(value.items) && value.items.every(isSummary)
}

function isAttachment(value: unknown): value is Attachment {
  return isRecord(value) && isCount(value.index) && typeof value.filename === 'string' &&
    typeof value.contentType === 'string' && isCount(value.size)
}

function isHeader(value: unknown): value is [string, string] {
  return Array.isArray(value) && value.length === 2 &&
    typeof value[0] === 'string' && typeof value[1] === 'string'
}

function isDetail(value: unknown): value is MessageDetail {
  return isRecord(value) && typeof value.id === 'string' && isAddress(value.from) &&
    Array.isArray(value.to) && value.to.every(isAddress) &&
    Array.isArray(value.cc) && value.cc.every(isAddress) && typeof value.subject === 'string' &&
    typeof value.date === 'string' && typeof value.receivedAt === 'string' && isCount(value.size) &&
    Array.isArray(value.headers) && value.headers.every(isHeader) && typeof value.text === 'string' &&
    typeof value.hasHtml === 'boolean' && Array.isArray(value.attachments) &&
    value.attachments.every(isAttachment) && Array.isArray(value.warnings) &&
    value.warnings.every((warning: unknown) => typeof warning === 'string')
}

// Keep the timeout active while reading the body, and never expose server payloads in errors.
async function request<T>(
  url: string,
  read: (response: Response) => Promise<T>,
  signal?: AbortSignal,
  method = 'GET',
): Promise<T> {
  const controller = new AbortController()
  const abort = (): void => controller.abort()
  signal?.addEventListener('abort', abort, { once: true })
  if (signal?.aborted) controller.abort()
  let timedOut = false
  const timer = window.setTimeout(() => {
    timedOut = true
    controller.abort()
  }, 15000)
  try {
    const response = await fetch(url, { method, signal: controller.signal, credentials: 'same-origin' })
    if (!response.ok) {
      const message = response.status === 401
        ? 'Authentication is required. Sign in to the server and retry.'
        : response.status === 403
          ? 'Access was denied. Check the server access configuration and retry.'
          : `The request failed (HTTP ${response.status}). Try again.`
      throw new ApiError(message, response.status)
    }
    return await read(response)
  } catch (error: unknown) {
    if (timedOut) throw new ApiError('The server took too long to respond. Try again.')
    if (signal?.aborted || error instanceof ApiError) throw error
    throw new ApiError('Unable to reach the server or read its response. Try again.')
  } finally {
    window.clearTimeout(timer)
    signal?.removeEventListener('abort', abort)
  }
}

async function readJSON<T>(response: Response, valid: (value: unknown) => value is T): Promise<T> {
  const value: unknown = await response.json()
  if (!valid(value)) throw new ApiError('The server returned an unexpected response. Try again.')
  return value
}

export function messageURL(id: string, suffix = ''): string {
  return `/api/mailbox/messages/${encodeURIComponent(id)}${suffix}`
}

export function downloadURL(id: string): string {
  return `/api/v1/messages/${encodeURIComponent(id)}/download`
}

export function attachmentURL(id: string, index: number): string {
  return messageURL(id, `/attachments/${encodeURIComponent(String(index))}`)
}

export function listMessages(query: string, start = 0, signal?: AbortSignal, limit = 50): Promise<MessagePage> {
  const params = new URLSearchParams({ start: String(start), limit: String(limit), q: query })
  return request(`/api/mailbox/messages?${params}`, (response) => readJSON(response, isPage), signal)
}

export function getMessage(id: string, signal?: AbortSignal): Promise<MessageDetail> {
  return request(messageURL(id), (response) => readJSON(response, isDetail), signal)
}

export function getRaw(id: string, signal?: AbortSignal): Promise<string> {
  return request(messageURL(id, '/raw'), (response) => response.text(), signal)
}

export function deleteMessage(id: string): Promise<void> {
  return request(`/api/v1/messages/${encodeURIComponent(id)}`, async () => undefined, undefined, 'DELETE')
}

export function clearMessages(): Promise<void> {
  return request('/api/v1/messages', async () => undefined, undefined, 'DELETE')
}

export function errorMessage(error: unknown): string {
  return error instanceof ApiError ? error.message : 'Something went wrong. Try again.'
}
