export type DashboardRequestErrorCode = 'disabled' | 'http' | 'network' | 'timeout'
export type DashboardActivityStatus = 'pending' | 'success' | 'error'

export type DashboardActivity = {
  path: string
  status: DashboardActivityStatus
  at: string
  message?: string
  statusCode?: number
}

export class DashboardRequestError extends Error {
  constructor(
    message: string,
    readonly code: DashboardRequestErrorCode,
    readonly status?: number,
  ) {
    super(message)
    this.name = 'DashboardRequestError'
  }
}

type FetchDashboardOptions = {
  method?: 'GET' | 'POST' | 'DELETE'
  body?: unknown
  timeoutMs?: number
}

type ActivityListener = (activity: DashboardActivity) => void
type ParseResponse<T> = (response: Response) => Promise<T>

const activityListeners = new Set<ActivityListener>()
let currentActivity: DashboardActivity | undefined

export function getDashboardActivity(): DashboardActivity | undefined {
  return currentActivity
}

export function subscribeDashboardActivity(listener: ActivityListener): () => void {
  activityListeners.add(listener)
  if (currentActivity) {
    listener(currentActivity)
  }
  return () => {
    activityListeners.delete(listener)
  }
}

export async function fetchJSON<T>(path: string, options: FetchDashboardOptions = {}): Promise<T> {
  return fetchDashboard(path, (response) => response.json() as Promise<T>, options)
}

export async function fetchText(path: string, options: FetchDashboardOptions = {}): Promise<string> {
  return fetchDashboard(path, (response) => response.text(), options)
}

export async function fetchNoContent(path: string, options: FetchDashboardOptions = {}): Promise<void> {
  return fetchDashboard(path, async () => undefined, options)
}

async function fetchDashboard<T>(
  path: string,
  parseResponse: ParseResponse<T>,
  options: FetchDashboardOptions = {},
): Promise<T> {
  const timeoutMs = options.timeoutMs ?? 5000
  const method = options.method ?? 'GET'
  const controller = new AbortController()
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs)
  const headers: Record<string, string> = { Accept: 'application/json' }
  if (options.body !== undefined) {
    headers['Content-Type'] = 'application/json'
  }

  try {
    publishActivity({ path, status: 'pending' })
    const response = await fetch(path, {
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
      method,
      signal: controller.signal,
    })

    if (!response.ok) {
      const message = await readErrorMessage(response)
      publishActivity({ path, status: 'error', statusCode: response.status, message })
      throw new DashboardRequestError(message, response.status === 503 ? 'disabled' : 'http', response.status)
    }

    const result = await parseResponse(response)
    publishActivity({ path, status: 'success', statusCode: response.status })
    return result
  } catch (error) {
    if (error instanceof DashboardRequestError) {
      throw error
    }
    if (error instanceof DOMException && error.name === 'AbortError') {
      publishActivity({ path, status: 'error', message: 'Dashboard request timed out' })
      throw new DashboardRequestError('Dashboard request timed out', 'timeout')
    }
    const message = error instanceof Error ? error.message : 'Dashboard request failed'
    publishActivity({ path, status: 'error', message })
    throw new DashboardRequestError(message, 'network')
  } finally {
    window.clearTimeout(timeout)
  }
}

function publishActivity(activity: Omit<DashboardActivity, 'at'>): void {
  currentActivity = { ...activity, at: new Date().toISOString() }
  activityListeners.forEach((listener) => listener(currentActivity as DashboardActivity))
}

async function readErrorMessage(response: Response): Promise<string> {
  const contentType = response.headers.get('Content-Type') ?? ''
  if (contentType.includes('application/json')) {
    const body = (await response.json().catch(() => undefined)) as { error?: unknown; message?: unknown } | undefined
    const message = body?.error ?? body?.message
    if (typeof message === 'string' && message.trim() !== '') {
      return message
    }
  }

  const text = await response.text().catch(() => '')
  if (text.trim() !== '') {
    return text.trim()
  }
  return `Dashboard request failed with status ${response.status}`
}
