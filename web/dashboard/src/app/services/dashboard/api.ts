import type { DashboardServicesResponse } from './types'

export class DashboardAPIError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message)
    this.name = 'DashboardAPIError'
  }
}

export async function fetchDashboardServices(timeoutMs = 5000): Promise<DashboardServicesResponse> {
  const controller = new AbortController()
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs)

  try {
    const response = await fetch('/api/dashboard/services', {
      headers: { Accept: 'application/json' },
      signal: controller.signal,
    })

    if (!response.ok) {
      const text = await response.text()
      throw new DashboardAPIError(text || 'Dashboard services request failed', response.status)
    }

    return (await response.json()) as DashboardServicesResponse
  } catch (error) {
    if (error instanceof DashboardAPIError) {
      throw error
    }
    throw new DashboardAPIError(error instanceof Error ? error.message : 'Dashboard request failed')
  } finally {
    window.clearTimeout(timeout)
  }
}
