import { DashboardRequestError, fetchJSON } from '../../api/client'
import type { DashboardServicesResponse } from './types'

export { DashboardRequestError as DashboardAPIError }

export async function fetchDashboardServices(timeoutMs = 5000): Promise<DashboardServicesResponse> {
  return fetchJSON<DashboardServicesResponse>('/api/dashboard/services', { timeoutMs })
}
