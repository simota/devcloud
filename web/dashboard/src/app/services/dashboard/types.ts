export type ServiceStatus = 'running' | 'disabled' | 'error'

export type DashboardService = {
  id: string
  name: string
  path: string
  status: ServiceStatus
  endpoint?: string
  description: string
}

export type DashboardServicesResponse = {
  services: DashboardService[]
}
