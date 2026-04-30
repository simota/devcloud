import type { DashboardService } from '../services/dashboard/types'

type ActivityFooterProps = {
  services: DashboardService[]
}

export function ActivityFooter({ services }: ActivityFooterProps): JSX.Element {
  const activeService = services.find((service) => service.path === window.location.pathname)

  return (
    <footer className="activity-footer">
      <span>Last request: /api/dashboard/services</span>
      <span>Active: {activeService?.name ?? 'Service index'}</span>
      <span>API status: {activeService?.status ?? 'running'}</span>
    </footer>
  )
}
