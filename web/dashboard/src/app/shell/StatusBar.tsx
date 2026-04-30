import type { DashboardService } from '../services/dashboard/types'

type StatusBarProps = {
  services: DashboardService[]
}

export function StatusBar({ services }: StatusBarProps): JSX.Element {
  const runningCount = services.filter((service) => service.status === 'running').length
  const label = `${runningCount}/${services.length} running`

  return (
    <div className="status-pill" aria-label={`Service status: ${label}`}>
      <span className="status-dot" />
      {label}
    </div>
  )
}
