import type { DashboardService } from '../services/dashboard/types'

type StatusBarProps = {
  services: DashboardService[]
}

export function StatusBar({ services }: StatusBarProps): JSX.Element {
  const runningCount = services.filter((service) => service.status === 'running').length
  const label = `${runningCount}/${services.length} running`
  const statusClass = services.length > 0 && runningCount === services.length ? 'running' : 'attention'

  return (
    <div className={`status-pill ${statusClass}`} aria-label={`Service status: ${label}`}>
      <span className="status-dot" />
      {label}
    </div>
  )
}
