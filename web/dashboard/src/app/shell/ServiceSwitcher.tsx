import type { DashboardService } from '../services/dashboard/types'
import { dashboardLink, normalizeDashboardPath } from '../dashboardPaths'

type ServiceSwitcherProps = {
  services: DashboardService[]
}

export function ServiceSwitcher({ services }: ServiceSwitcherProps): JSX.Element {
  const path = normalizeDashboardPath(window.location.pathname)

  return (
    <nav className="service-switcher" aria-label="Services">
      <a
        aria-current={path === '/' ? 'page' : undefined}
        className={path === '/' ? 'active' : undefined}
        href={dashboardLink('/')}
      >
        Services
      </a>
      {services.map((service) => (
        <a
          aria-current={path === normalizeDashboardPath(service.path) ? 'page' : undefined}
          aria-label={`${service.name}: ${service.status}`}
          className={path === normalizeDashboardPath(service.path) ? 'active' : undefined}
          href={dashboardLink(service.path)}
          key={service.id}
        >
          <span>{service.name}</span>
          <span className={`switcher-status ${serviceStatusClass(service.status)}`}>
            <span className="status-dot" />
            {service.status}
          </span>
        </a>
      ))}
    </nav>
  )
}

function serviceStatusClass(status: string): string {
  return status === 'running' ? 'running' : 'disabled'
}
