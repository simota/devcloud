import type { DashboardService } from '../services/dashboard/types'

type ServiceSwitcherProps = {
  services: DashboardService[]
}

export function ServiceSwitcher({ services }: ServiceSwitcherProps): JSX.Element {
  const path = window.location.pathname

  return (
    <nav className="service-switcher" aria-label="Services">
      {services.map((service) => (
        <a
          aria-current={path === service.path ? 'page' : undefined}
          className={path === service.path ? 'active' : undefined}
          href={service.path}
          key={service.id}
        >
          {service.name}
        </a>
      ))}
    </nav>
  )
}
