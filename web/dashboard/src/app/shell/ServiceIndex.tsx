import type { DashboardService } from '../services/dashboard/types'
import { EmptyState } from '../../ui/EmptyState'
import { Panel } from '../../ui/Panel'
import { dashboardLink } from '../dashboardPaths'

type ServiceIndexProps = {
  services: DashboardService[]
}

export function ServiceIndex({ services }: ServiceIndexProps): JSX.Element {
  const runningCount = services.filter((service) => service.status === 'running').length
  const disabledCount = services.filter((service) => service.status === 'disabled').length

  if (services.length === 0) {
    return (
      <section className="service-index-page" aria-label="Services">
        <EmptyState title="No dashboard services" description="Configured services will appear here when they are available." />
      </section>
    )
  }

  return (
    <section className="service-index-page" aria-label="Services">
      <header className="service-index-summary">
        <div>
          <h1>Services</h1>
          <p>Local dashboard registry from /api/dashboard/services.</p>
        </div>
        <dl className="health-summary" aria-label="Service health summary">
          <div>
            <dt>Running</dt>
            <dd>{runningCount}</dd>
          </div>
          <div>
            <dt>Disabled</dt>
            <dd>{disabledCount}</dd>
          </div>
          <div>
            <dt>Total</dt>
            <dd>{services.length}</dd>
          </div>
        </dl>
      </header>

      <div className="service-index">
        {services.map((service) => (
          <a className="service-card" href={dashboardLink(service.path)} key={service.id}>
            <Panel>
              <div className="service-card-head">
                <h2>{service.name}</h2>
                <span className={`service-status ${service.status}`}>{service.status}</span>
              </div>
              <p>{service.description}</p>
              {service.endpoint ? (
                <dl className="endpoint-summary">
                  <dt>Endpoint</dt>
                  <dd>
                    <code>{service.endpoint}</code>
                  </dd>
                </dl>
              ) : null}
            </Panel>
          </a>
        ))}
      </div>
    </section>
  )
}
