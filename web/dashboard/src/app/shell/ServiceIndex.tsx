import type { DashboardService } from '../services/dashboard/types'
import { Panel } from '../../ui/Panel'

type ServiceIndexProps = {
  services: DashboardService[]
}

export function ServiceIndex({ services }: ServiceIndexProps): JSX.Element {
  return (
    <section className="service-index" aria-label="Services">
      {services.map((service) => (
        <a className="service-card" href={service.path} key={service.id}>
          <Panel>
            <div className="service-card-head">
              <h2>{service.name}</h2>
              <span className={`service-status ${service.status}`}>{service.status}</span>
            </div>
            <p>{service.description}</p>
            {service.endpoint ? <code>{service.endpoint}</code> : null}
          </Panel>
        </a>
      ))}
    </section>
  )
}
