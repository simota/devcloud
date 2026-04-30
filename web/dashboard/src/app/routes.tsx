import type { DashboardService } from './services/dashboard/types'
import { ServiceIndex } from './shell/ServiceIndex'
import { MailDashboard } from './services/mail/MailDashboard'
import { S3Dashboard } from './services/s3/S3Dashboard'

type RouteProps = {
  services: DashboardService[]
  path: string
}

export function renderRoute({ services, path }: RouteProps): JSX.Element {
  if (path === '/mail') {
    return <MailDashboard />
  }
  if (path === '/s3') {
    return <S3Dashboard />
  }
  return <ServiceIndex services={services} />
}
