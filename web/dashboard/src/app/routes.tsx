import type { DashboardService } from './services/dashboard/types'
import { ServiceIndex } from './shell/ServiceIndex'
import { MailDashboard } from './services/mail/MailDashboard'
import { S3Dashboard } from './services/s3/S3Dashboard'
import { DynamoDBDashboard } from './services/dynamodb/DynamoDBDashboard'

type RouteProps = {
  services: DashboardService[]
  path: string
}

export function renderRoute({ services, path }: RouteProps): JSX.Element {
  if (path === '/mail') {
    return <MailDashboard service={services.find((service) => service.id === 'mail')} />
  }
  if (path === '/s3') {
    return <S3Dashboard service={services.find((service) => service.id === 's3')} />
  }
  if (path === '/dynamodb') {
    return <DynamoDBDashboard service={services.find((service) => service.id === 'dynamodb')} />
  }
  return <ServiceIndex services={services} />
}
