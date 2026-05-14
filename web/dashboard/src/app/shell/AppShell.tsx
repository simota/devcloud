import type { ReactNode } from 'react'
import type { DashboardService } from '../services/dashboard/types'
import { ServiceSwitcher } from './ServiceSwitcher'
import { StatusBar } from './StatusBar'
import { ActivityFooter } from './ActivityFooter'
import { NotificationsToggle } from './NotificationsToggle'
import { Button } from '../../ui/Button'
import { dashboardLink } from '../dashboardPaths'
import { useNotifications, useEventNotifications } from '../api/hooks/useNotifications'

type AppShellProps = {
  services: DashboardService[]
  children: ReactNode
}

export function AppShell({ services, children }: AppShellProps): JSX.Element {
  const notifications = useNotifications()
  useEventNotifications({ enabled: notifications.enabled, permission: notifications.permission })

  return (
    <div className="app-shell">
      <header className="top-bar">
        <div className="brand-block">
          <a className="brand-title" href={dashboardLink('/')}>
            devcloud
          </a>
          <StatusBar services={services} />
        </div>
        <div className="top-actions">
          <NotificationsToggle notifications={notifications} />
          <ServiceSwitcher services={services} />
          <Button onClick={() => window.location.reload()}>Refresh</Button>
        </div>
      </header>
      <main className="service-surface">{children}</main>
      <ActivityFooter services={services} />
    </div>
  )
}
