import { useEffect, useState } from 'react'
import { getDashboardActivity, subscribeDashboardActivity, type DashboardActivity } from '../api/client'
import type { DashboardService } from '../services/dashboard/types'
import { normalizeDashboardPath } from '../dashboardPaths'

type ActivityFooterProps = {
  services: DashboardService[]
}

export function ActivityFooter({ services }: ActivityFooterProps): JSX.Element {
  const activePath = normalizeDashboardPath(window.location.pathname)
  const activeService = services.find((service) => service.path === activePath)
  const [activity, setActivity] = useState<DashboardActivity | undefined>(() => getDashboardActivity())

  useEffect(() => subscribeDashboardActivity(setActivity), [])

  return (
    <footer className="activity-footer">
      <span>Last request: {activity?.path ?? '/api/dashboard/services'}</span>
      <span>Active: {activeService?.name ?? 'Service index'}</span>
      <span>
        API status: <ActivityStatus activity={activity} fallback={activeService?.status ?? 'running'} />
      </span>
      <span>Storage: {activeService?.storagePath ?? 'registry'}</span>
    </footer>
  )
}

type ActivityStatusProps = {
  activity?: DashboardActivity
  fallback: string
}

function ActivityStatus({ activity, fallback }: ActivityStatusProps): JSX.Element {
  if (!activity) {
    return <span>{fallback}</span>
  }

  const label =
    activity.status === 'success' && activity.statusCode
      ? `${activity.status} ${activity.statusCode}`
      : activity.status
  return (
    <span className={`activity-status ${activity.status}`} title={activity.message}>
      {label}
    </span>
  )
}
