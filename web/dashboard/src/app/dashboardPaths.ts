const dashboardBasePath = '/dashboard'

export function normalizeDashboardPath(pathname: string): string {
  if (pathname === dashboardBasePath || pathname === `${dashboardBasePath}/`) {
    return '/'
  }
  if (pathname.startsWith(`${dashboardBasePath}/`)) {
    return pathname.slice(dashboardBasePath.length) || '/'
  }
  return pathname
}

export function dashboardLink(path: string, currentPathname = window.location.pathname): string {
  if (path === dashboardBasePath || path.startsWith(`${dashboardBasePath}/`)) {
    return path
  }
  if (!currentPathname.startsWith(dashboardBasePath)) {
    return path
  }
  return path === '/' ? `${dashboardBasePath}/` : `${dashboardBasePath}${path}`
}
