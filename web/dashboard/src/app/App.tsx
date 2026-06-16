import { Component, Suspense, type ReactNode } from 'react'
import { readDashboardServices, resetDashboardServices } from './serviceResource'
import { renderRoute } from './routes'
import { AppShell } from './shell/AppShell'
import { ConfirmProvider } from '../ui/Confirm'
import { EmptyState } from '../ui/EmptyState'
import { normalizeDashboardPath } from './dashboardPaths'

type ErrorBoundaryState = {
  error?: Error
}

class ErrorBoundary extends Component<{ children: ReactNode }, ErrorBoundaryState> {
  state: ErrorBoundaryState = {}

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error }
  }

  render(): ReactNode {
    if (this.state.error) {
      return (
        <EmptyState
          title="Dashboard request failed"
          description={this.state.error.message}
          actionLabel="Retry"
          onAction={() => {
            resetDashboardServices()
            this.setState({ error: undefined })
          }}
        />
      )
    }
    return this.props.children
  }
}

function DashboardApp(): JSX.Element {
  const { services } = readDashboardServices()
  const path = normalizeDashboardPath(window.location.pathname)

  return <AppShell services={services}>{renderRoute({ services, path })}</AppShell>
}

export function App(): JSX.Element {
  return (
    <ErrorBoundary>
      <Suspense fallback={<EmptyState title="Loading dashboard" description="Checking local service status." />}>
        <ConfirmProvider>
          <DashboardApp />
        </ConfirmProvider>
      </Suspense>
    </ErrorBoundary>
  )
}
