import { fetchDashboardServices } from './services/dashboard/api'
import type { DashboardServicesResponse } from './services/dashboard/types'

type ResourceState =
  | { status: 'pending'; promise: Promise<void> }
  | { status: 'success'; value: DashboardServicesResponse }
  | { status: 'error'; error: Error }

let state: ResourceState = createState()

function createState(): ResourceState {
  const pending: ResourceState = {
    status: 'pending',
    promise: fetchDashboardServices()
      .then((value) => {
        state = { status: 'success', value }
      })
      .catch((error: Error) => {
        state = { status: 'error', error }
      }),
  }
  return pending
}

export function readDashboardServices(): DashboardServicesResponse {
  if (state.status === 'pending') {
    throw state.promise
  }
  if (state.status === 'error') {
    throw state.error
  }
  return state.value
}

export function resetDashboardServices(): void {
  state = createState()
}
