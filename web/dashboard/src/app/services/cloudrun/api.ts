import { fetchJSON } from '../../api/client'
import type {
  CloudRunInstancesResponse,
  CloudRunLogsResponse,
  CloudRunRevisionsResponse,
  CloudRunServicesResponse,
  CloudRunStatus,
} from './types'

export async function getCloudRunStatus(): Promise<CloudRunStatus> {
  return fetchJSON<CloudRunStatus>('/api/cloudrun/status')
}

export async function listCloudRunServices(): Promise<CloudRunServicesResponse> {
  return fetchJSON<CloudRunServicesResponse>('/api/cloudrun/services')
}

export async function listCloudRunInstances(): Promise<CloudRunInstancesResponse> {
  return fetchJSON<CloudRunInstancesResponse>('/api/cloudrun/instances')
}

/** `name` is the full resource name `projects/<p>/locations/<l>/services/<s>`. */
function servicePath(name: string): string {
  const [, project, , location, , service] = name.split('/')
  return [project, location, service].map(encodeURIComponent).join('/')
}

export async function listCloudRunRevisions(name: string): Promise<CloudRunRevisionsResponse> {
  return fetchJSON<CloudRunRevisionsResponse>(`/api/cloudrun/services/${servicePath(name)}/revisions`)
}

export async function getCloudRunLogs(name: string): Promise<CloudRunLogsResponse> {
  return fetchJSON<CloudRunLogsResponse>(`/api/cloudrun/services/${servicePath(name)}/logs`)
}
