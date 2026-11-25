import { fetchJSON } from '../../api/client'
import type {
  CloudRunCreateServiceInput,
  CloudRunIamPolicy,
  CloudRunInstancesResponse,
  CloudRunLogsResponse,
  CloudRunOperation,
  CloudRunRevisionsResponse,
  CloudRunServicesResponse,
  CloudRunStatus,
  CloudRunUpdateServiceInput,
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

export async function createCloudRunService(input: CloudRunCreateServiceInput): Promise<CloudRunOperation> {
  return fetchJSON<CloudRunOperation>('/api/cloudrun/services/create', {
    method: 'POST',
    body: input,
  })
}

export async function updateCloudRunService(
  name: string,
  input: CloudRunUpdateServiceInput,
): Promise<CloudRunOperation> {
  return fetchJSON<CloudRunOperation>(`/api/cloudrun/services/${servicePath(name)}/update`, {
    method: 'POST',
    body: input,
  })
}

export async function deleteCloudRunService(name: string, confirmation: string): Promise<CloudRunOperation> {
  return fetchJSON<CloudRunOperation>(`/api/cloudrun/services/${servicePath(name)}`, {
    method: 'DELETE',
    body: { confirmation },
  })
}

export async function deleteCloudRunRevision(serviceName: string, revisionId: string): Promise<CloudRunOperation> {
  return fetchJSON<CloudRunOperation>(
    `/api/cloudrun/services/${servicePath(serviceName)}/revisions/${encodeURIComponent(revisionId)}`,
    {
      method: 'DELETE',
    },
  )
}

export async function getCloudRunIamPolicy(name: string): Promise<CloudRunIamPolicy> {
  return fetchJSON<CloudRunIamPolicy>(`/api/cloudrun/services/${servicePath(name)}/iam`)
}

export async function setCloudRunIamPolicy(name: string, policy: CloudRunIamPolicy): Promise<CloudRunIamPolicy> {
  return fetchJSON<CloudRunIamPolicy>(`/api/cloudrun/services/${servicePath(name)}/iam`, {
    method: 'POST',
    body: { policy },
  })
}
