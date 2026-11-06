export type CloudRunStatus = {
  service: string
  status: string
  running: boolean
  endpoint: string
  project: string
  region: string
  storagePath: string
  serviceCount: number
  instanceCount: number
}

export type CloudRunContainer = {
  image: string
  command?: string[]
  args?: string[]
  env?: { name: string; value?: string }[]
  ports?: { name?: string; containerPort: number }[]
}

export type CloudRunService = {
  name: string
  uid: string
  generation: string
  createTime: string
  updateTime: string
  uri: string
  urls?: string[]
  labels?: Record<string, string>
  latestReadyRevision: string
  latestCreatedRevision: string
  ingress: string
  template: { containers: CloudRunContainer[] }
  terminalCondition?: { type: string; state: string }
}

export type CloudRunRevision = {
  name: string
  createTime: string
  containers: CloudRunContainer[]
  conditions?: { type: string; state: string }[]
}

export type CloudRunInstance = {
  service: string
  revision: string
  port: number
  pid?: number
  mode: string
  startedAt: string
  requestCount: number
}

export type CloudRunServicesResponse = { services: CloudRunService[] }
export type CloudRunRevisionsResponse = { revisions: CloudRunRevision[] }
export type CloudRunInstancesResponse = { instances: CloudRunInstance[] }
export type CloudRunLogsResponse = { lines: string[] }
