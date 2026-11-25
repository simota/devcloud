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
  invokerIamDisabled?: boolean
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

export type CloudRunIamBinding = {
  role: string
  members: string[]
}

export type CloudRunIamPolicy = {
  version?: number
  etag?: string
  bindings?: CloudRunIamBinding[]
}

export type CloudRunOperation = {
  name: string
  done: boolean
  response?: CloudRunService
}

export type CloudRunCreateServiceInput = {
  project?: string
  location?: string
  serviceId: string
  service: {
    ingress?: string
    labels?: Record<string, string>
    template: {
      containers: CloudRunContainer[]
    }
  }
}

export type CloudRunUpdateServiceInput = {
  updateMask?: string
  service: {
    ingress?: string
    labels?: Record<string, string>
    template?: {
      containers: CloudRunContainer[]
    }
  }
}

export type CloudRunServicesResponse = { services: CloudRunService[] }
export type CloudRunRevisionsResponse = { revisions: CloudRunRevision[] }
export type CloudRunInstancesResponse = { instances: CloudRunInstance[] }
export type CloudRunLogsResponse = { lines: string[] }
