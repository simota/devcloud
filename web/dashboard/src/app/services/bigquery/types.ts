export type BigQueryStatus = {
  status: string
  running: boolean
  endpoint: string
  project: string
  location: string
  authMode: string
  storagePath: string
  datasetCount: number
  jobCount: number
}

export type BigQueryField = {
  name: string
  type?: string
  mode?: string
  fields?: BigQueryField[]
}

export type BigQuerySchema = {
  fields?: BigQueryField[]
}

export type BigQueryTable = {
  id: string
  projectId: string
  datasetId: string
  tableId: string
  type: string
  friendlyName?: string
  description?: string
  numRows: string
  numBytes: string
  schema: BigQuerySchema
}

export type BigQueryDataset = {
  id: string
  projectId: string
  datasetId: string
  location?: string
  friendlyName?: string
  description?: string
  tables: BigQueryTable[]
}

export type BigQueryRow = {
  insertId?: string
  insertedAt?: string
  json: Record<string, unknown>
}

export type BigQueryJob = {
  projectId: string
  jobId: string
  location?: string
  state: string
}

export type BigQueryProjectsResponse = {
  projects: Array<{
    projectId: string
    location: string
    datasetCount: number
    jobCount: number
    datasets: BigQueryDataset[]
    jobs: BigQueryJob[]
  }>
}

export type BigQueryDatasetsResponse = {
  projectId: string
  datasets: BigQueryDataset[]
}

export type BigQueryRowsResponse = {
  projectId: string
  datasetId: string
  tableId: string
  rows: BigQueryRow[]
}

export type BigQueryJobsResponse = {
  projectId: string
  jobs: BigQueryJob[]
}
