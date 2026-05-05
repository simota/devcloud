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
  job?: BigQueryJobResource
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

export type BigQueryJobResponse = {
  projectId: string
  jobId: string
  job: BigQueryJob
}

export type BigQueryJobReference = {
  projectId: string
  jobId: string
  location?: string
}

export type BigQueryJobResource = {
  kind?: string
  id?: string
  selfLink?: string
  jobReference: BigQueryJobReference
  configuration?: {
    dryRun?: boolean
    query?: {
      query?: string
      useLegacySql?: boolean
      queryParameters?: unknown[]
    }
  }
  status: {
    state: string
  }
  statistics?: {
    creationTime?: string
    startTime?: string
    endTime?: string
    query?: {
      totalRows?: string
      cacheHit?: boolean
      dryRun?: boolean
    }
  }
}

export type BigQueryTableCell = {
  v: unknown
}

export type BigQueryTableDataRow = {
  f: BigQueryTableCell[]
}

export type BigQueryQueryRequest = {
  query: string
  maxResults?: number
  dryRun?: boolean
  useLegacySql: false
}

export type BigQueryQueryResponse = {
  kind: string
  schema?: BigQuerySchema
  jobReference: BigQueryJobReference
  totalRows: string
  pageToken?: string
  rows?: BigQueryTableDataRow[]
  jobComplete: boolean
  cacheHit: boolean
}

export type BigQueryDatasetCreateRequest = {
  datasetReference: {
    datasetId: string
    projectId?: string
  }
  location?: string
  friendlyName?: string
  description?: string
  labels?: Record<string, string>
}

export type BigQueryDatasetResource = BigQueryDatasetCreateRequest & {
  kind?: string
  id?: string
  selfLink?: string
  etag?: string
  creationTime?: string
  lastModifiedTime?: string
}

export type BigQueryTableCreateRequest = {
  tableReference: {
    tableId: string
    datasetId?: string
    projectId?: string
  }
  schema?: BigQuerySchema
  friendlyName?: string
  description?: string
  type?: string
  labels?: Record<string, string>
}

export type BigQueryTableResource = BigQueryTableCreateRequest & {
  kind?: string
  id?: string
  selfLink?: string
  etag?: string
  creationTime?: string
  lastModifiedTime?: string
  numRows?: string
  numBytes?: string
  location?: string
}

export type BigQueryInsertAllRow = {
  insertId?: string
  json: Record<string, unknown>
}

export type BigQueryInsertAllRequest = {
  skipInvalidRows?: boolean
  ignoreUnknownValues?: boolean
  rows: BigQueryInsertAllRow[]
}

export type BigQueryInsertError = {
  index: number
  errors: Array<{
    reason: string
    location?: string
    message: string
  }>
}

export type BigQueryInsertAllResponse = {
  kind: string
  insertErrors?: BigQueryInsertError[]
}
