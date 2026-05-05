export type RedshiftStatus = {
  service: string
  status: string
  running: boolean
  sqlEndpoint: string
  apiEndpoint: string
  region: string
  clusterCount: number
  storagePath: string
  backendKind: string
  backendMode: string
}

export type RedshiftCluster = {
  clusterIdentifier: string
  nodeType: string
  clusterStatus: string
  databaseName: string
  masterUsername: string
  endpoint: {
    address: string
    port: number
  }
  numberOfNodes: number
  tags?: Array<{
    key: string
    value: string
  }>
}

export type RedshiftSchema = {
  name: string
  owner: string
  tableCount: number
}

export type RedshiftTable = {
  schema: string
  name: string
  columnCount: number
  rowCount: number
  distStyle: string
  distKey: string
  sortKeys: string[]
}

export type RedshiftColumn = {
  schema: string
  table: string
  name: string
  ordinal: number
  dataType: string
  encoding: string
  defaultValue: string
  identity: boolean
}

export type RedshiftCatalog = {
  database: string
  schemas: RedshiftSchema[]
  tables: RedshiftTable[]
  columns: RedshiftColumn[]
}

export type RedshiftStatement = {
  id: string
  clusterIdentifier: string
  database: string
  dbUser: string
  sessionId?: string
  queryPreview: string
  queryRedacted: boolean
  queryTruncated: boolean
  status: string
  error?: string
  createdAt: number
  updatedAt: number
  hasResultSet: boolean
  resultRows: number
  redshiftQueryId: number
}

export type RedshiftClustersResponse = {
  clusters: RedshiftCluster[]
}

export type RedshiftCatalogResponse = {
  catalog: RedshiftCatalog
}

export type RedshiftStatementsResponse = {
  statements: RedshiftStatement[]
}

export type RedshiftQueryRequest = {
  sql: string
  maxRows: number
}

export type RedshiftQueryField = {
  name: string
  typeName: string
}

export type RedshiftQueryResult = {
  statement: RedshiftStatement
  columns: RedshiftQueryField[]
  rows: string[][]
  rowCount: number
  commandTag: string
}

export type RedshiftQueryResponse = {
  result: RedshiftQueryResult
}
