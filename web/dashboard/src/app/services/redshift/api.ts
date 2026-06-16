import { fetchJSON } from '../../api/client'
import type {
  RedshiftCatalogResponse,
  RedshiftClustersResponse,
  RedshiftQueryRequest,
  RedshiftQueryResponse,
  RedshiftStatementsResponse,
  RedshiftStatus,
} from './types'

export async function getRedshiftStatus(): Promise<RedshiftStatus> {
  return fetchJSON<RedshiftStatus>('/api/redshift/status')
}

export async function listRedshiftClusters(): Promise<RedshiftClustersResponse> {
  return fetchJSON<RedshiftClustersResponse>('/api/redshift/clusters')
}

export async function getRedshiftCatalog(): Promise<RedshiftCatalogResponse> {
  return fetchJSON<RedshiftCatalogResponse>('/api/redshift/catalog')
}

export async function listRedshiftStatements(): Promise<RedshiftStatementsResponse> {
  return fetchJSON<RedshiftStatementsResponse>('/api/redshift/statements')
}

export async function runRedshiftQuery(input: RedshiftQueryRequest): Promise<RedshiftQueryResponse> {
  return fetchJSON<RedshiftQueryResponse>('/api/redshift/query', {
    method: 'POST',
    body: input,
    timeoutMs: 15000,
  })
}
