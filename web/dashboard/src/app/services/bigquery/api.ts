import { fetchJSON } from '../../api/client'
import type {
  BigQueryDatasetsResponse,
  BigQueryDatasetCreateRequest,
  BigQueryDatasetResource,
  BigQueryInsertAllRequest,
  BigQueryInsertAllResponse,
  BigQueryJobsResponse,
  BigQueryJobResponse,
  BigQueryProjectsResponse,
  BigQueryQueryRequest,
  BigQueryQueryResponse,
  BigQueryRowsResponse,
  BigQueryStatus,
  BigQueryTableCreateRequest,
  BigQueryTableResource,
} from './types'

export async function getBigQueryStatus(): Promise<BigQueryStatus> {
  return fetchJSON<BigQueryStatus>('/api/bigquery/status')
}

export async function listBigQueryProjects(): Promise<BigQueryProjectsResponse> {
  return fetchJSON<BigQueryProjectsResponse>('/api/bigquery/projects')
}

export async function listBigQueryDatasets(projectId: string): Promise<BigQueryDatasetsResponse> {
  return fetchJSON<BigQueryDatasetsResponse>(`/api/bigquery/projects/${encodeURIComponent(projectId)}/datasets`)
}

export async function listBigQueryRows(
  projectId: string,
  datasetId: string,
  tableId: string,
  limit = 100,
): Promise<BigQueryRowsResponse> {
  return fetchJSON<BigQueryRowsResponse>(
    `/api/bigquery/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables/${encodeURIComponent(tableId)}/rows?limit=${encodeURIComponent(String(limit))}`,
  )
}

export async function listBigQueryJobs(projectId: string): Promise<BigQueryJobsResponse> {
  return fetchJSON<BigQueryJobsResponse>(`/api/bigquery/projects/${encodeURIComponent(projectId)}/jobs`)
}

export async function getBigQueryJob(projectId: string, jobId: string): Promise<BigQueryJobResponse> {
  return fetchJSON<BigQueryJobResponse>(
    `/api/bigquery/projects/${encodeURIComponent(projectId)}/jobs/${encodeURIComponent(jobId)}`,
  )
}

export async function runBigQueryQuery(
  projectId: string,
  request: BigQueryQueryRequest,
): Promise<BigQueryQueryResponse> {
  return fetchJSON<BigQueryQueryResponse>(`/api/bigquery/projects/${encodeURIComponent(projectId)}/queries`, {
    method: 'POST',
    body: request,
    timeoutMs: 15000,
  })
}

export async function createBigQueryDataset(
  projectId: string,
  request: BigQueryDatasetCreateRequest,
): Promise<BigQueryDatasetResource> {
  return fetchJSON<BigQueryDatasetResource>(`/api/bigquery/projects/${encodeURIComponent(projectId)}/datasets`, {
    method: 'POST',
    body: request,
  })
}

export async function createBigQueryTable(
  projectId: string,
  datasetId: string,
  request: BigQueryTableCreateRequest,
): Promise<BigQueryTableResource> {
  return fetchJSON<BigQueryTableResource>(
    `/api/bigquery/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables`,
    {
      method: 'POST',
      body: request,
    },
  )
}

export async function insertBigQueryRows(
  projectId: string,
  datasetId: string,
  tableId: string,
  request: BigQueryInsertAllRequest,
): Promise<BigQueryInsertAllResponse> {
  return fetchJSON<BigQueryInsertAllResponse>(
    `/api/bigquery/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables/${encodeURIComponent(tableId)}/insertAll`,
    {
      method: 'POST',
      body: request,
    },
  )
}
