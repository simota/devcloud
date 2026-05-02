import { fetchJSON } from '../../api/client'
import type {
  BigQueryDatasetsResponse,
  BigQueryJobsResponse,
  BigQueryProjectsResponse,
  BigQueryRowsResponse,
  BigQueryStatus,
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
