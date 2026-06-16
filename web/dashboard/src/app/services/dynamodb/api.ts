import { fetchJSON } from '../../api/client'
import type {
  DynamoDBIndexesResponse,
  DynamoDBItemsResponse,
  DynamoDBOperationResponse,
  DynamoDBQueryScanResponse,
  DynamoDBStatus,
  DynamoDBStreamsResponse,
  DynamoDBTableResponse,
  DynamoDBTablesResponse,
  DynamoDBTTLResponse,
} from './types'

export async function getDynamoDBStatus(): Promise<DynamoDBStatus> {
  return fetchJSON<DynamoDBStatus>('/api/dynamodb/status')
}

export async function listDynamoDBTables(): Promise<DynamoDBTablesResponse> {
  return fetchJSON<DynamoDBTablesResponse>('/api/dynamodb/tables')
}

export async function createDynamoDBTable(input: Record<string, unknown>): Promise<DynamoDBOperationResponse> {
  return fetchJSON<DynamoDBOperationResponse>('/api/dynamodb/tables', {
    method: 'POST',
    body: { input },
  })
}

export async function listDynamoDBItems(tableName: string, limit = 100): Promise<DynamoDBItemsResponse> {
  return fetchJSON<DynamoDBItemsResponse>(
    `/api/dynamodb/tables/${encodeURIComponent(tableName)}/items?limit=${encodeURIComponent(String(limit))}`,
  )
}

export async function putDynamoDBItem(tableName: string, input: Record<string, unknown>): Promise<DynamoDBOperationResponse> {
  return fetchJSON<DynamoDBOperationResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/items`, {
    method: 'POST',
    body: { input },
  })
}

export async function updateDynamoDBItem(tableName: string, input: Record<string, unknown>): Promise<DynamoDBOperationResponse> {
  return fetchJSON<DynamoDBOperationResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/items/update`, {
    method: 'POST',
    body: { input },
  })
}

export async function deleteDynamoDBItem(
  tableName: string,
  input: Record<string, unknown>,
  confirmation: string,
): Promise<DynamoDBOperationResponse> {
  return fetchJSON<DynamoDBOperationResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/items/delete`, {
    method: 'POST',
    body: { input, confirmation },
  })
}

export async function queryDynamoDBItems(
  tableName: string,
  input: Record<string, unknown>,
): Promise<DynamoDBQueryScanResponse> {
  return fetchJSON<DynamoDBQueryScanResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/query`, {
    method: 'POST',
    body: { input },
  })
}

export async function scanDynamoDBItems(
  tableName: string,
  input: Record<string, unknown>,
): Promise<DynamoDBQueryScanResponse> {
  return fetchJSON<DynamoDBQueryScanResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/scan`, {
    method: 'POST',
    body: { input },
  })
}

export async function getDynamoDBTable(tableName: string): Promise<DynamoDBTableResponse> {
  return fetchJSON<DynamoDBTableResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}`)
}

export async function getDynamoDBIndexes(tableName: string): Promise<DynamoDBIndexesResponse> {
  return fetchJSON<DynamoDBIndexesResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/indexes`)
}

export async function getDynamoDBTTL(tableName: string): Promise<DynamoDBTTLResponse> {
  return fetchJSON<DynamoDBTTLResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/ttl`)
}

export async function updateDynamoDBTTL(tableName: string, input: Record<string, unknown>): Promise<DynamoDBOperationResponse> {
  return fetchJSON<DynamoDBOperationResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/ttl`, {
    method: 'POST',
    body: { input },
  })
}

export async function deleteDynamoDBTable(
  tableName: string,
  input: Record<string, unknown>,
  confirmation: string,
): Promise<DynamoDBOperationResponse> {
  return fetchJSON<DynamoDBOperationResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/delete`, {
    method: 'POST',
    body: { input, confirmation },
  })
}

export async function getDynamoDBStreams(tableName: string): Promise<DynamoDBStreamsResponse> {
  return fetchJSON<DynamoDBStreamsResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/streams`)
}
