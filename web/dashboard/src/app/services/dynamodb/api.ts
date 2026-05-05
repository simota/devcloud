import { fetchJSON } from '../../api/client'
import type {
  DynamoDBIndexesResponse,
  DynamoDBItemsResponse,
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

export async function listDynamoDBItems(tableName: string, limit = 100): Promise<DynamoDBItemsResponse> {
  return fetchJSON<DynamoDBItemsResponse>(
    `/api/dynamodb/tables/${encodeURIComponent(tableName)}/items?limit=${encodeURIComponent(String(limit))}`,
  )
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

export async function getDynamoDBStreams(tableName: string): Promise<DynamoDBStreamsResponse> {
  return fetchJSON<DynamoDBStreamsResponse>(`/api/dynamodb/tables/${encodeURIComponent(tableName)}/streams`)
}
