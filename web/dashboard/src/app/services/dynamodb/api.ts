import { fetchJSON } from '../../api/client'
import type { DynamoDBItemsResponse, DynamoDBStatus, DynamoDBTablesResponse } from './types'

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
