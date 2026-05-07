import type { DashboardService } from '../dashboard/types'
import { maxRecentDynamoDBOperations, recentDynamoDBOperationsStorageKey } from './constants'
import type { RecentDynamoDBOperation } from './state'
import type {
  DynamoDBStatus,
  DynamoDBStreamsResponse,
  DynamoDBTableSummary,
} from './types'

export function disabledStatus(service?: DashboardService): DynamoDBStatus {
  return {
    status: 'disabled',
    running: false,
    endpoint: service?.endpoint ?? 'http://127.0.0.1:8000',
    region: 'us-east-1',
    storagePath: service?.storagePath ?? '.devcloud/data/dynamodb',
    tableCount: 0,
  }
}

export function keySchemaLabel(table: DynamoDBTableSummary): string {
  const keys = table.keySchema ?? []
  if (keys.length === 0) {
    return 'No key schema'
  }
  return keys.map((key) => `${key.AttributeName} ${key.KeyType}`).join(' / ')
}

export function indexCount(table: DynamoDBTableSummary): number {
  return (table.globalSecondaryIndexes ?? []).length + (table.localSecondaryIndexes ?? []).length
}

export function indexNames(table: DynamoDBTableSummary): string {
  const indexes = [...(table.globalSecondaryIndexes ?? []), ...(table.localSecondaryIndexes ?? [])]
  if (indexes.length === 0) {
    return 'none'
  }
  return indexes.map((index) => index.IndexName).join(', ')
}

export function attributeDefinitionsLabel(table: DynamoDBTableSummary): string {
  const attributes = table.attributeDefinitions ?? []
  if (attributes.length === 0) {
    return 'none'
  }
  return attributes.map((attribute) => `${attribute.AttributeName} ${attribute.AttributeType}`).join(', ')
}

export function ttlLabel(table: DynamoDBTableSummary): string {
  const ttl = table.timeToLiveDescription
  if (!ttl || ttl.TimeToLiveStatus === '') {
    return 'not configured'
  }
  return ttl.AttributeName ? `${ttl.TimeToLiveStatus} on ${ttl.AttributeName}` : ttl.TimeToLiveStatus
}

export function streamLabel(streams: DynamoDBStreamsResponse): string {
  if (!streams.streamEnabled) {
    return 'disabled'
  }
  const viewType = streams.streamSpecification?.StreamViewType ?? 'enabled'
  return streams.latestStreamLabel ? `${viewType} (${streams.latestStreamLabel})` : viewType
}

export function normalizedItemLimit(value: string): number {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 100
  }
  return Math.min(parsed, 1000)
}

export type ParsedJSONForm = { ok: true; value: Record<string, unknown> } | { ok: false; message: string }

export function parseJSONForm(value: string): ParsedJSONForm {
  try {
    const parsed = JSON.parse(value) as unknown
    if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
      return { ok: false, message: 'Input must be a JSON object.' }
    }
    return { ok: true, value: parsed as Record<string, unknown> }
  } catch (error) {
    return { ok: false, message: error instanceof Error ? error.message : 'Input must be valid JSON.' }
  }
}

export function parseOptionalJSONForm(value: string): { ok: true; value?: Record<string, unknown> } | { ok: false; message: string } {
  if (value.trim() === '') {
    return { ok: true }
  }
  const parsed = parseJSONForm(value)
  if (!parsed.ok) {
    return parsed
  }
  return { ok: true, value: parsed.value }
}

export function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'null'
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

export function attributeText(value: unknown): string {
  if (value === null || value === undefined) {
    return ''
  }
  if (typeof value === 'string') {
    return value
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value)
  }
  return JSON.stringify(value)
}

export function formatBytes(size: number): string {
  if (!Number.isFinite(size) || size < 0) {
    return 'unknown'
  }
  if (size < 1024) {
    return `${size} B`
  }
  return `${(size / 1024).toFixed(1)} KB`
}

export function readRecentDynamoDBOperations(): RecentDynamoDBOperation[] {
  if (typeof window === 'undefined') {
    return []
  }
  try {
    const raw = window.localStorage.getItem(recentDynamoDBOperationsStorageKey)
    if (!raw) {
      return []
    }
    const parsed = JSON.parse(raw) as unknown
    if (!Array.isArray(parsed)) {
      return []
    }
    return parsed.filter(isRecentDynamoDBOperation).slice(0, maxRecentDynamoDBOperations)
  } catch {
    return []
  }
}

export function writeRecentDynamoDBOperations(operations: RecentDynamoDBOperation[]): void {
  if (typeof window === 'undefined') {
    return
  }
  try {
    if (operations.length === 0) {
      window.localStorage.removeItem(recentDynamoDBOperationsStorageKey)
      return
    }
    window.localStorage.setItem(recentDynamoDBOperationsStorageKey, JSON.stringify(operations.slice(0, maxRecentDynamoDBOperations)))
  } catch {
    // Best-effort UI history only; dashboard operations must not fail because localStorage is unavailable.
  }
}

export function isRecentDynamoDBOperation(value: unknown): value is RecentDynamoDBOperation {
  if (!value || Array.isArray(value) || typeof value !== 'object') {
    return false
  }
  const candidate = value as Partial<RecentDynamoDBOperation>
  return (
    (candidate.operation === 'Query' || candidate.operation === 'Scan') &&
    typeof candidate.id === 'string' &&
    typeof candidate.tableName === 'string' &&
    typeof candidate.limit === 'number' &&
    typeof candidate.expressionSummary === 'string' &&
    typeof candidate.count === 'number' &&
    typeof candidate.scannedCount === 'number' &&
    typeof candidate.page === 'number' &&
    typeof candidate.hasMore === 'boolean' &&
    typeof candidate.createdAt === 'string'
  )
}

export function buildRecentDynamoDBOperation(input: {
  count: number
  hasMore: boolean
  indexName: string
  limit: number
  mode: 'Query' | 'Scan'
  page: number
  scannedCount: number
  tableName: string
  usesExpressionAttributeValues: boolean
}): RecentDynamoDBOperation {
  return {
    id: `${Date.now()}-${input.mode}-${input.tableName}-${input.page}`,
    operation: input.mode,
    tableName: input.tableName,
    indexName: input.indexName.trim() || undefined,
    limit: input.limit,
    expressionSummary: input.usesExpressionAttributeValues ? 'expression values present' : 'no expression values',
    count: input.count,
    scannedCount: input.scannedCount,
    page: input.page,
    hasMore: input.hasMore,
    createdAt: new Date().toISOString(),
  }
}

export function formatRecentOperationTime(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return 'unknown'
  }
  return date.toLocaleString()
}
