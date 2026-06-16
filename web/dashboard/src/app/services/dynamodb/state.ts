import type {
  DynamoDBIndex,
  DynamoDBItemSnapshot,
  DynamoDBStatus,
  DynamoDBStreamsResponse,
  DynamoDBTableSummary,
  DynamoDBTimeToLiveDescription,
} from './types'

export type TablesState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: DynamoDBStatus; tables: DynamoDBTableSummary[] }
  | { status: 'error'; message: string }

export type ItemsState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; items: DynamoDBItemSnapshot[] }
  | { status: 'error'; message: string }

export type TableDetailState =
  | { status: 'idle' }
  | { status: 'loading' }
  | {
      status: 'success'
      table: DynamoDBTableSummary
      globalSecondaryIndexes: DynamoDBIndex[]
      localSecondaryIndexes: DynamoDBIndex[]
      ttl?: DynamoDBTimeToLiveDescription
      streams: DynamoDBStreamsResponse
    }
  | { status: 'error'; message: string }

export type QueryScanPageState = {
  pageHistory: Array<Record<string, unknown> | undefined>
  pageIndex: number
  selectedItemIndex: number
}

export type RecentDynamoDBOperation = {
  id: string
  operation: 'Query' | 'Scan'
  tableName: string
  indexName?: string
  limit: number
  expressionSummary: string
  count: number
  scannedCount: number
  page: number
  hasMore: boolean
  createdAt: string
}
