export type DynamoDBStatus = {
  status: string
  running: boolean
  endpoint: string
  region: string
  storagePath: string
  tableCount: number
}

export type DynamoDBKeySchemaElement = {
  AttributeName: string
  KeyType: string
}

export type DynamoDBAttributeDefinition = {
  AttributeName: string
  AttributeType: string
}

export type DynamoDBIndex = {
  IndexName: string
  KeySchema?: DynamoDBKeySchemaElement[]
  ItemCount?: number
}

export type DynamoDBStreamSpecification = {
  StreamEnabled: boolean
  StreamViewType?: string
}

export type DynamoDBTimeToLiveDescription = {
  AttributeName?: string
  TimeToLiveStatus: string
}

export type DynamoDBTableSummary = {
  tableName: string
  tableStatus: string
  itemCount: number
  keySchema?: DynamoDBKeySchemaElement[]
  attributeDefinitions?: DynamoDBAttributeDefinition[]
  globalSecondaryIndexes?: DynamoDBIndex[]
  localSecondaryIndexes?: DynamoDBIndex[]
  latestStreamArn?: string
  latestStreamLabel?: string
  streamSpecification?: DynamoDBStreamSpecification
  timeToLiveDescription?: DynamoDBTimeToLiveDescription
}

export type DynamoDBItemSnapshot = {
  key: Record<string, unknown>
  item: Record<string, unknown>
}

export type DynamoDBTablesResponse = {
  tables: DynamoDBTableSummary[]
}

export type DynamoDBItemsResponse = {
  tableName: string
  items: DynamoDBItemSnapshot[]
}

export type DynamoDBTableResponse = {
  table: DynamoDBTableSummary
}

export type DynamoDBIndexesResponse = {
  tableName: string
  globalSecondaryIndexes?: DynamoDBIndex[]
  localSecondaryIndexes?: DynamoDBIndex[]
}

export type DynamoDBTTLResponse = {
  tableName: string
  timeToLiveDescription?: DynamoDBTimeToLiveDescription
}

export type DynamoDBStreamsResponse = {
  tableName: string
  streamEnabled: boolean
  latestStreamArn?: string
  latestStreamLabel?: string
  streamSpecification?: DynamoDBStreamSpecification
}
