import { useCallback, useEffect, useMemo, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import type { DashboardService } from '../dashboard/types'
import {
  getDynamoDBIndexes,
  getDynamoDBStatus,
  getDynamoDBStreams,
  getDynamoDBTable,
  getDynamoDBTTL,
  listDynamoDBItems,
  listDynamoDBTables,
} from './api'
import type {
  DynamoDBIndex,
  DynamoDBItemSnapshot,
  DynamoDBStatus,
  DynamoDBStreamsResponse,
  DynamoDBTableSummary,
  DynamoDBTimeToLiveDescription,
} from './types'

type TablesState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: DynamoDBStatus; tables: DynamoDBTableSummary[] }
  | { status: 'error'; message: string }

type ItemsState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; items: DynamoDBItemSnapshot[] }
  | { status: 'error'; message: string }

type TableDetailState =
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

type DynamoDBDashboardProps = {
  service?: DashboardService
}

export function DynamoDBDashboard({ service }: DynamoDBDashboardProps): JSX.Element {
  const [tablesState, setTablesState] = useState<TablesState>({ status: 'loading' })
  const [itemsState, setItemsState] = useState<ItemsState>({ status: 'idle' })
  const [tableDetailState, setTableDetailState] = useState<TableDetailState>({ status: 'idle' })
  const [activeTableName, setActiveTableName] = useState<string>()
  const [activeItemIndex, setActiveItemIndex] = useState(0)
  const [tableFilter, setTableFilter] = useState('')
  const [itemFilter, setItemFilter] = useState('')
  const [itemLimit, setItemLimit] = useState('100')
  const [keyLookupValues, setKeyLookupValues] = useState<Record<string, string>>({})
  const [keyLookupMessage, setKeyLookupMessage] = useState('')
  const isDisabled = service?.status === 'disabled'

  const refreshTables = useCallback(() => {
    if (isDisabled) {
      setTablesState({ status: 'success', statusPayload: disabledStatus(service), tables: [] })
      setItemsState({ status: 'idle' })
      setTableDetailState({ status: 'idle' })
      setActiveTableName(undefined)
      return
    }

    setTablesState({ status: 'loading' })
    Promise.all([getDynamoDBStatus(), listDynamoDBTables()])
      .then(([statusPayload, { tables }]) => {
        setTablesState({ status: 'success', statusPayload, tables })
        setActiveTableName((current) =>
          current && tables.some((table) => table.tableName === current) ? current : tables[0]?.tableName,
        )
      })
      .catch((error: Error) => {
        setTablesState({ status: 'error', message: error.message })
      })
  }, [isDisabled, service])

  useEffect(() => {
    refreshTables()
  }, [refreshTables])

  const tables = tablesState.status === 'success' ? tablesState.tables : []
  const activeTable = tables.find((table) => table.tableName === activeTableName)

  const refreshItems = useCallback(() => {
    if (!activeTableName || isDisabled) {
      setItemsState({ status: 'idle' })
      return
    }
    setItemsState({ status: 'loading' })
    listDynamoDBItems(activeTableName, normalizedItemLimit(itemLimit))
      .then(({ items }) => {
        setActiveItemIndex(0)
        setItemsState({ status: 'success', items })
      })
      .catch((error: Error) => {
        setItemsState({ status: 'error', message: error.message })
      })
  }, [activeTableName, isDisabled, itemLimit])

  useEffect(() => {
    refreshItems()
  }, [refreshItems])

  const refreshTableDetail = useCallback(() => {
    if (!activeTableName || isDisabled) {
      setTableDetailState({ status: 'idle' })
      return
    }
    setTableDetailState({ status: 'loading' })
    Promise.all([
      getDynamoDBTable(activeTableName),
      getDynamoDBIndexes(activeTableName),
      getDynamoDBTTL(activeTableName),
      getDynamoDBStreams(activeTableName),
    ])
      .then(([table, indexes, ttl, streams]) => {
        setTableDetailState({
          status: 'success',
          table: table.table,
          globalSecondaryIndexes: indexes.globalSecondaryIndexes ?? [],
          localSecondaryIndexes: indexes.localSecondaryIndexes ?? [],
          ttl: ttl.timeToLiveDescription,
          streams,
        })
      })
      .catch((error: Error) => {
        setTableDetailState({ status: 'error', message: error.message })
      })
  }, [activeTableName, isDisabled])

  useEffect(() => {
    refreshTableDetail()
  }, [refreshTableDetail])

  const filteredTables = useMemo(() => {
    const query = tableFilter.trim().toLowerCase()
    if (query === '') {
      return tables
    }
    return tables.filter((table) => table.tableName.toLowerCase().includes(query))
  }, [tables, tableFilter])

  const filteredItems = useMemo(() => {
    const items = itemsState.status === 'success' ? itemsState.items : []
    const query = itemFilter.trim().toLowerCase()
    if (query === '') {
      return items
    }
    return items.filter((entry) => JSON.stringify(entry).toLowerCase().includes(query))
  }, [itemsState, itemFilter])

  const selectedItem = filteredItems[Math.min(activeItemIndex, Math.max(filteredItems.length - 1, 0))]

  if (isDisabled) {
    return (
      <Panel title="DynamoDB">
        <EmptyState
          title="DynamoDB is disabled"
          description="Enable the DynamoDB service in devcloud config to inspect tables and items."
        />
      </Panel>
    )
  }

  function selectTable(tableName: string): void {
    setActiveTableName(tableName)
    setActiveItemIndex(0)
    setItemFilter('')
    setKeyLookupValues({})
    setKeyLookupMessage('')
  }

  function updateKeyLookupValue(attributeName: string, value: string): void {
    setKeyLookupValues((current) => ({ ...current, [attributeName]: value }))
    setKeyLookupMessage('')
  }

  function findLoadedItemByKey(): void {
    if (!activeTable || itemsState.status !== 'success') {
      setKeyLookupMessage('Load table items before using key lookup.')
      return
    }
    const keyAttributes = activeTable.keySchema?.map((key) => key.AttributeName) ?? []
    const expectedValues = keyAttributes
      .map((attributeName) => [attributeName, keyLookupValues[attributeName]?.trim() ?? ''] as const)
      .filter(([, value]) => value !== '')
    if (expectedValues.length === 0) {
      setKeyLookupMessage('Enter at least one key value.')
      return
    }
    const matchedIndex = itemsState.items.findIndex((entry) =>
      expectedValues.every(([attributeName, expected]) => attributeText(entry.item[attributeName]) === expected),
    )
    if (matchedIndex < 0) {
      setKeyLookupMessage(`No loaded item matched ${expectedValues.map(([key]) => key).join(' / ')}.`)
      return
    }
    setItemFilter('')
    setActiveItemIndex(matchedIndex)
    setKeyLookupMessage(`Selected loaded item ${matchedIndex + 1}.`)
  }

  return (
    <div className="dynamodb-workspace">
      <Panel title="Tables">
        <div className="dynamodb-toolbar">
          <label className="compact-filter">
            <span>Filter</span>
            <input
              aria-label="Filter DynamoDB tables"
              onChange={(event) => setTableFilter(event.target.value)}
              placeholder="table name"
              type="search"
              value={tableFilter}
            />
          </label>
          <Button onClick={refreshTables}>Refresh</Button>
        </div>
        {tablesState.status === 'loading' ? (
          <EmptyState title="Loading tables" description="Reading local DynamoDB table metadata." />
        ) : null}
        {tablesState.status === 'error' ? (
          <EmptyState
            title="DynamoDB tables unavailable"
            description={tablesState.message}
            actionLabel="Retry"
            onAction={refreshTables}
          />
        ) : null}
        {tablesState.status === 'success' ? (
          <TableList tables={filteredTables} activeTableName={activeTableName} onSelectTable={selectTable} />
        ) : null}
      </Panel>

      <Panel title="Items">
        <div className="dynamodb-toolbar">
          <span className="toolbar-count">
            {activeTable ? `${filteredItems.length} shown / ${activeTable.itemCount} reported` : 'Select a table'}
          </span>
          <label className="compact-filter">
            <span>Filter</span>
            <input
              aria-label="Filter DynamoDB items"
              disabled={!activeTable}
              onChange={(event) => {
                setActiveItemIndex(0)
                setItemFilter(event.target.value)
              }}
              placeholder="attribute value"
              type="search"
              value={itemFilter}
            />
          </label>
          <label className="compact-filter small">
            <span>Limit</span>
            <input
              aria-label="Limit DynamoDB items"
              disabled={!activeTable}
              inputMode="numeric"
              onChange={(event) => setItemLimit(event.target.value)}
              value={itemLimit}
            />
          </label>
          <Button disabled={!activeTable} onClick={refreshItems}>
            Refresh
          </Button>
        </div>
        <ItemBrowser
          activeIndex={activeItemIndex}
          items={filteredItems}
          itemsState={itemsState}
          onSelectIndex={setActiveItemIndex}
          tableName={activeTableName}
        />
        <KeyLookup
          message={keyLookupMessage}
          onFind={findLoadedItemByKey}
          onUpdateValue={updateKeyLookupValue}
          table={activeTable}
          values={keyLookupValues}
        />
      </Panel>

      <Panel title="Inspector">
        <TableInspector
          detailState={tableDetailState}
          item={selectedItem}
          onRefreshDetail={refreshTableDetail}
          status={tablesState.status === 'success' ? tablesState.statusPayload : undefined}
          table={activeTable}
        />
      </Panel>
    </div>
  )
}

type KeyLookupProps = {
  message: string
  onFind: () => void
  onUpdateValue: (attributeName: string, value: string) => void
  table?: DynamoDBTableSummary
  values: Record<string, string>
}

function KeyLookup({ message, onFind, onUpdateValue, table, values }: KeyLookupProps): JSX.Element | null {
  const keys = table?.keySchema ?? []
  if (!table || keys.length === 0) {
    return null
  }
  return (
    <div className="dynamodb-key-lookup">
      <span className="inspector-label">Key lookup</span>
      <div className="pubsub-action-row">
        {keys.map((key) => (
          <label className="compact-filter" key={key.AttributeName}>
            <span>
              {key.AttributeName} {key.KeyType}
            </span>
            <input
              aria-label={`DynamoDB key lookup ${key.AttributeName}`}
              onChange={(event) => onUpdateValue(key.AttributeName, event.target.value)}
              placeholder={key.AttributeName}
              value={values[key.AttributeName] ?? ''}
            />
          </label>
        ))}
        <Button onClick={onFind}>Find loaded item</Button>
      </div>
      {message ? <p className="inspector-muted">{message}</p> : null}
    </div>
  )
}

type TableListProps = {
  tables: DynamoDBTableSummary[]
  activeTableName?: string
  onSelectTable: (tableName: string) => void
}

function TableList({ tables, activeTableName, onSelectTable }: TableListProps): JSX.Element {
  if (tables.length === 0) {
    return <EmptyState title="No tables" description="Tables created through the DynamoDB API will appear here." />
  }

  return (
    <div className="dynamodb-table-list" aria-label="DynamoDB tables">
      {tables.map((table) => (
        <button
          className={table.tableName === activeTableName ? 'dynamodb-table-row active' : 'dynamodb-table-row'}
          key={table.tableName}
          onClick={() => onSelectTable(table.tableName)}
        >
          <span className="table-row-top">
            <span className="table-row-name">{table.tableName}</span>
            <span className="count-pill">{table.itemCount}</span>
          </span>
          <span className="table-row-meta">{keySchemaLabel(table)}</span>
          <span className="table-row-tags">
            <span>{table.tableStatus}</span>
            <span>{indexCount(table)} indexes</span>
            <span>{table.streamSpecification?.StreamEnabled ? 'streams on' : 'streams off'}</span>
          </span>
        </button>
      ))}
    </div>
  )
}

type ItemBrowserProps = {
  activeIndex: number
  items: DynamoDBItemSnapshot[]
  itemsState: ItemsState
  tableName?: string
  onSelectIndex: (index: number) => void
}

function ItemBrowser({ activeIndex, items, itemsState, onSelectIndex, tableName }: ItemBrowserProps): JSX.Element {
  if (!tableName) {
    return <EmptyState title="No table selected" description="Choose a table to inspect its stored items." />
  }
  if (itemsState.status === 'loading') {
    return <EmptyState title="Loading items" description={`Reading items from ${tableName}.`} />
  }
  if (itemsState.status === 'error') {
    return <EmptyState title="DynamoDB items unavailable" description={itemsState.message} />
  }
  if (items.length === 0) {
    return <EmptyState title="No items" description={`No loaded items in ${tableName} match the current filter.`} />
  }

  return (
    <div className="dynamodb-item-table-wrap">
      <table className="dynamodb-item-table">
        <thead>
          <tr>
            <th scope="col">Key</th>
            <th scope="col">Attributes</th>
            <th scope="col">Size</th>
          </tr>
        </thead>
        <tbody>
          {items.map((entry, index) => (
            <tr
              className={index === activeIndex ? 'item-row active' : 'item-row'}
              key={`${JSON.stringify(entry.key)}-${index}`}
              onClick={() => onSelectIndex(index)}
            >
              <td>
                <code>{JSON.stringify(entry.key)}</code>
              </td>
              <td>
                <AttributePreview item={entry.item} />
              </td>
              <td>{formatBytes(JSON.stringify(entry.item).length)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

type AttributePreviewProps = {
  item: Record<string, unknown>
}

function AttributePreview({ item }: AttributePreviewProps): JSX.Element {
  const attributes = Object.entries(item)
    .filter(([key]) => key !== 'pk' && key !== 'sk')
    .slice(0, 6)

  if (attributes.length === 0) {
    return <span className="service-status">key only</span>
  }

  return (
    <span className="attribute-preview">
      {attributes.map(([key, value]) => (
        <span className="attribute-chip" key={key}>
          {key}: {formatValue(value)}
        </span>
      ))}
    </span>
  )
}

type TableInspectorProps = {
  detailState: TableDetailState
  table?: DynamoDBTableSummary
  item?: DynamoDBItemSnapshot
  onRefreshDetail: () => void
  status?: DynamoDBStatus
}

function TableInspector({ detailState, table, item, onRefreshDetail, status }: TableInspectorProps): JSX.Element {
  if (!table) {
    return <EmptyState title="Inspector" description="Table schema and selected item JSON will appear here." />
  }
  const detailTable = detailState.status === 'success' ? detailState.table : table
  const globalSecondaryIndexes =
    detailState.status === 'success' ? detailState.globalSecondaryIndexes : table.globalSecondaryIndexes ?? []
  const localSecondaryIndexes =
    detailState.status === 'success' ? detailState.localSecondaryIndexes : table.localSecondaryIndexes ?? []
  const ttl = detailState.status === 'success' ? detailState.ttl : table.timeToLiveDescription
  const streams =
    detailState.status === 'success'
      ? detailState.streams
      : {
          tableName: table.tableName,
          streamEnabled: table.streamSpecification?.StreamEnabled ?? false,
          latestStreamArn: table.latestStreamArn,
          latestStreamLabel: table.latestStreamLabel,
          streamSpecification: table.streamSpecification,
        }

  return (
    <div className="dynamodb-inspector">
      <section>
        <div className="inspector-heading">
          <div>
            <span className="inspector-label">Table</span>
            <h3>{detailTable.tableName}</h3>
          </div>
          <Button onClick={onRefreshDetail}>Refresh detail</Button>
        </div>
        {detailState.status === 'loading' ? (
          <p className="inspector-muted">Loading detail metadata.</p>
        ) : null}
        {detailState.status === 'error' ? (
          <p className="operation-message error">{detailState.message}</p>
        ) : null}
        <dl className="inspector-list">
          <div>
            <dt>Status</dt>
            <dd>{detailTable.tableStatus}</dd>
          </div>
          <div>
            <dt>Endpoint</dt>
            <dd>
              <code>{status?.endpoint ?? 'unknown'}</code>
            </dd>
          </div>
          <div>
            <dt>Region</dt>
            <dd>{status?.region ?? 'unknown'}</dd>
          </div>
          <div>
            <dt>Key schema</dt>
            <dd>{keySchemaLabel(detailTable)}</dd>
          </div>
          <div>
            <dt>Attributes</dt>
            <dd>{attributeDefinitionsLabel(detailTable)}</dd>
          </div>
          <div>
            <dt>TTL</dt>
            <dd>{ttlLabel({ ...detailTable, timeToLiveDescription: ttl })}</dd>
          </div>
          <div>
            <dt>Streams</dt>
            <dd>{streamLabel(streams)}</dd>
          </div>
        </dl>
      </section>
      <section>
        <span className="inspector-label">Indexes</span>
        <IndexSummary globalSecondaryIndexes={globalSecondaryIndexes} localSecondaryIndexes={localSecondaryIndexes} />
      </section>
      <section>
        <span className="inspector-label">Streams</span>
        <pre className="mail-preview">{JSON.stringify(streams, null, 2)}</pre>
      </section>
      <section>
        <span className="inspector-label">Selected item</span>
        {item ? (
          <pre className="mail-preview">{JSON.stringify(item.item, null, 2)}</pre>
        ) : (
          <p className="inspector-muted">Select an item row to inspect JSON.</p>
        )}
      </section>
    </div>
  )
}

type IndexSummaryProps = {
  globalSecondaryIndexes: DynamoDBIndex[]
  localSecondaryIndexes: DynamoDBIndex[]
}

function IndexSummary({ globalSecondaryIndexes, localSecondaryIndexes }: IndexSummaryProps): JSX.Element {
  const indexes = [
    ...globalSecondaryIndexes.map((index) => ({ ...index, type: 'GSI' })),
    ...localSecondaryIndexes.map((index) => ({ ...index, type: 'LSI' })),
  ]
  if (indexes.length === 0) {
    return <p className="inspector-muted">No secondary indexes.</p>
  }
  return (
    <div className="dynamodb-item-table-wrap">
      <table className="dynamodb-item-table compact">
        <thead>
          <tr>
            <th scope="col">Type</th>
            <th scope="col">Name</th>
            <th scope="col">Key schema</th>
            <th scope="col">Items</th>
          </tr>
        </thead>
        <tbody>
          {indexes.map((index) => (
            <tr key={`${index.type}-${index.IndexName}`}>
              <td>{index.type}</td>
              <td>{index.IndexName}</td>
              <td>{keySchemaLabel({ tableName: index.IndexName, tableStatus: '', itemCount: 0, keySchema: index.KeySchema })}</td>
              <td>{index.ItemCount ?? 0}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function disabledStatus(service?: DashboardService): DynamoDBStatus {
  return {
    status: 'disabled',
    running: false,
    endpoint: service?.endpoint ?? 'http://127.0.0.1:8000',
    region: 'us-east-1',
    storagePath: service?.storagePath ?? '.devcloud/data/dynamodb',
    tableCount: 0,
  }
}

function keySchemaLabel(table: DynamoDBTableSummary): string {
  const keys = table.keySchema ?? []
  if (keys.length === 0) {
    return 'No key schema'
  }
  return keys.map((key) => `${key.AttributeName} ${key.KeyType}`).join(' / ')
}

function indexCount(table: DynamoDBTableSummary): number {
  return (table.globalSecondaryIndexes ?? []).length + (table.localSecondaryIndexes ?? []).length
}

function indexNames(table: DynamoDBTableSummary): string {
  const indexes = [...(table.globalSecondaryIndexes ?? []), ...(table.localSecondaryIndexes ?? [])]
  if (indexes.length === 0) {
    return 'none'
  }
  return indexes.map((index) => index.IndexName).join(', ')
}

function attributeDefinitionsLabel(table: DynamoDBTableSummary): string {
  const attributes = table.attributeDefinitions ?? []
  if (attributes.length === 0) {
    return 'none'
  }
  return attributes.map((attribute) => `${attribute.AttributeName} ${attribute.AttributeType}`).join(', ')
}

function ttlLabel(table: DynamoDBTableSummary): string {
  const ttl = table.timeToLiveDescription
  if (!ttl || ttl.TimeToLiveStatus === '') {
    return 'not configured'
  }
  return ttl.AttributeName ? `${ttl.TimeToLiveStatus} on ${ttl.AttributeName}` : ttl.TimeToLiveStatus
}

function streamLabel(streams: DynamoDBStreamsResponse): string {
  if (!streams.streamEnabled) {
    return 'disabled'
  }
  const viewType = streams.streamSpecification?.StreamViewType ?? 'enabled'
  return streams.latestStreamLabel ? `${viewType} (${streams.latestStreamLabel})` : viewType
}

function normalizedItemLimit(value: string): number {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 100
  }
  return Math.min(parsed, 1000)
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'null'
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

function attributeText(value: unknown): string {
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

function formatBytes(size: number): string {
  if (!Number.isFinite(size) || size < 0) {
    return 'unknown'
  }
  if (size < 1024) {
    return `${size} B`
  }
  return `${(size / 1024).toFixed(1)} KB`
}
