import { useCallback, useEffect, useMemo, useState, type FormEvent } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import type { DashboardService } from '../dashboard/types'
import {
  createDynamoDBTable,
  deleteDynamoDBItem,
  deleteDynamoDBTable,
  getDynamoDBIndexes,
  getDynamoDBStatus,
  getDynamoDBStreams,
  getDynamoDBTable,
  getDynamoDBTTL,
  listDynamoDBItems,
  listDynamoDBTables,
  putDynamoDBItem,
  queryDynamoDBItems,
  scanDynamoDBItems,
  updateDynamoDBItem,
  updateDynamoDBTTL,
} from './api'
import type {
  DynamoDBIndex,
  DynamoDBItemSnapshot,
  DynamoDBQueryScanResponse,
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

type QueryScanPageState = {
  pageHistory: Array<Record<string, unknown> | undefined>
  pageIndex: number
  selectedItemIndex: number
}

type RecentDynamoDBOperation = {
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
  const [createTableJSON, setCreateTableJSON] = useState(defaultCreateTableJSON)
  const [putItemJSON, setPutItemJSON] = useState(defaultPutItemJSON)
  const [updateItemJSON, setUpdateItemJSON] = useState(defaultUpdateItemJSON)
  const [deleteItemJSON, setDeleteItemJSON] = useState(defaultDeleteItemJSON)
  const [ttlJSON, setTTLJSON] = useState(defaultTTLJSON)
  const [deleteTableJSON, setDeleteTableJSON] = useState('{}')
  const [deleteItemConfirmation, setDeleteItemConfirmation] = useState('')
  const [deleteTableConfirmation, setDeleteTableConfirmation] = useState('')
  const [deleteItemAcknowledged, setDeleteItemAcknowledged] = useState(false)
  const [deleteTableAcknowledged, setDeleteTableAcknowledged] = useState(false)
  const [operationMessage, setOperationMessage] = useState('')
  const [operationError, setOperationError] = useState('')
  const [busyOperation, setBusyOperation] = useState<string>()
  const [queryScanMode, setQueryScanMode] = useState<'Query' | 'Scan'>('Query')
  const [queryScanIndexName, setQueryScanIndexName] = useState('')
  const [queryScanLimit, setQueryScanLimit] = useState('25')
  const [queryKeyCondition, setQueryKeyCondition] = useState('pk = :pk')
  const [scanFilterExpression, setScanFilterExpression] = useState('')
  const [queryScanExpressionAttributeValues, setQueryScanExpressionAttributeValues] = useState(defaultExpressionAttributeValuesJSON)
  const [queryScanMessage, setQueryScanMessage] = useState('')
  const [queryScanError, setQueryScanError] = useState('')
  const [queryScanResult, setQueryScanResult] = useState<DynamoDBQueryScanResponse>()
  const [queryScanPageState, setQueryScanPageState] = useState<QueryScanPageState>({
    pageHistory: [undefined],
    pageIndex: 0,
    selectedItemIndex: 0,
  })
  const [recentOperations, setRecentOperations] = useState<RecentDynamoDBOperation[]>(readRecentDynamoDBOperations)
  const isDisabled = service?.status === 'disabled'

  useEffect(() => {
    writeRecentDynamoDBOperations(recentOperations)
  }, [recentOperations])

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
    setDeleteItemAcknowledged(false)
    setDeleteTableAcknowledged(false)
    setDeleteItemConfirmation('')
    setDeleteTableConfirmation('')
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

  async function runOperation(name: string, action: () => Promise<string>): Promise<void> {
    setBusyOperation(name)
    setOperationError('')
    setOperationMessage('')
    try {
      const message = await action()
      setOperationMessage(message)
      refreshTables()
      refreshItems()
      refreshTableDetail()
    } catch (error) {
      setOperationError(error instanceof Error ? error.message : 'DynamoDB operation failed')
    } finally {
      setBusyOperation(undefined)
    }
  }

  function handleCreateTable(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const input = parseJSONForm(createTableJSON)
    if (!input.ok) {
      setOperationError(input.message)
      return
    }
    const tableName = typeof input.value.TableName === 'string' ? input.value.TableName : ''
    if (tableName.trim() === '') {
      setOperationError('CreateTable input requires TableName.')
      return
    }
    void runOperation('create-table', async () => {
      await createDynamoDBTable(input.value)
      setActiveTableName(tableName)
      return `Created table ${tableName}.`
    })
  }

  function handlePutItem(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!activeTableName) {
      setOperationError('Select a table before putting an item.')
      return
    }
    const input = parseJSONForm(putItemJSON)
    if (!input.ok) {
      setOperationError(input.message)
      return
    }
    void runOperation('put-item', async () => {
      await putDynamoDBItem(activeTableName, input.value)
      return `Put item in ${activeTableName}.`
    })
  }

  function handleUpdateItem(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!activeTableName) {
      setOperationError('Select a table before updating an item.')
      return
    }
    const input = parseJSONForm(updateItemJSON)
    if (!input.ok) {
      setOperationError(input.message)
      return
    }
    void runOperation('update-item', async () => {
      await updateDynamoDBItem(activeTableName, input.value)
      return `Updated item in ${activeTableName}.`
    })
  }

  function handleDeleteItem(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!activeTableName) {
      setOperationError('Select a table before deleting an item.')
      return
    }
    if (!deleteItemAcknowledged) {
      setOperationError('Acknowledge the DeleteItem destructive action before confirming the table name.')
      return
    }
    if (deleteItemConfirmation !== activeTableName) {
      setOperationError('DeleteItem confirmation must match the selected table name.')
      return
    }
    const input = parseJSONForm(deleteItemJSON)
    if (!input.ok) {
      setOperationError(input.message)
      return
    }
    void runOperation('delete-item', async () => {
      await deleteDynamoDBItem(activeTableName, input.value, deleteItemConfirmation)
      setDeleteItemConfirmation('')
      setDeleteItemAcknowledged(false)
      return `Deleted item from ${activeTableName}.`
    })
  }

  function handleUpdateTTL(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!activeTableName) {
      setOperationError('Select a table before updating TTL.')
      return
    }
    const input = parseJSONForm(ttlJSON)
    if (!input.ok) {
      setOperationError(input.message)
      return
    }
    void runOperation('ttl', async () => {
      await updateDynamoDBTTL(activeTableName, input.value)
      return `Updated TTL for ${activeTableName}.`
    })
  }

  function handleDeleteTable(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    if (!activeTableName) {
      setOperationError('Select a table before deleting it.')
      return
    }
    if (!deleteTableAcknowledged) {
      setOperationError('Acknowledge the DeleteTable destructive action before confirming the table name.')
      return
    }
    if (deleteTableConfirmation !== activeTableName) {
      setOperationError('DeleteTable confirmation must match the selected table name.')
      return
    }
    const input = parseJSONForm(deleteTableJSON)
    if (!input.ok) {
      setOperationError(input.message)
      return
    }
    void runOperation('delete-table', async () => {
      await deleteDynamoDBTable(activeTableName, input.value, deleteTableConfirmation)
      setDeleteTableConfirmation('')
      setDeleteTableAcknowledged(false)
      setActiveTableName(undefined)
      return `Deleted table ${activeTableName}.`
    })
  }

  function buildQueryScanInput(startKey?: Record<string, unknown>): { ok: true; value: Record<string, unknown> } | { ok: false; message: string } {
    if (!activeTableName) {
      return { ok: false, message: 'Select a table before running Query or Scan.' }
    }
    const limit = normalizedItemLimit(queryScanLimit)
    const expressionAttributeValues = parseOptionalJSONForm(queryScanExpressionAttributeValues)
    if (!expressionAttributeValues.ok) {
      return { ok: false, message: expressionAttributeValues.message }
    }
    const input: Record<string, unknown> = { Limit: limit }
    const indexName = queryScanIndexName.trim()
    if (indexName !== '') {
      input.IndexName = indexName
    }
    if (expressionAttributeValues.value) {
      input.ExpressionAttributeValues = expressionAttributeValues.value
    }
    if (queryScanMode === 'Query') {
      const keyCondition = queryKeyCondition.trim()
      if (keyCondition === '') {
        return { ok: false, message: 'Query requires KeyConditionExpression.' }
      }
      input.KeyConditionExpression = keyCondition
    } else {
      const filterExpression = scanFilterExpression.trim()
      if (filterExpression !== '') {
        input.FilterExpression = filterExpression
      }
    }
    if (startKey) {
      input.ExclusiveStartKey = startKey
    }
    return { ok: true, value: input }
  }

  async function runQueryScanPage(startKey: Record<string, unknown> | undefined, pageIndex: number, pageHistory: Array<Record<string, unknown> | undefined>): Promise<void> {
    if (!activeTableName) {
      setQueryScanError('Select a table before running Query or Scan.')
      return
    }
    const input = buildQueryScanInput(startKey)
    if (!input.ok) {
      setQueryScanError(input.message)
      return
    }
    setBusyOperation(queryScanMode.toLowerCase())
    setQueryScanError('')
    setQueryScanMessage('')
    setQueryScanResult(undefined)
    try {
      const result =
        queryScanMode === 'Query'
          ? await queryDynamoDBItems(activeTableName, input.value)
          : await scanDynamoDBItems(activeTableName, input.value)
      setQueryScanResult(result)
      setQueryScanPageState({ pageHistory, pageIndex, selectedItemIndex: 0 })
      setRecentOperations((current) =>
        [
          buildRecentDynamoDBOperation({
            count: result.Count ?? result.Items?.length ?? 0,
            hasMore: Boolean(result.LastEvaluatedKey),
            indexName: queryScanIndexName,
            limit: normalizedItemLimit(queryScanLimit),
            mode: queryScanMode,
            page: pageIndex + 1,
            scannedCount: result.ScannedCount ?? 0,
            tableName: activeTableName,
            usesExpressionAttributeValues: Boolean(input.value.ExpressionAttributeValues),
          }),
          ...current,
        ].slice(0, maxRecentDynamoDBOperations),
      )
      setQueryScanMessage(`${queryScanMode} page ${pageIndex + 1} returned ${result.Count ?? result.Items?.length ?? 0} item(s).`)
    } catch (error) {
      setQueryScanError(error instanceof Error ? error.message : `${queryScanMode} failed`)
    } finally {
      setBusyOperation(undefined)
    }
  }

  async function handleQueryScan(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault()
    await runQueryScanPage(undefined, 0, [undefined])
  }

  function nextPage(): void {
    if (!queryScanResult?.LastEvaluatedKey) {
      return
    }
    const pageIndex = queryScanPageState.pageIndex + 1
    const pageHistory = queryScanPageState.pageHistory.slice(0, pageIndex)
    pageHistory[pageIndex] = queryScanResult.LastEvaluatedKey
    void runQueryScanPage(queryScanResult.LastEvaluatedKey, pageIndex, pageHistory)
  }

  function previousPage(): void {
    if (queryScanPageState.pageIndex <= 0) {
      return
    }
    const pageIndex = queryScanPageState.pageIndex - 1
    const startKey = queryScanPageState.pageHistory[pageIndex]
    void runQueryScanPage(startKey, pageIndex, queryScanPageState.pageHistory)
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

      <Panel title="Operations">
        {operationError ? <p className="operation-message error">{operationError}</p> : null}
        {operationMessage ? <p className="operation-message success">{operationMessage}</p> : null}
        <DynamoDBOperationForms
          activeTableName={activeTableName}
          busyOperation={busyOperation}
          createTableJSON={createTableJSON}
          deleteItemConfirmation={deleteItemConfirmation}
          deleteItemAcknowledged={deleteItemAcknowledged}
          deleteItemJSON={deleteItemJSON}
          deleteTableConfirmation={deleteTableConfirmation}
          deleteTableAcknowledged={deleteTableAcknowledged}
          deleteTableJSON={deleteTableJSON}
          onCreateTable={handleCreateTable}
          onDeleteItem={handleDeleteItem}
          onDeleteTable={handleDeleteTable}
          onPutItem={handlePutItem}
          onUpdateItem={handleUpdateItem}
          onUpdateTTL={handleUpdateTTL}
          putItemJSON={putItemJSON}
          setCreateTableJSON={setCreateTableJSON}
          setDeleteItemConfirmation={setDeleteItemConfirmation}
          setDeleteItemAcknowledged={setDeleteItemAcknowledged}
          setDeleteItemJSON={setDeleteItemJSON}
          setDeleteTableConfirmation={setDeleteTableConfirmation}
          setDeleteTableAcknowledged={setDeleteTableAcknowledged}
          setDeleteTableJSON={setDeleteTableJSON}
          setPutItemJSON={setPutItemJSON}
          setTTLJSON={setTTLJSON}
          setUpdateItemJSON={setUpdateItemJSON}
          ttlJSON={ttlJSON}
          updateItemJSON={updateItemJSON}
        />
      </Panel>

      <Panel title="Query / Scan">
        <DynamoDBQueryScanForm
          activeTableName={activeTableName}
          busyOperation={busyOperation}
          expressionAttributeValues={queryScanExpressionAttributeValues}
          filterExpression={scanFilterExpression}
          indexName={queryScanIndexName}
          keyConditionExpression={queryKeyCondition}
          limit={queryScanLimit}
          message={queryScanMessage}
          mode={queryScanMode}
          onNextPage={nextPage}
          onPreviousPage={previousPage}
          onSubmit={(event) => {
            void handleQueryScan(event)
          }}
          pageIndex={queryScanPageState.pageIndex}
          result={queryScanResult}
          selectedItemIndex={queryScanPageState.selectedItemIndex}
          setExpressionAttributeValues={setQueryScanExpressionAttributeValues}
          setFilterExpression={setScanFilterExpression}
          setIndexName={setQueryScanIndexName}
          setKeyConditionExpression={setQueryKeyCondition}
          setLimit={setQueryScanLimit}
          setMode={setQueryScanMode}
          setSelectedItemIndex={(selectedItemIndex) =>
            setQueryScanPageState((current) => ({ ...current, selectedItemIndex }))
          }
          error={queryScanError}
        />
        <RecentOperationHistory
          operations={recentOperations}
          onClear={() => setRecentOperations([])}
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

type RecentOperationHistoryProps = {
  operations: RecentDynamoDBOperation[]
  onClear: () => void
}

function RecentOperationHistory({ operations, onClear }: RecentOperationHistoryProps): JSX.Element {
  return (
    <section className="dynamodb-recent-operations">
      <div className="inspector-heading">
        <div>
          <span className="inspector-label">Recent operation history</span>
          <p className="inspector-muted">Stored locally without item payloads, credentials, or pagination keys.</p>
        </div>
        <Button disabled={operations.length === 0} onClick={onClear} type="button">
          Clear
        </Button>
      </div>
      {operations.length === 0 ? (
        <p className="inspector-muted">Run Query or Scan to record local operation metadata.</p>
      ) : (
        <div className="dynamodb-item-table-wrap">
          <table className="dynamodb-item-table compact">
            <thead>
              <tr>
                <th scope="col">When</th>
                <th scope="col">Operation</th>
                <th scope="col">Table</th>
                <th scope="col">Result</th>
              </tr>
            </thead>
            <tbody>
              {operations.map((operation) => (
                <tr key={operation.id}>
                  <td>{formatRecentOperationTime(operation.createdAt)}</td>
                  <td>
                    <span className="attribute-preview">
                      <span className="attribute-chip">{operation.operation}</span>
                      <span className="attribute-chip">page {operation.page}</span>
                      {operation.indexName ? <span className="attribute-chip">index: {operation.indexName}</span> : null}
                    </span>
                  </td>
                  <td>
                    <code>{operation.tableName}</code>
                  </td>
                  <td>
                    <span className="attribute-preview">
                      <span className="attribute-chip">count: {operation.count}</span>
                      <span className="attribute-chip">scanned: {operation.scannedCount}</span>
                      <span className="attribute-chip">limit: {operation.limit}</span>
                      <span className="attribute-chip">{operation.hasMore ? 'has next page' : 'last page'}</span>
                      <span className="attribute-chip">{operation.expressionSummary}</span>
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
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

type DynamoDBOperationFormsProps = {
  activeTableName?: string
  busyOperation?: string
  createTableJSON: string
  putItemJSON: string
  updateItemJSON: string
  deleteItemJSON: string
  ttlJSON: string
  deleteTableJSON: string
  deleteItemConfirmation: string
  deleteTableConfirmation: string
  deleteItemAcknowledged: boolean
  deleteTableAcknowledged: boolean
  onCreateTable: (event: FormEvent<HTMLFormElement>) => void
  onPutItem: (event: FormEvent<HTMLFormElement>) => void
  onUpdateItem: (event: FormEvent<HTMLFormElement>) => void
  onDeleteItem: (event: FormEvent<HTMLFormElement>) => void
  onUpdateTTL: (event: FormEvent<HTMLFormElement>) => void
  onDeleteTable: (event: FormEvent<HTMLFormElement>) => void
  setCreateTableJSON: (value: string) => void
  setPutItemJSON: (value: string) => void
  setUpdateItemJSON: (value: string) => void
  setDeleteItemJSON: (value: string) => void
  setTTLJSON: (value: string) => void
  setDeleteTableJSON: (value: string) => void
  setDeleteItemConfirmation: (value: string) => void
  setDeleteTableConfirmation: (value: string) => void
  setDeleteItemAcknowledged: (value: boolean) => void
  setDeleteTableAcknowledged: (value: boolean) => void
}

function DynamoDBOperationForms({
  activeTableName,
  busyOperation,
  createTableJSON,
  putItemJSON,
  updateItemJSON,
  deleteItemJSON,
  ttlJSON,
  deleteTableJSON,
  deleteItemConfirmation,
  deleteTableConfirmation,
  deleteItemAcknowledged,
  deleteTableAcknowledged,
  onCreateTable,
  onPutItem,
  onUpdateItem,
  onDeleteItem,
  onUpdateTTL,
  onDeleteTable,
  setCreateTableJSON,
  setPutItemJSON,
  setUpdateItemJSON,
  setDeleteItemJSON,
  setTTLJSON,
  setDeleteTableJSON,
  setDeleteItemConfirmation,
  setDeleteTableConfirmation,
  setDeleteItemAcknowledged,
  setDeleteTableAcknowledged,
}: DynamoDBOperationFormsProps): JSX.Element {
  const tableActionDisabled = !activeTableName

  return (
    <div className="dynamodb-operation-stack">
      <JSONOperationForm
        buttonLabel="Create table"
        disabled={busyOperation === 'create-table'}
        json={createTableJSON}
        label="CreateTable input"
        onChange={setCreateTableJSON}
        onSubmit={onCreateTable}
      />
      <JSONOperationForm
        buttonLabel="Put item"
        disabled={tableActionDisabled || busyOperation === 'put-item'}
        json={putItemJSON}
        label="PutItem input"
        onChange={setPutItemJSON}
        onSubmit={onPutItem}
      />
      <JSONOperationForm
        buttonLabel="Update item"
        disabled={tableActionDisabled || busyOperation === 'update-item'}
        json={updateItemJSON}
        label="UpdateItem input"
        onChange={setUpdateItemJSON}
        onSubmit={onUpdateItem}
      />
      <JSONOperationForm
        buttonLabel="Update TTL"
        disabled={tableActionDisabled || busyOperation === 'ttl'}
        json={ttlJSON}
        label="UpdateTimeToLive input"
        onChange={setTTLJSON}
        onSubmit={onUpdateTTL}
      />
      <JSONOperationForm
        buttonClassName="danger"
        buttonLabel="Delete item"
        confirmation={deleteItemConfirmation}
        destructiveAcknowledgement={deleteItemAcknowledged}
        destructiveAcknowledgementLabel="Step 1: I understand this deletes the item identified by the JSON key."
        confirmationLabel={`Type ${activeTableName ?? 'table name'} to delete item`}
        disabled={
          tableActionDisabled ||
          !deleteItemAcknowledged ||
          deleteItemConfirmation !== activeTableName ||
          busyOperation === 'delete-item'
        }
        json={deleteItemJSON}
        label="DeleteItem input"
        onChange={setDeleteItemJSON}
        onDestructiveAcknowledgementChange={setDeleteItemAcknowledged}
        onConfirmationChange={setDeleteItemConfirmation}
        onSubmit={onDeleteItem}
      />
      <JSONOperationForm
        buttonClassName="danger"
        buttonLabel="Delete table"
        confirmation={deleteTableConfirmation}
        destructiveAcknowledgement={deleteTableAcknowledged}
        destructiveAcknowledgementLabel="Step 1: I understand this deletes the selected table and its local items."
        confirmationLabel={`Type ${activeTableName ?? 'table name'} to delete table`}
        disabled={
          tableActionDisabled ||
          !deleteTableAcknowledged ||
          deleteTableConfirmation !== activeTableName ||
          busyOperation === 'delete-table'
        }
        json={deleteTableJSON}
        label="DeleteTable input"
        onChange={setDeleteTableJSON}
        onDestructiveAcknowledgementChange={setDeleteTableAcknowledged}
        onConfirmationChange={setDeleteTableConfirmation}
        onSubmit={onDeleteTable}
      />
    </div>
  )
}

type DynamoDBQueryScanFormProps = {
  activeTableName?: string
  busyOperation?: string
  error: string
  expressionAttributeValues: string
  filterExpression: string
  indexName: string
  keyConditionExpression: string
  limit: string
  message: string
  mode: 'Query' | 'Scan'
  pageIndex: number
  result?: DynamoDBQueryScanResponse
  selectedItemIndex: number
  onNextPage: () => void
  onPreviousPage: () => void
  onSubmit: (event: FormEvent<HTMLFormElement>) => void
  setExpressionAttributeValues: (value: string) => void
  setFilterExpression: (value: string) => void
  setIndexName: (value: string) => void
  setKeyConditionExpression: (value: string) => void
  setLimit: (value: string) => void
  setMode: (value: 'Query' | 'Scan') => void
  setSelectedItemIndex: (value: number) => void
}

function DynamoDBQueryScanForm({
  activeTableName,
  busyOperation,
  error,
  expressionAttributeValues,
  filterExpression,
  indexName,
  keyConditionExpression,
  limit,
  message,
  mode,
  onNextPage,
  onPreviousPage,
  onSubmit,
  pageIndex,
  result,
  selectedItemIndex,
  setExpressionAttributeValues,
  setFilterExpression,
  setIndexName,
  setKeyConditionExpression,
  setLimit,
  setMode,
  setSelectedItemIndex,
}: DynamoDBQueryScanFormProps): JSX.Element {
  const disabled = !activeTableName || busyOperation === 'query' || busyOperation === 'scan'
  const items = result?.Items ?? []
  const hasPreviousPage = pageIndex > 0
  const hasNextPage = Boolean(result?.LastEvaluatedKey)
  const selectedResultItem = items[Math.min(selectedItemIndex, Math.max(items.length - 1, 0))]

  return (
    <div className="dynamodb-query-scan">
      {error ? <p className="operation-message error">{error}</p> : null}
      {message ? <p className="operation-message success">{message}</p> : null}
      <form className="dynamodb-query-scan-form" onSubmit={onSubmit}>
        <div className="segmented-control" aria-label="DynamoDB read operation">
          <button
            className={mode === 'Query' ? 'active' : ''}
            onClick={() => setMode('Query')}
            type="button"
          >
            Query
          </button>
          <button className={mode === 'Scan' ? 'active' : ''} onClick={() => setMode('Scan')} type="button">
            Scan
          </button>
        </div>
        <label className="compact-filter">
          <span>TableName</span>
          <input aria-label="DynamoDB Query Scan table name" disabled value={activeTableName ?? ''} />
        </label>
        <label className="compact-filter">
          <span>IndexName</span>
          <input
            aria-label="DynamoDB Query Scan index name"
            onChange={(event) => setIndexName(event.target.value)}
            placeholder="optional"
            value={indexName}
          />
        </label>
        <label className="compact-filter small">
          <span>Limit</span>
          <input
            aria-label="DynamoDB Query Scan limit"
            inputMode="numeric"
            onChange={(event) => setLimit(event.target.value)}
            value={limit}
          />
        </label>
        {mode === 'Query' ? (
          <label className="compact-filter wide">
            <span>KeyConditionExpression</span>
            <input
              aria-label="DynamoDB Query key condition expression"
              onChange={(event) => setKeyConditionExpression(event.target.value)}
              placeholder="pk = :pk"
              value={keyConditionExpression}
            />
          </label>
        ) : (
          <label className="compact-filter wide">
            <span>FilterExpression</span>
            <input
              aria-label="DynamoDB Scan filter expression"
              onChange={(event) => setFilterExpression(event.target.value)}
              placeholder="optional"
              value={filterExpression}
            />
          </label>
        )}
        <label className="redshift-sql-editor">
          <span>ExpressionAttributeValues JSON</span>
          <textarea
            aria-label="DynamoDB Query Scan expression attribute values JSON"
            onChange={(event) => setExpressionAttributeValues(event.target.value)}
            spellCheck={false}
            value={expressionAttributeValues}
          />
        </label>
        <Button disabled={disabled} type="submit">
          Run {mode}
        </Button>
      </form>
      {result ? (
        <div className="redshift-query-result">
          <div className="dynamodb-toolbar">
            <span className="toolbar-count">
              Page {pageIndex + 1} / Count {result.Count ?? items.length} / Scanned {result.ScannedCount ?? 0}
            </span>
            <span className="toolbar-count">
              {hasNextPage ? 'More results available' : 'End of results'}
            </span>
            <div className="pubsub-action-row">
              <Button disabled={disabled || !hasPreviousPage} onClick={onPreviousPage} type="button">
                Previous page
              </Button>
              <Button disabled={disabled || !hasNextPage} onClick={onNextPage} type="button">
                Next page
              </Button>
            </div>
          </div>
          {items.length > 0 ? (
            <div className="dynamodb-query-result-grid">
              <div className="dynamodb-item-table-wrap">
                <table className="dynamodb-item-table compact">
                  <thead>
                    <tr>
                      <th scope="col">#</th>
                      <th scope="col">Item preview</th>
                    </tr>
                  </thead>
                  <tbody>
                    {items.map((item, index) => (
                      <tr
                        className={index === selectedItemIndex ? 'item-row active' : 'item-row'}
                        key={`${pageIndex}-${index}-${JSON.stringify(item).length}`}
                        onClick={() => setSelectedItemIndex(index)}
                      >
                        <td>{index + 1}</td>
                        <td>
                          <AttributePreview item={item} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <section>
                <span className="inspector-label">Selected result item JSON</span>
                <pre className="mail-preview">{JSON.stringify(selectedResultItem, null, 2)}</pre>
              </section>
            </div>
          ) : (
            <p className="inspector-muted">No items returned.</p>
          )}
        </div>
      ) : null}
    </div>
  )
}

type JSONOperationFormProps = {
  buttonClassName?: string
  buttonLabel: string
  confirmation?: string
  confirmationLabel?: string
  destructiveAcknowledgement?: boolean
  destructiveAcknowledgementLabel?: string
  disabled: boolean
  json: string
  label: string
  onChange: (value: string) => void
  onConfirmationChange?: (value: string) => void
  onDestructiveAcknowledgementChange?: (value: boolean) => void
  onSubmit: (event: FormEvent<HTMLFormElement>) => void
}

function JSONOperationForm({
  buttonClassName,
  buttonLabel,
  confirmation,
  confirmationLabel,
  destructiveAcknowledgement,
  destructiveAcknowledgementLabel,
  disabled,
  json,
  label,
  onChange,
  onConfirmationChange,
  onDestructiveAcknowledgementChange,
  onSubmit,
}: JSONOperationFormProps): JSX.Element {
  return (
    <form className="dynamodb-operation-form" onSubmit={onSubmit}>
      <label className="redshift-sql-editor">
        <span>{label}</span>
        <textarea
          aria-label={label}
          onChange={(event) => onChange(event.target.value)}
          spellCheck={false}
          value={json}
        />
      </label>
      {onDestructiveAcknowledgementChange ? (
        <label className="destructive-confirmation">
          <input
            aria-label={destructiveAcknowledgementLabel}
            checked={Boolean(destructiveAcknowledgement)}
            onChange={(event) => onDestructiveAcknowledgementChange(event.target.checked)}
            type="checkbox"
          />
          <span>{destructiveAcknowledgementLabel}</span>
        </label>
      ) : null}
      {onConfirmationChange ? (
        <label className="compact-filter">
          <span>Step 2: {confirmationLabel}</span>
          <input
            aria-label={confirmationLabel}
            onChange={(event) => onConfirmationChange(event.target.value)}
            value={confirmation ?? ''}
          />
        </label>
      ) : null}
      <Button className={buttonClassName} disabled={disabled} type="submit">
        {buttonLabel}
      </Button>
    </form>
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

type ParsedJSONForm = { ok: true; value: Record<string, unknown> } | { ok: false; message: string }

function parseJSONForm(value: string): ParsedJSONForm {
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

function parseOptionalJSONForm(value: string): { ok: true; value?: Record<string, unknown> } | { ok: false; message: string } {
  if (value.trim() === '') {
    return { ok: true }
  }
  const parsed = parseJSONForm(value)
  if (!parsed.ok) {
    return parsed
  }
  return { ok: true, value: parsed.value }
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

const recentDynamoDBOperationsStorageKey = 'devcloud.dynamodb.recentOperations.v1'
const maxRecentDynamoDBOperations = 10

function readRecentDynamoDBOperations(): RecentDynamoDBOperation[] {
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

function writeRecentDynamoDBOperations(operations: RecentDynamoDBOperation[]): void {
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

function isRecentDynamoDBOperation(value: unknown): value is RecentDynamoDBOperation {
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

function buildRecentDynamoDBOperation(input: {
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

function formatRecentOperationTime(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return 'unknown'
  }
  return date.toLocaleString()
}

const defaultCreateTableJSON = `{
  "TableName": "Demo",
  "AttributeDefinitions": [
    { "AttributeName": "pk", "AttributeType": "S" }
  ],
  "KeySchema": [
    { "AttributeName": "pk", "KeyType": "HASH" }
  ],
  "BillingMode": "PAY_PER_REQUEST"
}`

const defaultPutItemJSON = `{
  "Item": {
    "pk": { "S": "user#1" },
    "name": { "S": "Ada" }
  }
}`

const defaultUpdateItemJSON = `{
  "Key": {
    "pk": { "S": "user#1" }
  },
  "UpdateExpression": "SET #name = :name",
  "ExpressionAttributeNames": {
    "#name": "name"
  },
  "ExpressionAttributeValues": {
    ":name": { "S": "Grace" }
  }
}`

const defaultDeleteItemJSON = `{
  "Key": {
    "pk": { "S": "user#1" }
  }
}`

const defaultTTLJSON = `{
  "TimeToLiveSpecification": {
    "Enabled": true,
    "AttributeName": "expiresAt"
  }
}`

const defaultExpressionAttributeValuesJSON = `{
  ":pk": { "S": "user#1" }
}`
