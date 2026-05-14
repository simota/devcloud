import { useCallback, useEffect, useMemo, useState, type FormEvent } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import { useEventSource } from '../../api/hooks/useEventSource'
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
import {
  defaultCreateTableJSON,
  defaultDeleteItemJSON,
  defaultExpressionAttributeValuesJSON,
  defaultPutItemJSON,
  defaultTTLJSON,
  defaultUpdateItemJSON,
  maxRecentDynamoDBOperations,
} from './constants'
import {
  attributeText,
  buildRecentDynamoDBOperation,
  disabledStatus,
  normalizedItemLimit,
  parseJSONForm,
  parseOptionalJSONForm,
  readRecentDynamoDBOperations,
  writeRecentDynamoDBOperations,
} from './helpers'
import { ItemBrowser, KeyLookup } from './ItemBrowser'
import { DynamoDBOperationForms } from './OperationForms'
import { DynamoDBQueryScanForm } from './QueryScanForm'
import { RecentOperationHistory } from './RecentOperationHistory'
import { TableInspector } from './TableInspector'
import { TableList } from './TableList'
import type {
  ItemsState,
  QueryScanPageState,
  RecentDynamoDBOperation,
  TableDetailState,
  TablesState,
} from './state'
import type { DynamoDBQueryScanResponse } from './types'

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

  useEventSource({ topics: ['dynamodb'], onEvent: refreshTables, enabled: !isDisabled })

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
