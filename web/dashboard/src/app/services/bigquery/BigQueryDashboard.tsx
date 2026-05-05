import { useCallback, useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import type { DashboardService } from '../dashboard/types'
import {
  createBigQueryDataset,
  createBigQueryTable,
  getBigQueryJob,
  getBigQueryStatus,
  insertBigQueryRows,
  listBigQueryDatasets,
  listBigQueryJobs,
  listBigQueryProjects,
  listBigQueryRows,
  runBigQueryQuery,
} from './api'
import type {
  BigQueryDataset,
  BigQueryDatasetCreateRequest,
  BigQueryInsertAllRow,
  BigQueryJob,
  BigQueryQueryResponse,
  BigQueryRow,
  BigQuerySchema,
  BigQueryStatus,
  BigQueryTable,
  BigQueryTableCreateRequest,
} from './types'

type CatalogState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: BigQueryStatus; datasets: BigQueryDataset[]; jobs: BigQueryJob[] }
  | { status: 'error'; message: string }

type RowsState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; rows: BigQueryRow[] }
  | { status: 'error'; message: string }

type QueryRunnerState =
  | { status: 'idle' }
  | { status: 'running' }
  | { status: 'success'; response: BigQueryQueryResponse }
  | { status: 'error'; message: string }

type OperationState =
  | { status: 'idle' }
  | { status: 'running'; label: string }
  | { status: 'success'; message: string; insertErrors?: string[] }
  | { status: 'error'; message: string }

type JobDetailState =
  | { status: 'idle' }
  | { status: 'loading'; jobId: string }
  | { status: 'success'; job: BigQueryJob }
  | { status: 'error'; message: string }

type BigQueryDashboardProps = {
  service?: DashboardService
}

export function BigQueryDashboard({ service }: BigQueryDashboardProps): JSX.Element {
  const [catalogState, setCatalogState] = useState<CatalogState>({ status: 'loading' })
  const [rowsState, setRowsState] = useState<RowsState>({ status: 'idle' })
  const [activeDatasetId, setActiveDatasetId] = useState<string>()
  const [activeTableId, setActiveTableId] = useState<string>()
  const [activeRowIndex, setActiveRowIndex] = useState(0)
  const [datasetFilter, setDatasetFilter] = useState('')
  const [rowFilter, setRowFilter] = useState('')
  const isDisabled = service?.status === 'disabled'

  const refreshCatalog = useCallback(() => {
    if (isDisabled) {
      setCatalogState({ status: 'success', statusPayload: disabledStatus(service), datasets: [], jobs: [] })
      setRowsState({ status: 'idle' })
      setActiveDatasetId(undefined)
      setActiveTableId(undefined)
      return
    }

    setCatalogState({ status: 'loading' })
    Promise.all([getBigQueryStatus(), listBigQueryProjects()])
      .then(async ([statusPayload, projects]) => {
        const project = projects.projects[0]
        const projectId = project?.projectId ?? statusPayload.project
        const [{ datasets }, { jobs }] = await Promise.all([listBigQueryDatasets(projectId), listBigQueryJobs(projectId)])
        setCatalogState({ status: 'success', statusPayload, datasets, jobs })
        setActiveDatasetId((current) =>
          current && datasets.some((dataset) => dataset.datasetId === current) ? current : datasets[0]?.datasetId,
        )
        setActiveTableId((current) => {
          const nextDataset =
            datasets.find((dataset) => dataset.datasetId === activeDatasetId) ?? datasets[0]
          return current && nextDataset?.tables.some((table) => table.tableId === current)
            ? current
            : nextDataset?.tables[0]?.tableId
        })
      })
      .catch((error: Error) => {
        setCatalogState({ status: 'error', message: error.message })
      })
  }, [activeDatasetId, isDisabled, service])

  useEffect(() => {
    refreshCatalog()
  }, [refreshCatalog])

  const datasets = catalogState.status === 'success' ? catalogState.datasets : []
  const activeDataset = datasets.find((dataset) => dataset.datasetId === activeDatasetId)
  const activeTable = activeDataset?.tables.find((table) => table.tableId === activeTableId)
  const projectId = catalogState.status === 'success' ? catalogState.statusPayload.project : service?.endpoint

  const refreshRows = useCallback(() => {
    if (!activeDataset || !activeTable || catalogState.status !== 'success' || isDisabled) {
      setRowsState({ status: 'idle' })
      return
    }
    setRowsState({ status: 'loading' })
    listBigQueryRows(catalogState.statusPayload.project, activeDataset.datasetId, activeTable.tableId)
      .then(({ rows }) => {
        setActiveRowIndex(0)
        setRowsState({ status: 'success', rows })
      })
      .catch((error: Error) => {
        setRowsState({ status: 'error', message: error.message })
      })
  }, [activeDataset, activeTable, catalogState, isDisabled])

  useEffect(() => {
    refreshRows()
  }, [refreshRows])

  const filteredDatasets = useMemo(() => {
    const query = datasetFilter.trim().toLowerCase()
    if (query === '') {
      return datasets
    }
    return datasets.filter((dataset) => dataset.datasetId.toLowerCase().includes(query))
  }, [datasets, datasetFilter])

  const filteredRows = useMemo(() => {
    const rows = rowsState.status === 'success' ? rowsState.rows : []
    const query = rowFilter.trim().toLowerCase()
    if (query === '') {
      return rows
    }
    return rows.filter((row) => JSON.stringify(row).toLowerCase().includes(query))
  }, [rowsState, rowFilter])

  const selectedRow = filteredRows[Math.min(activeRowIndex, Math.max(filteredRows.length - 1, 0))]

  if (isDisabled) {
    return (
      <Panel title="BigQuery">
        <EmptyState
          title="BigQuery is disabled"
          description="Enable the BigQuery service in devcloud config to inspect projects, datasets, tables, rows, and jobs."
        />
      </Panel>
    )
  }

  function selectDataset(dataset: BigQueryDataset): void {
    setActiveDatasetId(dataset.datasetId)
    setActiveTableId(dataset.tables[0]?.tableId)
    setActiveRowIndex(0)
    setRowFilter('')
  }

  function selectTable(table: BigQueryTable): void {
    setActiveTableId(table.tableId)
    setActiveRowIndex(0)
    setRowFilter('')
  }

  return (
    <div className="dynamodb-workspace">
      <Panel title="Datasets">
        <div className="dynamodb-toolbar">
          <label className="compact-filter">
            <span>Filter</span>
            <input
              aria-label="Filter BigQuery datasets"
              onChange={(event) => setDatasetFilter(event.target.value)}
              placeholder="dataset id"
              type="search"
              value={datasetFilter}
            />
          </label>
          <Button onClick={refreshCatalog}>Refresh</Button>
        </div>
        {catalogState.status === 'loading' ? (
          <EmptyState title="Loading datasets" description="Reading local BigQuery catalog metadata." />
        ) : null}
        {catalogState.status === 'error' ? (
          <EmptyState
            title="BigQuery catalog unavailable"
            description={catalogState.message}
            actionLabel="Retry"
            onAction={refreshCatalog}
          />
        ) : null}
        {catalogState.status === 'success' ? (
          <DatasetList
            activeDatasetId={activeDatasetId}
            activeTableId={activeTableId}
            datasets={filteredDatasets}
            onSelectDataset={selectDataset}
            onSelectTable={selectTable}
          />
        ) : null}
      </Panel>

      <Panel title="Rows">
        <BigQueryQueryRunner
          disabled={isDisabled || catalogState.status !== 'success'}
          projectId={projectId ?? 'devcloud'}
          onQuerySuccess={refreshCatalog}
        />
        <BigQueryManagementPanel
          activeDatasetId={activeDataset?.datasetId}
          activeTableId={activeTable?.tableId}
          disabled={isDisabled || catalogState.status !== 'success'}
          projectId={projectId ?? 'devcloud'}
          onMutationSuccess={() => {
            refreshCatalog()
            refreshRows()
          }}
        />
        <div className="dynamodb-toolbar">
          <span className="toolbar-count">
            {activeTable ? `${filteredRows.length} shown / ${activeTable.numRows} reported` : 'Select a table'}
          </span>
          <label className="compact-filter">
            <span>Filter</span>
            <input
              aria-label="Filter BigQuery rows"
              disabled={!activeTable}
              onChange={(event) => {
                setActiveRowIndex(0)
                setRowFilter(event.target.value)
              }}
              placeholder="row value"
              type="search"
              value={rowFilter}
            />
          </label>
          <Button disabled={!activeTable} onClick={refreshRows}>
            Refresh
          </Button>
        </div>
        <RowBrowser
          activeIndex={activeRowIndex}
          rows={filteredRows}
          rowsState={rowsState}
          tableName={activeTable?.tableId}
          onSelectIndex={setActiveRowIndex}
        />
      </Panel>

      <Panel title="Inspector">
        <BigQueryInspector
          dataset={activeDataset}
          jobs={catalogState.status === 'success' ? catalogState.jobs : []}
          project={projectId ?? 'unknown'}
          row={selectedRow}
          status={catalogState.status === 'success' ? catalogState.statusPayload : undefined}
          table={activeTable}
        />
      </Panel>
    </div>
  )
}

type BigQueryManagementPanelProps = {
  activeDatasetId?: string
  activeTableId?: string
  disabled: boolean
  projectId: string
  onMutationSuccess: () => void
}

function BigQueryManagementPanel({
  activeDatasetId,
  activeTableId,
  disabled,
  onMutationSuccess,
  projectId,
}: BigQueryManagementPanelProps): JSX.Element {
  const [datasetId, setDatasetId] = useState('')
  const [datasetLocation, setDatasetLocation] = useState('US')
  const [datasetDescription, setDatasetDescription] = useState('')
  const [datasetRawMode, setDatasetRawMode] = useState(false)
  const [datasetRawJSON, setDatasetRawJSON] = useState('{\n  "datasetReference": {\n    "datasetId": "dashboard_ops"\n  },\n  "location": "US"\n}')
  const [tableDatasetId, setTableDatasetId] = useState('')
  const [tableId, setTableId] = useState('')
  const [tableDescription, setTableDescription] = useState('')
  const [schemaFields, setSchemaFields] = useState('event_id:STRING:REQUIRED\ncreated_at:TIMESTAMP\npayload:STRING')
  const [tableRawMode, setTableRawMode] = useState(false)
  const [tableRawJSON, setTableRawJSON] = useState(
    '{\n  "tableReference": {\n    "tableId": "events"\n  },\n  "schema": {\n    "fields": [\n      { "name": "event_id", "type": "STRING", "mode": "REQUIRED" }\n    ]\n  }\n}',
  )
  const [rowDatasetId, setRowDatasetId] = useState('')
  const [rowTableId, setRowTableId] = useState('')
  const [insertId, setInsertId] = useState('')
  const [rowJSON, setRowJSON] = useState('{\n  "event_id": "evt-1",\n  "payload": "local test"\n}')
  const [operationState, setOperationState] = useState<OperationState>({ status: 'idle' })

  useEffect(() => {
    if (activeDatasetId) {
      setTableDatasetId((current) => current || activeDatasetId)
      setRowDatasetId((current) => current || activeDatasetId)
    }
  }, [activeDatasetId])

  useEffect(() => {
    if (activeTableId) {
      setRowTableId((current) => current || activeTableId)
    }
  }, [activeTableId])

  const mutationDisabled = disabled || operationState.status === 'running'

  function submitDataset(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const request: BigQueryDatasetCreateRequest | Error = datasetRawMode
      ? parseJSONRecord<BigQueryDatasetCreateRequest>(datasetRawJSON, 'Dataset raw JSON')
      : {
          datasetReference: { datasetId: datasetId.trim() },
          location: datasetLocation.trim() || undefined,
          description: datasetDescription.trim() || undefined,
        }
    if (request instanceof Error) {
      setOperationState({ status: 'error', message: request.message })
      return
    }
    const requestedDatasetId = readNestedString(request, ['datasetReference', 'datasetId'])
    if (!requestedDatasetId) {
      setOperationState({ status: 'error', message: 'Dataset ID is required.' })
      return
    }

    setOperationState({ status: 'running', label: 'Creating dataset' })
    createBigQueryDataset(projectId, request)
      .then((response) => {
        setOperationState({ status: 'success', message: `Created dataset ${response.datasetReference.datasetId}.` })
        setTableDatasetId(response.datasetReference.datasetId)
        setRowDatasetId(response.datasetReference.datasetId)
        onMutationSuccess()
      })
      .catch((error: Error) => {
        setOperationState({ status: 'error', message: error.message })
      })
  }

  function submitTable(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const targetDatasetId = tableDatasetId.trim() || activeDatasetId
    if (!targetDatasetId) {
      setOperationState({ status: 'error', message: 'Choose a dataset before creating a table.' })
      return
    }
    const parsedSchema = tableRawMode ? undefined : parseSchemaFields(schemaFields)
    if (parsedSchema instanceof Error) {
      setOperationState({ status: 'error', message: parsedSchema.message })
      return
    }
    const request: BigQueryTableCreateRequest | Error = tableRawMode
      ? parseJSONRecord<BigQueryTableCreateRequest>(tableRawJSON, 'Table raw JSON')
      : {
          tableReference: { tableId: tableId.trim() },
          schema: parsedSchema,
          description: tableDescription.trim() || undefined,
        }
    if (request instanceof Error) {
      setOperationState({ status: 'error', message: request.message })
      return
    }
    const requestedTableId = readNestedString(request, ['tableReference', 'tableId'])
    if (!requestedTableId) {
      setOperationState({ status: 'error', message: 'Table ID is required.' })
      return
    }

    setOperationState({ status: 'running', label: 'Creating table' })
    createBigQueryTable(projectId, targetDatasetId, request)
      .then((response) => {
        setOperationState({ status: 'success', message: `Created table ${targetDatasetId}.${response.tableReference.tableId}.` })
        setRowDatasetId(targetDatasetId)
        setRowTableId(response.tableReference.tableId)
        onMutationSuccess()
      })
      .catch((error: Error) => {
        setOperationState({ status: 'error', message: error.message })
      })
  }

  function submitRows(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const targetDatasetId = rowDatasetId.trim() || activeDatasetId
    const targetTableId = rowTableId.trim() || activeTableId
    if (!targetDatasetId || !targetTableId) {
      setOperationState({ status: 'error', message: 'Choose a dataset and table before inserting rows.' })
      return
    }
    const parsedRows = parseInsertRows(rowJSON, insertId.trim())
    if (parsedRows instanceof Error) {
      setOperationState({ status: 'error', message: parsedRows.message })
      return
    }

    setOperationState({ status: 'running', label: 'Inserting rows' })
    insertBigQueryRows(projectId, targetDatasetId, targetTableId, { rows: parsedRows })
      .then((response) => {
        const insertErrors = response.insertErrors?.map((error) => {
          const messages = error.errors.map((entry) => entry.message).join('; ')
          return `row ${error.index}: ${messages}`
        })
        setOperationState({
          status: 'success',
          message: insertErrors?.length ? 'Insert completed with partial insert errors.' : 'Inserted row data.',
          insertErrors,
        })
        onMutationSuccess()
      })
      .catch((error: Error) => {
        setOperationState({ status: 'error', message: error.message })
      })
  }

  return (
    <section className="redshift-query-runner" aria-label="BigQuery management controls">
      <div className="dynamodb-toolbar">
        <span className="inspector-label">Local management</span>
        <span className="toolbar-count">datasets.insert / tables.insert / tabledata.insertAll</span>
      </div>
      {disabled ? (
        <p className="inspector-muted">BigQuery management controls are disabled until the service is active.</p>
      ) : null}
      {operationState.status === 'running' ? (
        <p className="operation-message">{operationState.label}.</p>
      ) : null}
      {operationState.status === 'success' ? (
        <div className="operation-message success">
          {operationState.message}
          {operationState.insertErrors?.length ? (
            <ul>
              {operationState.insertErrors.map((error) => (
                <li key={error}>{error}</li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}
      {operationState.status === 'error' ? <p className="operation-message error">{operationState.message}</p> : null}

      <form className="dynamodb-operation-form" onSubmit={submitDataset}>
        <div className="dynamodb-toolbar">
          <span className="inspector-label">Create dataset</span>
          <label className="compact-filter small">
            <span>raw JSON</span>
            <input
              aria-label="Use raw JSON for BigQuery dataset"
              checked={datasetRawMode}
              disabled={mutationDisabled}
              onChange={(event) => setDatasetRawMode(event.target.checked)}
              type="checkbox"
            />
          </label>
        </div>
        {datasetRawMode ? (
          <label className="redshift-sql-editor">
            <span>Dataset raw JSON</span>
            <textarea
              aria-label="BigQuery dataset raw JSON"
              disabled={mutationDisabled}
              onChange={(event) => setDatasetRawJSON(event.target.value)}
              rows={7}
              spellCheck={false}
              value={datasetRawJSON}
            />
          </label>
        ) : (
          <div className="pubsub-action-row">
            <label className="compact-filter">
              <span>Dataset ID</span>
              <input
                aria-label="BigQuery dataset ID"
                disabled={mutationDisabled}
                onChange={(event) => setDatasetId(event.target.value)}
                placeholder="dashboard_ops"
                value={datasetId}
              />
            </label>
            <label className="compact-filter small">
              <span>Location</span>
              <input
                aria-label="BigQuery dataset location"
                disabled={mutationDisabled}
                onChange={(event) => setDatasetLocation(event.target.value)}
                placeholder="US"
                value={datasetLocation}
              />
            </label>
            <label className="compact-filter wide">
              <span>Description</span>
              <input
                aria-label="BigQuery dataset description"
                disabled={mutationDisabled}
                onChange={(event) => setDatasetDescription(event.target.value)}
                placeholder="local dashboard dataset"
                value={datasetDescription}
              />
            </label>
          </div>
        )}
        <Button disabled={mutationDisabled} type="submit">
          Create dataset
        </Button>
      </form>

      <form className="dynamodb-operation-form" onSubmit={submitTable}>
        <div className="dynamodb-toolbar">
          <span className="inspector-label">Create table</span>
          <label className="compact-filter small">
            <span>raw JSON</span>
            <input
              aria-label="Use raw JSON for BigQuery table"
              checked={tableRawMode}
              disabled={mutationDisabled}
              onChange={(event) => setTableRawMode(event.target.checked)}
              type="checkbox"
            />
          </label>
        </div>
        <div className="pubsub-action-row">
          <label className="compact-filter">
            <span>Dataset ID</span>
            <input
              aria-label="BigQuery table dataset ID"
              disabled={mutationDisabled}
              onChange={(event) => setTableDatasetId(event.target.value)}
              placeholder={activeDatasetId ?? 'dataset'}
              value={tableDatasetId}
            />
          </label>
          {!tableRawMode ? (
            <>
              <label className="compact-filter">
                <span>Table ID</span>
                <input
                  aria-label="BigQuery table ID"
                  disabled={mutationDisabled}
                  onChange={(event) => setTableId(event.target.value)}
                  placeholder="events"
                  value={tableId}
                />
              </label>
              <label className="compact-filter wide">
                <span>Description</span>
                <input
                  aria-label="BigQuery table description"
                  disabled={mutationDisabled}
                  onChange={(event) => setTableDescription(event.target.value)}
                  placeholder="local events table"
                  value={tableDescription}
                />
              </label>
            </>
          ) : null}
        </div>
        {tableRawMode ? (
          <label className="redshift-sql-editor">
            <span>Table raw JSON</span>
            <textarea
              aria-label="BigQuery table raw JSON"
              disabled={mutationDisabled}
              onChange={(event) => setTableRawJSON(event.target.value)}
              rows={9}
              spellCheck={false}
              value={tableRawJSON}
            />
          </label>
        ) : (
          <label className="redshift-sql-editor">
            <span>Schema fields</span>
            <textarea
              aria-label="BigQuery table schema fields"
              disabled={mutationDisabled}
              onChange={(event) => setSchemaFields(event.target.value)}
              rows={4}
              spellCheck={false}
              value={schemaFields}
            />
          </label>
        )}
        <Button disabled={mutationDisabled} type="submit">
          Create table
        </Button>
      </form>

      <form className="dynamodb-operation-form" onSubmit={submitRows}>
        <span className="inspector-label">Insert row</span>
        <div className="pubsub-action-row">
          <label className="compact-filter">
            <span>Dataset ID</span>
            <input
              aria-label="BigQuery insert dataset ID"
              disabled={mutationDisabled}
              onChange={(event) => setRowDatasetId(event.target.value)}
              placeholder={activeDatasetId ?? 'dataset'}
              value={rowDatasetId}
            />
          </label>
          <label className="compact-filter">
            <span>Table ID</span>
            <input
              aria-label="BigQuery insert table ID"
              disabled={mutationDisabled}
              onChange={(event) => setRowTableId(event.target.value)}
              placeholder={activeTableId ?? 'table'}
              value={rowTableId}
            />
          </label>
          <label className="compact-filter">
            <span>Insert ID</span>
            <input
              aria-label="BigQuery insert ID"
              disabled={mutationDisabled}
              onChange={(event) => setInsertId(event.target.value)}
              placeholder="optional"
              value={insertId}
            />
          </label>
        </div>
        <label className="redshift-sql-editor">
          <span>Row JSON</span>
          <textarea
            aria-label="BigQuery insert row JSON"
            disabled={mutationDisabled}
            onChange={(event) => setRowJSON(event.target.value)}
            rows={6}
            spellCheck={false}
            value={rowJSON}
          />
        </label>
        <p className="inspector-muted">JSON validation runs locally before tabledata.insertAll. Row payloads are not written to logs.</p>
        <Button disabled={mutationDisabled} type="submit">
          Insert row
        </Button>
      </form>
    </section>
  )
}

type BigQueryQueryRunnerProps = {
  disabled: boolean
  projectId: string
  onQuerySuccess: () => void
}

function BigQueryQueryRunner({ disabled, projectId, onQuerySuccess }: BigQueryQueryRunnerProps): JSX.Element {
  const [query, setQuery] = useState('SELECT * FROM `analytics.people` LIMIT 10')
  const [maxResults, setMaxResults] = useState('25')
  const [dryRun, setDryRun] = useState(false)
  const [runnerState, setRunnerState] = useState<QueryRunnerState>({ status: 'idle' })
  const [selectedResultIndex, setSelectedResultIndex] = useState(0)

  const result = runnerState.status === 'success' ? runnerState.response : undefined
  const resultRows = result?.rows ?? []
  const selectedResultRow = resultRows[Math.min(selectedResultIndex, Math.max(resultRows.length - 1, 0))]

  function submitQuery(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault()
    const sql = query.trim()
    if (sql === '') {
      setRunnerState({ status: 'error', message: 'Query text is required.' })
      return
    }
    const parsedMaxResults = Number.parseInt(maxResults, 10)
    if (!Number.isInteger(parsedMaxResults) || parsedMaxResults < 1 || parsedMaxResults > 1000) {
      setRunnerState({ status: 'error', message: 'Max results must be between 1 and 1000.' })
      return
    }

    setRunnerState({ status: 'running' })
    runBigQueryQuery(projectId, {
      query: sql,
      maxResults: parsedMaxResults,
      dryRun,
      useLegacySql: false,
    })
      .then((response) => {
        setSelectedResultIndex(0)
        setRunnerState({ status: 'success', response })
        onQuerySuccess()
      })
      .catch((error: Error) => {
        setRunnerState({ status: 'error', message: error.message })
      })
  }

  return (
    <section className="redshift-query-runner" aria-label="BigQuery SQL query runner">
      <div className="dynamodb-toolbar">
        <span className="inspector-label">SQL query runner</span>
        <span className="toolbar-count">
          {result ? `${result.totalRows} rows / ${result.jobReference.jobId}` : 'useLegacySql=false'}
        </span>
      </div>
      <form className="redshift-query-form" onSubmit={submitQuery}>
        <label className="redshift-sql-editor">
          <span>Query text</span>
          <textarea
            aria-label="BigQuery SQL query text"
            disabled={disabled || runnerState.status === 'running'}
            onChange={(event) => setQuery(event.target.value)}
            rows={5}
            spellCheck={false}
            value={query}
          />
        </label>
        <div className="pubsub-action-row">
          <label className="compact-filter small">
            <span>Max results</span>
            <input
              aria-label="BigQuery query max results"
              disabled={disabled || runnerState.status === 'running'}
              max={1000}
              min={1}
              onChange={(event) => setMaxResults(event.target.value)}
              type="number"
              value={maxResults}
            />
          </label>
          <label className="compact-filter small">
            <span>Dry run</span>
            <input
              aria-label="BigQuery dry run"
              checked={dryRun}
              disabled={disabled || runnerState.status === 'running'}
              onChange={(event) => setDryRun(event.target.checked)}
              type="checkbox"
            />
          </label>
          <Button disabled={disabled || runnerState.status === 'running'} type="submit">
            {runnerState.status === 'running' ? 'Running' : dryRun ? 'Dry run' : 'Run query'}
          </Button>
        </div>
      </form>
      {disabled ? <p className="inspector-muted">BigQuery query controls are disabled until the service is active.</p> : null}
      {runnerState.status === 'error' ? <p className="operation-message error">{runnerState.message}</p> : null}
      {result ? (
        <BigQueryQueryResult
          response={result}
          selectedRow={selectedResultRow}
          selectedRowIndex={selectedResultIndex}
          onSelectRow={setSelectedResultIndex}
        />
      ) : null}
    </section>
  )
}

type BigQueryQueryResultProps = {
  response: BigQueryQueryResponse
  selectedRow?: { f: Array<{ v: unknown }> }
  selectedRowIndex: number
  onSelectRow: (index: number) => void
}

function BigQueryQueryResult({
  onSelectRow,
  response,
  selectedRow,
  selectedRowIndex,
}: BigQueryQueryResultProps): JSX.Element {
  const fields = response.schema?.fields ?? []
  const rows = response.rows ?? []
  const columnCount = Math.max(fields.length, rows[0]?.f.length ?? 0)
  const columns = Array.from({ length: columnCount }, (_, index) => ({
    name: fields[index]?.name ?? `column_${index + 1}`,
    type: fields[index]?.type ?? 'UNKNOWN',
  }))

  return (
    <div className="redshift-query-result">
      <div className="attribute-preview" aria-label="BigQuery query summary">
        <span className="attribute-chip">{response.jobComplete ? 'complete' : 'running'}</span>
        <span className="attribute-chip">{response.cacheHit ? 'cache hit' : 'cache miss'}</span>
        <span className="attribute-chip">{response.totalRows} rows</span>
        <span className="attribute-chip">{response.jobReference.jobId}</span>
      </div>
      {rows.length === 0 ? (
        <p className="inspector-muted">Query completed without loaded result rows.</p>
      ) : (
        <div className="dynamodb-query-result-grid">
          <div className="dynamodb-item-table-wrap">
            <table className="dynamodb-item-table compact">
              <thead>
                <tr>
                  {columns.map((column) => (
                    <th key={column.name} scope="col">
                      {column.name}
                      <span className="query-column-type">{column.type}</span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row, rowIndex) => (
                  <tr
                    className={rowIndex === selectedRowIndex ? 'item-row active' : 'item-row'}
                    key={`${response.jobReference.jobId}-${rowIndex}`}
                    onClick={() => onSelectRow(rowIndex)}
                  >
                    {columns.map((column, columnIndex) => (
                      <td key={column.name}>{formatValue(row.f[columnIndex]?.v)}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <section>
            <span className="inspector-label">Selected result JSON</span>
            <pre className="mail-preview">{JSON.stringify(bigQueryRowObject(fields, selectedRow), null, 2)}</pre>
          </section>
        </div>
      )}
      <section>
        <span className="inspector-label">Job reference</span>
        <pre className="mail-preview">{JSON.stringify(response.jobReference, null, 2)}</pre>
      </section>
    </div>
  )
}

type DatasetListProps = {
  datasets: BigQueryDataset[]
  activeDatasetId?: string
  activeTableId?: string
  onSelectDataset: (dataset: BigQueryDataset) => void
  onSelectTable: (table: BigQueryTable) => void
}

function DatasetList({
  activeDatasetId,
  activeTableId,
  datasets,
  onSelectDataset,
  onSelectTable,
}: DatasetListProps): JSX.Element {
  if (datasets.length === 0) {
    return <EmptyState title="No datasets" description="Datasets created through the BigQuery API will appear here." />
  }

  return (
    <div className="dynamodb-table-list" aria-label="BigQuery datasets">
      {datasets.map((dataset) => (
        <section
          className={dataset.datasetId === activeDatasetId ? 'dynamodb-table-row active' : 'dynamodb-table-row'}
          key={dataset.datasetId}
        >
          <button className="object-select" onClick={() => onSelectDataset(dataset)}>
            <span className="table-row-top">
              <span className="table-row-name">{dataset.datasetId}</span>
              <span className="count-pill">{dataset.tables.length}</span>
            </span>
            <span className="table-row-meta">{dataset.location || 'default location'} dataset</span>
          </button>
          <span className="table-row-tags">
            {dataset.tables.length === 0 ? <span>no tables</span> : null}
            {dataset.tables.map((table) => (
              <button
                className="attribute-chip"
                key={table.tableId}
                onClick={() => onSelectTable(table)}
                type="button"
              >
                {table.tableId === activeTableId ? '>' : ''} {table.tableId} ({table.numRows})
              </button>
            ))}
          </span>
        </section>
      ))}
    </div>
  )
}

type RowBrowserProps = {
  activeIndex: number
  rows: BigQueryRow[]
  rowsState: RowsState
  tableName?: string
  onSelectIndex: (index: number) => void
}

function RowBrowser({ activeIndex, rows, rowsState, tableName, onSelectIndex }: RowBrowserProps): JSX.Element {
  if (!tableName) {
    return <EmptyState title="No table selected" description="Choose a BigQuery table to inspect stored rows." />
  }
  if (rowsState.status === 'loading') {
    return <EmptyState title="Loading rows" description={`Reading rows from ${tableName}.`} />
  }
  if (rowsState.status === 'error') {
    return <EmptyState title="BigQuery rows unavailable" description={rowsState.message} />
  }
  if (rows.length === 0) {
    return <EmptyState title="No rows" description={`No loaded rows in ${tableName} match the current filter.`} />
  }

  return (
    <div className="dynamodb-item-table-wrap">
      <table className="dynamodb-item-table">
        <thead>
          <tr>
            <th scope="col">Insert ID</th>
            <th scope="col">Values</th>
            <th scope="col">Inserted</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr
              className={index === activeIndex ? 'item-row active' : 'item-row'}
              key={`${row.insertId ?? 'row'}-${index}`}
              onClick={() => onSelectIndex(index)}
            >
              <td>
                <code>{row.insertId ?? index + 1}</code>
              </td>
              <td>
                <AttributePreview item={row.json} />
              </td>
              <td>{row.insertedAt ?? 'unknown'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

type BigQueryInspectorProps = {
  dataset?: BigQueryDataset
  jobs: BigQueryJob[]
  project: string
  row?: BigQueryRow
  status?: BigQueryStatus
  table?: BigQueryTable
}

function BigQueryInspector({ dataset, jobs, project, row, status, table }: BigQueryInspectorProps): JSX.Element {
  const [selectedJobId, setSelectedJobId] = useState<string>()
  const [jobDetailState, setJobDetailState] = useState<JobDetailState>({ status: 'idle' })
  const recentQueryJobs = useMemo(() => recentJobs(jobs), [jobs])

  useEffect(() => {
    if (jobs.length === 0) {
      setSelectedJobId(undefined)
      setJobDetailState({ status: 'idle' })
      return
    }
    setSelectedJobId((current) => (current && jobs.some((job) => job.jobId === current) ? current : jobs[0].jobId))
  }, [jobs])

  useEffect(() => {
    if (!selectedJobId) {
      setJobDetailState({ status: 'idle' })
      return
    }
    setJobDetailState({ status: 'loading', jobId: selectedJobId })
    getBigQueryJob(project, selectedJobId)
      .then((response) => {
        setJobDetailState({ status: 'success', job: response.job })
      })
      .catch((error: Error) => {
        setJobDetailState({ status: 'error', message: error.message })
      })
  }, [project, selectedJobId])

  if (!dataset) {
    return <EmptyState title="Inspector" description="Dataset, table schema, selected row, and jobs will appear here." />
  }

  return (
    <div className="dynamodb-inspector">
      <section>
        <span className="inspector-label">Catalog</span>
        <h3>{table ? `${dataset.datasetId}.${table.tableId}` : dataset.datasetId}</h3>
        <dl className="inspector-list">
          <div>
            <dt>Project</dt>
            <dd>{project}</dd>
          </div>
          <div>
            <dt>Endpoint</dt>
            <dd>
              <code>{status?.endpoint ?? 'unknown'}</code>
            </dd>
          </div>
          <div>
            <dt>Location</dt>
            <dd>{status?.location ?? dataset.location ?? 'unknown'}</dd>
          </div>
          <div>
            <dt>Rows</dt>
            <dd>{table?.numRows ?? 'select a table'}</dd>
          </div>
          <div>
            <dt>Schema</dt>
            <dd>{table ? schemaLabel(table.schema) : 'select a table'}</dd>
          </div>
          <div>
            <dt>Jobs</dt>
            <dd>{jobs.length === 0 ? 'none' : jobs.map((job) => `${job.jobId} ${job.state}`).join(', ')}</dd>
          </div>
        </dl>
      </section>
      <section>
        <span className="inspector-label">Recent query metadata</span>
        {recentQueryJobs.length === 0 ? (
          <p className="inspector-muted">Query jobs will appear after running SQL.</p>
        ) : (
          <div className="dynamodb-table-list compact-list" aria-label="BigQuery recent query jobs">
            {recentQueryJobs.map((job) => (
              <button
                className={job.jobId === selectedJobId ? 'object-select active' : 'object-select'}
                key={job.jobId}
                onClick={() => setSelectedJobId(job.jobId)}
                type="button"
              >
                <span className="table-row-top">
                  <span className="table-row-name">{job.jobId}</span>
                  <span className="count-pill">{job.state}</span>
                </span>
                <span className="table-row-meta">{jobMetadataLabel(job)}</span>
              </button>
            ))}
          </div>
        )}
      </section>
      <section>
        <span className="inspector-label">Selected job JSON</span>
        {jobDetailState.status === 'loading' ? <p className="inspector-muted">Loading job detail.</p> : null}
        {jobDetailState.status === 'error' ? <p className="operation-message error">{jobDetailState.message}</p> : null}
        {jobDetailState.status === 'success' ? (
          <pre className="mail-preview">{JSON.stringify(sanitizedJobDetail(jobDetailState.job), null, 2)}</pre>
        ) : null}
      </section>
      <section>
        <span className="inspector-label">Selected row</span>
        {row ? (
          <pre className="mail-preview">{JSON.stringify(row.json, null, 2)}</pre>
        ) : (
          <p className="inspector-muted">Select a row to inspect JSON.</p>
        )}
      </section>
    </div>
  )
}

function AttributePreview({ item }: { item: Record<string, unknown> }): JSX.Element {
  const attributes = Object.entries(item).slice(0, 6)
  if (attributes.length === 0) {
    return <span className="service-status">empty row</span>
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

function schemaLabel(schema: { fields?: Array<{ name: string; type?: string; mode?: string }> }): string {
  const fields = schema.fields ?? []
  if (fields.length === 0) {
    return 'No schema fields'
  }
  return fields.map((field) => `${field.name} ${field.type ?? 'STRING'} ${field.mode ?? 'NULLABLE'}`).join(' / ')
}

function recentJobs(jobs: BigQueryJob[]): BigQueryJob[] {
  return [...jobs].sort((left, right) => jobCreationTime(right) - jobCreationTime(left)).slice(0, 8)
}

function jobCreationTime(job: BigQueryJob): number {
  const raw = job.job?.statistics?.creationTime
  if (!raw) {
    return 0
  }
  const millis = Number.parseInt(raw, 10)
  return Number.isFinite(millis) ? millis : 0
}

function jobMetadataLabel(job: BigQueryJob): string {
  const query = job.job?.statistics?.query
  const totalRows = query?.totalRows ?? '0'
  const dryRun = query?.dryRun ? 'dry run' : 'executed'
  const cache = query?.cacheHit ? 'cache hit' : 'cache miss'
  return `${dryRun} / ${totalRows} rows / ${cache}`
}

function sanitizedJobDetail(job: BigQueryJob): Record<string, unknown> {
  const detail = (job.job ?? job) as Record<string, unknown>
  const cloned = JSON.parse(JSON.stringify(detail)) as Record<string, unknown>
  const configuration = cloned.configuration
  if (isRecord(configuration) && isRecord(configuration.query)) {
    delete configuration.query.queryParameters
  }
  return cloned
}

function bigQueryRowObject(
  fields: Array<{ name: string }>,
  row?: { f: Array<{ v: unknown }> },
): Record<string, unknown> {
  if (!row) {
    return {}
  }
  return row.f.reduce<Record<string, unknown>>((record, cell, index) => {
    record[fields[index]?.name ?? `column_${index + 1}`] = cell.v
    return record
  }, {})
}

function parseJSONRecord<T extends Record<string, unknown>>(source: string, label: string): T | Error {
  try {
    const parsed = JSON.parse(source) as unknown
    if (!isRecord(parsed)) {
      return new Error(`${label} must be a JSON object.`)
    }
    return parsed as T
  } catch {
    return new Error(`${label} has a JSON validation error.`)
  }
}

function parseSchemaFields(source: string): BigQuerySchema | Error {
  const fields = source
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [name, type = 'STRING', mode] = line.split(':').map((part) => part.trim())
      if (!name) {
        return new Error('Schema field names are required.')
      }
      return {
        name,
        type: type || 'STRING',
        mode: mode || undefined,
      }
    })

  const error = fields.find((field): field is Error => field instanceof Error)
  if (error) {
    return error
  }
  return { fields }
}

function parseInsertRows(source: string, insertId?: string): BigQueryInsertAllRow[] | Error {
  let parsed: unknown
  try {
    parsed = JSON.parse(source) as unknown
  } catch {
    return new Error('Row JSON has a JSON validation error.')
  }

  if (Array.isArray(parsed)) {
    const rows: BigQueryInsertAllRow[] = []
    for (const [index, row] of parsed.entries()) {
      if (!isRecord(row)) {
        return new Error(`Row ${index + 1} must be a JSON object.`)
      }
      rows.push({ json: row })
    }
    return rows
  }

  if (!isRecord(parsed)) {
    return new Error('Row JSON must be a JSON object or an array of JSON objects.')
  }
  return [{ insertId: insertId || undefined, json: parsed }]
}

function readNestedString(record: Record<string, unknown>, path: string[]): string | undefined {
  let current: unknown = record
  for (const segment of path) {
    if (!isRecord(current)) {
      return undefined
    }
    current = current[segment]
  }
  return typeof current === 'string' && current.trim() !== '' ? current.trim() : undefined
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function disabledStatus(service?: DashboardService): BigQueryStatus {
  return {
    status: 'disabled',
    running: false,
    endpoint: service?.endpoint ?? 'http://127.0.0.1:9050',
    project: 'devcloud',
    location: 'US',
    authMode: 'relaxed',
    storagePath: service?.storagePath ?? '.devcloud/data/bigquery',
    datasetCount: 0,
    jobCount: 0,
  }
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
