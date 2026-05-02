import { useCallback, useEffect, useMemo, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import type { DashboardService } from '../dashboard/types'
import { getBigQueryStatus, listBigQueryDatasets, listBigQueryJobs, listBigQueryProjects, listBigQueryRows } from './api'
import type { BigQueryDataset, BigQueryJob, BigQueryRow, BigQueryStatus, BigQueryTable } from './types'

type CatalogState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: BigQueryStatus; datasets: BigQueryDataset[]; jobs: BigQueryJob[] }
  | { status: 'error'; message: string }

type RowsState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; rows: BigQueryRow[] }
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
