import { useCallback, useEffect, useMemo, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import type { DashboardService } from '../dashboard/types'
import {
  getBigQueryStatus,
  listBigQueryDatasets,
  listBigQueryJobs,
  listBigQueryProjects,
  listBigQueryRows,
} from './api'
import { DatasetList } from './DatasetList'
import { disabledStatus } from './helpers'
import { BigQueryInspector } from './Inspector'
import { BigQueryManagementPanel } from './ManagementPanel'
import { BigQueryQueryRunner } from './QueryRunner'
import { RowBrowser } from './RowBrowser'
import type { CatalogState, RowsState } from './state'
import type { BigQueryDataset, BigQueryTable } from './types'

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
