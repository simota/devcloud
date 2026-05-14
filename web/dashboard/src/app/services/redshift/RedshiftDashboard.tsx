import { useCallback, useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { useEventSource } from '../../api/hooks/useEventSource'
import type { DashboardService } from '../dashboard/types'
import { getRedshiftCatalog, getRedshiftStatus, listRedshiftClusters, listRedshiftStatements, runRedshiftQuery } from './api'
import type { RedshiftCatalog, RedshiftCluster, RedshiftQueryResult, RedshiftStatement, RedshiftStatus, RedshiftTable } from './types'

type RedshiftState =
  | { status: 'loading' }
  | {
      status: 'success'
      statusPayload: RedshiftStatus
      clusters: RedshiftCluster[]
      catalog: RedshiftCatalog
      statements: RedshiftStatement[]
    }
  | { status: 'error'; message: string }

type RedshiftDashboardProps = {
  service?: DashboardService
}

export function RedshiftDashboard({ service }: RedshiftDashboardProps): JSX.Element {
  const [state, setState] = useState<RedshiftState>({ status: 'loading' })
  const [activeTableKey, setActiveTableKey] = useState<string>()
  const [tableFilter, setTableFilter] = useState('')
  const isDisabled = service?.status === 'disabled'

  const refresh = useCallback(() => {
    if (isDisabled) {
      setState({
        status: 'success',
        statusPayload: disabledStatus(service),
        clusters: [],
        catalog: { database: 'dev', schemas: [], tables: [], columns: [] },
        statements: [],
      })
      setActiveTableKey(undefined)
      return
    }

    setState({ status: 'loading' })
    Promise.all([getRedshiftStatus(), listRedshiftClusters(), getRedshiftCatalog(), listRedshiftStatements()])
      .then(([statusPayload, clustersPayload, catalogPayload, statementsPayload]) => {
        const tables = catalogPayload.catalog.tables
        setState({
          status: 'success',
          statusPayload,
          clusters: clustersPayload.clusters,
          catalog: catalogPayload.catalog,
          statements: statementsPayload.statements,
        })
        setActiveTableKey((current) => (current && tables.some((table) => tableKey(table) === current) ? current : tableKey(tables[0])))
      })
      .catch((error: Error) => {
        setState({ status: 'error', message: error.message })
      })
  }, [isDisabled, service])

  useEffect(() => {
    refresh()
  }, [refresh])

  useEventSource({ topics: ['redshift'], onEvent: refresh, enabled: !isDisabled })

  const catalog = state.status === 'success' ? state.catalog : undefined
  const tables = catalog?.tables ?? []
  const filteredTables = useMemo(() => {
    const query = tableFilter.trim().toLowerCase()
    if (query === '') {
      return tables
    }
    return tables.filter((table) => `${table.schema}.${table.name}`.toLowerCase().includes(query))
  }, [tableFilter, tables])
  const activeTable = tables.find((table) => tableKey(table) === activeTableKey)
  const activeColumns = catalog?.columns.filter((column) => activeTable && column.schema === activeTable.schema && column.table === activeTable.name) ?? []

  if (isDisabled) {
    return (
      <Panel title="Redshift">
        <EmptyState
          title="Redshift is disabled"
          description="Enable the Redshift service in devcloud config to inspect clusters, catalog metadata, and statements."
        />
      </Panel>
    )
  }

  return (
    <div className="dynamodb-workspace">
      <Panel title="Clusters">
        <div className="dynamodb-toolbar">
          <span className="toolbar-count">{state.status === 'success' ? `${state.clusters.length} clusters` : 'Loading'}</span>
          <Button onClick={refresh}>Refresh</Button>
        </div>
        {state.status === 'loading' ? <EmptyState title="Loading Redshift" description="Reading local Redshift metadata." /> : null}
        {state.status === 'error' ? (
          <EmptyState title="Redshift unavailable" description={state.message} actionLabel="Retry" onAction={refresh} />
        ) : null}
        {state.status === 'success' ? <ClusterList clusters={state.clusters} status={state.statusPayload} /> : null}
      </Panel>

      <Panel title="Catalog">
        <div className="dynamodb-toolbar">
          <span className="toolbar-count">{`${filteredTables.length} shown / ${tables.length} tables`}</span>
          <label className="compact-filter">
            <span>Filter</span>
            <input
              aria-label="Filter Redshift tables"
              onChange={(event) => setTableFilter(event.target.value)}
              placeholder="schema or table"
              type="search"
              value={tableFilter}
            />
          </label>
        </div>
        <TableList activeTableKey={activeTableKey} tables={filteredTables} onSelectTable={(table) => setActiveTableKey(tableKey(table))} />
      </Panel>

      <Panel title="Inspector">
        <RedshiftInspector
          columns={activeColumns}
          onQuerySuccess={refresh}
          statements={state.status === 'success' ? state.statements : []}
          status={state.status === 'success' ? state.statusPayload : undefined}
          table={activeTable}
        />
      </Panel>
    </div>
  )
}

function ClusterList({ clusters, status }: { clusters: RedshiftCluster[]; status: RedshiftStatus }): JSX.Element {
  if (clusters.length === 0) {
    return <EmptyState title="No clusters" description="Local Redshift cluster metadata will appear here." />
  }
  return (
    <div className="dynamodb-table-list" aria-label="Redshift clusters">
      {clusters.map((cluster) => (
        <section className="dynamodb-table-row" key={cluster.clusterIdentifier}>
          <span className="table-row-top">
            <span className="table-row-name">{cluster.clusterIdentifier}</span>
            <span className="count-pill">{cluster.clusterStatus}</span>
          </span>
          <span className="table-row-meta">
            {cluster.databaseName} on {cluster.endpoint.address}:{cluster.endpoint.port}
          </span>
          <span className="table-row-tags">
            <span>{cluster.nodeType}</span>
            <span>{cluster.numberOfNodes} node</span>
            <span>{status.region}</span>
          </span>
        </section>
      ))}
    </div>
  )
}

function TableList({
  activeTableKey,
  tables,
  onSelectTable,
}: {
  activeTableKey?: string
  tables: RedshiftTable[]
  onSelectTable: (table: RedshiftTable) => void
}): JSX.Element {
  if (tables.length === 0) {
    return <EmptyState title="No tables" description="Tables created through Redshift SQL or Data API clients will appear here." />
  }
  return (
    <div className="dynamodb-table-list" aria-label="Redshift tables">
      {tables.map((table) => (
        <button
          className={tableKey(table) === activeTableKey ? 'dynamodb-table-row active' : 'dynamodb-table-row'}
          key={tableKey(table)}
          onClick={() => onSelectTable(table)}
          type="button"
        >
          <span className="table-row-top">
            <span className="table-row-name">{table.name}</span>
            <span className="count-pill">{table.rowCount}</span>
          </span>
          <span className="table-row-meta">{table.schema} schema</span>
          <span className="table-row-tags">
            <span>{table.columnCount} columns</span>
            {table.distKey ? <span>distkey {table.distKey}</span> : null}
            {table.sortKeys.length > 0 ? <span>sortkey {table.sortKeys.join(', ')}</span> : null}
          </span>
        </button>
      ))}
    </div>
  )
}

function RedshiftInspector({
  columns,
  onQuerySuccess,
  statements,
  status,
  table,
}: {
  columns: RedshiftCatalog['columns']
  onQuerySuccess: () => void
  statements: RedshiftStatement[]
  status?: RedshiftStatus
  table?: RedshiftTable
}): JSX.Element {
  return (
    <div className="dynamodb-inspector">
      <RedshiftQueryRunner onQuerySuccess={onQuerySuccess} />
      {!table ? <EmptyState title="Inspector" description="Table columns and recent statements will appear here." /> : null}
      {table ? (
      <section>
        <span className="inspector-label">Table</span>
        <h3>{`${table.schema}.${table.name}`}</h3>
        <dl className="inspector-list">
          <div>
            <dt>SQL endpoint</dt>
            <dd>
              <code>{status?.sqlEndpoint ?? '127.0.0.1:5439'}</code>
            </dd>
          </div>
          <div>
            <dt>API endpoint</dt>
            <dd>
              <code>{status?.apiEndpoint ?? 'http://127.0.0.1:9099'}</code>
            </dd>
          </div>
          <div>
            <dt>Rows</dt>
            <dd>{table.rowCount}</dd>
          </div>
          <div>
            <dt>Distribution</dt>
            <dd>{table.distStyle || 'auto'} {table.distKey ? `on ${table.distKey}` : ''}</dd>
          </div>
          <div>
            <dt>Sort keys</dt>
            <dd>{table.sortKeys.length > 0 ? table.sortKeys.join(', ') : 'none'}</dd>
          </div>
        </dl>
      </section>
      ) : null}
      {table ? (
      <section>
        <span className="inspector-label">Columns</span>
        {columns.length === 0 ? (
          <p className="inspector-muted">No column metadata recorded.</p>
        ) : (
          <div className="attribute-preview">
            {columns.map((column) => (
              <span className="attribute-chip" key={`${column.table}-${column.name}`}>
                {column.name} {column.dataType}
                {column.encoding ? ` encode ${column.encoding}` : ''}
                {column.identity ? ' identity' : ''}
              </span>
            ))}
          </div>
        )}
      </section>
      ) : null}
      <section>
        <span className="inspector-label">Recent statements</span>
        {statements.length === 0 ? (
          <p className="inspector-muted">No Redshift statements recorded.</p>
        ) : (
          <div className="dynamodb-item-table-wrap">
            <table className="dynamodb-item-table">
              <thead>
                <tr>
                  <th scope="col">ID</th>
                  <th scope="col">Status</th>
                  <th scope="col">Rows</th>
                  <th scope="col">SQL</th>
                </tr>
              </thead>
              <tbody>
                {statements.slice(0, 8).map((statement) => (
                  <tr className="item-row" key={statement.id}>
                    <td>
                      <code>{statement.redshiftQueryId}</code>
                    </td>
                    <td>{statement.status}</td>
                    <td>{statement.resultRows}</td>
                    <td>
                      {statement.queryPreview}
                      {statement.queryRedacted || statement.queryTruncated ? (
                        <span className="attribute-chip">{statement.queryRedacted ? 'redacted' : 'truncated'}</span>
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  )
}

function RedshiftQueryRunner({ onQuerySuccess }: { onQuerySuccess: () => void }): JSX.Element {
  const [querySql, setQuerySql] = useState('select 1')
  const [maxRows, setMaxRows] = useState('100')
  const [queryResult, setQueryResult] = useState<RedshiftQueryResult>()
  const [queryError, setQueryError] = useState<string>()
  const [querySuccess, setQuerySuccess] = useState<string>()
  const [isRunning, setIsRunning] = useState(false)

  const submitQuery = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (querySql.trim() === '') {
      setQueryResult(undefined)
      setQuerySuccess(undefined)
      setQueryError('SQL is required.')
      return
    }

    const requestedMaxRows = Number.parseInt(maxRows, 10)
    const safeMaxRows = Number.isFinite(requestedMaxRows) ? Math.max(1, requestedMaxRows) : 100
    setMaxRows(String(safeMaxRows))
    setIsRunning(true)
    setQueryError(undefined)
    setQuerySuccess(undefined)
    runRedshiftQuery({ sql: querySql, maxRows: safeMaxRows })
      .then((response) => {
        setQueryResult(response.result)
        setQuerySuccess(`${response.result.statement.status}: ${response.result.commandTag || 'statement completed'}`)
        onQuerySuccess()
      })
      .catch(() => {
        setQueryResult(undefined)
        setQuerySuccess(undefined)
        setQueryError('Query failed. Check SQL syntax and local Redshift state.')
      })
      .finally(() => setIsRunning(false))
  }

  return (
    <section className="redshift-query-runner" aria-label="Redshift query runner">
      <div className="dynamodb-toolbar">
        <span className="inspector-label">Query runner</span>
        <span className="toolbar-count">{queryResult ? `${queryResult.rowCount} rows` : 'Ready'}</span>
      </div>
      <form className="redshift-query-form" onSubmit={submitQuery}>
        <label className="redshift-sql-editor">
          <span>SQL</span>
          <textarea
            aria-label="Redshift SQL"
            onChange={(event) => setQuerySql(event.target.value)}
            rows={5}
            spellCheck={false}
            value={querySql}
          />
        </label>
        <div className="pubsub-action-row">
          <label className="compact-filter small">
            <span>Max rows</span>
            <input
              min={1}
              onChange={(event) => setMaxRows(event.target.value)}
              type="number"
              value={maxRows}
            />
          </label>
          <Button disabled={isRunning} type="submit">
            {isRunning ? 'Running' : 'Run query'}
          </Button>
        </div>
      </form>
      {queryError ? <p className="operation-message error">{queryError}</p> : null}
      {querySuccess ? <p className="operation-message success">{querySuccess}</p> : null}
      {queryResult ? <RedshiftQueryResultTable queryResult={queryResult} /> : null}
    </section>
  )
}

function RedshiftQueryResultTable({ queryResult }: { queryResult: RedshiftQueryResult }): JSX.Element {
  return (
    <div className="redshift-query-result">
      <div className="attribute-preview" aria-label="Redshift query summary">
        <span className="attribute-chip">{queryResult.statement.status}</span>
        <span className="attribute-chip">{queryResult.commandTag}</span>
        <span className="attribute-chip">{queryResult.rowCount} rows</span>
      </div>
      {queryResult.columns.length === 0 ? (
        <p className="inspector-muted">Statement completed without a result set.</p>
      ) : (
        <div className="dynamodb-item-table-wrap">
          <table className="dynamodb-item-table">
            <thead>
              <tr>
                {queryResult.columns.map((column) => (
                  <th scope="col" key={column.name}>
                    {column.name}
                    <span className="query-column-type">{column.typeName}</span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {queryResult.rows.length === 0 ? (
                <tr>
                  <td colSpan={queryResult.columns.length}>No rows returned.</td>
                </tr>
              ) : (
                queryResult.rows.map((row, rowIndex) => (
                  <tr className="item-row" key={rowIndex}>
                    {queryResult.columns.map((column, columnIndex) => (
                      <td key={`${rowIndex}-${column.name}`}>{String(row[columnIndex] ?? '')}</td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function disabledStatus(service?: DashboardService): RedshiftStatus {
  return {
    service: 'redshift',
    status: 'disabled',
    running: false,
    sqlEndpoint: '127.0.0.1:5439',
    apiEndpoint: service?.endpoint ?? 'http://127.0.0.1:9099',
    region: 'us-east-1',
    clusterCount: 0,
    storagePath: service?.storagePath ?? '.devcloud/data/redshift',
    backendKind: 'postgres',
    backendMode: 'managed',
  }
}

function tableKey(table?: RedshiftTable): string | undefined {
  if (!table) {
    return undefined
  }
  return `${table.schema}.${table.name}`
}
