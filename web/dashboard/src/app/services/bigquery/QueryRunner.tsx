import { useState } from 'react'
import type { FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { runBigQueryQuery } from './api'
import { bigQueryRowObject, formatValue } from './helpers'
import type { QueryRunnerState } from './state'
import type { BigQueryQueryResponse } from './types'

type BigQueryQueryRunnerProps = {
  disabled: boolean
  projectId: string
  onQuerySuccess: () => void
}

export function BigQueryQueryRunner({ disabled, projectId, onQuerySuccess }: BigQueryQueryRunnerProps): JSX.Element {
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
