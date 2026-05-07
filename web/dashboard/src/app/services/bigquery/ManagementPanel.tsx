import { useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { createBigQueryDataset, createBigQueryTable, insertBigQueryRows } from './api'
import { parseInsertRows, parseJSONRecord, parseSchemaFields, readNestedString } from './helpers'
import type { OperationState } from './state'
import type { BigQueryDatasetCreateRequest, BigQueryTableCreateRequest } from './types'

type BigQueryManagementPanelProps = {
  activeDatasetId?: string
  activeTableId?: string
  disabled: boolean
  projectId: string
  onMutationSuccess: () => void
}

export function BigQueryManagementPanel({
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
