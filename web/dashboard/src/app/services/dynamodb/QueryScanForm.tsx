import type { FormEvent } from 'react'
import { Button } from '../../../ui/Button'
import { AttributePreview } from './ItemBrowser'
import type { DynamoDBQueryScanResponse } from './types'

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

export function DynamoDBQueryScanForm({
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
