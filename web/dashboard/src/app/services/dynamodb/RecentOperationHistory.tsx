import { Button } from '../../../ui/Button'
import { formatRecentOperationTime } from './helpers'
import type { RecentDynamoDBOperation } from './state'

type RecentOperationHistoryProps = {
  operations: RecentDynamoDBOperation[]
  onClear: () => void
}

export function RecentOperationHistory({ operations, onClear }: RecentOperationHistoryProps): JSX.Element {
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
