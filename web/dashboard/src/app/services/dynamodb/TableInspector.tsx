import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { attributeDefinitionsLabel, keySchemaLabel, streamLabel, ttlLabel } from './helpers'
import type { TableDetailState } from './state'
import type {
  DynamoDBIndex,
  DynamoDBItemSnapshot,
  DynamoDBStatus,
  DynamoDBTableSummary,
} from './types'

type TableInspectorProps = {
  detailState: TableDetailState
  table?: DynamoDBTableSummary
  item?: DynamoDBItemSnapshot
  onRefreshDetail: () => void
  status?: DynamoDBStatus
}

export function TableInspector({ detailState, table, item, onRefreshDetail, status }: TableInspectorProps): JSX.Element {
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
