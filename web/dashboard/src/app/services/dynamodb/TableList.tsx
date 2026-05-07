import { EmptyState } from '../../../ui/EmptyState'
import { indexCount, keySchemaLabel } from './helpers'
import type { DynamoDBTableSummary } from './types'

type TableListProps = {
  tables: DynamoDBTableSummary[]
  activeTableName?: string
  onSelectTable: (tableName: string) => void
}

export function TableList({ tables, activeTableName, onSelectTable }: TableListProps): JSX.Element {
  if (tables.length === 0) {
    return <EmptyState title="No tables" description="Tables created through the DynamoDB API will appear here." />
  }

  return (
    <div className="dynamodb-table-list" aria-label="DynamoDB tables">
      {tables.map((table) => (
        <button
          className={table.tableName === activeTableName ? 'dynamodb-table-row active' : 'dynamodb-table-row'}
          key={table.tableName}
          onClick={() => onSelectTable(table.tableName)}
        >
          <span className="table-row-top">
            <span className="table-row-name">{table.tableName}</span>
            <span className="count-pill">{table.itemCount}</span>
          </span>
          <span className="table-row-meta">{keySchemaLabel(table)}</span>
          <span className="table-row-tags">
            <span>{table.tableStatus}</span>
            <span>{indexCount(table)} indexes</span>
            <span>{table.streamSpecification?.StreamEnabled ? 'streams on' : 'streams off'}</span>
          </span>
        </button>
      ))}
    </div>
  )
}
