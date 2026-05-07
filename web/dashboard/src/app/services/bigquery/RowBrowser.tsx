import { EmptyState } from '../../../ui/EmptyState'
import { formatValue } from './helpers'
import type { RowsState } from './state'
import type { BigQueryRow } from './types'

type RowBrowserProps = {
  activeIndex: number
  rows: BigQueryRow[]
  rowsState: RowsState
  tableName?: string
  onSelectIndex: (index: number) => void
}

export function RowBrowser({ activeIndex, rows, rowsState, tableName, onSelectIndex }: RowBrowserProps): JSX.Element {
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
