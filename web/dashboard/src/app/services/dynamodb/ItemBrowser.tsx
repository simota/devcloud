import { Button } from '../../../ui/Button'
import { EmptyState } from '../../../ui/EmptyState'
import { formatBytes, formatValue } from './helpers'
import type { ItemsState } from './state'
import type { DynamoDBItemSnapshot, DynamoDBTableSummary } from './types'

type ItemBrowserProps = {
  activeIndex: number
  items: DynamoDBItemSnapshot[]
  itemsState: ItemsState
  tableName?: string
  onSelectIndex: (index: number) => void
}

export function ItemBrowser({ activeIndex, items, itemsState, onSelectIndex, tableName }: ItemBrowserProps): JSX.Element {
  if (!tableName) {
    return <EmptyState title="No table selected" description="Choose a table to inspect its stored items." />
  }
  if (itemsState.status === 'loading') {
    return <EmptyState title="Loading items" description={`Reading items from ${tableName}.`} />
  }
  if (itemsState.status === 'error') {
    return <EmptyState title="DynamoDB items unavailable" description={itemsState.message} />
  }
  if (items.length === 0) {
    return <EmptyState title="No items" description={`No loaded items in ${tableName} match the current filter.`} />
  }

  return (
    <div className="dynamodb-item-table-wrap">
      <table className="dynamodb-item-table">
        <thead>
          <tr>
            <th scope="col">Key</th>
            <th scope="col">Attributes</th>
            <th scope="col">Size</th>
          </tr>
        </thead>
        <tbody>
          {items.map((entry, index) => (
            <tr
              className={index === activeIndex ? 'item-row active' : 'item-row'}
              key={`${JSON.stringify(entry.key)}-${index}`}
              onClick={() => onSelectIndex(index)}
            >
              <td>
                <code>{JSON.stringify(entry.key)}</code>
              </td>
              <td>
                <AttributePreview item={entry.item} />
              </td>
              <td>{formatBytes(JSON.stringify(entry.item).length)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

type AttributePreviewProps = {
  item: Record<string, unknown>
}

export function AttributePreview({ item }: AttributePreviewProps): JSX.Element {
  const attributes = Object.entries(item)
    .filter(([key]) => key !== 'pk' && key !== 'sk')
    .slice(0, 6)

  if (attributes.length === 0) {
    return <span className="service-status">key only</span>
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

type KeyLookupProps = {
  message: string
  onFind: () => void
  onUpdateValue: (attributeName: string, value: string) => void
  table?: DynamoDBTableSummary
  values: Record<string, string>
}

export function KeyLookup({ message, onFind, onUpdateValue, table, values }: KeyLookupProps): JSX.Element | null {
  const keys = table?.keySchema ?? []
  if (!table || keys.length === 0) {
    return null
  }
  return (
    <div className="dynamodb-key-lookup">
      <span className="inspector-label">Key lookup</span>
      <div className="pubsub-action-row">
        {keys.map((key) => (
          <label className="compact-filter" key={key.AttributeName}>
            <span>
              {key.AttributeName} {key.KeyType}
            </span>
            <input
              aria-label={`DynamoDB key lookup ${key.AttributeName}`}
              onChange={(event) => onUpdateValue(key.AttributeName, event.target.value)}
              placeholder={key.AttributeName}
              value={values[key.AttributeName] ?? ''}
            />
          </label>
        ))}
        <Button onClick={onFind}>Find loaded item</Button>
      </div>
      {message ? <p className="inspector-muted">{message}</p> : null}
    </div>
  )
}
