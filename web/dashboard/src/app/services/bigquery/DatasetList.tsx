import { EmptyState } from '../../../ui/EmptyState'
import type { BigQueryDataset, BigQueryTable } from './types'

type DatasetListProps = {
  datasets: BigQueryDataset[]
  activeDatasetId?: string
  activeTableId?: string
  onSelectDataset: (dataset: BigQueryDataset) => void
  onSelectTable: (table: BigQueryTable) => void
}

export function DatasetList({
  activeDatasetId,
  activeTableId,
  datasets,
  onSelectDataset,
  onSelectTable,
}: DatasetListProps): JSX.Element {
  if (datasets.length === 0) {
    return <EmptyState title="No datasets" description="Datasets created through the BigQuery API will appear here." />
  }

  return (
    <div className="dynamodb-table-list" aria-label="BigQuery datasets">
      {datasets.map((dataset) => (
        <section
          className={dataset.datasetId === activeDatasetId ? 'dynamodb-table-row active' : 'dynamodb-table-row'}
          key={dataset.datasetId}
        >
          <button className="object-select" onClick={() => onSelectDataset(dataset)}>
            <span className="table-row-top">
              <span className="table-row-name">{dataset.datasetId}</span>
              <span className="count-pill">{dataset.tables.length}</span>
            </span>
            <span className="table-row-meta">{dataset.location || 'default location'} dataset</span>
          </button>
          <span className="table-row-tags">
            {dataset.tables.length === 0 ? <span>no tables</span> : null}
            {dataset.tables.map((table) => (
              <button
                className="attribute-chip"
                key={table.tableId}
                onClick={() => onSelectTable(table)}
                type="button"
              >
                {table.tableId === activeTableId ? '>' : ''} {table.tableId} ({table.numRows})
              </button>
            ))}
          </span>
        </section>
      ))}
    </div>
  )
}
