import type {
  BigQueryDataset,
  BigQueryJob,
  BigQueryQueryResponse,
  BigQueryRow,
  BigQueryStatus,
} from './types'

export type CatalogState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: BigQueryStatus; datasets: BigQueryDataset[]; jobs: BigQueryJob[] }
  | { status: 'error'; message: string }

export type RowsState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; rows: BigQueryRow[] }
  | { status: 'error'; message: string }

export type QueryRunnerState =
  | { status: 'idle' }
  | { status: 'running' }
  | { status: 'success'; response: BigQueryQueryResponse }
  | { status: 'error'; message: string }

export type OperationState =
  | { status: 'idle' }
  | { status: 'running'; label: string }
  | { status: 'success'; message: string; insertErrors?: string[] }
  | { status: 'error'; message: string }

export type JobDetailState =
  | { status: 'idle' }
  | { status: 'loading'; jobId: string }
  | { status: 'success'; job: BigQueryJob }
  | { status: 'error'; message: string }
