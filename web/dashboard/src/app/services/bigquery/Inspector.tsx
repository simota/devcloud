import { useEffect, useMemo, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { getBigQueryJob } from './api'
import { jobMetadataLabel, recentJobs, sanitizedJobDetail, schemaLabel } from './helpers'
import type { JobDetailState } from './state'
import type {
  BigQueryDataset,
  BigQueryJob,
  BigQueryRow,
  BigQueryStatus,
  BigQueryTable,
} from './types'

type BigQueryInspectorProps = {
  dataset?: BigQueryDataset
  jobs: BigQueryJob[]
  project: string
  row?: BigQueryRow
  status?: BigQueryStatus
  table?: BigQueryTable
}

export function BigQueryInspector({ dataset, jobs, project, row, status, table }: BigQueryInspectorProps): JSX.Element {
  const [selectedJobId, setSelectedJobId] = useState<string>()
  const [jobDetailState, setJobDetailState] = useState<JobDetailState>({ status: 'idle' })
  const recentQueryJobs = useMemo(() => recentJobs(jobs), [jobs])

  useEffect(() => {
    if (jobs.length === 0) {
      setSelectedJobId(undefined)
      setJobDetailState({ status: 'idle' })
      return
    }
    setSelectedJobId((current) => (current && jobs.some((job) => job.jobId === current) ? current : jobs[0].jobId))
  }, [jobs])

  useEffect(() => {
    if (!selectedJobId) {
      setJobDetailState({ status: 'idle' })
      return
    }
    setJobDetailState({ status: 'loading', jobId: selectedJobId })
    getBigQueryJob(project, selectedJobId)
      .then((response) => {
        setJobDetailState({ status: 'success', job: response.job })
      })
      .catch((error: Error) => {
        setJobDetailState({ status: 'error', message: error.message })
      })
  }, [project, selectedJobId])

  if (!dataset) {
    return <EmptyState title="Inspector" description="Dataset, table schema, selected row, and jobs will appear here." />
  }

  return (
    <div className="dynamodb-inspector">
      <section>
        <span className="inspector-label">Catalog</span>
        <h3>{table ? `${dataset.datasetId}.${table.tableId}` : dataset.datasetId}</h3>
        <dl className="inspector-list">
          <div>
            <dt>Project</dt>
            <dd>{project}</dd>
          </div>
          <div>
            <dt>Endpoint</dt>
            <dd>
              <code>{status?.endpoint ?? 'unknown'}</code>
            </dd>
          </div>
          <div>
            <dt>Location</dt>
            <dd>{status?.location ?? dataset.location ?? 'unknown'}</dd>
          </div>
          <div>
            <dt>Rows</dt>
            <dd>{table?.numRows ?? 'select a table'}</dd>
          </div>
          <div>
            <dt>Schema</dt>
            <dd>{table ? schemaLabel(table.schema) : 'select a table'}</dd>
          </div>
          <div>
            <dt>Jobs</dt>
            <dd>{jobs.length === 0 ? 'none' : jobs.map((job) => `${job.jobId} ${job.state}`).join(', ')}</dd>
          </div>
        </dl>
      </section>
      <section>
        <span className="inspector-label">Recent query metadata</span>
        {recentQueryJobs.length === 0 ? (
          <p className="inspector-muted">Query jobs will appear after running SQL.</p>
        ) : (
          <div className="dynamodb-table-list compact-list" aria-label="BigQuery recent query jobs">
            {recentQueryJobs.map((job) => (
              <button
                className={job.jobId === selectedJobId ? 'object-select active' : 'object-select'}
                key={job.jobId}
                onClick={() => setSelectedJobId(job.jobId)}
                type="button"
              >
                <span className="table-row-top">
                  <span className="table-row-name">{job.jobId}</span>
                  <span className="count-pill">{job.state}</span>
                </span>
                <span className="table-row-meta">{jobMetadataLabel(job)}</span>
              </button>
            ))}
          </div>
        )}
      </section>
      <section>
        <span className="inspector-label">Selected job JSON</span>
        {jobDetailState.status === 'loading' ? <p className="inspector-muted">Loading job detail.</p> : null}
        {jobDetailState.status === 'error' ? <p className="operation-message error">{jobDetailState.message}</p> : null}
        {jobDetailState.status === 'success' ? (
          <pre className="mail-preview">{JSON.stringify(sanitizedJobDetail(jobDetailState.job), null, 2)}</pre>
        ) : null}
      </section>
      <section>
        <span className="inspector-label">Selected row</span>
        {row ? (
          <pre className="mail-preview">{JSON.stringify(row.json, null, 2)}</pre>
        ) : (
          <p className="inspector-muted">Select a row to inspect JSON.</p>
        )}
      </section>
    </div>
  )
}
