import { EmptyState } from '../../../ui/EmptyState'
import type { S3ObjectSummary } from './types'

type ObjectInspectorProps = {
  bucketName?: string
  object?: S3ObjectSummary
}

export function ObjectInspector({ bucketName, object }: ObjectInspectorProps): JSX.Element {
  if (!object) {
    return (
      <EmptyState
        title="Inspector"
        description={
          bucketName
            ? `Select an object in ${bucketName} to inspect metadata and download actions.`
            : 'Object metadata and download actions will appear here.'
        }
      />
    )
  }

  const metadataEntries = Object.entries(object.metadata ?? {})

  return (
    <div className="object-inspector">
      <div>
        <span className="inspector-label">Object key</span>
        <code>{object.key}</code>
      </div>
      <dl className="inspector-list">
        <div>
          <dt>Size</dt>
          <dd>{formatBytes(object.size)}</dd>
        </div>
        <div>
          <dt>ETag</dt>
          <dd>
            <code>{object.etag || 'unknown'}</code>
          </dd>
        </div>
        <div>
          <dt>Content type</dt>
          <dd>{object.contentType || 'application/octet-stream'}</dd>
        </div>
        <div>
          <dt>Last modified</dt>
          <dd>{formatDate(object.lastModified)}</dd>
        </div>
      </dl>
      <div>
        <span className="inspector-label">Metadata</span>
        {metadataEntries.length === 0 ? (
          <p className="inspector-muted">No user metadata.</p>
        ) : (
          <dl className="metadata-list">
            {metadataEntries.map(([key, value]) => (
              <div key={key}>
                <dt>{key}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
        )}
      </div>
      <a className="compat-link inspector-download" href={safeS3DownloadURL(object.downloadUrl)}>
        Download object
      </a>
    </div>
  )
}

function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value < 0) {
    return 'unknown'
  }
  if (value < 1024) {
    return `${value} B`
  }
  const units = ['KB', 'MB', 'GB', 'TB']
  let size = value / 1024
  let unitIndex = 0
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex += 1
  }
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unitIndex]}`
}

function formatDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value || 'unknown'
  }
  return date.toLocaleString()
}

function safeS3DownloadURL(value: string): string {
  return value.startsWith('/api/s3/') ? value : '#'
}
