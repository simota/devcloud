import { type FormEvent, useEffect, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Button } from '../../../ui/Button'
import type { S3ObjectSummary } from './types'

type ObjectInspectorProps = {
  bucketName?: string
  disabled: boolean
  object?: S3ObjectSummary
  onCopyObject: (destinationKey: string) => Promise<void>
}

export function ObjectInspector({ bucketName, disabled, object, onCopyObject }: ObjectInspectorProps): JSX.Element {
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
      <CopyObjectForm disabled={disabled} object={object} onCopyObject={onCopyObject} />
    </div>
  )
}

type CopyObjectFormProps = {
  disabled: boolean
  object: S3ObjectSummary
  onCopyObject: (destinationKey: string) => Promise<void>
}

function CopyObjectForm({ disabled, object, onCopyObject }: CopyObjectFormProps): JSX.Element {
  const [destinationKey, setDestinationKey] = useState(`${object.key}.copy`)
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    setDestinationKey(`${object.key}.copy`)
  }, [object.key])

  async function submitCopy(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault()
    const key = destinationKey.trim()
    if (disabled || busy || key === '') {
      return
    }
    setBusy(true)
    try {
      await onCopyObject(key)
    } finally {
      setBusy(false)
    }
  }

  return (
    <form className="s3-copy-form" onSubmit={submitCopy}>
      <label className="prefix-filter">
        <span>Copy source</span>
        <input aria-label="S3 copy source" disabled value={object.s3Uri} />
      </label>
      <label className="prefix-filter">
        <span>Copy destination key</span>
        <input
          aria-label="S3 copy destination key"
          disabled={disabled || busy}
          onChange={(event) => setDestinationKey(event.target.value)}
          value={destinationKey}
        />
      </label>
      <Button disabled={disabled || busy || destinationKey.trim() === ''} type="submit">
        {busy ? 'Copying' : 'CopyObject'}
      </Button>
    </form>
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
