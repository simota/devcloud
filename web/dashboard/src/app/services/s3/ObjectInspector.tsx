import { type FormEvent, useEffect, useState } from 'react'
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
      <div className="s3-inspector-empty">
        <InspectorIcon />
        <p className="s3-inspector-empty-title">No object selected</p>
        <p className="s3-inspector-empty-body">
          {bucketName
            ? `Pick an object in ${bucketName} to see metadata, copy, and download actions.`
            : 'Select a bucket first, then click an object to inspect it here.'}
        </p>
      </div>
    )
  }

  const metadataEntries = Object.entries(object.metadata ?? {})

  return (
    <div className="s3-inspector">
      <div className="s3-inspector-section">
        <span className="s3-field-label">Object</span>
        <code className="s3-inspector-key" title={object.key}>{object.key}</code>
        <code className="s3-inspector-uri" title={object.s3Uri}>{object.s3Uri}</code>
      </div>
      <dl className="s3-inspector-list">
        <div>
          <dt>Size</dt>
          <dd>{formatBytes(object.size)}</dd>
        </div>
        <div>
          <dt>Content type</dt>
          <dd>{object.contentType || 'application/octet-stream'}</dd>
        </div>
        <div>
          <dt>Last modified</dt>
          <dd>{formatDate(object.lastModified)}</dd>
        </div>
        <div>
          <dt>ETag</dt>
          <dd><code>{object.etag || 'unknown'}</code></dd>
        </div>
      </dl>
      <div className="s3-inspector-section">
        <span className="s3-field-label">Metadata</span>
        {metadataEntries.length === 0 ? (
          <p className="s3-inspector-muted">No user metadata.</p>
        ) : (
          <dl className="s3-inspector-metadata">
            {metadataEntries.map(([key, value]) => (
              <div key={key}>
                <dt>{key}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
        )}
      </div>
      <a className="s3-inspector-download" href={safeS3DownloadURL(object.downloadUrl)}>
        <DownloadIcon />
        <span>Download object</span>
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
    <form className="s3-inspector-copy" onSubmit={submitCopy}>
      <label className="s3-field">
        <span>Copy to key</span>
        <input
          aria-label="S3 copy destination key"
          className="s3-text-input"
          disabled={disabled || busy}
          onChange={(event) => setDestinationKey(event.target.value)}
          value={destinationKey}
        />
      </label>
      <Button disabled={disabled || busy || destinationKey.trim() === ''} type="submit">
        {busy ? 'Copying…' : 'Copy'}
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

function InspectorIcon(): JSX.Element {
  return (
    <svg aria-hidden width="36" height="36" viewBox="0 0 36 36" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M7 11l11-6 11 6v14L18 31 7 25z" />
      <path d="M7 11l11 6 11-6" />
      <path d="M18 17v14" />
    </svg>
  )
}

function DownloadIcon(): JSX.Element {
  return (
    <svg aria-hidden width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M8 2.5v8" />
      <path d="M4.5 7L8 10.5 11.5 7" />
      <path d="M3 13h10" />
    </svg>
  )
}
