import { type FormEvent, useEffect, useRef, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Button } from '../../../ui/Button'
import { dangerConfirm, useConfirm } from '../../../ui/Confirm'
import { abortS3MultipartUpload, listS3MultipartUploads, listS3Objects } from './api'
import type { S3MultipartUploadSummary, S3ObjectSummary } from './types'

type ObjectBrowserProps = {
  bucketName?: string
  activeObjectKey?: string
  disabled: boolean
  refreshNonce: number
  onClearObject: () => void
  onDeleteObject: (object: S3ObjectSummary) => void
  onUploadObject: (input: { key: string; file: File; contentType: string; metadata: Record<string, string> }) => Promise<void>
  onSelectObject: (object: S3ObjectSummary) => void
}

type ObjectsState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; objects: S3ObjectSummary[] }
  | { status: 'error'; message: string }

export function ObjectBrowser({
  bucketName,
  activeObjectKey,
  disabled,
  refreshNonce,
  onClearObject,
  onDeleteObject,
  onUploadObject,
  onSelectObject,
}: ObjectBrowserProps): JSX.Element {
  const [prefix, setPrefix] = useState('')
  const [objectsState, setObjectsState] = useState<ObjectsState>({ status: 'idle' })

  useEffect(() => {
    onClearObject()

    if (!bucketName) {
      setObjectsState({ status: 'idle' })
      return
    }

    let cancelled = false
    setObjectsState({ status: 'loading' })
    listS3Objects(bucketName, prefix.trim())
      .then(({ objects }) => {
        if (!cancelled) {
          setObjectsState({ status: 'success', objects })
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setObjectsState({ status: 'error', message: error.message })
        }
      })

    return () => {
      cancelled = true
    }
  }, [bucketName, onClearObject, prefix, refreshNonce])

  if (!bucketName) {
    return <EmptyState title="Select a bucket" description="Choose a bucket to browse its objects." />
  }

  const objects = objectsState.status === 'success' ? objectsState.objects : []

  return (
    <div className="s3-browser">
      <div className="s3-browser-toolbar">
        <SearchInput prefix={prefix} onChange={setPrefix} />
        <UploadObjectToggle disabled={disabled} onUploadObject={onUploadObject} />
      </div>
      <MultipartUploads bucketName={bucketName} disabled={disabled} />
      {objectsState.status === 'loading' ? (
        <EmptyState title="Loading objects" description={`Reading objects in ${bucketName}.`} />
      ) : null}
      {objectsState.status === 'error' ? (
        <EmptyState title="Objects unavailable" description={objectsState.message} />
      ) : null}
      {objectsState.status === 'success' && objects.length === 0 ? (
        <EmptyState
          title="No objects"
          description={
            prefix.trim() === ''
              ? `Objects uploaded to ${bucketName} will appear here.`
              : `No objects in ${bucketName} match this prefix.`
          }
        />
      ) : null}
      {objectsState.status === 'success' && objects.length > 0 ? (
        <div className="s3-object-table-wrap">
          <table className="s3-object-table">
            <thead>
              <tr>
                <th scope="col">Key</th>
                <th scope="col">Size</th>
                <th scope="col">Modified</th>
                <th scope="col">Type</th>
                <th aria-label="Row actions" className="s3-object-actions-head" scope="col" />
              </tr>
            </thead>
            <tbody>
              {objects.map((object) => {
                const active = object.key === activeObjectKey
                return (
                  <tr className={active ? 's3-object-row active' : 's3-object-row'} key={object.key}>
                    <td>
                      <button className="s3-object-key" onClick={() => onSelectObject(object)} title={object.key} type="button">
                        {object.key}
                      </button>
                    </td>
                    <td className="s3-object-numeric">{formatBytes(object.size)}</td>
                    <td className="s3-object-numeric">{formatDate(object.lastModified)}</td>
                    <td className="s3-object-type">{object.contentType || 'application/octet-stream'}</td>
                    <td className="s3-object-actions">
                      <a
                        aria-label={`Download ${object.key}`}
                        className="s3-icon-button neutral"
                        href={safeS3DownloadURL(object.downloadUrl)}
                        title="Download"
                      >
                        <DownloadIcon />
                      </a>
                      <button
                        aria-label={`Delete ${object.key}`}
                        className="s3-icon-button danger"
                        disabled={disabled}
                        onClick={() => onDeleteObject(object)}
                        title="Delete"
                        type="button"
                      >
                        <TrashIcon />
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  )
}

type MultipartUploadsState =
  | { status: 'loading' }
  | { status: 'success'; uploads: S3MultipartUploadSummary[] }
  | { status: 'error'; message: string }

type MultipartUploadsProps = {
  bucketName: string
  disabled: boolean
}

function MultipartUploads({ bucketName, disabled }: MultipartUploadsProps): JSX.Element | null {
  const [state, setState] = useState<MultipartUploadsState>({ status: 'loading' })
  const [refreshNonce, setRefreshNonce] = useState(0)
  const [busyUploadId, setBusyUploadId] = useState('')
  const confirm = useConfirm()

  useEffect(() => {
    let cancelled = false
    setState({ status: 'loading' })
    listS3MultipartUploads(bucketName)
      .then(({ uploads }) => {
        if (!cancelled) {
          setState({ status: 'success', uploads })
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState({ status: 'error', message: error.message })
        }
      })
    return () => {
      cancelled = true
    }
  }, [bucketName, refreshNonce])

  async function abortUpload(upload: S3MultipartUploadSummary): Promise<void> {
    if (disabled || busyUploadId !== '') {
      return
    }
    const ok = await confirm(
      dangerConfirm({
        title: 'Abort multipart upload',
        description: 'Pending parts will be discarded and the upload will be unrecoverable.',
        target: upload.uploadId,
        confirmLabel: 'Abort upload',
      }),
    )
    if (!ok) {
      return
    }
    setBusyUploadId(upload.uploadId)
    try {
      await abortS3MultipartUpload(bucketName, upload.uploadId)
      setRefreshNonce((current) => current + 1)
    } finally {
      setBusyUploadId('')
    }
  }

  if (state.status !== 'success' || state.uploads.length === 0) {
    return null
  }

  return (
    <details className="s3-multipart-details">
      <summary>
        <span>Incomplete multipart uploads</span>
        <span className="s3-multipart-count">{state.uploads.length}</span>
      </summary>
      <ul className="s3-multipart-list">
        {state.uploads.map((upload) => (
          <li className="s3-multipart-item" key={upload.uploadId}>
            <div className="s3-multipart-meta">
              <strong>{upload.key}</strong>
              <code>{upload.uploadId}</code>
              <span>{formatDate(upload.initiated)}</span>
            </div>
            <Button
              aria-label={`Abort multipart upload ${upload.uploadId} with confirmation`}
              className="danger"
              disabled={disabled || busyUploadId !== ''}
              onClick={() => void abortUpload(upload)}
            >
              {busyUploadId === upload.uploadId ? 'Aborting' : 'Abort'}
            </Button>
          </li>
        ))}
      </ul>
    </details>
  )
}

type SearchInputProps = {
  prefix: string
  onChange: (prefix: string) => void
}

function SearchInput({ prefix, onChange }: SearchInputProps): JSX.Element {
  return (
    <label className="s3-search">
      <SearchIcon />
      <input
        aria-label="Filter objects by prefix"
        onChange={(event) => onChange(event.target.value)}
        placeholder="Filter by prefix (e.g. docs/)"
        type="search"
        value={prefix}
      />
    </label>
  )
}

type UploadObjectToggleProps = {
  disabled: boolean
  onUploadObject: (input: { key: string; file: File; contentType: string; metadata: Record<string, string> }) => Promise<void>
}

function UploadObjectToggle({ disabled, onUploadObject }: UploadObjectToggleProps): JSX.Element {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [key, setKey] = useState('')
  const [contentType, setContentType] = useState('')
  const [metadataText, setMetadataText] = useState('')
  const [busy, setBusy] = useState(false)
  const detailsRef = useRef<HTMLDetailsElement>(null)

  async function submitUpload(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault()
    const file = fileInputRef.current?.files?.[0]
    if (disabled || busy || key.trim() === '' || !file) {
      return
    }
    setBusy(true)
    try {
      await onUploadObject({
        key: key.trim(),
        file,
        contentType: contentType.trim() || file.type || 'application/octet-stream',
        metadata: parseMetadata(metadataText),
      })
      setKey('')
      setContentType('')
      setMetadataText('')
      if (fileInputRef.current) {
        fileInputRef.current.value = ''
      }
      if (detailsRef.current) {
        detailsRef.current.open = false
      }
    } finally {
      setBusy(false)
    }
  }

  return (
    <details className="s3-upload-details" ref={detailsRef}>
      <summary>
        <PlusIcon />
        <span>Upload object</span>
      </summary>
      <form className="s3-upload-form" onSubmit={submitUpload}>
        <label className="s3-field">
          <span>Key</span>
          <input
            aria-label="S3 upload object key"
            className="s3-text-input"
            disabled={disabled || busy}
            onChange={(event) => setKey(event.target.value)}
            placeholder="docs/readme.txt"
            value={key}
          />
        </label>
        <label className="s3-field">
          <span>Content-Type</span>
          <input
            aria-label="S3 upload content type"
            className="s3-text-input"
            disabled={disabled || busy}
            onChange={(event) => setContentType(event.target.value)}
            placeholder="text/plain"
            value={contentType}
          />
        </label>
        <label className="s3-field s3-field-full">
          <span>File</span>
          <input aria-label="S3 upload file" className="s3-file-input" disabled={disabled || busy} ref={fileInputRef} type="file" />
        </label>
        <label className="s3-field s3-field-full">
          <span>Metadata (x-amz-meta)</span>
          <textarea
            aria-label="S3 upload metadata"
            className="s3-text-input"
            disabled={disabled || busy}
            onChange={(event) => setMetadataText(event.target.value)}
            placeholder="source=dashboard&#10;purpose=local-test"
            rows={2}
            value={metadataText}
          />
        </label>
        <div className="s3-upload-actions">
          <Button disabled={disabled || busy || key.trim() === ''} type="submit">
            {busy ? 'Uploading…' : 'Upload'}
          </Button>
        </div>
      </form>
    </details>
  )
}

function parseMetadata(value: string): Record<string, string> {
  return value.split(/\r?\n/).reduce<Record<string, string>>((metadata, line) => {
    const trimmed = line.trim()
    if (trimmed === '') {
      return metadata
    }
    const [key, ...rest] = trimmed.split('=')
    const cleanKey = key.trim()
    const cleanValue = rest.join('=').trim()
    if (cleanKey !== '') {
      metadata[cleanKey] = cleanValue
    }
    return metadata
  }, {})
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

function SearchIcon(): JSX.Element {
  return (
    <svg aria-hidden width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="7" cy="7" r="4.5" />
      <path d="M13.5 13.5L10.5 10.5" />
    </svg>
  )
}

function PlusIcon(): JSX.Element {
  return (
    <svg aria-hidden width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round">
      <path d="M8 3v10" />
      <path d="M3 8h10" />
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

function TrashIcon(): JSX.Element {
  return (
    <svg aria-hidden width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M2.5 4h11" />
      <path d="M6 4V2.5h4V4" />
      <path d="M4 4l.7 9.2a1 1 0 0 0 1 .9h4.6a1 1 0 0 0 1-.9L12 4" />
      <path d="M6.5 7v5" />
      <path d="M9.5 7v5" />
    </svg>
  )
}
