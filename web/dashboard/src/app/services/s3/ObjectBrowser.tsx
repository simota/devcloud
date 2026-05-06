import { type FormEvent, useEffect, useRef, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Button } from '../../../ui/Button'
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

  if (objectsState.status === 'loading') {
    return (
      <div className="object-browser">
        <PrefixFilter prefix={prefix} onChange={setPrefix} />
        <UploadObjectForm disabled={disabled} onUploadObject={onUploadObject} />
        <MultipartUploads bucketName={bucketName} disabled={disabled} />
        <EmptyState title="Loading objects" description={`Reading objects in ${bucketName}.`} />
      </div>
    )
  }

  if (objectsState.status === 'error') {
    return (
      <div className="object-browser">
        <PrefixFilter prefix={prefix} onChange={setPrefix} />
        <UploadObjectForm disabled={disabled} onUploadObject={onUploadObject} />
        <MultipartUploads bucketName={bucketName} disabled={disabled} />
        <EmptyState title="Objects unavailable" description={objectsState.message} />
      </div>
    )
  }

  if (objectsState.status !== 'success' || objectsState.objects.length === 0) {
    return (
      <div className="object-browser">
        <PrefixFilter prefix={prefix} onChange={setPrefix} />
        <UploadObjectForm disabled={disabled} onUploadObject={onUploadObject} />
        <MultipartUploads bucketName={bucketName} disabled={disabled} />
        <EmptyState
          title="No objects"
          description={
            prefix.trim() === ''
              ? `Objects uploaded to ${bucketName} will appear here.`
              : `No objects in ${bucketName} match this prefix.`
          }
        />
      </div>
    )
  }

  return (
    <div className="object-browser">
      <PrefixFilter prefix={prefix} onChange={setPrefix} />
      <UploadObjectForm disabled={disabled} onUploadObject={onUploadObject} />
      <MultipartUploads bucketName={bucketName} disabled={disabled} />
      <div className="object-table-wrap">
        <table className="object-table">
          <thead>
            <tr>
              <th scope="col">Key</th>
              <th scope="col">Size</th>
              <th scope="col">Modified</th>
              <th scope="col">Type</th>
              <th scope="col">Action</th>
            </tr>
          </thead>
          <tbody>
            {objectsState.objects.map((object) => (
              <tr className={object.key === activeObjectKey ? 'object-row active' : 'object-row'} key={object.key}>
                <td>
                  <button className="object-select" onClick={() => onSelectObject(object)}>
                    <span className="object-key">{object.key}</span>
                  </button>
                  <code>{object.s3Uri}</code>
                </td>
                <td>{formatBytes(object.size)}</td>
                <td>{formatDate(object.lastModified)}</td>
                <td>{object.contentType || 'application/octet-stream'}</td>
                <td>
                  <a className="object-action" href={safeS3DownloadURL(object.downloadUrl)}>
                    Download
                  </a>
                  <Button
                    aria-label={`Delete object ${object.key} with confirmation`}
                    className="danger"
                    disabled={disabled}
                    onClick={() => onDeleteObject(object)}
                  >
                    Delete
                  </Button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
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

function MultipartUploads({ bucketName, disabled }: MultipartUploadsProps): JSX.Element {
  const [state, setState] = useState<MultipartUploadsState>({ status: 'loading' })
  const [refreshNonce, setRefreshNonce] = useState(0)
  const [busyUploadId, setBusyUploadId] = useState('')

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
    if (window.prompt(`Confirm AbortMultipartUpload by typing ${upload.uploadId}`) !== upload.uploadId) {
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

  if (state.status === 'loading') {
    return <p className="inspector-muted">Loading multipart uploads.</p>
  }
  if (state.status === 'error') {
    return <p className="inspector-muted">Multipart uploads unavailable: {state.message}</p>
  }
  if (state.uploads.length === 0) {
    return <p className="inspector-muted">No incomplete multipart uploads.</p>
  }

  return (
    <div className="multipart-upload-list" aria-label="S3 multipart uploads">
      <span className="inspector-label">Multipart uploads</span>
      {state.uploads.map((upload) => (
        <div className="multipart-upload-row" key={upload.uploadId}>
          <div>
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
        </div>
      ))}
    </div>
  )
}

type PrefixFilterProps = {
  prefix: string
  onChange: (prefix: string) => void
}

function PrefixFilter({ prefix, onChange }: PrefixFilterProps): JSX.Element {
  return (
    <label className="prefix-filter">
      <span>Prefix</span>
      <input
        aria-label="Filter objects by prefix"
        onChange={(event) => onChange(event.target.value)}
        placeholder="docs/"
        type="search"
        value={prefix}
      />
    </label>
  )
}

type UploadObjectFormProps = {
  disabled: boolean
  onUploadObject: (input: { key: string; file: File; contentType: string; metadata: Record<string, string> }) => Promise<void>
}

function UploadObjectForm({ disabled, onUploadObject }: UploadObjectFormProps): JSX.Element {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [key, setKey] = useState('')
  const [contentType, setContentType] = useState('')
  const [metadataText, setMetadataText] = useState('')
  const [busy, setBusy] = useState(false)

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
    } finally {
      setBusy(false)
    }
  }

  return (
    <form className="s3-object-form" onSubmit={submitUpload}>
      <label className="prefix-filter">
        <span>PutObject key</span>
        <input
          aria-label="S3 upload object key"
          disabled={disabled || busy}
          onChange={(event) => setKey(event.target.value)}
          placeholder="docs/readme.txt"
          value={key}
        />
      </label>
      <label className="prefix-filter">
        <span>Content-Type</span>
        <input
          aria-label="S3 upload content type"
          disabled={disabled || busy}
          onChange={(event) => setContentType(event.target.value)}
          placeholder="text/plain"
          value={contentType}
        />
      </label>
      <label className="prefix-filter s3-file-input">
        <span>File</span>
        <input aria-label="S3 upload file" disabled={disabled || busy} ref={fileInputRef} type="file" />
      </label>
      <label className="prefix-filter s3-metadata-input">
        <span>x-amz-meta</span>
        <textarea
          aria-label="S3 upload metadata"
          disabled={disabled || busy}
          onChange={(event) => setMetadataText(event.target.value)}
          placeholder="source=dashboard&#10;purpose=local-test"
          value={metadataText}
        />
      </label>
      <Button disabled={disabled || busy || key.trim() === ''} type="submit">
        {busy ? 'Uploading' : 'Upload'}
      </Button>
    </form>
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
