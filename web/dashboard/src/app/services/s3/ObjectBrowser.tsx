import { useEffect, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { listS3Objects } from './api'
import type { S3ObjectSummary } from './types'

type ObjectBrowserProps = {
  bucketName?: string
  activeObjectKey?: string
  onClearObject: () => void
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
  onClearObject,
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
  }, [bucketName, onClearObject, prefix])

  if (!bucketName) {
    return <EmptyState title="Select a bucket" description="Choose a bucket to browse its objects." />
  }

  if (objectsState.status === 'loading') {
    return (
      <div className="object-browser">
        <PrefixFilter prefix={prefix} onChange={setPrefix} />
        <EmptyState title="Loading objects" description={`Reading objects in ${bucketName}.`} />
      </div>
    )
  }

  if (objectsState.status === 'error') {
    return (
      <div className="object-browser">
        <PrefixFilter prefix={prefix} onChange={setPrefix} />
        <EmptyState title="Objects unavailable" description={objectsState.message} />
      </div>
    )
  }

  if (objectsState.status !== 'success' || objectsState.objects.length === 0) {
    return (
      <div className="object-browser">
        <PrefixFilter prefix={prefix} onChange={setPrefix} />
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
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
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
