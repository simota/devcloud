import { useCallback, useEffect, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import { dangerConfirm, useConfirm } from '../../../ui/Confirm'
import { useEventSource } from '../../api/hooks/useEventSource'
import type { DashboardService } from '../dashboard/types'
import {
  createGCSBucket,
  deleteGCSBucket,
  deleteGCSObject,
  deleteGCSUploadSession,
  getGCSObject,
  getGCSStatus,
  listGCSBuckets,
  listGCSObjects,
  listGCSUploadSessions,
} from './api'
import type { GCSBucketSummary, GCSObjectSummary, GCSStatus, GCSUploadSessionSummary } from './types'

type DashboardState =
  | { status: 'loading' }
  | { status: 'success'; statusPayload: GCSStatus; buckets: GCSBucketSummary[]; sessions: GCSUploadSessionSummary[] }
  | { status: 'error'; message: string }

type ObjectsState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; objects: GCSObjectSummary[] }
  | { status: 'error'; message: string }

type GCSDashboardProps = {
  service?: DashboardService
}

export function GCSDashboard({ service }: GCSDashboardProps): JSX.Element {
  const [dashboardState, setDashboardState] = useState<DashboardState>({ status: 'loading' })
  const [objectsState, setObjectsState] = useState<ObjectsState>({ status: 'idle' })
  const [activeBucket, setActiveBucket] = useState<string>()
  const [activeObject, setActiveObject] = useState<GCSObjectSummary>()
  const [prefix, setPrefix] = useState('')
  const [bucketName, setBucketName] = useState('')
  const [message, setMessage] = useState<string>()
  const [objectsRefreshNonce, setObjectsRefreshNonce] = useState(0)
  const isDisabled = service?.status === 'disabled'
  const confirm = useConfirm()

  const refresh = useCallback(() => {
    if (isDisabled) {
      setDashboardState({ status: 'success', statusPayload: disabledStatus(service), buckets: [], sessions: [] })
      setObjectsState({ status: 'idle' })
      setActiveBucket(undefined)
      setActiveObject(undefined)
      return
    }
    setDashboardState({ status: 'loading' })
    Promise.all([getGCSStatus(), listGCSBuckets(), listGCSUploadSessions()])
      .then(([statusPayload, bucketsResponse, sessionsResponse]) => {
        setDashboardState({
          status: 'success',
          statusPayload,
          buckets: bucketsResponse.buckets,
          sessions: sessionsResponse.sessions,
        })
        setActiveBucket((current) =>
          current && bucketsResponse.buckets.some((bucket) => bucket.name === current)
            ? current
            : bucketsResponse.buckets[0]?.name,
        )
      })
      .catch((error: Error) => {
        setDashboardState({ status: 'error', message: error.message })
      })
  }, [isDisabled, service])

  useEffect(() => {
    refresh()
  }, [refresh])

  const onGCSEvent = useCallback(
    (event: { type: string }) => {
      refresh()
      if (event.type.startsWith('gcs.object.')) {
        setObjectsRefreshNonce((n) => n + 1)
      }
    },
    [refresh],
  )

  useEventSource({ topics: ['gcs'], onEvent: onGCSEvent, enabled: !isDisabled })

  useEffect(() => {
    setActiveObject(undefined)
    if (!activeBucket || isDisabled) {
      setObjectsState({ status: 'idle' })
      return
    }
    let cancelled = false
    setObjectsState({ status: 'loading' })
    listGCSObjects(activeBucket, prefix.trim())
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
  }, [activeBucket, isDisabled, prefix, objectsRefreshNonce])

  const buckets = dashboardState.status === 'success' ? dashboardState.buckets : []
  const sessions = dashboardState.status === 'success' ? dashboardState.sessions : []
  const statusPayload = dashboardState.status === 'success' ? dashboardState.statusPayload : undefined

  function selectBucket(bucket: string): void {
    setActiveBucket(bucket)
    setActiveObject(undefined)
  }

  function inspectObject(object: GCSObjectSummary): void {
    if (!activeBucket) {
      return
    }
    getGCSObject(activeBucket, object.name)
      .then(setActiveObject)
      .catch((error: Error) => setMessage(error.message))
  }

  function createBucket(): void {
    const name = bucketName.trim()
    if (!name || isDisabled) {
      return
    }
    createGCSBucket(name)
      .then(() => {
        setBucketName('')
        setMessage(`Created bucket ${name}`)
        refresh()
      })
      .catch((error: Error) => setMessage(error.message))
  }

  async function confirmDeleteBucket(bucket: string): Promise<void> {
    if (isDisabled) {
      return
    }
    const ok = await confirm(
      dangerConfirm({
        title: 'Delete bucket',
        description: 'All objects and metadata in this bucket will be removed. This cannot be undone.',
        target: bucket,
      }),
    )
    if (!ok) {
      return
    }
    deleteGCSBucket(bucket)
      .then(() => {
        setMessage(`Deleted bucket ${bucket}`)
        refresh()
      })
      .catch((error: Error) => setMessage(error.message))
  }

  async function confirmDeleteObject(object: GCSObjectSummary): Promise<void> {
    if (!activeBucket || isDisabled) {
      return
    }
    const ok = await confirm(
      dangerConfirm({
        title: 'Delete object',
        description: 'This object will be permanently removed from the bucket.',
        target: object.name,
      }),
    )
    if (!ok) {
      return
    }
    deleteGCSObject(activeBucket, object.name)
      .then(() => {
        setMessage(`Deleted object ${object.gcsUri}`)
        setActiveObject(undefined)
        setPrefix((current) => current)
        refreshObjects(activeBucket, prefix, setObjectsState)
      })
      .catch((error: Error) => setMessage(error.message))
  }

  async function confirmDeleteSession(session: GCSUploadSessionSummary): Promise<void> {
    if (isDisabled) {
      return
    }
    const ok = await confirm(
      dangerConfirm({
        title: 'Delete upload session',
        description: 'In-progress upload state will be discarded.',
        target: session.id,
      }),
    )
    if (!ok) {
      return
    }
    deleteGCSUploadSession(session.id)
      .then(() => {
        setMessage(`Deleted upload session ${session.id}`)
        refresh()
      })
      .catch((error: Error) => setMessage(error.message))
  }

  if (isDisabled) {
    return (
      <Panel title="devcloud GCS">
        <EmptyState title="GCS is disabled" description="Enable the GCS service in devcloud config to inspect local buckets and objects." />
        <StatusSummary status={statusPayload ?? disabledStatus(service)} />
      </Panel>
    )
  }

  return (
    <div className="gcs-workspace">
      <Panel title="devcloud GCS">
        <div className="s3-toolbar">
          <span>{buckets.length} buckets</span>
          <Button onClick={refresh}>Refresh</Button>
        </div>
        {dashboardState.status === 'loading' ? <EmptyState title="Loading GCS" description="Reading local GCS status." /> : null}
        {dashboardState.status === 'error' ? (
          <EmptyState title="GCS unavailable" description={dashboardState.message} actionLabel="Retry" onAction={refresh} />
        ) : null}
        {dashboardState.status === 'success' ? (
          <div className="gcs-sidebar">
            <StatusSummary status={dashboardState.statusPayload} />
            <CreateBucketForm bucketName={bucketName} disabled={isDisabled} onChange={setBucketName} onCreate={createBucket} />
            <BucketList
              activeBucket={activeBucket}
              buckets={buckets}
              disabled={isDisabled}
              onDeleteBucket={confirmDeleteBucket}
              onSelectBucket={selectBucket}
            />
          </div>
        ) : null}
      </Panel>

      <Panel title="Objects">
        <ObjectBrowser
          activeObjectName={activeObject?.name}
          bucketName={activeBucket}
          disabled={isDisabled}
          objectsState={objectsState}
          onDeleteObject={confirmDeleteObject}
          onInspectObject={inspectObject}
          onPrefixChange={setPrefix}
          prefix={prefix}
        />
      </Panel>

      <Panel title="Inspector">
        <ObjectInspector bucketName={activeBucket} object={activeObject} />
      </Panel>

      <Panel title="Upload sessions">
        <UploadSessions disabled={isDisabled} onDeleteSession={confirmDeleteSession} sessions={sessions} />
      </Panel>

      {message ? <p className="inspector-muted gcs-message">{message}</p> : null}
    </div>
  )
}

type StatusSummaryProps = {
  status: GCSStatus
}

function StatusSummary({ status }: StatusSummaryProps): JSX.Element {
  return (
    <dl className="inspector-list">
      <div>
        <dt>Status</dt>
        <dd>{status.running ? 'running' : 'disabled'}</dd>
      </div>
      <div>
        <dt>Project</dt>
        <dd>{status.project}</dd>
      </div>
      <div>
        <dt>Endpoint</dt>
        <dd>
          <code>{status.endpoint}</code>
        </dd>
      </div>
      <div>
        <dt>Upload sessions</dt>
        <dd>
          <code>{status.uploadSessionPath}</code>
        </dd>
      </div>
    </dl>
  )
}

type CreateBucketFormProps = {
  bucketName: string
  disabled: boolean
  onChange: (value: string) => void
  onCreate: () => void
}

function CreateBucketForm({ bucketName, disabled, onChange, onCreate }: CreateBucketFormProps): JSX.Element {
  return (
    <div className="gcs-create-bucket">
      <label className="prefix-filter">
        <span>Create bucket</span>
        <input
          aria-label="GCS bucket name"
          disabled={disabled}
          onChange={(event) => onChange(event.target.value)}
          placeholder="local-bucket"
          value={bucketName}
        />
      </label>
      <Button disabled={disabled || bucketName.trim() === ''} onClick={onCreate}>
        Create
      </Button>
    </div>
  )
}

type BucketListProps = {
  activeBucket?: string
  buckets: GCSBucketSummary[]
  disabled: boolean
  onDeleteBucket: (bucketName: string) => void
  onSelectBucket: (bucketName: string) => void
}

function BucketList({ activeBucket, buckets, disabled, onDeleteBucket, onSelectBucket }: BucketListProps): JSX.Element {
  if (buckets.length === 0) {
    return <EmptyState title="No buckets" description="Buckets created through the GCS API will appear here." />
  }
  return (
    <div className="bucket-list">
      {buckets.map((bucket) => (
        <div className={bucket.name === activeBucket ? 'bucket-item active gcs-bucket-row' : 'bucket-item gcs-bucket-row'} key={bucket.name}>
          <button className="object-select" onClick={() => onSelectBucket(bucket.name)}>
            <span className="bucket-name">{bucket.name}</span>
            <span className="bucket-meta">{bucket.gcsUri}</span>
            <span className="bucket-meta">Created {formatDate(bucket.timeCreated)}</span>
          </button>
          <span className="count-pill">{bucket.objectCount}</span>
          <Button className="danger" disabled={disabled} onClick={() => onDeleteBucket(bucket.name)}>
            Delete bucket
          </Button>
        </div>
      ))}
    </div>
  )
}

type ObjectBrowserProps = {
  activeObjectName?: string
  bucketName?: string
  disabled: boolean
  objectsState: ObjectsState
  prefix: string
  onDeleteObject: (object: GCSObjectSummary) => void
  onInspectObject: (object: GCSObjectSummary) => void
  onPrefixChange: (prefix: string) => void
}

function ObjectBrowser({
  activeObjectName,
  bucketName,
  disabled,
  objectsState,
  onDeleteObject,
  onInspectObject,
  onPrefixChange,
  prefix,
}: ObjectBrowserProps): JSX.Element {
  if (!bucketName) {
    return <EmptyState title="Select a bucket" description="Choose a bucket to browse objects and metadata." />
  }
  return (
    <div className="object-browser">
      <label className="prefix-filter">
        <span>Prefix</span>
        <input
          aria-label="Filter GCS objects by prefix"
          onChange={(event) => onPrefixChange(event.target.value)}
          placeholder="docs/"
          type="search"
          value={prefix}
        />
      </label>
      {objectsState.status === 'loading' ? <EmptyState title="Loading objects" description={`Reading objects in ${bucketName}.`} /> : null}
      {objectsState.status === 'error' ? <EmptyState title="Objects unavailable" description={objectsState.message} /> : null}
      {objectsState.status === 'success' && objectsState.objects.length === 0 ? (
        <EmptyState title="No objects" description={`No objects in ${bucketName} match the current prefix.`} />
      ) : null}
      {objectsState.status === 'success' && objectsState.objects.length > 0 ? (
        <div className="object-table-wrap">
          <table className="object-table">
            <thead>
              <tr>
                <th scope="col">Name</th>
                <th scope="col">Size</th>
                <th scope="col">Updated</th>
                <th scope="col">Class</th>
                <th scope="col">Actions</th>
              </tr>
            </thead>
            <tbody>
              {objectsState.objects.map((object) => (
                <tr className={object.name === activeObjectName ? 'object-row active' : 'object-row'} key={object.name}>
                  <td>
                    <button className="object-select" onClick={() => onInspectObject(object)}>
                      <span className="object-key">{object.name}</span>
                    </button>
                    <code>{object.gcsUri}</code>
                  </td>
                  <td>{formatBytes(object.size)}</td>
                  <td>{formatDate(object.updated)}</td>
                  <td>{object.storageClass || 'STANDARD'}</td>
                  <td>
                    <a className="object-action" href={safeGCSDownloadURL(object.downloadUrl)}>
                      Download
                    </a>
                    <Button className="danger" disabled={disabled} onClick={() => onDeleteObject(object)}>
                      Delete object
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  )
}

type ObjectInspectorProps = {
  bucketName?: string
  object?: GCSObjectSummary
}

function ObjectInspector({ bucketName, object }: ObjectInspectorProps): JSX.Element {
  if (!object) {
    return (
      <EmptyState
        title="Inspector"
        description={
          bucketName
            ? `Select an object in ${bucketName} to inspect GCS metadata.`
            : 'Object metadata, generation, and download links will appear here.'
        }
      />
    )
  }
  const metadataEntries = Object.entries(object.metadata ?? {})
  return (
    <div className="object-inspector">
      <div>
        <span className="inspector-label">gs:// URI</span>
        <code>{object.gcsUri}</code>
      </div>
      <dl className="inspector-list">
        <div>
          <dt>generation</dt>
          <dd>{object.generation}</dd>
        </div>
        <div>
          <dt>metageneration</dt>
          <dd>{object.metageneration}</dd>
        </div>
        <div>
          <dt>storageClass</dt>
          <dd>{object.storageClass || 'STANDARD'}</dd>
        </div>
        <div>
          <dt>crc32c</dt>
          <dd>{object.crc32c || 'unknown'}</dd>
        </div>
        <div>
          <dt>contentType</dt>
          <dd>{object.contentType || 'application/octet-stream'}</dd>
        </div>
        <div>
          <dt>ETag</dt>
          <dd>
            <code>{object.etag || 'unknown'}</code>
          </dd>
        </div>
      </dl>
      <div>
        <span className="inspector-label">metadata</span>
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
      <a className="compat-link inspector-download" href={safeGCSDownloadURL(object.downloadUrl)}>
        Download object
      </a>
    </div>
  )
}

type UploadSessionsProps = {
  disabled: boolean
  sessions: GCSUploadSessionSummary[]
  onDeleteSession: (session: GCSUploadSessionSummary) => void
}

function UploadSessions({ disabled, onDeleteSession, sessions }: UploadSessionsProps): JSX.Element {
  if (sessions.length === 0) {
    return <EmptyState title="No upload sessions" description="Active resumable upload sessions will appear here." />
  }
  return (
    <div className="object-table-wrap">
      <table className="object-table">
        <thead>
          <tr>
            <th scope="col">Session</th>
            <th scope="col">Object</th>
            <th scope="col">Received</th>
            <th scope="col">Created</th>
            <th scope="col">Action</th>
          </tr>
        </thead>
        <tbody>
          {sessions.map((session) => (
            <tr key={session.id}>
              <td>
                <code>{session.id}</code>
              </td>
              <td>
                <span className="object-key">{session.name}</span>
                <code>{`gs://${session.bucket}/${session.name}`}</code>
              </td>
              <td>{formatBytes(session.receivedBytes)}</td>
              <td>{formatDate(session.createdAt)}</td>
              <td>
                <Button className="danger" disabled={disabled} onClick={() => onDeleteSession(session)}>
                  Delete session
                </Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function refreshObjects(bucket: string, prefix: string, setObjectsState: (state: ObjectsState) => void): void {
  setObjectsState({ status: 'loading' })
  listGCSObjects(bucket, prefix.trim())
    .then(({ objects }) => setObjectsState({ status: 'success', objects }))
    .catch((error: Error) => setObjectsState({ status: 'error', message: error.message }))
}

function disabledStatus(service?: DashboardService): GCSStatus {
  return {
    status: 'disabled',
    running: false,
    endpoint: service?.endpoint ?? 'http://127.0.0.1:4443',
    project: 'devcloud',
    storagePath: service?.storagePath ?? '.devcloud/data/s3',
    uploadSessionPath: '.devcloud/data/gcs/upload_sessions',
  }
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

function safeGCSDownloadURL(value: string): string {
  return value.startsWith('/api/gcs/') ? value : '#'
}
