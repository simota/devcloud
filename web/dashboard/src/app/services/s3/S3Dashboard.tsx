import { useCallback, useEffect, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import type { DashboardService } from '../dashboard/types'
import { BucketSidebar } from './BucketSidebar'
import { ObjectBrowser } from './ObjectBrowser'
import { ObjectInspector } from './ObjectInspector'
import { copyS3Object, createS3Bucket, deleteS3Bucket, deleteS3Object, listS3Buckets, uploadS3Object } from './api'
import type { S3BucketSummary, S3ObjectSummary } from './types'

type BucketsState =
  | { status: 'loading' }
  | { status: 'success'; buckets: S3BucketSummary[] }
  | { status: 'error'; message: string }

type S3DashboardProps = {
  service?: DashboardService
}

export function S3Dashboard({ service }: S3DashboardProps): JSX.Element {
  const [bucketsState, setBucketsState] = useState<BucketsState>({ status: 'loading' })
  const [activeBucket, setActiveBucket] = useState<string>()
  const [activeObject, setActiveObject] = useState<S3ObjectSummary>()
  const [bucketName, setBucketName] = useState('')
  const [message, setMessage] = useState('')
  const [objectsRefreshNonce, setObjectsRefreshNonce] = useState(0)
  const isDisabled = service?.status === 'disabled'

  const refreshBuckets = useCallback(() => {
    if (isDisabled) {
      setBucketsState({ status: 'success', buckets: [] })
      setActiveBucket(undefined)
      setActiveObject(undefined)
      return
    }
    setBucketsState({ status: 'loading' })
    listS3Buckets()
      .then(({ buckets }) => {
        setBucketsState({ status: 'success', buckets })
        setActiveBucket((current) =>
          current && buckets.some((bucket) => bucket.name === current) ? current : buckets[0]?.name,
        )
      })
      .catch((error: Error) => {
        setBucketsState({ status: 'error', message: error.message })
      })
  }, [isDisabled])

  useEffect(() => {
    refreshBuckets()
  }, [refreshBuckets])

  const buckets = bucketsState.status === 'success' ? bucketsState.buckets : []
  const selectedBucket = buckets.find((bucket) => bucket.name === activeBucket)?.name
  const selectedObject = activeObject && selectedBucket ? activeObject : undefined

  const clearActiveObject = useCallback(() => {
    setActiveObject(undefined)
  }, [])

  if (isDisabled) {
    return (
      <Panel title="S3">
        <EmptyState
          title="S3 is disabled"
          description="Enable the S3 service in devcloud config to browse buckets and objects."
        />
        <a className="compat-link" href="/s3">
          Open current S3 dashboard
        </a>
      </Panel>
    )
  }

  function selectBucket(bucketName: string): void {
    setActiveBucket(bucketName)
    setActiveObject(undefined)
  }

  function createBucket(): void {
    const name = bucketName.trim()
    if (isDisabled || name === '') {
      return
    }
    createS3Bucket(name)
      .then((bucket) => {
        setMessage(`Created bucket ${bucket.name}`)
        setBucketName('')
        setActiveBucket(bucket.name)
        refreshBuckets()
      })
      .catch((error: Error) => setMessage(error.message))
  }

  function confirmDeleteBucket(bucketName: string): void {
    if (isDisabled || window.prompt(`Confirm DeleteBucket by typing ${bucketName}`) !== bucketName) {
      return
    }
    deleteS3Bucket(bucketName)
      .then(() => {
        setMessage(`Deleted bucket ${bucketName}`)
        setActiveBucket((current) => (current === bucketName ? undefined : current))
        setActiveObject(undefined)
        refreshBuckets()
      })
      .catch((error: Error) => setMessage(error.message))
  }

  function confirmDeleteObject(object: S3ObjectSummary): void {
    if (!selectedBucket || isDisabled || window.prompt(`Confirm DeleteObject by typing ${object.key}`) !== object.key) {
      return
    }
    deleteS3Object(selectedBucket, object.key)
      .then(() => {
        setMessage(`Deleted object ${object.s3Uri}`)
        setActiveObject(undefined)
        setObjectsRefreshNonce((current) => current + 1)
        refreshBuckets()
      })
      .catch((error: Error) => setMessage(error.message))
  }

  async function uploadObject(input: {
    key: string
    file: File
    contentType: string
    metadata: Record<string, string>
  }): Promise<void> {
    if (!selectedBucket || isDisabled) {
      return
    }
    try {
      const object = await uploadS3Object({
        bucketName: selectedBucket,
        objectKey: input.key,
        body: input.file,
        contentType: input.contentType,
        metadata: input.metadata,
      })
      setMessage(`Uploaded object ${object.s3Uri}`)
      setActiveObject(object)
      setObjectsRefreshNonce((current) => current + 1)
      refreshBuckets()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'S3 upload failed')
      throw error
    }
  }

  async function copyObject(destinationKey: string): Promise<void> {
    if (!selectedBucket || !selectedObject || isDisabled) {
      return
    }
    try {
      const object = await copyS3Object({
        sourceBucketName: selectedBucket,
        sourceObjectKey: selectedObject.key,
        destinationBucketName: selectedBucket,
        destinationObjectKey: destinationKey,
      })
      setMessage(`Copied object ${selectedObject.s3Uri} to ${object.s3Uri}`)
      setActiveObject(object)
      setObjectsRefreshNonce((current) => current + 1)
      refreshBuckets()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'S3 copy failed')
      throw error
    }
  }

  return (
    <div className="s3-workspace">
      <Panel title="Buckets">
        <div className="s3-toolbar">
          <span>{buckets.length} buckets</span>
          <Button onClick={refreshBuckets}>Refresh</Button>
        </div>
        {bucketsState.status === 'loading' ? (
          <EmptyState title="Loading buckets" description="Reading the local S3 bucket registry." />
        ) : null}
        {bucketsState.status === 'error' ? (
          <EmptyState
            title="S3 buckets unavailable"
            description={bucketsState.message}
            actionLabel="Retry"
            onAction={refreshBuckets}
          />
        ) : null}
        {bucketsState.status === 'success' ? (
          <div className="gcs-sidebar">
            <CreateBucketForm bucketName={bucketName} disabled={isDisabled} onChange={setBucketName} onCreate={createBucket} />
            <BucketSidebar
              buckets={buckets}
              activeBucket={selectedBucket}
              disabled={isDisabled}
              onDeleteBucket={confirmDeleteBucket}
              onSelectBucket={selectBucket}
            />
          </div>
        ) : null}
      </Panel>
      <Panel title="Object browser">
        <ObjectBrowser
          bucketName={selectedBucket}
          activeObjectKey={selectedObject?.key}
          disabled={isDisabled}
          refreshNonce={objectsRefreshNonce}
          onClearObject={clearActiveObject}
          onDeleteObject={confirmDeleteObject}
          onUploadObject={uploadObject}
          onSelectObject={setActiveObject}
        />
      </Panel>
      <Panel title="Inspector">
        <ObjectInspector bucketName={selectedBucket} disabled={isDisabled} object={selectedObject} onCopyObject={copyObject} />
      </Panel>
      <a className="compat-link" href="/s3">
        Open current S3 dashboard
      </a>
      {message ? <p className="inspector-muted gcs-message">{message}</p> : null}
    </div>
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
        <span>CreateBucket</span>
        <input
          aria-label="S3 bucket name"
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
