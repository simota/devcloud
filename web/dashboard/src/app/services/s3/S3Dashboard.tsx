import { useCallback, useEffect, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import { dangerConfirm, useConfirm } from '../../../ui/Confirm'
import { useDashboardEvents } from '../../api/hooks/useDashboardEvents'
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
  const confirm = useConfirm()

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

  const onS3Event = useCallback(
    (event: { type: string }) => {
      // Bucket-level events refresh the bucket list. Object-level events also
      // bump the nonce so the open ObjectBrowser re-fetches its listing.
      refreshBuckets()
      if (event.type.startsWith('s3.object.')) {
        setObjectsRefreshNonce((n) => n + 1)
      }
    },
    [refreshBuckets],
  )

  useDashboardEvents({ topics: ['s3'], onEvent: onS3Event, enabled: !isDisabled })

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

  async function confirmDeleteBucket(bucketName: string): Promise<void> {
    if (isDisabled) {
      return
    }
    const ok = await confirm(
      dangerConfirm({
        title: 'Delete bucket',
        description: 'All objects and metadata in this bucket will be removed. This cannot be undone.',
        target: bucketName,
      }),
    )
    if (!ok) {
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

  async function confirmDeleteObject(object: S3ObjectSummary): Promise<void> {
    if (!selectedBucket || isDisabled) {
      return
    }
    const ok = await confirm(
      dangerConfirm({
        title: 'Delete object',
        description: 'This object will be permanently removed from the bucket.',
        target: object.key,
      }),
    )
    if (!ok) {
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
        <div className="s3-bucket-toolbar">
          <span className="s3-bucket-count">{buckets.length === 1 ? '1 bucket' : `${buckets.length} buckets`}</span>
          <button
            aria-label="Refresh buckets"
            className="s3-icon-button neutral"
            onClick={refreshBuckets}
            type="button"
          >
            <RefreshIcon />
          </button>
        </div>
        <CreateBucketForm
          bucketName={bucketName}
          disabled={isDisabled}
          onChange={setBucketName}
          onCreate={createBucket}
        />
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
          <BucketSidebar
            buckets={buckets}
            activeBucket={selectedBucket}
            disabled={isDisabled}
            onDeleteBucket={confirmDeleteBucket}
            onSelectBucket={selectBucket}
          />
        ) : null}
      </Panel>
      <Panel title={selectedBucket ? `Objects · ${selectedBucket}` : 'Objects'}>
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
      {message ? <p className="s3-status-banner">{message}</p> : null}
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
    <form
      className="s3-bucket-create"
      onSubmit={(event) => {
        event.preventDefault()
        onCreate()
      }}
    >
      <input
        aria-label="New bucket name"
        className="s3-text-input"
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        placeholder="New bucket name"
        value={bucketName}
      />
      <Button disabled={disabled || bucketName.trim() === ''} type="submit">
        Create
      </Button>
    </form>
  )
}

function RefreshIcon(): JSX.Element {
  return (
    <svg aria-hidden width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M13.5 8a5.5 5.5 0 1 1-1.61-3.89" />
      <path d="M13.5 3v3h-3" />
    </svg>
  )
}
