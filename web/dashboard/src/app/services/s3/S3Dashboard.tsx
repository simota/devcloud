import { useCallback, useEffect, useState } from 'react'
import { EmptyState } from '../../../ui/EmptyState'
import { Panel } from '../../../ui/Panel'
import { Button } from '../../../ui/Button'
import type { DashboardService } from '../dashboard/types'
import { BucketSidebar } from './BucketSidebar'
import { ObjectBrowser } from './ObjectBrowser'
import { ObjectInspector } from './ObjectInspector'
import { listS3Buckets } from './api'
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
          <BucketSidebar buckets={buckets} activeBucket={selectedBucket} onSelectBucket={selectBucket} />
        ) : null}
      </Panel>
      <Panel title="Object browser">
        <ObjectBrowser
          bucketName={selectedBucket}
          activeObjectKey={selectedObject?.key}
          onClearObject={clearActiveObject}
          onSelectObject={setActiveObject}
        />
      </Panel>
      <Panel title="Inspector">
        <ObjectInspector bucketName={selectedBucket} object={selectedObject} />
      </Panel>
      <a className="compat-link" href="/s3">
        Open current S3 dashboard
      </a>
    </div>
  )
}
