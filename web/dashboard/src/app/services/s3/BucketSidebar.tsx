import { EmptyState } from '../../../ui/EmptyState'
import { Button } from '../../../ui/Button'
import type { S3BucketSummary } from './types'

type BucketSidebarProps = {
  buckets: S3BucketSummary[]
  activeBucket?: string
  disabled: boolean
  onDeleteBucket: (bucketName: string) => void
  onSelectBucket: (bucketName: string) => void
}

export function BucketSidebar({
  buckets,
  activeBucket,
  disabled,
  onDeleteBucket,
  onSelectBucket,
}: BucketSidebarProps): JSX.Element {
  if (buckets.length === 0) {
    return <EmptyState title="No buckets" description="Buckets created through the S3 API will appear here." />
  }

  return (
    <div className="bucket-list">
      {buckets.map((bucket) => (
        <div className={bucket.name === activeBucket ? 'bucket-item active gcs-bucket-row' : 'bucket-item gcs-bucket-row'} key={bucket.name}>
          <button className="object-select" onClick={() => onSelectBucket(bucket.name)}>
            <span className="bucket-name">{bucket.name}</span>
            <span className="bucket-meta">Created {formatDate(bucket.creationDate)}</span>
          </button>
          <span className="count-pill">{bucket.objectCount}</span>
          <Button
            aria-label={`Delete bucket ${bucket.name} with confirmation`}
            className="danger"
            disabled={disabled}
            onClick={() => onDeleteBucket(bucket.name)}
          >
            Delete bucket
          </Button>
        </div>
      ))}
    </div>
  )
}

function formatDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value || 'unknown'
  }
  return date.toLocaleString()
}
