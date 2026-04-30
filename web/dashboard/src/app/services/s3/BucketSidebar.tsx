import { EmptyState } from '../../../ui/EmptyState'
import type { S3BucketSummary } from './types'

type BucketSidebarProps = {
  buckets: S3BucketSummary[]
  activeBucket?: string
  onSelectBucket: (bucketName: string) => void
}

export function BucketSidebar({ buckets, activeBucket, onSelectBucket }: BucketSidebarProps): JSX.Element {
  if (buckets.length === 0) {
    return <EmptyState title="No buckets" description="Buckets created through the S3 API will appear here." />
  }

  return (
    <div className="bucket-list">
      {buckets.map((bucket) => (
        <button
          className={bucket.name === activeBucket ? 'bucket-item active' : 'bucket-item'}
          key={bucket.name}
          onClick={() => onSelectBucket(bucket.name)}
        >
          <span>
            <span className="bucket-name">{bucket.name}</span>
            <span className="bucket-meta">Created {formatDate(bucket.creationDate)}</span>
          </span>
          <span className="count-pill">{bucket.objectCount}</span>
        </button>
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
