import { EmptyState } from '../../../ui/EmptyState'
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
    <ul className="s3-bucket-list" aria-label="S3 buckets">
      {buckets.map((bucket) => {
        const active = bucket.name === activeBucket
        const createdLabel = formatRelativeDate(bucket.creationDate)
        const objectsLabel = formatObjectCount(bucket.objectCount)
        const tooltip = `${bucket.name}\n${objectsLabel} · created ${formatDate(bucket.creationDate)}`
        return (
          <li className={active ? 's3-bucket-row active' : 's3-bucket-row'} key={bucket.name} title={tooltip}>
            <button
              className="s3-bucket-select"
              onClick={() => onSelectBucket(bucket.name)}
              title={tooltip}
              type="button"
            >
              <span className="s3-bucket-icon" aria-hidden>
                <BucketIcon />
              </span>
              <span className="s3-bucket-info">
                <span className="s3-bucket-name" title={bucket.name}>{bucket.name}</span>
                <span className="s3-bucket-meta">
                  {objectsLabel} · {createdLabel}
                </span>
              </span>
            </button>
            <button
              aria-label={`Delete bucket ${bucket.name}`}
              className="s3-icon-button danger s3-bucket-delete"
              disabled={disabled}
              onClick={() => onDeleteBucket(bucket.name)}
              type="button"
            >
              <TrashIcon />
            </button>
          </li>
        )
      })}
    </ul>
  )
}

function BucketIcon(): JSX.Element {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M2.5 4.5h11l-1 8.5a1 1 0 0 1-1 .9H4.5a1 1 0 0 1-1-.9z" />
      <path d="M2.5 4.5L4 2.5h8l1.5 2" />
      <path d="M6 7v4" />
      <path d="M10 7v4" />
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

function formatObjectCount(count: number): string {
  if (!Number.isFinite(count) || count < 0) {
    return '— objects'
  }
  return count === 1 ? '1 object' : `${count} objects`
}

function formatDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value || 'unknown'
  }
  return date.toLocaleString()
}

function formatRelativeDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return 'created —'
  }
  const diffMs = Date.now() - date.getTime()
  const minute = 60 * 1000
  const hour = 60 * minute
  const day = 24 * hour
  const week = 7 * day
  const month = 30 * day
  const year = 365 * day
  if (diffMs < minute) {
    return 'just now'
  }
  if (diffMs < hour) {
    const minutes = Math.floor(diffMs / minute)
    return `${minutes}m ago`
  }
  if (diffMs < day) {
    const hours = Math.floor(diffMs / hour)
    return `${hours}h ago`
  }
  if (diffMs < week) {
    const days = Math.floor(diffMs / day)
    return `${days}d ago`
  }
  if (diffMs < month) {
    const weeks = Math.floor(diffMs / week)
    return `${weeks}w ago`
  }
  if (diffMs < year) {
    const months = Math.floor(diffMs / month)
    return `${months}mo ago`
  }
  const years = Math.floor(diffMs / year)
  return `${years}y ago`
}
