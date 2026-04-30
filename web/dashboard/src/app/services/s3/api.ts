import type { S3BucketSummary } from './types'

export type S3BucketsResponse = {
  buckets: S3BucketSummary[]
}

export async function listS3Buckets(): Promise<S3BucketsResponse> {
  const response = await fetch('/api/s3/buckets', { headers: { Accept: 'application/json' } })
  if (!response.ok) {
    throw new Error('S3 buckets request failed')
  }
  return (await response.json()) as S3BucketsResponse
}
