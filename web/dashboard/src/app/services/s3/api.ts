import { fetchJSON } from '../../api/client'
import type { S3BucketSummary, S3ObjectSummary } from './types'

export type S3BucketsResponse = {
  buckets: S3BucketSummary[]
}

export type S3ObjectsResponse = {
  bucket: string
  prefix: string
  objects: S3ObjectSummary[]
}

export async function listS3Buckets(): Promise<S3BucketsResponse> {
  return fetchJSON<S3BucketsResponse>('/api/s3/buckets')
}

export async function listS3Objects(bucketName: string, prefix = ''): Promise<S3ObjectsResponse> {
  const params = new URLSearchParams()
  if (prefix !== '') {
    params.set('prefix', prefix)
  }
  const query = params.toString()
  return fetchJSON<S3ObjectsResponse>(
    `/api/s3/buckets/${encodeURIComponent(bucketName)}/objects${query ? `?${query}` : ''}`,
  )
}
