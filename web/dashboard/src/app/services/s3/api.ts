import { fetchJSON, fetchNoContent } from '../../api/client'
import type { S3BucketSummary, S3MultipartUploadSummary, S3ObjectSummary } from './types'

export type S3BucketsResponse = {
  buckets: S3BucketSummary[]
}

export type S3ObjectsResponse = {
  bucket: string
  prefix: string
  objects: S3ObjectSummary[]
}

export type S3MultipartUploadsResponse = {
  bucket: string
  uploads: S3MultipartUploadSummary[]
}

export async function listS3Buckets(): Promise<S3BucketsResponse> {
  return fetchJSON<S3BucketsResponse>('/api/s3/buckets')
}

export async function createS3Bucket(name: string): Promise<S3BucketSummary> {
  return fetchJSON<S3BucketSummary>('/api/s3/buckets', { method: 'POST', body: { name } })
}

export async function deleteS3Bucket(bucketName: string): Promise<void> {
  return fetchNoContent(`/api/s3/buckets/${encodeURIComponent(bucketName)}`, { method: 'DELETE' })
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

export async function deleteS3Object(bucketName: string, objectKey: string): Promise<void> {
  return fetchNoContent(`/api/s3/buckets/${encodeURIComponent(bucketName)}/objects/${encodeURIComponent(objectKey)}`, {
    method: 'DELETE',
  })
}

export async function listS3MultipartUploads(bucketName: string): Promise<S3MultipartUploadsResponse> {
  return fetchJSON<S3MultipartUploadsResponse>(`/api/s3/buckets/${encodeURIComponent(bucketName)}/multipart`)
}

export async function abortS3MultipartUpload(bucketName: string, uploadId: string): Promise<void> {
  return fetchNoContent(`/api/s3/buckets/${encodeURIComponent(bucketName)}/multipart/${encodeURIComponent(uploadId)}`, {
    method: 'DELETE',
  })
}

export type UploadS3ObjectInput = {
  bucketName: string
  objectKey: string
  body: Blob
  contentType?: string
  metadata?: Record<string, string>
}

export async function uploadS3Object(input: UploadS3ObjectInput): Promise<S3ObjectSummary> {
  return fetchS3ObjectMutation(
    `/api/s3/buckets/${encodeURIComponent(input.bucketName)}/objects/${encodeURIComponent(input.objectKey)}`,
    {
      body: input.body,
      contentType: input.contentType,
      metadata: input.metadata,
    },
  )
}

export type CopyS3ObjectInput = {
  sourceBucketName: string
  sourceObjectKey: string
  destinationBucketName: string
  destinationObjectKey: string
}

export async function copyS3Object(input: CopyS3ObjectInput): Promise<S3ObjectSummary> {
  return fetchS3ObjectMutation(
    `/api/s3/buckets/${encodeURIComponent(input.destinationBucketName)}/objects/${encodeURIComponent(input.destinationObjectKey)}`,
    {
      copySource: `/${encodeURIComponent(input.sourceBucketName)}/${encodeURIComponent(input.sourceObjectKey)}`,
    },
  )
}

type FetchS3ObjectMutationOptions = {
  body?: BodyInit
  contentType?: string
  copySource?: string
  metadata?: Record<string, string>
}

async function fetchS3ObjectMutation(path: string, options: FetchS3ObjectMutationOptions): Promise<S3ObjectSummary> {
  const headers = new Headers({ Accept: 'application/json' })
  if (options.contentType?.trim()) {
    headers.set('Content-Type', options.contentType.trim())
  }
  if (options.copySource) {
    headers.set('x-amz-copy-source', options.copySource)
  }
  for (const [key, value] of Object.entries(options.metadata ?? {})) {
    const cleanKey = key.trim().toLowerCase()
    if (cleanKey !== '' && value !== '') {
      headers.set(`x-amz-meta-${cleanKey}`, value)
    }
  }

  return fetchJSON<S3ObjectSummary>(path, {
    rawBody: options.body,
    headers,
    method: 'PUT',
  })
}
