import { fetchJSON, fetchNoContent } from '../../api/client'
import type { GCSBucketSummary, GCSObjectSummary, GCSStatus, GCSUploadSessionSummary } from './types'

export type GCSBucketsResponse = {
  buckets: GCSBucketSummary[]
}

export type GCSObjectsResponse = {
  bucket: string
  prefix: string
  objects: GCSObjectSummary[]
}

export type GCSUploadSessionsResponse = {
  sessions: GCSUploadSessionSummary[]
}

export async function getGCSStatus(): Promise<GCSStatus> {
  return fetchJSON<GCSStatus>('/api/gcs/status')
}

export async function listGCSBuckets(): Promise<GCSBucketsResponse> {
  return fetchJSON<GCSBucketsResponse>('/api/gcs/buckets')
}

export async function createGCSBucket(name: string): Promise<GCSBucketSummary> {
  return fetchJSON<GCSBucketSummary>('/api/gcs/buckets', { method: 'POST', body: { name } })
}

export async function deleteGCSBucket(bucketName: string): Promise<void> {
  return fetchNoContent(`/api/gcs/buckets/${encodeURIComponent(bucketName)}`, { method: 'DELETE' })
}

export async function listGCSObjects(bucketName: string, prefix = ''): Promise<GCSObjectsResponse> {
  const params = new URLSearchParams()
  if (prefix !== '') {
    params.set('prefix', prefix)
  }
  const query = params.toString()
  return fetchJSON<GCSObjectsResponse>(
    `/api/gcs/buckets/${encodeURIComponent(bucketName)}/objects${query ? `?${query}` : ''}`,
  )
}

export async function getGCSObject(bucketName: string, objectName: string): Promise<GCSObjectSummary> {
  return fetchJSON<GCSObjectSummary>(
    `/api/gcs/buckets/${encodeURIComponent(bucketName)}/objects/${encodeURIComponent(objectName)}`,
  )
}

export async function deleteGCSObject(bucketName: string, objectName: string): Promise<void> {
  return fetchNoContent(`/api/gcs/buckets/${encodeURIComponent(bucketName)}/objects/${encodeURIComponent(objectName)}`, {
    method: 'DELETE',
  })
}

export async function listGCSUploadSessions(): Promise<GCSUploadSessionsResponse> {
  return fetchJSON<GCSUploadSessionsResponse>('/api/gcs/upload-sessions')
}

export async function deleteGCSUploadSession(sessionID: string): Promise<void> {
  return fetchNoContent(`/api/gcs/uploads/${encodeURIComponent(sessionID)}`, { method: 'DELETE' })
}
