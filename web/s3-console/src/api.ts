export interface Info {
  authMode: 'relaxed' | 'strict'
  region: string
}

export interface Bucket {
  name: string
  creationDate: string
  objectCount: number
}

export interface S3Object {
  key: string
  size: number
  etag: string
  contentType: string
  lastModified: string
}

/** Mirrors the server's request body limit (`MAX_BODY_BYTES`). */
export const MAX_UPLOAD_BYTES = 128 * 1024 * 1024

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, init)
  if (!response.ok) {
    const body = await response.json().catch(() => null) as { error?: string } | null
    throw new Error(body?.error ?? `Request failed with status ${response.status}`)
  }
  return (response.status === 204 || !response.headers.get('content-type')?.includes('json')
    ? undefined
    : await response.json()) as T
}

function bucketPath(bucket: string): string {
  return `/api/buckets/${encodeURIComponent(bucket)}`
}

/** The key is a query value because URL parsing resolves `.` and `..` path segments. */
export function objectUrl(bucket: string, key: string): string {
  return `${bucketPath(bucket)}/object?key=${encodeURIComponent(key)}`
}

export const getInfo = () => request<Info>('/api/info')

export const listBuckets = async () =>
  (await request<{ buckets: Bucket[] }>('/api/buckets')).buckets

export const createBucket = (bucket: string) =>
  request<void>(bucketPath(bucket), { method: 'PUT' })

export const deleteBucket = (bucket: string) =>
  request<void>(bucketPath(bucket), { method: 'DELETE' })

export const listObjects = async (bucket: string, prefix: string) =>
  (await request<{ objects: S3Object[] }>(
    `${bucketPath(bucket)}/objects?prefix=${encodeURIComponent(prefix)}`,
  )).objects

export const uploadObject = (bucket: string, key: string, file: File) =>
  request<void>(objectUrl(bucket, key), {
    method: 'PUT',
    body: file,
    headers: file.type ? { 'Content-Type': file.type } : undefined,
  })

export const deleteObject = (bucket: string, key: string) =>
  request<void>(objectUrl(bucket, key), { method: 'DELETE' })
