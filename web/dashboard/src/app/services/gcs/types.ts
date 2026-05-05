export type GCSStatus = {
  status: 'running' | 'disabled' | string
  running: boolean
  endpoint: string
  project: string
  storagePath: string
  uploadSessionPath: string
}

export type GCSBucketSummary = {
  name: string
  timeCreated: string
  objectCount: number
  gcsUri: string
}

export type GCSObjectSummary = {
  name: string
  size: number
  etag: string
  contentType: string
  crc32c?: string
  storageClass: string
  updated: string
  metadata?: Record<string, string>
  generation: string
  metageneration: string
  gcsUri: string
  downloadUrl: string
}

export type GCSUploadSessionSummary = {
  id: string
  bucket: string
  name: string
  contentType?: string
  createdAt: string
  receivedBytes: number
}
