export type S3BucketSummary = {
  name: string
  creationDate: string
  objectCount: number
}

export type S3ObjectSummary = {
  key: string
  size: number
  etag: string
  contentType: string
  lastModified: string
  metadata?: Record<string, string>
  s3Uri: string
  downloadUrl: string
}

export type S3MultipartUploadSummary = {
  key: string
  uploadId: string
  initiated: string
  contentType: string
  metadata?: Record<string, string>
}
