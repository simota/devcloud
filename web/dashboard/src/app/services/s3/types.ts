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
  s3Uri: string
  downloadUrl: string
}
