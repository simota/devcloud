export interface Bucket {
  name: string;
  region: string;
  createdAt: string;
  objectCount: number;
  totalBytes: number;
  versioning: "Off" | "Enabled" | "Suspended";
}

export interface S3Object {
  key: string;
  size: number;
  etag: string;
  contentType: string;
  lastModified: string;
  storageClass: string;
  isPrefix?: boolean;
}

export interface ObjectDetail extends S3Object {
  bucket: string;
  metadata: Record<string, string>;
  versionId: string;
  s3Uri: string;
  endpointUrl: string;
  previewText?: string;
  previewType?: "text" | "json" | "image" | "binary" | "none";
}

export interface ObjectVersion {
  versionId: string;
  isLatest: boolean;
  isDeleteMarker: boolean;
  size: number;
  etag: string;
  lastModified: string;
}

export interface MultipartUpload {
  uploadId: string;
  key: string;
  initiated: string;
  parts: number;
  uploadedSize: number;
}

export interface ActivityEntry {
  method: string;
  path: string;
  timestamp: string;
  statusCode: number;
}

export interface S3Status {
  running: boolean;
  endpoint: string;
  region: string;
  authMode: "relaxed" | "strict" | "off";
  version: string;
  storagePath: string;
}

export type MobileView = "buckets" | "objects" | "inspector";

export type SortKey = "name" | "size" | "lastModified";
export type SortDir = "asc" | "desc";
