# GCS Compatibility Design

## Summary

`devcloud` に Google Cloud Storage 互換のローカル object storage server を追加する。

目標は「Google Cloud Storage client libraries、`gcloud storage` / `gsutil`、および GCS JSON API 利用アプリケーションが endpoint override だけで基本 workflow を実行できる」ことである。完全互換は範囲が広いため、実装は段階化する。ただし内部設計は最初から S3 と同じ Object Core を共有し、GCS 固有の `generation`、`metageneration`、resumable upload、V4 signed URL、IAM/policy surface を拡張できる形にする。

## Document Control

| Field | Value |
| --- | --- |
| Audience | devcloud implementers, reviewers, future agent loops |
| Status | Draft |
| Owner | devcloud maintainers |
| Reviewer | TBD |
| Related docs | `docs/spec-v0.md`, `docs/design-s3-compat.md`, `docs/design-dashboard-shell.md` |
| Primary references | Google Cloud Storage JSON API, XML API, request endpoints, resumable uploads, V4 signed URLs |

## Compatibility Goal

### Definition

ここでの GCS 互換は、以下を満たす状態を指す。

1. Google Cloud Storage client libraries が local endpoint を指定するだけで接続できる。
2. JSON API の bucket/object/resource schema、query parameter、HTTP status、JSON error response が主要操作で一致する。
3. object media upload/download、metadata operation、range request、precondition、generation/metageneration が再現される。
4. resumable upload session が中断・再開・完了できる。
5. V4 signed URL を XML API endpoint 互換で検証できる。
6. S3 互換サーバーと同じ Object Core 上で、同じ bucket/object body を GCS semantics から操作できる。
7. IAM、ACL、retention、lifecycle、notification などを段階的に追加できる。

### Compatibility Levels

| Level | Name | Purpose |
| --- | --- | --- |
| L0 | Client Smoke | JSON API client / curl / emulator-style endpoint で疎通できる |
| L1 | Bucket/Object Core | bucket CRUD、object CRUD、list、metadata、range、copy が通る |
| L2 | Upload Workflows | multipart upload、media upload、resumable upload が通る |
| L3 | Generations | `generation` / `metageneration`、precondition、version-aware read/delete が通る |
| L4 | Policy Surface | ACL compatibility、IAM policy、CORS、retention、lifecycle response が通る |
| L5 | Operational Parity | notification、Pub/Sub integration、compose/rewrite edge cases、soft delete、requester pays などを追加 |

MVP は L1 + L2 の一部を対象にする。最終的な「完全互換」は L5 までを含む長期目標とする。

## Goals

- REQ-001: `devcloud up` で GCS JSON API endpoint を起動する。
- REQ-002: GCS client libraries が endpoint override で bucket 作成、object upload/download/list/delete を実行できる。
- REQ-003: `gcloud storage` または `gsutil` の主要操作を local endpoint へ向けて実行できる。
- REQ-004: Object Core を S3 と共有し、同じ bucket/object を S3 API と GCS API の両方から確認できる。
- REQ-005: JSON API response と error schema を GCS 互換で返す。
- REQ-006: `generation` と `metageneration` を保存し、precondition query を検証できる。
- REQ-007: resumable upload session を filesystem backed storage に保存できる。
- REQ-008: V4 signed URL を local credential で検証できる。
- REQ-009: dashboard/API から GCS bucket/object の状態を確認できる。
- REQ-010: `devcloud reset` で GCS data と upload session を削除できる。

## Non-Goals

- 実際の Google Cloud IAM、OAuth token introspection、Service Account Credentials API とは連携しない。
- Cloud KMS、CMEK、CSEK の実暗号化処理は初期実装では行わない。metadata preservation から始める。
- multi-region / dual-region の実ストレージ配置や RPO は再現しない。
- billing、quota、organization policy、VPC Service Controls は再現しない。
- Autoclass、Storage Intelligence、Storage Transfer Service は対象外とする。
- soft delete と Object Retention の法的保持保証はしない。

## User Experience

```bash
devcloud init
devcloud up
```

JSON API with curl:

```bash
export DEV_GCS=http://127.0.0.1:4443
export DEV_PROJECT=devcloud

curl -sS -X POST \
  "${DEV_GCS}/storage/v1/b?project=${DEV_PROJECT}" \
  -H "Content-Type: application/json" \
  -d '{"name":"demo","location":"US","storageClass":"STANDARD"}'

curl -sS -X POST \
  "${DEV_GCS}/upload/storage/v1/b/demo/o?uploadType=media&name=hello.txt" \
  -H "Content-Type: text/plain" \
  --data-binary 'hello from devcloud gcs'

curl -sS "${DEV_GCS}/storage/v1/b/demo/o"
curl -sS "${DEV_GCS}/download/storage/v1/b/demo/o/hello.txt?alt=media"
```

Client libraries should use:

```txt
endpoint: http://127.0.0.1:4443
project: devcloud
credentials: local emulator credential or anonymous relaxed mode
```

## Scope

### v0.1 GCS MVP

```txt
Daemon:
  GCS JSON API endpoint  http://127.0.0.1:4443
  Dashboard/API          http://127.0.0.1:8025

Bucket API:
  POST   /storage/v1/b?project={project}          buckets.insert
  GET    /storage/v1/b/{bucket}                   buckets.get
  GET    /storage/v1/b?project={project}          buckets.list
  DELETE /storage/v1/b/{bucket}                   buckets.delete

Object API:
  POST   /upload/storage/v1/b/{bucket}/o?uploadType=media&name={name}
                                                   objects.insert media upload
  POST   /upload/storage/v1/b/{bucket}/o?uploadType=multipart
                                                   objects.insert multipart upload
  POST   /upload/storage/v1/b/{bucket}/o?uploadType=resumable
                                                   objects.insert resumable init
  PUT    {sessionURI}                              resumable upload chunk/finalize
  GET    /storage/v1/b/{bucket}/o/{object}         objects.get metadata
  GET    /download/storage/v1/b/{bucket}/o/{object}?alt=media
                                                   objects.get media
  GET    /storage/v1/b/{bucket}/o                  objects.list
  DELETE /storage/v1/b/{bucket}/o/{object}         objects.delete
  POST   /storage/v1/b/{srcBucket}/o/{srcObject}/copyTo/b/{dstBucket}/o/{dstObject}
                                                   objects.copy

Compatibility:
  JSON success/error response
  object resource fields used by client libraries
  generation / metageneration basics
  ifGenerationMatch / ifGenerationNotMatch
  ifMetagenerationMatch / ifMetagenerationNotMatch
  user metadata
  Range GET for media download
  CRC32C / MD5 metadata where practical
```

### v0.2 Upload and Composition

```txt
objects.compose
objects.rewrite with rewriteToken
resumable upload status query
chunked resumable upload with Content-Range
projection=full/noAcl
fields partial response
prefix/delimiter/includeTrailingDelimiter
versions=true
```

### Later

```txt
XML API endpoint
V4 signed URL verification
HMAC interoperability credentials
OAuth bearer token relaxed/strict modes
Bucket IAM get/set/testIamPermissions
ACL compatibility resources
CORS
Lifecycle
Retention policy
Object holds
Object contexts
Soft delete
Requester Pays
Pub/Sub notification integration
```

## Architecture

```txt
GCS client library / gcloud / gsutil / curl
        |
        v
+----------------------------+
| GCS HTTP Gateway           | :4443
| JSON API + Upload API      |
+----------------------------+
        |
        v
+----------------------------+
| GCS API Adapter            |
| request -> Object Command  |
+----------------------------+
        |
        v
+----------------------------+
| Object Service             |
| provider-neutral semantics |
+----------------------------+
        |                         |
        v                         v
+------------------------+   +-------------------+
| Object Metadata Store  |   | Blob Store        |
| buckets/objects/index  |   | object bodies     |
+------------------------+   +-------------------+
        |
        v
+----------------------------+
| S3 Adapter / GCS Adapter   |
| shared Object Core         |
+----------------------------+
        |
        v
+----------------------------+
| Dashboard/API              | :8025
+----------------------------+
```

## Repository Layout

```txt
internal/
  app/
    config.go
    daemon.go

  protocol/
    google/
      auth/
        verifier.go
        signed_url.go
      jsonapi/
        response.go
        error.go
        partial_response.go

  services/
    object/
      model.go
      service.go
      bucket.go
      object.go
      upload.go
      generation.go
    gcs/
      server.go
      router.go
      handlers_bucket.go
      handlers_object.go
      handlers_upload.go
      resources.go
      errors.go

  storage/
    blob/
    objectstore/
      store.go
      index.go
      upload_session.go

  dashboard/
    gcs_static.go
```

現在の `internal/services/s3` は S3 固有 store として実装されている。GCS 実装では、先に provider-neutral `services/object` / `storage/objectstore` へ段階抽出し、S3 adapter は既存挙動を維持したまま同じ core を呼ぶ形へ移行する。

## API Mapping

### Buckets

| GCS operation | Method / path | Object Core command |
| --- | --- | --- |
| `buckets.insert` | `POST /storage/v1/b?project={project}` | `CreateBucket` |
| `buckets.get` | `GET /storage/v1/b/{bucket}` | `GetBucket` |
| `buckets.list` | `GET /storage/v1/b?project={project}` | `ListBuckets` |
| `buckets.patch` | `PATCH /storage/v1/b/{bucket}` | `UpdateBucketMetadata` |
| `buckets.update` | `PUT /storage/v1/b/{bucket}` | `ReplaceBucketMetadata` |
| `buckets.delete` | `DELETE /storage/v1/b/{bucket}` | `DeleteBucket` |

### Objects

| GCS operation | Method / path | Object Core command |
| --- | --- | --- |
| `objects.insert` media | `POST /upload/storage/v1/b/{bucket}/o?uploadType=media&name={object}` | `PutObject` |
| `objects.insert` multipart | `POST /upload/storage/v1/b/{bucket}/o?uploadType=multipart` | `PutObjectWithMetadata` |
| `objects.insert` resumable init | `POST /upload/storage/v1/b/{bucket}/o?uploadType=resumable` | `CreateUploadSession` |
| resumable upload | `PUT {sessionURI}` | `UploadSessionChunk/Finalize` |
| `objects.get` metadata | `GET /storage/v1/b/{bucket}/o/{object}` | `GetObjectMetadata` |
| `objects.get` media | `GET /download/storage/v1/b/{bucket}/o/{object}?alt=media` | `GetObjectBody` |
| `objects.list` | `GET /storage/v1/b/{bucket}/o` | `ListObjects` |
| `objects.delete` | `DELETE /storage/v1/b/{bucket}/o/{object}` | `DeleteObject` |
| `objects.copy` | `POST /storage/v1/b/{srcBucket}/o/{srcObject}/copyTo/b/{dstBucket}/o/{dstObject}` | `CopyObject` |
| `objects.compose` | `POST /storage/v1/b/{bucket}/o/{object}/compose` | `ComposeObject` |
| `objects.rewrite` | `POST /storage/v1/b/{srcBucket}/o/{srcObject}/rewriteTo/b/{dstBucket}/o/{dstObject}` | `RewriteObject` |

## Resource Model

### Bucket

```go
type Bucket struct {
    ID             string
    Name           string
    Project        string
    Location       string
    StorageClass   string
    Generation     int64
    Metageneration int64
    CreatedAt      time.Time
    UpdatedAt      time.Time
    Labels         map[string]string
    Versioning     bool
    CORS           []CORSRule
    Lifecycle      []LifecycleRule
}
```

### Object

```go
type Object struct {
    Bucket         string
    Name           string
    Generation     int64
    Metageneration int64
    Size           int64
    ETag           string
    MD5Hash        string
    CRC32C         string
    ContentType    string
    ContentEncoding string
    CacheControl   string
    ContentDisposition string
    Metadata       map[string]string
    BlobID         string
    CreatedAt      time.Time
    UpdatedAt      time.Time
    DeletedAt      *time.Time
}
```

GCS の `generation` は object body version、`metageneration` は metadata revision として扱う。S3 の `versionId` は Object Core の version identity から adapter 側で変換する。

## Storage Layout

```txt
.devcloud/
  data/
    object/
      buckets/
        {bucket}/
          bucket.json
          objects/
            {escaped-object-key}/
              generations/
                {generation}/
                  object.json
                  body -> blob ref
      uploads/
        gcs/
          {upload-id}/
            session.json
            chunks/
      blobs/
        sha256/
```

既存 S3 data は後方互換を維持する。Object Core 抽出時は migration ではなく lazy read-through を優先し、`devcloud reset` の削除対象に両方の layout を含める。

## Request Handling

### JSON API Routing

JSON API は `/storage/v1/...`、upload API は `/upload/storage/v1/...`、download API は `/download/storage/v1/...` を受ける。

```txt
GET    /storage/v1/b
POST   /storage/v1/b
GET    /storage/v1/b/{bucket}
PATCH  /storage/v1/b/{bucket}
DELETE /storage/v1/b/{bucket}

GET    /storage/v1/b/{bucket}/o
GET    /storage/v1/b/{bucket}/o/{object}
DELETE /storage/v1/b/{bucket}/o/{object}
POST   /upload/storage/v1/b/{bucket}/o
GET    /download/storage/v1/b/{bucket}/o/{object}
```

object name は path segment と query parameter の両方で percent-decoding を厳密に扱う。`/` を含む object name は single object key として保持する。

### Upload Types

| `uploadType` | Request | Behavior |
| --- | --- | --- |
| `media` | body only | query `name` を object name とし、metadata は headers から補完 |
| `multipart` | metadata JSON + media body | multipart/related を parse して metadata と body を保存 |
| `resumable` | init then session upload | session URI を返し、`Content-Range` で chunk/finalize |

resumable upload session は process restart 後も再開できるよう `.devcloud/data/object/uploads/gcs` に保存する。

### Preconditions

MVP で以下を実装する。

| Parameter | Meaning |
| --- | --- |
| `ifGenerationMatch` | 現在世代が一致する場合のみ変更 |
| `ifGenerationNotMatch` | 現在世代が一致しない場合のみ変更 |
| `ifMetagenerationMatch` | metadata revision が一致する場合のみ変更 |
| `ifMetagenerationNotMatch` | metadata revision が一致しない場合のみ変更 |

失敗時は `412 Precondition Failed` の JSON error を返す。

## Authentication

### Modes

| Mode | Purpose |
| --- | --- |
| `relaxed` | local development default。Authorization なしを許可 |
| `bearer-dev` | configured bearer token のみ検証 |
| `oauth-relaxed` | `Authorization: Bearer ...` の形式だけ検証し、token introspection は行わない |
| `strict` | local service account / HMAC / signed URL の署名を検証 |

### Signed URLs

GCS signed URL は XML API endpoint 用であるため、V4 signed URL 対応は XML API route と一緒に実装する。local では以下を許可する。

- RSA service account key based V4 signature
- HMAC interoperability credential based V4 signature
- max expiration 7 days
- signed headers and canonical resource validation

## Error Model

JSON API error response は以下の形を返す。

```json
{
  "error": {
    "code": 404,
    "message": "Not Found",
    "errors": [
      {
        "domain": "global",
        "reason": "notFound",
        "message": "Not Found"
      }
    ]
  }
}
```

代表 mapping:

| Condition | HTTP | Reason |
| --- | --- | --- |
| bucket missing | 404 | `notFound` |
| object missing | 404 | `notFound` |
| invalid bucket name | 400 | `invalid` |
| duplicate bucket | 409 | `conflict` |
| non-empty bucket delete | 409 | `conflict` |
| precondition failed | 412 | `conditionNotMet` |
| invalid upload session | 404 | `notFound` |
| unsupported feature | 501 | `notImplemented` |
| auth failed | 401 / 403 | `authError` / `forbidden` |

## Dashboard

GCS dashboard は共通 dashboard shell に追加する。

```txt
GET /gcs
GET /api/gcs/status
GET /api/gcs/buckets
POST /api/gcs/buckets
GET /api/gcs/buckets/{bucket}
DELETE /api/gcs/buckets/{bucket}
GET /api/gcs/buckets/{bucket}/objects?prefix=&delimiter=&pageToken=
GET /api/gcs/buckets/{bucket}/objects/{encodedKey}
GET /api/gcs/buckets/{bucket}/objects/{encodedKey}/download
DELETE /api/gcs/buckets/{bucket}/objects/{encodedKey}
GET /api/gcs/uploads
DELETE /api/gcs/uploads/{uploadId}
```

UI は S3 Object Explorer の構造を再利用し、GCS 固有列として `generation`、`metageneration`、`storageClass`、`crc32c` を表示する。

## Implementation Plan

### Phase 0: Design and Test Harness

- IMPL-001: `docs/design-gcs-compat.md` を確定する。
- IMPL-002: `scripts/gcs-autoloop/verify.sh` を追加し、stage based verification を用意する。
- IMPL-003: curl based JSON API smoke test fixtures を追加する。

### Phase 1: Config and Daemon

- IMPL-010: config に `server.gcsPort`、`services.gcs`、`auth.google` を追加する。
- IMPL-011: `.devcloud/config.yaml` initialization と reset 対象に GCS storage を追加する。
- IMPL-012: `devcloud up` で GCS endpoint を起動する。

### Phase 2: Object Core Extraction

- IMPL-020: S3 store から provider-neutral object model を抽出する。
- IMPL-021: bucket/object CRUD、metadata、body blob 保存を Object Core に移す。
- IMPL-022: S3 adapter の既存テストを通し、後方互換を守る。

### Phase 3: GCS JSON API MVP

- IMPL-030: buckets insert/get/list/delete を実装する。
- IMPL-031: objects insert/get/list/delete/copy を実装する。
- IMPL-032: JSON error response と resource schema を実装する。
- IMPL-033: media download と Range GET を実装する。

### Phase 4: Resumable Upload and Preconditions

- IMPL-040: resumable upload init/status/chunk/finalize を実装する。
- IMPL-041: generation/metageneration を永続化する。
- IMPL-042: precondition query を実装する。
- IMPL-043: compose/rewrite の基本 path を実装する。

### Phase 5: Auth and Signed URL

- IMPL-050: bearer local auth mode を追加する。
- IMPL-051: GCS V4 signed URL canonical request 検証を実装する。
- IMPL-052: XML API endpoint の signed URL GET/PUT smoke を実装する。

### Phase 6: Dashboard

- IMPL-060: `/api/dashboard/services` に GCS を追加する。
- IMPL-061: `/gcs` route と React dashboard page を追加する。
- IMPL-062: bucket/object/upload session inspection を追加する。

## Verification Plan

### Unit Tests

- bucket name validation
- JSON resource encoding
- object name path decoding
- generation/metageneration increments
- precondition success/failure
- JSON error response mapping
- resumable upload session lifecycle
- signed URL canonical request verification

### Integration Tests

```bash
go test ./...
VERIFY_STAGE=foundation bash scripts/gcs-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh
```

### Acceptance Criteria

- AC-001: Given `devcloud up` is running, when `GET /storage/v1/b?project=devcloud` is called, then JSON bucket list is returned.
- AC-002: Given a valid bucket request, when `POST /storage/v1/b?project=devcloud` is called, then the bucket is persisted and visible from `buckets.list`.
- AC-003: Given a bucket exists, when `objects.insert` media upload is called, then object metadata and body are retrievable.
- AC-004: Given an object exists, when media download uses `Range: bytes=0-4`, then response is `206` and returns only that byte range.
- AC-005: Given an object generation exists, when a mismatched `ifGenerationMatch` is supplied, then response is `412`.
- AC-006: Given a resumable upload session is created, when chunks are uploaded with valid `Content-Range`, then final object is committed once complete.
- AC-007: Given the same bucket/object is created through GCS, when S3 list/get is called, then the object is visible through S3 adapter after Object Core extraction.
- AC-008: Given GCS dashboard is opened, when bucket/object data exists, then `/gcs` shows bucket, object, metadata, and generation fields.

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| GCS complete compatibility is too broad | Scope explosion | compatibility levels and phase gates |
| S3 store extraction breaks existing S3 behavior | Regression | keep S3 tests as required gate before GCS merge |
| Client libraries rely on HTTPS-only assumptions | Local endpoint friction | document supported emulator-style endpoint behavior; allow HTTP locally |
| OAuth behavior is hard to emulate safely | Auth mismatch | start with relaxed/bearer-dev modes; make strict mode explicit |
| Resumable upload sessions leak disk data | Storage bloat | reset cleanup and session TTL |
| XML API and signed URL scope diverge from JSON API | Compatibility gaps | treat signed URL as XML API milestone, not JSON MVP |

## Open Questions

1. Should GCS use dedicated port `4443`, or share a unified Google API gateway port?
2. Should Object Core extraction happen before any GCS handler, or should GCS start with a temporary adapter over the current S3 store?
3. Which clients are the first compatibility target: Go client library, Python client library, `gcloud storage`, or `gsutil`?
4. Should strict OAuth token validation ever call external Google services, or remain fully offline?
5. Should GCS and S3 bucket namespaces be identical by default?

## References

- Google Cloud Storage request endpoints: https://cloud.google.com/storage/docs/request-endpoints
- Google Cloud Storage JSON API overview: https://cloud.google.com/storage/docs/json_api
- Google Cloud Storage JSON API resources and methods: https://cloud.google.com/storage/docs/json_api/v1
- Google Cloud Storage Buckets resource: https://cloud.google.com/storage/docs/json_api/v1/buckets
- Google Cloud Storage Objects resource: https://cloud.google.com/storage/docs/json_api/v1/objects
- Google Cloud Storage resumable uploads: https://cloud.google.com/storage/docs/resumable-uploads
- Google Cloud Storage signed URLs: https://cloud.google.com/storage/docs/access-control/signed-urls
- Google Cloud Storage V4 signing process: https://cloud.google.com/storage/docs/access-control/signing-urls-manually

## Change History

| Date | Change |
| --- | --- |
| 2026-04-30 | Initial draft |
