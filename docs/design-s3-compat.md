# S3 Compatibility Design

## Summary

`devcloud` の次の主要サービスとして、Amazon S3 互換のローカル object storage server を実装する。

目標は「AWS SDK / AWS CLI / 一般的な S3 client が endpoint override だけで動く」ことである。完全互換は範囲が広いため、実装は段階化する。ただし内部設計は最初から multipart upload、versioning、metadata、policy、event integration を拡張できる形にする。

## Compatibility Goal

### Definition

ここでの S3 互換は、以下を満たす状態を指す。

1. AWS SDK / AWS CLI が `endpoint_url` または endpoint override だけで接続できる。
2. SigV4 signed request と presigned URL を検証できる。
3. S3 REST XML API の request path、query parameter、header、XML response、XML error response が主要操作で一致する。
4. ETag、Last-Modified、Content-Length、Content-Type、user metadata、range request など、アプリケーションが依存しやすい observable behavior を再現する。
5. multipart upload、versioning、delete marker、bucket policy、CORS、lifecycle などを段階的に追加できる。

### Compatibility Levels

| Level | Name | Purpose |
| --- | --- | --- |
| L0 | SDK Smoke | AWS CLI / SDK の基本操作が通る |
| L1 | Object Core | bucket/object CRUD、list、metadata、range、copy が通る |
| L2 | Multipart | multipart upload と large object workflow が通る |
| L3 | Versioning | versioned bucket、delete marker、version-aware GET/DELETE が通る |
| L4 | Policy Surface | CORS、bucket policy、ACL compatibility response が通る |
| L5 | Operational Parity | lifecycle、event notification、website hosting、inventory など周辺機能を追加 |

MVP は L1 + L2 の一部を対象にする。最終的な「完全互換」は L5 までを含む長期目標とする。

## Goals

- `devcloud up` で S3 endpoint を起動する。
- AWS CLI で bucket 作成、object upload/download/list/delete ができる。
- AWS SDK / JavaScript / Python の基本操作が endpoint override だけで通る。
- local filesystem backed storage に object body、metadata、version state を保存する。
- S3 XML response と error code を返す。
- SigV4 authorization と presigned URL を local credential で検証する。
- dashboard/API から bucket/object の状態を確認できる。
- `devcloud reset` で S3 data を削除できる。

## Non-Goals

- 実際の AWS IAM と連携しない。
- KMS、SSE-KMS、SSE-C の暗号処理は初期実装では行わない。header compatibility と metadata preservation から始める。
- S3 Express One Zone / directory bucket は初期対象外。
- Object Lock compliance mode の法的保持保証はしない。
- Glacier / Deep Archive の実ストレージ階層は再現しない。
- AWS billing、CloudTrail、Access Analyzer は再現しない。

## User Experience

```bash
devcloud init
devcloud up
```

AWS CLI:

```bash
export AWS_ACCESS_KEY_ID=dev
export AWS_SECRET_ACCESS_KEY=dev
export AWS_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:14566 s3api create-bucket --bucket demo
aws --endpoint-url http://127.0.0.1:14566 s3 cp README.md s3://demo/README.md
aws --endpoint-url http://127.0.0.1:14566 s3 ls s3://demo/
aws --endpoint-url http://127.0.0.1:14566 s3 cp s3://demo/README.md /tmp/devcloud-readme.md
```

SDK clients should use:

```txt
endpoint: http://127.0.0.1:14566
region: us-east-1
accessKeyId: dev
secretAccessKey: dev
forcePathStyle: true
```

Virtual-hosted style is supported after path-style compatibility is stable.

## Scope

### v0.1 S3 MVP

```txt
Daemon:
  S3 REST XML endpoint  http://127.0.0.1:14566
  Dashboard/API         http://127.0.0.1:18025

Bucket API:
  GET    /                         ListBuckets
  PUT    /{bucket}                 CreateBucket
  HEAD   /{bucket}                 HeadBucket
  GET    /{bucket}                 ListObjectsV2 / ListObjects
  DELETE /{bucket}                 DeleteBucket

Object API:
  PUT    /{bucket}/{key}           PutObject
  HEAD   /{bucket}/{key}           HeadObject
  GET    /{bucket}/{key}           GetObject
  DELETE /{bucket}/{key}           DeleteObject
  PUT    /{bucket}/{key}?copy      CopyObject

Multipart API:
  POST   /{bucket}/{key}?uploads   CreateMultipartUpload
  PUT    /{bucket}/{key}?partNumber=N&uploadId=ID
  GET    /{bucket}/{key}?uploadId=ID
  POST   /{bucket}/{key}?uploadId=ID
  DELETE /{bucket}/{key}?uploadId=ID
  GET    /{bucket}?uploads

Compatibility:
  SigV4 signed request
  presigned URL
  path-style addressing
  XML success/error response
  ETag and checksum basics
  user metadata x-amz-meta-*
  Range GET
```

### Later

```txt
Virtual-hosted style
ListObjectVersions
Bucket versioning
Delete marker
Object tags
Bucket policy
CORS
ACL compatibility mode
Lifecycle expiration
Website hosting
Event notification to local SQS/PubSub core
SSE-S3 metadata compatibility
Object Lock headers
SelectObjectContent
```

## Architecture

```txt
AWS CLI / SDK / S3 client
        |
        v
+-----------------------+
| S3 HTTP Gateway       | :14566
| REST XML + SigV4      |
+-----------------------+
        |
        v
+-----------------------+
| S3 API Adapter        |
| request -> command    |
+-----------------------+
        |
        v
+-----------------------+
| Object Service        |
| bucket/object rules   |
+-----------------------+
        |                     |
        v                     v
+-------------------+   +-------------------+
| Object Metadata   |   | Blob Store        |
| bucket/object DB  |   | object bodies     |
+-------------------+   +-------------------+
        |
        v
+-----------------------+
| Dashboard/API         | :18025
+-----------------------+
```

## Repository Layout

```txt
orchestrator/
  src/
    config.rs
    supervisor.rs

services/
  s3/
    src/
      model.rs
      store.rs
      store_objects.rs
      store_multipart.rs
      responses.rs
      http.rs
      xml.rs
      validation.rs

  dashboard/
    src/
      s3.rs
```

`services/s3` は object metadata、bucket state、multipart state、XML/HTTP response shaping を Rust crate 内に閉じ込める。GCS 互換 API は同じ local object semantics を共有する。

## Configuration

Default config:

```yaml
project: dev

server:
  smtpPort: 11025
  dashboardPort: 18025
  s3Port: 14566

auth:
  smtp:
    mode: off
  aws:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    region: us-east-1

storage:
  path: .devcloud/data

services:
  mail:
    enabled: true
    maxMessageBytes: 10485760
  s3:
    enabled: true
    pathStyle: true
    virtualHostStyle: false
    maxObjectBytes: 5368709120
    multipart:
      minPartBytes: 5242880
      maxParts: 10000
```

`auth.aws.mode`:

| Mode | Behavior |
| --- | --- |
| `off` | Authorization header がなくても許可する |
| `relaxed` | SigV4 形式を検証し、credential scope と access key を確認する。signature mismatch は warning にできる |
| `strict` | canonical request から signature を完全検証する |

MVP は `relaxed` を既定にし、E2E では `strict` を追加検証する。

## Data Layout

```txt
.devcloud/
  data/
    blobs/
      ab/
        cd/
          abcdef...blob
    s3/
      buckets.jsonl
      objects.jsonl
      versions.jsonl
      multipart.jsonl
      buckets/
        demo/
          policy.json
          cors.json
          lifecycle.json
```

Object body は `blob.Store` に保存し、S3 metadata は `objectstore` に保存する。

```json
{"bucket":"demo","key":"README.md","versionId":"null","etag":"\"9a0364b9e99bb480dd25e1f0284c8555\"","size":1234,"contentType":"text/markdown","blob":"abc...","lastModified":"2026-04-30T10:00:00Z","deletedAt":null}
```

## Core Models

```go
type Bucket struct {
	Name      string
	Region    string
	CreatedAt time.Time
	Versioning VersioningState
}

type ObjectVersion struct {
	Bucket       string
	Key          string
	VersionID    string
	IsLatest     bool
	DeleteMarker bool
	ETag         string
	Size         int64
	ContentType  string
	Metadata     map[string]string
	Blob         blob.ID
	LastModified time.Time
	DeletedAt    *time.Time
}

type MultipartUpload struct {
	Bucket      string
	Key         string
	UploadID    string
	InitiatedAt time.Time
	Metadata    map[string]string
	Parts       []MultipartPart
}

type MultipartPart struct {
	PartNumber   int
	ETag         string
	Size         int64
	Blob         blob.ID
	LastModified time.Time
}
```

```go
type ObjectStore interface {
	CreateBucket(ctx context.Context, input CreateBucketInput) (Bucket, error)
	DeleteBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]Bucket, error)
	PutObject(ctx context.Context, input PutObjectInput, body io.Reader) (ObjectVersion, error)
	GetObject(ctx context.Context, input GetObjectInput) (ObjectVersion, io.ReadCloser, error)
	DeleteObject(ctx context.Context, input DeleteObjectInput) (DeleteObjectResult, error)
	ListObjects(ctx context.Context, input ListObjectsInput) (ListObjectsResult, error)
	CreateMultipartUpload(ctx context.Context, input CreateMultipartUploadInput) (MultipartUpload, error)
	UploadPart(ctx context.Context, input UploadPartInput, body io.Reader) (MultipartPart, error)
	CompleteMultipartUpload(ctx context.Context, input CompleteMultipartUploadInput) (ObjectVersion, error)
	AbortMultipartUpload(ctx context.Context, input AbortMultipartUploadInput) error
}
```

## HTTP Routing

### Addressing

Path-style:

```txt
GET /demo/README.md
Host: 127.0.0.1:14566
```

Virtual-hosted style:

```txt
GET /README.md
Host: demo.localhost:14566
```

MVP は path-style を優先する。virtual-hosted style は `*.localhost` と explicit host mapping の両方を後続で扱う。

### Operation Detection

S3 REST API は method + path + query parameter で operation を判定する。

```txt
GET /                       -> ListBuckets
PUT /{bucket}               -> CreateBucket
GET /{bucket}?list-type=2   -> ListObjectsV2
POST /{bucket}/{key}?uploads -> CreateMultipartUpload
PUT /{bucket}/{key}?partNumber=N&uploadId=ID -> UploadPart
POST /{bucket}/{key}?uploadId=ID -> CompleteMultipartUpload
```

Router は query parameter の存在を boolean として扱う。`?uploads` のように値が空の query も operation 判定に使う。

## SigV4 Design

### Inputs

- HTTP method
- canonical URI
- canonical query string
- canonical headers
- signed headers
- payload hash
- credential scope
- request timestamp

### MVP Behavior

1. `Authorization` header または presigned URL query を parse する。
2. access key が config と一致するか確認する。
3. region/service scope を検証する。service は `s3`。
4. `x-amz-content-sha256` がある場合、payload hash を検証する。
5. `auth.aws.mode=strict` の場合、HMAC chain で signature を検証する。

### Presigned URL

`X-Amz-Algorithm`、`X-Amz-Credential`、`X-Amz-Date`、`X-Amz-Expires`、`X-Amz-SignedHeaders`、`X-Amz-Signature` を処理する。

`X-Amz-Expires` の期限切れは `AccessDenied` を返す。

## Response Compatibility

### Headers

All S3 responses should include:

```txt
x-amz-request-id
x-amz-id-2
Date
Server: AmazonS3
```

Object responses:

```txt
ETag
Last-Modified
Content-Length
Content-Type
Accept-Ranges: bytes
x-amz-meta-*
x-amz-version-id
```

### Error XML

```xml
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
  <BucketName>demo</BucketName>
  <RequestId>...</RequestId>
  <HostId>...</HostId>
</Error>
```

Error mapping:

| Condition | HTTP | S3 Code |
| --- | ---: | --- |
| bucket missing | 404 | `NoSuchBucket` |
| key missing | 404 | `NoSuchKey` |
| bucket already exists | 409 | `BucketAlreadyOwnedByYou` |
| bucket not empty | 409 | `BucketNotEmpty` |
| invalid bucket name | 400 | `InvalidBucketName` |
| invalid part | 400 | `InvalidPart` |
| signature mismatch | 403 | `SignatureDoesNotMatch` |
| expired presigned URL | 403 | `AccessDenied` |

## Object Semantics

### PutObject

- Body を blob store に保存する。
- `Content-Type`、`Content-Encoding`、`Cache-Control`、`Content-Disposition`、`x-amz-meta-*` を metadata に保存する。
- Single-part ETag は raw body の MD5 hex を quoted string で返す。
- `If-Match` / `If-None-Match` は後続で対応する。

### GetObject

- `Range: bytes=start-end` に対応する。
- `HEAD` は body を返さず、同じ metadata header を返す。
- versioning 無効時は latest non-delete object を返す。

### CopyObject

- `x-amz-copy-source` を parse し、source object body と metadata を複製する。
- `x-amz-metadata-directive: REPLACE` の場合、request metadata を採用する。
- MVP では same-server bucket/key copy のみ対応する。

### DeleteObject

- versioning 無効時は latest object を論理削除する。
- versioning 有効時は delete marker を追加する。
- `versionId` 指定時はその version を削除する。

## List Semantics

### ListObjectsV2

Support:

- `prefix`
- `delimiter`
- `max-keys`
- `continuation-token`
- `start-after`
- `encoding-type=url`

Return:

- lexicographic order by key
- `Contents`
- `CommonPrefixes`
- `IsTruncated`
- `NextContinuationToken`

MVP の pagination token は opaque base64 JSON とする。

### ListObjects v1

`marker` / `next-marker` を v2 と同じ index scan の wrapper として実装する。

## Multipart Upload

### Flow

```txt
CreateMultipartUpload
  -> UploadPart N
  -> UploadPart N+1
  -> ListParts
  -> CompleteMultipartUpload
```

### Rules

- Part number は 1..10000。
- Complete 時は request XML の part order と ETag を検証する。
- 最終part以外は `minPartBytes` 以上を要求する。MVPのlocal modeでは warning only にできる。
- Multipart ETag は `<md5-of-part-md5s>-<part-count>` とする。
- Abort 後の uploadId は `NoSuchUpload`。
- Incomplete multipart parts は lifecycle cleanup の対象にする。

## Versioning

Versioning state:

```txt
Off
Enabled
Suspended
```

MVP は internal model を先に用意し、API は後続で公開する。

Versioning enabled:

- `PUT` は新しい version ID を発行する。
- `GET` without versionId は latest non-delete marker を返す。
- `DELETE` without versionId は delete marker を追加する。
- `DELETE` with versionId は該当 version を削除する。

## Policy, ACL, and Auth

### Bucket Policy

初期実装は policy document の保存と取得を先に行う。

Policy enforcement は段階化する。

1. `off`: 保存のみ
2. `basic`: principal/action/resource の allow/deny を評価
3. `condition`: simple condition を評価

### ACL

modern S3 では ACL を使わない構成が多いため、MVP では compatibility response を優先する。

- `x-amz-acl` は受け付けて metadata に保存する。
- `GetBucketAcl` / `GetObjectAcl` は owner full-control の固定レスポンスから開始する。

## Dashboard / API

Mail dashboard と同じ HTTP server に S3 view を追加する。

```txt
GET /api/s3/buckets
GET /api/s3/buckets/{bucket}
GET /api/s3/buckets/{bucket}/objects?prefix=
GET /api/s3/buckets/{bucket}/objects/{key}
DELETE /api/s3/buckets/{bucket}
DELETE /api/s3/buckets/{bucket}/objects/{key}
```

Dashboard requirements:

- bucket list
- object browser
- object metadata view
- raw/download action
- multipart uploads view
- version history view after versioning support

## Testing Strategy

### Unit Tests

- bucket name validation
- REST operation routing
- XML response serialization
- XML error serialization
- SigV4 canonical request generation
- ETag calculation
- objectstore index mutation
- multipart complete validation

### Integration Tests

```bash
cargo test --workspace
VERIFY_STAGE=s3-foundation bash scripts/s3-autoloop/verify.sh
```

### E2E Tests

AWS CLI:

```bash
aws --endpoint-url http://127.0.0.1:14566 s3api create-bucket --bucket demo
aws --endpoint-url http://127.0.0.1:14566 s3api put-object --bucket demo --key hello.txt --body README.md
aws --endpoint-url http://127.0.0.1:14566 s3api head-object --bucket demo --key hello.txt
aws --endpoint-url http://127.0.0.1:14566 s3api get-object --bucket demo --key hello.txt /tmp/hello.txt
aws --endpoint-url http://127.0.0.1:14566 s3api delete-object --bucket demo --key hello.txt
aws --endpoint-url http://127.0.0.1:14566 s3api delete-bucket --bucket demo
```

SDK matrix:

- AWS SDK v2
- boto3
- AWS SDK for JavaScript v3

### Contract Fixtures

Store golden XML for:

- ListBuckets
- ListObjectsV2
- CreateMultipartUpload
- CompleteMultipartUpload
- ListParts
- common errors

## Implementation Phases

### Phase 1: Foundation

- Config: `server.s3Port`, `services.s3`, `auth.aws`
- S3 HTTP server lifecycle in daemon
- request ID middleware
- XML response/error helpers
- path-style router
- no-auth or relaxed-auth mode

Exit criteria:

```bash
cargo test --workspace
curl http://127.0.0.1:14566/ returns ListAllMyBucketsResult
```

### Phase 2: Bucket/Object CRUD

- CreateBucket / HeadBucket / DeleteBucket
- PutObject / HeadObject / GetObject / DeleteObject
- ListBuckets / ListObjectsV2
- metadata and ETag
- Range GET

Exit criteria:

```bash
aws --endpoint-url http://127.0.0.1:14566 s3 cp README.md s3://demo/README.md
aws --endpoint-url http://127.0.0.1:14566 s3 cp s3://demo/README.md /tmp/README.md
```

### Phase 3: SigV4 Strict + Presigned URL

- Authorization header verification
- presigned URL verification
- `UNSIGNED-PAYLOAD`
- payload hash validation

Exit criteria:

```bash
aws s3api --endpoint-url http://127.0.0.1:14566 list-buckets
```

with `auth.aws.mode=strict`.

### Phase 4: Multipart

- CreateMultipartUpload
- UploadPart
- ListParts
- CompleteMultipartUpload
- AbortMultipartUpload
- ListMultipartUploads

Exit criteria:

```bash
aws --endpoint-url http://127.0.0.1:14566 s3 cp large.bin s3://demo/large.bin
```

### Phase 5: Versioning and Copy

- PutBucketVersioning / GetBucketVersioning
- ListObjectVersions
- version-aware GetObject / DeleteObject
- CopyObject

### Phase 6: Policy and Peripheral APIs

- CORS
- bucket policy storage and basic enforcement
- ACL compatibility
- lifecycle expiration
- event notification hooks

## Acceptance Criteria

1. `cargo test --workspace` passes.
2. `devcloud up` starts S3 endpoint on `127.0.0.1:14566`.
3. AWS CLI can create/delete buckets and put/get/head/delete/list objects.
4. AWS SDK v2, boto3, and AWS SDK for JavaScript v3 pass smoke tests with endpoint override.
5. SigV4 strict mode validates signed requests and presigned URLs.
6. ListObjectsV2 pagination, prefix, delimiter, and max-keys behave like S3 for tested fixtures.
7. Multipart upload can complete and returns multipart ETag.
8. Error responses use S3-compatible XML and status codes.
9. Dashboard/API can inspect buckets, objects, metadata, and multipart state.
10. Runtime data stays under `.devcloud/`.

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| "complete S3 compatibility" is too broad | Scope explosion | compatibility levels and phase gates |
| SigV4 canonicalization bugs | SDK incompatibility | golden tests from real SDK requests |
| XML response drift | client parsing failures | fixture-based contract tests |
| multipart edge cases | large upload failures | implement contract tests before optimization |
| filesystem index corruption | data loss during local dev | append-only log + atomic rewrite + recovery tests |
| virtual-hosted local DNS | client setup friction | path-style first, virtual-hosted later |
| versioning semantics complexity | delete/list bugs | internal version model from phase 1 |

## Open Questions

1. Should S3 share the existing AWS gateway port `14566`, or use a dedicated S3 port like `9000`?
2. Should `auth.aws.mode` default to `relaxed` for developer ergonomics, or `strict` for compatibility confidence?
3. Should dashboard object download stream through dashboard API, or link directly to S3 endpoint?
4. Should `mock/` include an S3 dashboard mock before implementation?

## References

- Amazon Simple Storage Service API Reference, API version `2006-03-01`.
- AWS SDK S3 operation references for multipart upload, object APIs, encryption headers, and directory bucket notes.
