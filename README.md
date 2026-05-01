# devcloud

Local cloud service emulator for development and E2E inspection.

`devcloud` runs a local dashboard plus compatible development endpoints for Mail, S3, GCS, and DynamoDB. It is designed for deterministic local tests and manual inspection, not for production workloads or full cloud-provider parity.

## Quick Start

Initialize local configuration and start all enabled services:

```bash
go run ./cmd/devcloud init
go run ./cmd/devcloud up
```

Open the dashboard:

```text
http://127.0.0.1:8025/
http://127.0.0.1:8025/dashboard/
```

Default local endpoints:

| Service | Endpoint | Dashboard |
| --- | --- | --- |
| Mail SMTP | `127.0.0.1:1025` | `http://127.0.0.1:8025/mail` |
| S3 | `http://127.0.0.1:4566` | `http://127.0.0.1:8025/s3` |
| GCS | `http://127.0.0.1:4443` | `http://127.0.0.1:8025/gcs` |
| DynamoDB | `http://127.0.0.1:8000` | `http://127.0.0.1:8025/dashboard/dynamodb` |

Useful commands:

```bash
go run ./cmd/devcloud help
go run ./cmd/devcloud init
go run ./cmd/devcloud up
go run ./cmd/devcloud dashboard
go run ./cmd/devcloud reset
```

## Configuration

Configuration lives at `.devcloud/config.yaml`. Runtime data is stored under `.devcloud/data` by default.

```yaml
project: dev

server:
  smtpPort: 1025
  dashboardPort: 8025
  s3Port: 4566
  gcsPort: 4443
  dynamodbPort: 8000

auth:
  smtp:
    mode: off
  s3:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  gcs:
    mode: relaxed
    project: devcloud
  dynamodb:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev

storage:
  path: .devcloud/data

services:
  mail:
    enabled: true
    maxMessageBytes: 10485760
  s3:
    enabled: true
    region: us-east-1
    pathStyle: true
    virtualHostStyle: false
    maxObjectBytes: 5368709120
    multipart:
      minPartBytes: 5242880
  gcs:
    enabled: true
    project: devcloud
    location: US
  dynamodb:
    enabled: true
    region: us-east-1
    billingMode: PAY_PER_REQUEST
    maxItemBytes: 400000
    maxTables: 256
```

## Support Matrix

Legend:

| Value | Meaning |
| --- | --- |
| Yes | Implemented and covered by tests or E2E smoke checks. |
| Partial | Useful local subset exists, but behavior is not complete provider parity. |
| No | Not implemented. Requests may fail, be ignored, or return a compatibility error. |

### Service Availability

| Capability | Mail | S3 | GCS | DynamoDB |
| --- | --- | --- | --- | --- |
| Local endpoint | Yes | Yes | Yes | Yes |
| Dashboard view | Yes | Yes | Yes | Yes |
| Persistent local storage | Yes | Yes | Yes | Yes |
| Configurable port | Yes | Yes | Yes | Yes |
| Enable/disable via config | Yes | Yes | Yes | Yes |
| Local relaxed auth mode | N/A | Yes | Yes | Yes |
| Strict cloud-grade auth/IAM | No | Partial | No | Partial |

### Mail

| Feature | Status | Notes |
| --- | --- | --- |
| SMTP receive | Yes | Supports local inbound SMTP for development. |
| `HELO` / `EHLO`, `MAIL FROM`, `RCPT TO`, `DATA`, `RSET`, `NOOP`, `QUIT` | Yes | Core SMTP smoke path. |
| Message parsing | Yes | Parses headers, text body, HTML body, and attachments. |
| Raw RFC 5322 source | Yes | Available through the dashboard API. |
| Dashboard inbox | Yes | Inspect messages and raw source. |
| Delete messages | Yes | Single-message and clear-all paths are available through dashboard API. |
| Outbound relay | No | devcloud is an inbox emulator, not an SMTP relay. |
| SMTP AUTH | No | Default `auth.smtp.mode` is `off`. |
| TLS / STARTTLS | No | Local plaintext development endpoint only. |
| IMAP / POP3 | No | Not implemented. |

### S3-Compatible API

| Feature | Status | Notes |
| --- | --- | --- |
| Path-style bucket/object routes | Yes | Default route model. |
| Virtual-host style routes | No | Config field exists, but runtime routing is path-style. |
| List buckets | Yes | `GET /`. |
| Create, head, list, delete bucket | Yes | Empty-bucket delete is supported. |
| Get bucket location | Yes | Returns configured region. |
| Put, head, get, delete object | Yes | Includes metadata and content headers. |
| Range GET | Yes | Supports byte ranges. |
| ListObjectsV2 | Yes | Prefix listing is covered. |
| CopyObject | Yes | Supports copy and metadata replacement. |
| Content-MD5 validation | Yes | Invalid and mismatched digests return S3-style errors. |
| Multipart upload | Yes | Create/upload/list/complete/abort local multipart flows. |
| Presigned URL validation | Yes | Covered for local SigV4 GET. |
| AWS SigV4 header auth | Partial | Relaxed mode is default; strict mode validates local credentials. |
| ACLs, bucket policy, IAM | No | Not implemented. |
| Versioning, lifecycle, replication | No | Not implemented. |
| SSE/KMS, Object Lock, notifications | No | Not implemented. |
| S3 Select / inventory / analytics | No | Not implemented. |

### GCS-Compatible JSON API

| Feature | Status | Notes |
| --- | --- | --- |
| JSON API bucket routes | Yes | `/storage/v1/b...`. |
| Create, get, list, delete bucket | Yes | Empty-bucket delete is supported. |
| Object media upload | Yes | `uploadType=media`. |
| Object multipart upload | Yes | `uploadType=multipart`. |
| Object resumable upload | Yes | Session start and final upload are supported. |
| Object metadata get/list/patch/delete | Yes | Includes generation and metageneration fields. |
| Object media download | Yes | `/download/storage/v1/...` and `alt=media`. |
| Range download | Yes | Supports byte ranges. |
| Copy, rewrite, compose | Yes | Local object-copy workflows. |
| Preconditions | Yes | Generation/metageneration mismatch returns `412`. |
| Pagination and prefix filters | Partial | Useful local subset for object listing. |
| OAuth bearer validation | Partial | Local relaxed modes only; no real Google IAM validation. |
| XML API | No | JSON API subset only. |
| IAM/ACLs, retention, lifecycle | No | Not implemented. |
| Pub/Sub notifications, signed URLs | No | Not implemented. |

### DynamoDB-Compatible API

| Feature | Status | Notes |
| --- | --- | --- |
| AWS JSON 1.0 endpoint | Yes | Uses `X-Amz-Target: DynamoDB_20120810.*`. |
| List/Create/Describe/Update/DeleteTable | Yes | Tables become `ACTIVE` immediately. |
| AttributeValue shapes | Yes | Supports string, number, binary, bool, null, map, list, and sets. |
| PutItem/GetItem/UpdateItem/DeleteItem | Yes | Includes condition expression and return value subsets. |
| Query and Scan | Yes | Supports key conditions, pagination, filters, and projections for local use. |
| Global secondary indexes | Partial | Queryable local index state for supported projection paths. |
| Local secondary indexes | Partial | Metadata is accepted; behavior is not full DynamoDB parity. |
| BatchGetItem / BatchWriteItem | Yes | Local batch subset. |
| TransactGetItems / TransactWriteItems | Yes | Local transaction subset. |
| PartiQL ExecuteStatement / BatchExecuteStatement / ExecuteTransaction | Partial | Supported statement subset; unsupported PartiQL is rejected. |
| TTL | Yes | TTL metadata and expiration are supported locally. |
| Streams | Partial | Stream metadata, shard iterators, and record reads exist for local inspection. |
| Backups and restore | Partial | Local backup metadata and restore flow. |
| Tags and resource policies | Partial | Local metadata only; no IAM enforcement. |
| DescribeLimits / DescribeEndpoints | Yes | Local compatibility responses. |
| AWS SigV4 header auth | Partial | Relaxed mode is default; strict mode validates local credentials. |
| DAX, global tables, autoscaling | No | Not implemented. |
| Real capacity accounting/throttling | No | Local deterministic behavior, not AWS capacity simulation. |
| IAM condition enforcement | No | Not implemented. |

### Dashboard API

| Feature | Status | Notes |
| --- | --- | --- |
| Service registry | Yes | `GET /api/dashboard/services`. |
| Mail messages API | Yes | List, fetch detail/raw, delete. |
| S3 dashboard API | Yes | Bucket/object listing, download links. |
| GCS dashboard API | Yes | Bucket/object/upload-session inspection. |
| DynamoDB dashboard API | Yes | Status, tables, table detail, indexes, TTL, streams, items. |
| Common React dashboard shell | Partial | Shared shell is available under `/dashboard/`; some legacy service pages still exist. |

## Verification

Run all Go tests:

```bash
go test ./...
```

Run acceptance gates:

```bash
VERIFY_STAGE=full bash scripts/mail-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/dynamodb-autoloop/verify.sh
```

Run E2E smoke tests:

```bash
scripts/mail-e2e.sh
scripts/s3-e2e.sh
scripts/gcs-e2e.sh
scripts/dynamodb-e2e.sh
```

Keep a service running after the E2E journey for browser/API inspection:

```bash
E2E_INTERACTIVE=true scripts/mail-e2e.sh
E2E_INTERACTIVE=true scripts/s3-e2e.sh
E2E_INTERACTIVE=true scripts/gcs-e2e.sh
E2E_INTERACTIVE=true E2E_DELETE_DATA=false scripts/dynamodb-e2e.sh
```

Override ports when defaults are already in use:

```bash
E2E_INTERACTIVE=true E2E_SMTP_PORT=1125 E2E_DASHBOARD_PORT=8125 scripts/mail-e2e.sh
E2E_INTERACTIVE=true E2E_S3_PORT=14566 E2E_DASHBOARD_PORT=18025 E2E_SMTP_PORT=11025 scripts/s3-e2e.sh
E2E_INTERACTIVE=true E2E_GCS_PORT=14443 E2E_DASHBOARD_PORT=18025 scripts/gcs-e2e.sh
E2E_INTERACTIVE=true E2E_DYNAMODB_PORT=18000 E2E_DASHBOARD_PORT=18025 scripts/dynamodb-e2e.sh
```

## Project Structure

| Path | Purpose |
| --- | --- |
| `cmd/devcloud` | Main CLI entry point. |
| `cmd/devcloudd` | Daemon entry point wiring. |
| `internal/app` | Config loading, workspace initialization, and daemon orchestration. |
| `internal/dashboard` | Local Web UI, React assets, and dashboard APIs. |
| `internal/services/mail` | SMTP inbox service. |
| `internal/services/s3` | S3-compatible HTTP service and filesystem-backed object store. |
| `internal/services/gcs` | GCS JSON API-compatible HTTP service. |
| `internal/services/dynamodb` | DynamoDB-compatible JSON API service. |
| `docs/` | Product and compatibility designs. |
| `mock/` | UI design mocks. |
| `scripts/*-autoloop/` | Bounded implementation-loop and verification scripts. |
| `scripts/*-e2e.sh` | End-to-end smoke tests. |

## Notes

- devcloud is a local emulator. It intentionally does not implement cloud IAM, billing, availability, or production security guarantees.
- Runtime data under `.devcloud/` should not be committed.
- Default development credentials are `dev/dev` for local S3 and DynamoDB strict-mode smoke tests.
- Compatibility targets are driven by the scripts and design docs in `docs/`; unsupported provider APIs should be added deliberately with tests.
