# devcloud

Local cloud service emulator for development and E2E inspection.

`devcloud` runs a local dashboard plus compatible development endpoints for Mail, S3, GCS, DynamoDB, BigQuery, SQS, Google Cloud Pub/Sub, and Redshift. It is designed for deterministic local tests and manual inspection, not for production workloads or full cloud-provider parity.

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
| BigQuery | `http://127.0.0.1:9050` | `http://127.0.0.1:8025/dashboard/bigquery` |
| SQS | `http://127.0.0.1:9324` | `http://127.0.0.1:8025/dashboard/sqs` |
| Pub/Sub gRPC | `127.0.0.1:8085` | `http://127.0.0.1:8025/dashboard/pubsub` |
| Pub/Sub REST | `http://127.0.0.1:8086` | `http://127.0.0.1:8025/dashboard/pubsub` |
| Redshift SQL | `127.0.0.1:5439` | `http://127.0.0.1:8025/dashboard/redshift` |
| Redshift API | `http://127.0.0.1:9099` | `http://127.0.0.1:8025/dashboard/redshift` |

Useful commands:

```bash
go run ./cmd/devcloud help
go run ./cmd/devcloud init
go run ./cmd/devcloud up
go run ./cmd/devcloud dashboard
go run ./cmd/devcloud reset
```

## BigQuery Dashboard Management

`/dashboard/bigquery` includes a compact local management console for BigQuery development workflows. It keeps the existing catalog browser for projects, datasets, tables, rows, schemas, and jobs, and adds a SQL query runner with `useLegacySql=false`, dry run, max results, result table, selected result JSON, and job reference.

The dashboard can create local datasets and tables and insert local table rows through guarded `datasets.insert`, `tables.insert`, and `tabledata.insertAll` flows. Guided forms cover common fields, raw JSON mode is available for request-shape testing, and the row editor validates JSON before calling insertAll while showing partial insert errors.

Safety boundaries: dashboard mutations go through `/api/bigquery/*` or the local BigQuery REST API path, never direct storage calls. The UI does not persist or log row payloads, credentials, Authorization headers, bearer tokens, or full request bodies. When BigQuery is disabled, query and mutation controls remain unavailable.

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
  bigqueryPort: 9050
  sqsPort: 9324
  pubsubGrpcPort: 8085
  pubsubRestPort: 8086
  redshiftPort: 5439
  redshiftAPIPort: 9099

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
  bigquery:
    mode: relaxed
    project: devcloud
    bearerToken: dev
  sqs:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"
  pubsub:
    mode: relaxed
    projectID: devcloud
    bearerToken: dev
  redshift:
    mode: relaxed
    user: dev
    password: dev
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"

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
  bigquery:
    enabled: true
    project: devcloud
    location: US
    maxRowsPerTable: 1000000
    maxRequestBytes: 10485760
    query:
      maxResultRows: 10000
      maxExecutionSeconds: 30
      defaultUseLegacySql: false
  sqs:
    enabled: true
    region: us-east-1
    queueUrlHost: 127.0.0.1
    maxQueues: 256
    maxMessageBytes: 1048576
    maxReceiveBatchSize: 10
    defaultVisibilityTimeoutSeconds: 30
    defaultDelaySeconds: 0
    defaultMessageRetentionSeconds: 345600
    defaultReceiveWaitTimeSeconds: 0
    schedulerIntervalSeconds: 1
  pubsub:
    enabled: true
    project: devcloud
    dataDir: .devcloud/data/pubsub
    messageDataDir: .devcloud/data/message
    defaultAckDeadlineSeconds: 10
    messageRetentionSeconds: 604800
    maxAckDeadlineSeconds: 600
    maxPullMessages: 1000
    pullWaitTimeoutSeconds: 1
    enableREST: true
    enableStreamingPull: true
    enablePush: false
  redshift:
    enabled: true
    region: us-east-1
    clusterIdentifier: devcloud
    database: dev
    dataDir: redshift
    nodeType: dc2.large
    numberOfNodes: 1
    maxStatementBytes: 16777216
    backend:
      kind: postgres
      mode: managed
      managed: true
      externalDsn: ""
    dataAPI:
      enabled: true
      maxResultBytes: 524288000
      maxResultRows: 10000
      statementRetentionSeconds: 86400
      sessionRetentionSeconds: 86400
    sql:
      enableExtendedProtocol: false
      maxResultRows: 10000
      defaultSearchPath: public
    copyUnload:
      enableLocalS3: true
      maxInputRowBytes: 4194304
```

## Support Matrix

Legend:

| Value | Meaning |
| --- | --- |
| Yes | Implemented and covered by tests or E2E smoke checks. |
| Partial | Useful local subset exists, but behavior is not complete provider parity. |
| No | Not implemented. Requests may fail, be ignored, or return a compatibility error. |

### Service Availability

| Capability | Mail | S3 | GCS | DynamoDB | BigQuery | SQS | Pub/Sub | Redshift |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Local endpoint | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Dashboard view | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Dashboard mutation actions | Partial | Partial | Partial | No | No | Partial | Yes | Yes |
| Persistent local storage | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Configurable port | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Enable/disable via config | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Local relaxed auth mode | N/A | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Strict cloud-grade auth/IAM | No | Partial | No | Partial | No | Partial | No | Partial |

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

DynamoDB dashboard management is available under `/dashboard/dynamodb` and the local `/api/dynamodb/*` dashboard API. The dashboard can inspect tables, items, indexes, TTL, and streams, and can run guarded local management flows for `CreateTable`, `PutItem`, `UpdateItem`, `UpdateTimeToLive`, `DeleteItem`, `DeleteTable`, `Query`, and `Scan`. Query and Scan expose result pagination with count, scanned count, next/previous controls, and selected result item JSON. Recent operation history is stored only in browser `localStorage` as metadata; it does not persist item payloads, credentials, full request payloads, or pagination keys. Dashboard mutation endpoints forward through the local DynamoDB JSON protocol path instead of editing storage directly. Destructive `DeleteItem` and `DeleteTable` flows require confirmation text matching the selected table name, and disabled DynamoDB services do not expose active mutation controls.

### Google Cloud Pub/Sub-Compatible API

| Feature | Status | Notes |
| --- | --- | --- |
| gRPC emulator endpoint | Yes | Implements local `google.pubsub.v1.Publisher`, `Subscriber`, and `SchemaService` surfaces. |
| REST v1 endpoint | Yes | Supports topic, subscription, publish, pull, ack, seek, schema, and IAM-compatible local workflows. |
| Google Cloud Pub/Sub client libraries | Yes | Use `PUBSUB_EMULATOR_HOST=127.0.0.1:8085` and `PUBSUB_PROJECT_ID=devcloud`. |
| Topic create/get/list/update/delete | Yes | Includes labels, retention, schema settings, and KMS metadata where applicable. |
| Subscription create/get/list/update/delete | Yes | Includes ack deadline, retain acked messages, filters, retry policy, dead-letter policy, push config, and ordering flags. |
| Publish / Pull / Acknowledge / ModifyAckDeadline | Yes | Local lease and redelivery behavior is covered by unit and E2E tests. |
| StreamingPull | Yes | Supports flow control, ack/modack/nack, cancellation, and ordering-key gates. |
| Ordering keys | Yes | Local ordered delivery gate is implemented for pull and streaming pull flows. |
| Snapshots and Seek | Yes | Snapshot CRUD and seek-to-time/snapshot flows are implemented. |
| Schemas | Yes | Schema CRUD, revisions, rollback/delete revision, and validate message are implemented locally. |
| Push subscriptions | Partial | Local HTTP push worker, retry policy, and OIDC/no-wrapper metadata are supported when push is enabled. |
| IAM endpoints | Partial | Local policy shape is supported; no real Google IAM enforcement. |
| Exactly-once delivery | Partial | Metadata is accepted; no real cloud-grade exactly-once guarantee. |
| Cloud Monitoring, quotas, billing | No | Not implemented. |
| Production durability / HA | No | Filesystem-backed local emulator only. |

Pub/Sub dashboard actions are available under `/dashboard/pubsub`:

- create topics
- create subscriptions
- publish a message to the selected topic
- pull messages from the selected subscription
- acknowledge pulled messages

### Redshift-Compatible API

| Feature | Status | Notes |
| --- | --- | --- |
| PostgreSQL wire endpoint | Yes | Listens on `127.0.0.1:5439` by default for local Redshift-style SQL clients. |
| PostgreSQL simple query protocol | Yes | Covers `psql` smoke workflows. |
| PostgreSQL extended query protocol | Partial | Supports Parse, Bind, Describe, Execute, Sync, Close, text bind parameters, portal resume, and safe unsupported errors for binary formats. |
| PostgreSQL execution backend | Yes | Default backend is managed local PostgreSQL; explicit memory fallback remains available for development continuity. |
| Redshift SQL translation | Partial | Handles local subsets for DDL/DML, Redshift table attributes, CTAS/views/materialized-view metadata, function rewrites, COPY, and UNLOAD. |
| COPY from local S3 | Yes | Local S3 side effect imports into the PostgreSQL-backed table path. |
| UNLOAD to local S3 | Yes | Query results can be exported through local S3 side effects. |
| Redshift Data API | Yes | Execute/describe/get-result/list workflows are covered by local gates. |
| Redshift management API | Partial | Cluster, parameter group, tags, credentials, and snapshot metadata workflows are local metadata. |
| Redshift Serverless API | Partial | Namespace and workgroup metadata are local compatibility responses. |
| Snapshot restore | Yes | Restores cluster metadata from local snapshot metadata without copying real AWS data. |
| System catalog / BI introspection | Partial | Provides representative `pg_catalog`, `information_schema`, Redshift system views, and workload metadata for common probes. |
| Dashboard query runner | Yes | Redshift dashboard supports status, catalog, table detail, statement history, and SQL query execution. |
| Real AWS Redshift / IAM / KMS / CloudWatch | No | Not implemented; devcloud does not make real AWS calls. |
| MPP / columnar execution / Spectrum / datashare | No | Out of local compatibility scope. |

### Dashboard API

| Feature | Status | Notes |
| --- | --- | --- |
| Service registry | Yes | `GET /api/dashboard/services`. |
| Mail messages API | Yes | List, fetch detail/raw, delete. |
| S3 dashboard API | Yes | Bucket/object listing, download links. |
| GCS dashboard API | Yes | Bucket/object/upload-session inspection. |
| DynamoDB dashboard API | Yes | Status, tables, table detail, indexes, TTL, streams, items, guarded management operations, Query, and Scan. |
| SQS dashboard API | Yes | Status, queues, messages, leases, DLQ, and purge. |
| Pub/Sub dashboard API | Yes | Status, topics, subscriptions, publish, pull, ack, and message metadata. |
| Redshift dashboard API | Yes | Status, clusters, catalog, table detail, query runner, and statement history. |
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
VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/pubsub-autoloop/verify.sh
VERIFY_STAGE=full-compat bash scripts/pubsub-full-compat-autoloop/verify.sh
VERIFY_STAGE=full-advanced bash scripts/redshift-advanced-compat-autoloop/verify.sh
```

Run E2E smoke tests:

```bash
scripts/mail-e2e.sh
scripts/s3-e2e.sh
scripts/gcs-e2e.sh
scripts/dynamodb-e2e.sh
scripts/bigquery-e2e.sh
scripts/sqs-e2e.sh
scripts/pubsub-e2e.sh
scripts/redshift-e2e.sh
```

Keep a service running after the E2E journey for browser/API inspection:

```bash
E2E_INTERACTIVE=true scripts/mail-e2e.sh
E2E_INTERACTIVE=true scripts/s3-e2e.sh
E2E_INTERACTIVE=true scripts/gcs-e2e.sh
E2E_INTERACTIVE=true E2E_DELETE_DATA=false scripts/dynamodb-e2e.sh
E2E_INTERACTIVE=true E2E_DELETE_DATA=false scripts/bigquery-e2e.sh
E2E_INTERACTIVE=true E2E_DELETE_DATA=false scripts/sqs-e2e.sh
E2E_INTERACTIVE=true scripts/pubsub-e2e.sh
```

Override ports when defaults are already in use:

```bash
E2E_INTERACTIVE=true E2E_SMTP_PORT=1125 E2E_DASHBOARD_PORT=8125 scripts/mail-e2e.sh
E2E_INTERACTIVE=true E2E_S3_PORT=14566 E2E_DASHBOARD_PORT=18025 E2E_SMTP_PORT=11025 scripts/s3-e2e.sh
E2E_INTERACTIVE=true E2E_GCS_PORT=14443 E2E_DASHBOARD_PORT=18025 scripts/gcs-e2e.sh
E2E_INTERACTIVE=true E2E_DYNAMODB_PORT=18000 E2E_DASHBOARD_PORT=18025 scripts/dynamodb-e2e.sh
E2E_INTERACTIVE=true E2E_BIGQUERY_PORT=19050 E2E_DASHBOARD_PORT=18025 scripts/bigquery-e2e.sh
E2E_INTERACTIVE=true E2E_SQS_PORT=19324 E2E_DASHBOARD_PORT=18025 scripts/sqs-e2e.sh
PUBSUB_GRPC_PORT=18085 PUBSUB_REST_PORT=18086 DASHBOARD_PORT=18025 E2E_INTERACTIVE=true scripts/pubsub-e2e.sh
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
| `internal/services/bigquery` | BigQuery-compatible REST API service. |
| `internal/services/sqs` | SQS-compatible JSON and Query API service. |
| `internal/services/pubsub` | Google Cloud Pub/Sub-compatible REST and gRPC service. |
| `docs/` | Product and compatibility designs. |
| `mock/` | UI design mocks. |
| `scripts/*-autoloop/` | Bounded implementation-loop and verification scripts. |
| `scripts/*-e2e.sh` | End-to-end smoke tests. |

## Notes

- devcloud is a local emulator. It intentionally does not implement cloud IAM, billing, availability, or production security guarantees.
- Runtime data under `.devcloud/` should not be committed.
- Default development credentials are `dev/dev` for local S3 and DynamoDB strict-mode smoke tests.
- Compatibility targets are driven by the scripts and design docs in `docs/`; unsupported provider APIs should be added deliberately with tests.
