# devcloud

Local cloud service emulator for development and E2E inspection.

`devcloud` runs a local dashboard plus compatible development endpoints for Mail, S3, GCS, DynamoDB, BigQuery, SQS, Google Cloud Pub/Sub, Redshift, and Redis. It is designed for deterministic local tests and manual inspection, not for production workloads or full cloud-provider parity.

## Quick Start

Initialize local configuration and start all enabled services:

```bash
cargo run -p devcloud-orchestrator -- init
cargo run -p devcloud-orchestrator -- up
```

Open the dashboard:

```text
http://127.0.0.1:8025/
http://127.0.0.1:8025/dashboard/
```

Default local endpoints:

| Service | Endpoint | Dashboard |
| --- | --- | --- |
| Mail SMTP | `127.0.0.1:1025` | `http://127.0.0.1:8025/dashboard/mail` |
| S3 | `http://127.0.0.1:4566` | `http://127.0.0.1:8025/dashboard/s3` |
| GCS | `http://127.0.0.1:4443` | `http://127.0.0.1:8025/dashboard/gcs` |
| DynamoDB | `http://127.0.0.1:8000` | `http://127.0.0.1:8025/dashboard/dynamodb` |
| BigQuery | `http://127.0.0.1:9050` | `http://127.0.0.1:8025/dashboard/bigquery` |
| SQS | `http://127.0.0.1:9324` | `http://127.0.0.1:8025/dashboard/sqs` |
| Pub/Sub gRPC | `127.0.0.1:8085` | `http://127.0.0.1:8025/dashboard/pubsub` |
| Pub/Sub REST | `http://127.0.0.1:8086` | `http://127.0.0.1:8025/dashboard/pubsub` |
| Redshift SQL | `127.0.0.1:5439` | `http://127.0.0.1:8025/dashboard/redshift` |
| Redshift API | `http://127.0.0.1:9099` | `http://127.0.0.1:8025/dashboard/redshift` |
| Redis | `redis://127.0.0.1:6379` | `http://127.0.0.1:8025/dashboard/redis` |

Useful commands:

```bash
cargo run -p devcloud-orchestrator -- help
cargo run -p devcloud-orchestrator -- init
cargo run -p devcloud-orchestrator -- up
cargo run -p devcloud-orchestrator -- dashboard
cargo run -p devcloud-orchestrator -- reset
```

## BigQuery Dashboard Management

`/dashboard/bigquery` includes a compact local management console for BigQuery development workflows. It keeps the existing catalog browser for projects, datasets, tables, rows, schemas, and jobs, and adds a SQL query runner with `useLegacySql=false`, dry run, max results, result table, selected result JSON, and job reference.

The dashboard can create local datasets and tables and insert local table rows through guarded `datasets.insert`, `tables.insert`, and `tabledata.insertAll` flows. Guided forms cover common fields, raw JSON mode is available for request-shape testing, and the row editor validates JSON before calling insertAll while showing partial insert errors.

Dashboard API clients can also start local BigQuery `jobs.insert` workflows through `/api/bigquery/projects/{projectId}/jobs`, including GCS-backed load/import and extract/export jobs that use devcloud GCS `gs://` URIs.

Safety boundaries: dashboard mutations go through `/api/bigquery/*` or the local BigQuery REST API path, never direct storage calls. The UI does not persist or log row payloads, credentials, Authorization headers, bearer tokens, or full request bodies. When BigQuery is disabled, query and mutation controls remain unavailable.

## Dashboard Frontend

The dashboard frontend source lives in `web/dashboard/`. The Vite build output
is written to `services/dashboard/assets/react`, which the Rust dashboard
crate embeds at compile time.

```bash
cd web/dashboard
npm install
npm run build
```

Do not edit files under `services/dashboard/assets/react` by hand; rebuild
the React app instead.

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
  redisPort: 6379

auth:
  smtp:
    mode: relaxed
    user: dev
    password: dev
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
  redis:
    mode: relaxed
    password: ""

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
  redis:
    enabled: true
    mode: managed
    binaryPath: ""
    externalUrl: ""
    dataDir: redis
    maxMemoryMB: 256
    appendOnly: false
```

## Support Matrix

Legend:

| Value | Meaning |
| --- | --- |
| Yes | Implemented and covered by tests or E2E smoke checks. |
| Partial | Useful local subset exists, but behavior is not complete provider parity. |
| No | Not implemented. Requests may fail, be ignored, or return a compatibility error. |

### Service Availability

| Capability | Mail | S3 | GCS | DynamoDB | BigQuery | SQS | Pub/Sub | Redshift | Redis |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Local endpoint | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Dashboard view | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Dashboard mutation actions | Partial | Partial | Partial | No | No | Partial | Yes | Yes | Partial |
| Persistent local storage | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Configurable port | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Enable/disable via config | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Local relaxed auth mode | N/A | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Strict cloud-grade auth/IAM | No | Partial | No | Partial | No | Partial | No | Partial | Partial |

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
| Virtual-host style routes | Partial | `{bucket}.localhost` Host-header requests route to the same local bucket handlers; path-style remains the default. |
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
| ACLs, bucket policy, IAM | Partial | Bucket policy and bucket/object ACL metadata endpoints are supported locally, including versionId-aware object ACL metadata; IAM enforcement is not implemented. |
| Versioning | Partial | Bucket versioning, generated and `null` version IDs, version-aware get/delete/copy-source, delete markers, multipart-complete version IDs/ETags, and local ListObjectVersions with key/version markers are supported. |
| Lifecycle | Partial | Bucket lifecycle metadata endpoints are supported; Enabled expiration rules for current objects are applied locally on S3 reads/lists. |
| Notifications | Partial | Bucket notification configuration metadata, including EventBridge metadata, is supported; matching local object create/delete flows append local event records. |
| SSE/KMS | Partial | SSE-S3 and SSE-KMS request metadata is stored locally and exposed through object read/write response headers; real KMS and SSE-C are not implemented. |
| Replication | Partial | Bucket replication configuration metadata is supported; enabled prefix rules replicate local object write/copy/multipart-complete flows and enabled delete marker replication to existing local destination buckets. |
| Object Lock | Partial | Bucket object lock configuration, object retention/legal-hold metadata, response headers, local delete guards, and governance retention bypass are supported. |
| S3 Select | Partial | Minimal SelectObjectContent supports `SELECT * FROM S3Object` for CSV and JSON Lines with eventstream responses; filtering and projections are not implemented. |
| Inventory / analytics | Partial | Bucket inventory and analytics configuration metadata endpoints are supported locally; CSV-format enabled inventory configs generate deterministic local reports under bucket storage. |

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

### BigQuery-Compatible REST API

| Feature | Status | Notes |
| --- | --- | --- |
| REST v2 project/dataset/table routes | Yes | Local catalog resources persist under `.devcloud/data/bigquery`. |
| Table schema and row APIs | Yes | `tables.insert`, `tables.get/list/patch/delete`, `tabledata.insertAll`, and `tabledata.list`. |
| Partitioning and clustering metadata | Yes | Time/range partitioning and clustering fields round-trip in table metadata. |
| View metadata and query execution | Partial | View table resources persist query metadata and can be queried through the local SELECT subset. |
| Routine metadata | Partial | `routines.insert/list/get/patch/update/delete` persist local UDF/procedure metadata; routines are not executable. |
| Jobs API | Yes | Query, query destination tables, load, copy, extract, get, list, cancel, and result workflows are covered locally. |
| GoogleSQL query execution | Partial | Deterministic local subset for common `SELECT` workflows; unsupported syntax fails closed. |
| Google Cloud BigQuery client libraries | Partial | Local dataset/table/row/query compatibility workflows are covered through Rust E2E gates with endpoint override. |
| IAM policy endpoints | Partial | Local policy shape is supported; no real Google IAM enforcement. |
| BigQuery Storage API, BI Engine, ML | No | Not implemented. |

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

### Redis-Compatible Endpoint

| Feature | Status | Notes |
| --- | --- | --- |
| Redis wire endpoint | Yes | Uses a real `redis-server` child process in managed mode; devcloud does not re-implement RESP. |
| External Redis mode | Yes | `services.redis.mode: external` validates `externalUrl` with `PING` and exposes the same dashboard surface. |
| Managed persistence | Yes | Runtime data is kept under `.devcloud/data/redis` by default. |
| String/hash/list/set/zset inspection | Partial | `/dashboard/redis` lists keys with SCAN and shows type-specific previews. |
| Command runner | Partial | Dashboard commands are restricted to the Redis allowlist in `docs/design-redis-compat.md`. |
| Destructive actions | Partial | Key delete, expire, and guarded `FLUSHDB` are supported; `FLUSHALL` and unsafe commands are rejected. |
| Redis AUTH | Partial | Relaxed mode does not set `requirepass`; strict mode passes the configured password to managed Redis. |
| Cluster, Sentinel, modules, RedisJSON, RediSearch | No | Out of local compatibility scope. |

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
| Redis dashboard API | Yes | Status, SCAN-based keys, key inspector, allowlisted command runner, delete, expire, and guarded `FLUSHDB`. |
| Common React dashboard shell | Yes | All service pages are served under `/dashboard/<svc>` from the shared React shell; compatibility `/mail`, `/s3`, `/gcs`, `/dynamodb`, `/bigquery` paths return 301 redirects. |

## Verification

### Local build and test

```bash
cargo build --workspace
cargo test --workspace
```

Service acceptance gates and E2E scripts are run locally because they require service-specific external tooling (`awscli-local`, `postgres` server binaries, `aws` CLI, etc.).

### Manual acceptance gates (per service)

Run before claiming a service MVP is complete or when investigating a service-level regression:

| Stage | Command |
| --- | --- |
| Mail MVP | `VERIFY_STAGE=full bash scripts/mail-autoloop/verify.sh` |
| S3 MVP | `VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh` |
| GCS MVP | `VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh` |
| DynamoDB MVP | `VERIFY_STAGE=full bash scripts/dynamodb-autoloop/verify.sh` |
| BigQuery MVP | `VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh` |
| SQS MVP | `VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh` |
| Pub/Sub MVP | `VERIFY_STAGE=full bash scripts/pubsub-autoloop/verify.sh` |
| Redis MVP | `VERIFY_STAGE=full bash scripts/redis-autoloop/verify.sh` |
| GCS SDK compat | `VERIFY_STAGE=full-sdk-compat bash scripts/gcs-sdk-compat-autoloop/verify.sh` |
| BigQuery SDK compat | `VERIFY_STAGE=full-sdk-compat bash scripts/bigquery-sdk-compat-autoloop/verify.sh` |
| Pub/Sub full compat | `VERIFY_STAGE=full-compat bash scripts/pubsub-full-compat-autoloop/verify.sh` |
| Redshift advanced compat | `VERIFY_STAGE=full-advanced bash scripts/redshift-advanced-compat-autoloop/verify.sh` |

### Manual E2E smoke

| Service | Script | Extra tool requirement |
| --- | --- | --- |
| Mail | `scripts/mail-e2e.sh` | none |
| S3 | `scripts/s3-e2e.sh` | `awscli-local` (`pipx install awscli-local`) |
| GCS | `scripts/gcs-e2e.sh` | none |
| DynamoDB | `scripts/dynamodb-e2e.sh` | `aws` CLI |
| BigQuery | `scripts/bigquery-e2e.sh` | none |
| SQS | `scripts/sqs-e2e.sh` | none |
| Pub/Sub | `scripts/pubsub-e2e.sh` | none |
| Redshift | `scripts/redshift-e2e.sh` | `psql`, `aws`, and `postgres` server binary on `PATH` for managed mode |
| Redis | `scripts/redis-e2e.sh` | `redis-cli` |

Useful env vars:

- `E2E_INTERACTIVE=true` keeps the daemon running after the journey for browser/API inspection.
- `E2E_DELETE_DATA=false` preserves stored data for inspection (DynamoDB, BigQuery, SQS).
- `E2E_<SVC>_PORT` and `E2E_DASHBOARD_PORT` override defaults when the standard ports are busy. Example: `E2E_INTERACTIVE=true E2E_S3_PORT=14566 E2E_DASHBOARD_PORT=18025 scripts/s3-e2e.sh`.

## Project Structure

| Path | Purpose |
| --- | --- |
| `orchestrator` | CLI, config loading, workspace initialization, and service supervisor. |
| `services/mail` | SMTP inbox service. |
| `services/s3` | S3-compatible HTTP service and filesystem-backed object store. |
| `services/gcs` | GCS JSON API-compatible HTTP service. |
| `services/dynamodb` | DynamoDB-compatible JSON API service. |
| `services/bigquery` | BigQuery-compatible REST API service. |
| `services/redis` | Redis-compatible managed/external service wrapper. |
| `services/sqs` | SQS-compatible JSON and Query API service. |
| `services/pubsub` | Google Cloud Pub/Sub-compatible REST and gRPC service. |
| `services/redshift` | Redshift SQL, Data API, and management API service. |
| `services/dashboard` | Local Web UI, embedded React assets, and dashboard APIs. |
| `docs/` | Product and compatibility designs. |
| `mock/` | UI design mocks. |
| `scripts/*-autoloop/` | Bounded implementation-loop and verification scripts. |
| `scripts/*-e2e.sh` | End-to-end smoke tests. |

## Notes

- devcloud is a local emulator. It intentionally does not implement cloud IAM, billing, availability, or production security guarantees.
- Runtime data under `.devcloud/` should not be committed.
- Default development credentials are `dev/dev` for local S3 and DynamoDB strict-mode smoke tests.
- Compatibility targets are driven by the scripts and design docs in `docs/`; unsupported provider APIs should be added deliberately with tests.
