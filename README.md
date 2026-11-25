# devcloud

Local cloud service emulator for development and E2E inspection.

`devcloud` runs a local dashboard plus compatible development endpoints for Mail, S3, GCS, DynamoDB, BigQuery, SQS, Google Cloud Pub/Sub, Redshift, Redis, Application Auto Scaling, SSM Parameter Store, STS, AWS Lambda, and Cloud Run. It is designed for deterministic local tests and manual inspection, not for production workloads or full cloud-provider parity.

## Quick Start

Initialize local configuration and start all enabled services:

```bash
cargo run -p devcloud-orchestrator -- init
cargo run -p devcloud-orchestrator -- up
```

Open the dashboard:

```text
http://127.0.0.1:18025/
http://127.0.0.1:18025/dashboard/
```

Default local endpoints:

| Service | Endpoint | Dashboard |
| --- | --- | --- |
| Mail SMTP | `127.0.0.1:11025` | `http://127.0.0.1:18025/dashboard/mail` |
| Mail IMAP | `127.0.0.1:11143` | `http://127.0.0.1:18025/dashboard/mail` |
| Mail POP3 | `127.0.0.1:11110` | `http://127.0.0.1:18025/dashboard/mail` |
| S3 | `http://127.0.0.1:14566` | `http://127.0.0.1:18025/dashboard/s3` |
| GCS | `http://127.0.0.1:14443` | `http://127.0.0.1:18025/dashboard/gcs` |
| DynamoDB | `http://127.0.0.1:18000` | `http://127.0.0.1:18025/dashboard/dynamodb` |
| BigQuery | `http://127.0.0.1:19050` (REST), `127.0.0.1:19051` (Storage gRPC) | `http://127.0.0.1:18025/dashboard/bigquery` |
| SQS | `http://127.0.0.1:19324` | `http://127.0.0.1:18025/dashboard/sqs` |
| Pub/Sub gRPC | `127.0.0.1:18085` | `http://127.0.0.1:18025/dashboard/pubsub` |
| Pub/Sub REST | `http://127.0.0.1:18086` | `http://127.0.0.1:18025/dashboard/pubsub` |
| Redshift SQL | `127.0.0.1:15439` | `http://127.0.0.1:18025/dashboard/redshift` |
| Redshift API | `http://127.0.0.1:19099` | `http://127.0.0.1:18025/dashboard/redshift` |
| Redis | `redis://127.0.0.1:16379` | `http://127.0.0.1:18025/dashboard/redis` |
| Application Auto Scaling | `http://127.0.0.1:18030` | `http://127.0.0.1:18025/dashboard/applicationautoscaling` |
| SSM Parameter Store | `http://127.0.0.1:18031` | `http://127.0.0.1:18025/dashboard/ssm` |
| STS | `http://127.0.0.1:18032` | `http://127.0.0.1:18025/dashboard/sts` |
| Lambda | `http://127.0.0.1:19010` | `http://127.0.0.1:18025/dashboard/lambda` |
| Cloud Run | `http://127.0.0.1:18095` (services: `http://<svc>.<region>.<project>.run.localhost:18095`) | `http://127.0.0.1:18025/dashboard/cloudrun` |

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
  smtpPort: 11025
  imapPort: 11143
  pop3Port: 11110
  mailHttpPort: 11080
  dashboardPort: 18025
  eventRelayPort: 18027
  s3Port: 14566
  gcsPort: 14443
  dynamodbPort: 18000
  bigqueryPort: 19050
  bigqueryStoragePort: 19051
  sqsPort: 19324
  pubsubGrpcPort: 18085
  pubsubRestPort: 18086
  redshiftPort: 15439
  redshiftAPIPort: 19099
  redisPort: 16379
  redisHttpPort: 16380
  appAutoScalingPort: 18030
  ssmPort: 18031
  stsPort: 18032
  lambdaPort: 19010
  cloudRunPort: 18095

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
  appAutoScaling:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"
  ssm:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"
  sts:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"
  lambda:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"
  cloudRun:
    mode: relaxed
    bearerToken: dev

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
  appAutoScaling:
    enabled: true
    region: us-east-1
  ssm:
    enabled: true
    region: us-east-1
  sts:
    enabled: true
    region: us-east-1
  lambda:
    enabled: true
    region: us-east-1
  cloudRun:
    enabled: true
    project: devcloud
    region: us-central1
    docker: false
```

## Support Matrix

Legend:

| Value | Meaning |
| --- | --- |
| Yes | Implemented and covered by tests or E2E smoke checks. |
| Partial | Useful local subset exists, but behavior is not complete provider parity. |
| No | Not implemented. Requests may fail, be ignored, or return a compatibility error. |

### Service Availability

| Capability | Mail | S3 | GCS | DynamoDB | BigQuery | SQS | Pub/Sub | Redshift | Redis | App Auto Scaling | Lambda | Cloud Run |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Local endpoint | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Dashboard view | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Dashboard mutation actions | Partial | Partial | Partial | Partial | Partial | Partial | Yes | Yes | Partial | Partial | Partial | Partial |
| Persistent local storage | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Configurable port | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Enable/disable via config | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Local relaxed auth mode | N/A | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Strict cloud-grade auth/IAM | Partial | Partial | Partial | Partial | Partial | Partial | Partial | Partial | Partial | Partial | Partial | Partial |

### Mail

| Feature | Status | Notes |
| --- | --- | --- |
| SMTP receive | Yes | Supports local inbound SMTP for development. A line may be up to 1 MiB, a message may have up to 1000 recipients, and each line must arrive within 5 minutes. Once a client has sent CRLF, only `<CRLF>.<CRLF>` ends DATA (a bare-LF `.` cannot smuggle a second message in); clients that only send bare LF may end with `.<LF>`. |
| `HELO` / `EHLO`, `MAIL FROM`, `RCPT TO`, `DATA`, `RSET`, `NOOP`, `QUIT` | Yes | Core SMTP smoke path. |
| Message parsing | Yes | Parses headers, text body, HTML body, and attachments. |
| Raw RFC 5322 source | Yes | Available through the dashboard API. |
| Dashboard inbox | Yes | Inspect messages and raw source. |
| Delete messages | Yes | Single-message and clear-all paths are available through dashboard API. |
| Outbound relay | No | devcloud is an inbox emulator, not an SMTP relay. |
| SMTP AUTH | Partial | `PLAIN` and `LOGIN` are available when `auth.smtp.mode` is not `off`; strict mode validates supplied credentials and answers `MAIL FROM` with `530 5.7.0 Authentication required` until the session has authenticated (`off`/`relaxed` still accept `MAIL FROM` without AUTH). The default mode is `off`. Per-user mailboxes and sender authorization are not emulated. |
| HTTP API authentication | Partial | Strict mode protects the introspection/control API with the configured SMTP Basic credentials; the dashboard forwards those credentials for reads and mutations. |
| Browser guards | Yes | Direct introspection/control HTTP endpoints reject public DNS Host names and cross-site mutations, using the Dashboard API host allowlist. SDK/CLI requests without Origin remain supported. |
| TLS / STARTTLS | Yes | Off by default to avoid breaking plaintext clients that auto-upgrade with system CA verification. Set `services.mail.tls.enabled: true` for a locally generated in-memory self-signed certificate (`localhost` / `127.0.0.1` / `::1`), or set `services.mail.tls.certFile` and `services.mail.tls.keyFile` (PEM) for custom CA-signed verification. Enables `STARTTLS` on SMTP/IMAP and `STLS` on POP3 (TLS 1.2/1.3); set `services.mail.tls.require: true` to refuse auth/mail commands before TLS. Optional implicit-TLS ports (`server.smtpsPort`, `server.imapsPort`, `server.pop3sPort`) serve SMTPS/IMAPS/POP3S. Standalone `devcloud-mail` reads `DEVCLOUD_MAIL_TLS_*` and `DEVCLOUD_MAIL_*S_ADDR`. |
| IMAP / POP3 | Partial | Shared mail store with IMAP4rev1 on `127.0.0.1:11143` and POP3 on `127.0.0.1:11110` (`0` disables a port). SMTP delivers to `INBOX`, which is also the POP3 mailbox. IMAP supports `CAPABILITY`, `STARTTLS`, `LOGIN`, `AUTHENTICATE PLAIN`, `CREATE`, `LIST`/`LSUB`, `STATUS`, `SELECT`/`EXAMINE`, `APPEND`, `IDLE`, `FETCH`/`UID FETCH` (including `ENVELOPE`, `BODY[]`/`BODY.PEEK[]` and partial slices), `STORE`/`UID STORE`, `SEARCH`/`UID SEARCH`, `EXPUNGE`, `CLOSE`, `UNSELECT`, `CHECK`, `NOOP`, and `LOGOUT`. Flat folders, UIDs, system flags and keywords persist across restarts. `APPEND` accepts one byte-counted literal (up to 1 MiB by default) with optional flags and internal date. `IDLE` reports arrivals, expunges and flag changes in the selected folder until `DONE` or the connection idle timeout (5 minutes by default). `LSUB` lists all folders. Folder hierarchy, delete/rename, explicit subscriptions and `COPY`/`MOVE` are outside this subset. POP3 supports `CAPA`, `STLS`, `USER`/`PASS`, `AUTH PLAIN`, `STAT`, `LIST`, `UIDL`, `RETR`, `TOP`, `DELE`, `RSET`, `NOOP`, and `QUIT` (commits deletions only on `QUIT`). Both honor `auth.smtp` (`relaxed`/`off` accept any credentials, `strict` validates configured credentials). |

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
| Presigned URL validation | Yes | Strict mode validates SigV4, SigV4a, and SigV2 presigned URLs, including expiry. |
| AWS SigV4 / SigV4a / SigV2 auth | Partial | Relaxed mode is default; strict mode validates SigV4 (including `aws-chunked` chunk and trailer signatures), SigV4a (`AWS4-ECDSA-P256-SHA256` with `x-amz-region-set` / `X-Amz-Region-Set`), and SigV2 (`Authorization: AWS ...` and query-string presigned URLs), with a 15-minute clock skew window. SigV4a `aws-chunked` uploads (`STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD[-TRAILER]`) verify every ECDSA chunk and trailer signature (`*`-padded signatures accepted). In every auth mode, `aws-chunked` trailer checksums (`x-amz-checksum-crc32`, `-crc32c`, `-crc64nvme`, `-sha1`, `-sha256`) are validated against the decoded payload (`BadDigest` on mismatch, `InvalidRequest` when a checksum named in `x-amz-trailer` is missing or malformed). Streaming payloads on presigned URLs still return `NotImplemented`. |
| Conditional writes (`If-None-Match`) | Yes | PutObject, CopyObject, and CompleteMultipartUpload enforce `If-None-Match` for both `*` and specific ETag values/lists atomically. |
| CORS | Yes | PutBucketCors/GetBucketCors/DeleteBucketCors, preflight, and response headers. |
| ACLs, bucket policy, IAM | Partial | Bucket policy and bucket/object ACL metadata endpoints (including versionId-aware object ACLs) are supported. In strict mode, bucket policies (`Allow`/`Deny`, `Principal`/`NotPrincipal`, `Action`/`NotAction`, `Resource`/`NotResource`, common `Condition` operators) and canned ACLs (`public-read`, `public-read-write`) are enforced locally. A local IAM registry (`PUT /_iam/config` on the S3 port) adds users and roles in any 12-digit account with identity policies and role trust policies; `POST /_iam/assume-role` issues temporary credentials with an optional session policy. In strict mode, registry keys are verified with their own secret, sessions need a matching `x-amz-security-token` and expire, and requests are authorized AWS-style: explicit `Deny` wins, owner-account (`000000000000`) principals need an identity or bucket policy `Allow`, other accounts need both, and session policies limit both. Permission boundaries, SCPs, and the real IAM/STS APIs are not emulated. |
| Versioning | Partial | Bucket versioning, generated and `null` version IDs, version-aware get/delete/copy-source, delete markers, multipart-complete version IDs/ETags, and local ListObjectVersions with key/version markers are supported. |
| Lifecycle | Partial | Bucket lifecycle metadata endpoints and local evaluation on reads/lists support `Expiration` (`Days`, `Date`, `ExpiredObjectDeleteMarker`), `Transition` (`Days`, `Date`, `StorageClass`), `NoncurrentVersionExpiration`, `NoncurrentVersionTransition`, and `AbortIncompleteMultipartUpload`. |
| Notifications | Partial | Bucket notification configuration metadata, including EventBridge metadata, is supported; matching local object create/delete flows append local event records. |
| SSE/KMS | Partial | SSE-S3 (`AES256`) and SSE-KMS (`aws:kms`, `aws:kms:dsse`) metadata round-trips locally; SSE-C (`AES256` customer-provided keys) validates keys/MD5 and encrypts objects at rest without persisting the key across Put/Get/Head/Range/Copy/Multipart/Select. SSE-KMS keys come from a local keyring (`/_kms/keys`, `/_kms/aliases` on the S3 port). In strict mode, writes resolve key IDs, key ARNs, and aliases (no key ID uses an auto-created AWS managed `alias/aws/s3` key), store the key ARN, and refuse missing (`KMS.NotFoundException`), disabled (`KMS.DisabledException`), or pending-deletion (`KMS.KMSInvalidStateException`) keys; GET, Select, and CopyObject reads of an object whose key became unusable are refused. Relaxed mode keeps the metadata round-trip: any key ID is stored and returned as given, and reads are never refused. SSE-KMS object bytes are not encrypted at rest (other services read the shared store directly), and the AWS KMS API itself is not emulated. |
| Replication | Partial | Bucket replication configuration metadata is supported; enabled prefix rules replicate local object write/copy/multipart-complete flows and enabled delete marker replication to existing local destination buckets. |
| Object Lock | Partial | Bucket object lock configuration, object retention/legal-hold metadata, response headers, local delete guards, and governance retention bypass are supported. |
| S3 Select | Partial | SelectObjectContent supports `SELECT` projections, aliases, `WHERE` filters (`AND`/`OR`/`NOT`, comparisons, `LIKE`, `IN`, `BETWEEN`, `IS [NOT] NULL`), `LIMIT`, `CAST`, scalar functions, and aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) over CSV (`USE`/`IGNORE`/`NONE`) and JSON (`LINES`/`DOCUMENT`) with CSV or JSON output in eventstream responses. `GROUP BY` (aggregates per group), `ORDER BY` (expressions, output aliases, positions, aggregates; `ASC`/`DESC`), and self-joins of the object (`[INNER] JOIN ... ON`, `CROSS JOIN`, comma joins with table aliases) are evaluated locally. `HAVING`, outer joins, and `UNION` are unimplemented. |
| Inventory / analytics | Partial | Bucket inventory and analytics configuration metadata endpoints are supported locally; CSV-format enabled inventory configs generate deterministic local reports under bucket storage. |

The standalone S3 image also ships a web console and a server-wide CORS default; see [`services/s3/README.md`](services/s3/README.md).

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
| OAuth bearer validation | Partial | `relaxed` (default) accepts anything, `oauth-relaxed` any bearer token, `bearer-dev` only the configured token. `strict` mode: the configured `auth.gcs.bearerToken` is the project owner (full access); extra tokens from `auth.gcs.principals` (`token=member[,token=member]`, e.g. `ci-token=serviceAccount:ci@p.iam.gserviceaccount.com`) authenticate as that IAM member; requests without a token are anonymous (`allUsers`); unknown tokens get `401`. Real Google OAuth/ID tokens are not validated. |
| XML API | Partial | Path-style (`/{bucket}/{object}`) and virtual-host (`{bucket}.localhost`, `{bucket}.storage.googleapis.com`) service/bucket/object CRUD, v1/v2 listing, copy (`x-goog-copy-source`), range/preconditions, `?acl`/`?lifecycle`/`?location`/`?storageClass`/`?versioning`, and XML multipart uploads (`POST ?uploads`, `PUT ?partNumber&uploadId`, `GET ?uploadId` list parts, `POST ?uploadId` complete with ascending-order/ETag/5 MiB minimum-part checks and `x-goog-if-*` preconditions, `DELETE ?uploadId` abort, `GET /{bucket}?uploads`); `x-goog-acl` on multipart initiate returns `NotImplemented`, and `x-goog-storage-class` / retention headers on initiate are not retained. |
| IAM / ACLs | Partial | Bucket IAM policy (`GET`/`PUT /iam`, ETag check, `/iam/testPermissions`), bucket/defaultObject/object ACL CRUD (JSON + XML `?acl`), predefined ACLs, Uniform Bucket-Level Access enforcement, and request-level IAM enforcement in `auth.gcs.mode: strict`: every JSON/XML/upload/download request is mapped to its `storage.*` permission and checked against the bucket IAM policy for the caller (configured `bearerToken` = project owner; tokens from `auth.gcs.principals` / `DEVCLOUD_GCS_PRINCIPALS` or `Server::add_access_token` = their IAM member; RSA/V2 signed URLs = `serviceAccount:{signer}`; anonymous = `allUsers` only; `user:`/`serviceAccount:`/`domain:`/`allUsers`/`allAuthenticatedUsers` members; copy/rewrite also need `storage.objects.create` on the destination bucket). Project-level calls (bucket list/create, introspection) are owner-only, conditional bindings never grant, and legacy ACLs are not consulted for authorization; real Google OAuth token validation is not implemented. Relaxed and other modes do not enforce IAM. |
| Retention | Partial | Bucket `retentionPolicy` (with `lockRetentionPolicy`), object holds (`temporaryHold`, `eventBasedHold`, `defaultEventBasedHold`), per-object retention (`Unlocked`/`Locked`, `overrideUnlockedRetention`), and delete/overwrite guards. |
| Lifecycle | Partial | Bucket lifecycle configuration (JSON + XML `?lifecycle`) with deterministic local evaluation of `Delete` and `SetStorageClass` rules (`age`, `createdBefore`, `matchesPrefix`, `matchesSuffix`, `matchesStorageClass`, `isLive`) on object reads/lists, honoring active retention/holds. |
| Pub/Sub notifications | Partial | `notificationConfigs` CRUD with `event_types`/`object_name_prefix`/`custom_attributes` and `JSON_API_V1`/`NONE` payload formats; matching `OBJECT_FINALIZE`/`OBJECT_METADATA_UPDATE`/`OBJECT_DELETE` flows append local event records (inspectable via `/_introspect/buckets/{b}/notification-events`) and optionally forward to a configured loopback Pub/Sub REST address (`DEVCLOUD_GCS_PUBSUB_REST_ADDR`). |
| Signed URLs | Partial | V4 `GOOG4-HMAC-SHA256`, V4 `GOOG4-RSA-SHA256`, and V2 RSA (`GoogleAccessId`/`Expires`/`Signature`) cryptographic verification (canonical request / string-to-sign, signed-header coverage, expiry) against RSA public keys registered per service account (PEM public/private key or service-account JSON, via `Server` APIs or `DEVCLOUD_GCS_SERVICE_ACCOUNT_KEYS=<json paths>`), and `response-*` header overrides. Unregistered RSA signers get structural/expiry checks only outside `strict` mode and are rejected in `strict`; keys are not fetched from Google's public certificate endpoint, and V2 HMAC-SHA1 signatures are not supported. |

Completed resumable uploads retain a small metadata receipt and notification marker in the shared object-store root until the workspace is reset; these contain no uploaded body and let retries return the original result after restarts, overwrites, or deletions.

### BigQuery-Compatible APIs

| Feature | Status | Notes |
| --- | --- | --- |
| REST v2 project/dataset/table routes | Yes | Local catalog resources persist under `.devcloud/data/bigquery`. |
| Table schema and row APIs | Yes | `tables.insert`, `tables.get/list/patch/delete`, `tabledata.insertAll`, and `tabledata.list`. |
| Partitioning and clustering metadata | Yes | Time/range partitioning and clustering fields round-trip in table metadata. |
| View metadata and query execution | Partial | View table resources persist query metadata and can be queried through the local SELECT subset. |
| Routine metadata and SQL execution | Partial | `routines.insert/list/get/patch/update/delete` persist local UDF/TVF/procedure metadata. SQL scalar UDFs execute in `SELECT` (with or without `FROM`), `WHERE`, views, and nested UDF calls; SQL TVFs execute in `FROM`; SQL `PROCEDURE`s execute via `CALL` for single-`SELECT` bodies. JavaScript/remote UDFs and procedural scripting fail closed. |
| Jobs API | Yes | Query, query destination tables, load, copy, extract, get, list, cancel, and result workflows are covered locally. |
| GoogleSQL query execution | Partial | Deterministic local subset for common `SELECT` workflows; unsupported syntax fails closed. |
| Google Cloud BigQuery client libraries | Partial | Local dataset/table/row/query compatibility workflows are covered through Rust E2E gates with endpoint override. |
| IAM policy endpoints | Partial | Dataset and table `getIamPolicy`/`setIamPolicy`/`testIamPermissions` persist local policies (bindings, including `condition`). `strict` mode: the configured `auth.bigquery.bearerToken` is the project owner (full access); extra tokens from `auth.bigquery.principals` (`token=member[,token=member]`, e.g. `ci-token=serviceAccount:ci@p.iam.gserviceaccount.com`) authenticate as that IAM member; requests without a token are anonymous (`allUsers`); unknown tokens get `401`. Principals and anonymous callers are authorized against the dataset policy, and for table calls the table policy too (`roles/bigquery.admin`/`dataOwner`/`dataEditor`/`dataViewer`/`metadataViewer` and `roles/owner`/`editor`/`viewer`), covering dataset, table, `tabledata`, routine, and model calls. Project-level calls (jobs, queries, dataset list/create, uploads, introspection) are owner-only, so principals cannot run queries. Conditional bindings never grant access; dataset `access` entries and project IAM are not used for authorization. |
| BigQuery ML | Partial | REST v2 `models.list/get/patch/delete` plus SQL `CREATE [OR REPLACE] MODEL`, `DROP MODEL`, `ML.PREDICT`, `ML.EVALUATE`, and `ML.WEIGHTS` for deterministic local `LINEAR_REG` and `LOGISTIC_REG` over numeric/boolean features; other model types and remote/Vertex AI models fail closed. |
| BI Engine | Partial | Reservation API v1 `GET`/`PATCH /v1/projects/{project}/locations/{location}/biReservation` persists local reservation size and `preferredTables`, and query jobs populate `statistics.query.biEngineStatistics` when size > 0; real in-memory columnar caching and acceleration are not emulated. |
| BigQuery Storage API | Partial | Plaintext gRPC on `server.bigqueryStoragePort` (default `19051`). `CreateReadSession` and `ReadRows` expose one Avro snapshot stream with top-level column selection and row offsets. Bidirectional `AppendRows` accepts Protobuf batches on the table's `_default` stream, with immediate REST and SQL visibility. |

Storage API rows support nullable and required scalar `STRING`, `BYTES`, `INT64`, `FLOAT64`, and `BOOL` columns. Local read sessions expire after six hours or on restart, with up to 16 sessions and 16 MiB of encoded row data and indexing per snapshot. Arrow, nested or repeated columns, read filters, historical snapshots, and managed write streams are outside this subset. The default write stream does not accept offsets and retries may append duplicates.

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
| Tags and resource policies | Partial | Local tag and resource-policy CRUD (`PutResourcePolicy`/`GetResourcePolicy`/`DeleteResourcePolicy`). In `relaxed` (default) and `signed-relaxed` modes resource policies are metadata-only; in `strict` mode attached policies are evaluated locally. |
| DescribeLimits / DescribeEndpoints | Yes | Local compatibility responses. |
| AWS SigV4 header auth | Partial | Relaxed mode is default; strict mode validates local credentials (supporting `dynamodb` and `dax` services) for SigV4 and SigV4a (`AWS4-ECDSA-P256-SHA256`, `X-Amz-Region-Set` must cover the configured region), and `aws-chunked` chunk and trailer signatures for both (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`, `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD[-TRAILER]`). `signed-relaxed` checks only the shape of either. |
| DAX | Partial | Control-plane metadata API (`AmazonDAXV3.*`: clusters, replication factor, node reboot, subnet groups, parameter groups, and tags) is emulated on the DynamoDB HTTP endpoint. The binary DAX data-plane cache protocol (Tube/CBOR on port 8111/9111) is not emulated. |
| Global tables | Partial | Metadata emulation for Global Tables v1 (`Create/Describe/List/UpdateGlobalTable`, `Describe/UpdateGlobalTableSettings`) and v2 (`UpdateTable` `ReplicaUpdates` and `DescribeTable.Table.Replicas` / `GlobalTableVersion`); no real cross-region replication. |
| Autoscaling | Partial | Table replica autoscaling metadata (`DescribeTableReplicaAutoScaling`, `UpdateTableReplicaAutoScaling`) is supported locally alongside the Application Auto Scaling service (`:18030`); real capacity-driven scaling is not simulated. |
| Real capacity accounting/throttling | No | Local deterministic behavior, not AWS capacity simulation. |
| IAM condition enforcement | Partial | Enforced in `strict` auth mode only (`relaxed` and `signed-relaxed` keep resource policies metadata-only). Evaluates resource policies attached via `PutResourcePolicy` (`Allow`/`Deny`, `Principal`, `Action`/`Resource` wildcards, `String*`/`Numeric*`/`Bool`/`Null`, `ForAllValues:`/`ForAnyValue:`, `IfExists`, and `${aws:*}` variables) over `dynamodb:LeadingKeys`, `dynamodb:Attributes`, `dynamodb:Select`, `dynamodb:ReturnValues`, `dynamodb:IndexName`, `dynamodb:EnclosingOperation`, `aws:ResourceTag/*`, `aws:RequestTag/*`, `aws:TagKeys`, `aws:RequestedRegion`, and `aws:username`/`aws:PrincipalArn`. Tables without a resource policy remain open. |

DynamoDB dashboard management is available under `/dashboard/dynamodb` and the local `/api/dynamodb/*` dashboard API. The dashboard can inspect tables, items, indexes, TTL, and streams, and can run guarded local management flows for `CreateTable`, `PutItem`, `UpdateItem`, `UpdateTimeToLive`, `DeleteItem`, `DeleteTable`, `Query`, and `Scan`. Query and Scan expose result pagination with count, scanned count, next/previous controls, and selected result item JSON. Recent operation history is stored only in browser `localStorage` as metadata; it does not persist item payloads, credentials, full request payloads, or pagination keys. Dashboard mutation endpoints forward through the local DynamoDB JSON protocol path instead of editing storage directly. Destructive `DeleteItem` and `DeleteTable` flows require confirmation text matching the selected table name, and disabled DynamoDB services do not expose active mutation controls.

### Google Cloud Pub/Sub-Compatible API

| Feature | Status | Notes |
| --- | --- | --- |
| gRPC emulator endpoint | Yes | Implements local `google.pubsub.v1.Publisher`, `Subscriber`, and `SchemaService` surfaces. |
| REST v1 endpoint | Yes | Supports topic, subscription, publish, pull, ack, seek, schema, and IAM-compatible local workflows. |
| Google Cloud Pub/Sub client libraries | Yes | Use `PUBSUB_EMULATOR_HOST=127.0.0.1:18085` and `PUBSUB_PROJECT_ID=devcloud`. |
| Topic create/get/list/update/delete | Yes | Includes labels, retention, schema settings, and KMS metadata where applicable. |
| Subscription create/get/list/update/delete | Yes | Includes ack deadline, retain acked messages, filters, retry policy, dead-letter policy, push config, and ordering flags. |
| Publish / Pull / Acknowledge / ModifyAckDeadline | Yes | Local lease and redelivery behavior is covered by unit and E2E tests. |
| StreamingPull | Yes | Supports flow control, ack/modack/nack, cancellation, and ordering-key gates. |
| Ordering keys | Yes | Local ordered delivery gate is implemented for pull and streaming pull flows. |
| Snapshots and Seek | Yes | Snapshot CRUD and seek-to-time/snapshot flows are implemented. |
| Schemas | Yes | Schema CRUD, revisions, rollback/delete revision, and validate message are implemented locally. |
| Push subscriptions | Partial | Local HTTP push worker, retry policy, and OIDC/no-wrapper metadata are supported when push is enabled. |
| IAM endpoints | Partial | Topic and subscription `getIamPolicy`/`setIamPolicy` persist local policies (`iam-policies.json` in the Pub/Sub data directory; a deleted resource's policy is dropped). `strict` mode: the configured `auth.pubsub.bearerToken` is the project owner (full access); extra tokens from `auth.pubsub.principals` (`token=member[,token=member]`, e.g. `ci-token=serviceAccount:ci@p.iam.gserviceaccount.com`) authenticate as that IAM member; requests without a token are anonymous (`allUsers`); unknown tokens get `401`. Over both REST and gRPC, principals and anonymous callers are authorized against the topic or subscription policy (`roles/pubsub.publisher` → publish, `roles/pubsub.subscriber` → pull/ack/modifyAckDeadline/StreamingPull/seek, `roles/pubsub.viewer`/`editor`/`admin`, `roles/viewer`/`editor`/`owner`). Project-level calls (create/list, snapshots, schemas, detach, introspection) are owner-only. `testIamPermissions` echoes the requested permissions; conditional bindings never grant access. In non-strict modes gRPC stays unauthenticated as before. |
| Exactly-once delivery | Partial | Metadata is accepted; no real cloud-grade exactly-once guarantee. |
| Cloud Monitoring, quotas, billing | Partial | `GET /_introspect/metrics` exposes deterministic local Pub/Sub monitoring counters and backlog ages (undelivered/in-flight counts, oldest unacked message age, per-topic and per-subscription breakdown). `GET /_introspect/quotas` exposes effective local quota limits and resource usage. `GET /_introspect/billing` reports current retained message count and content bytes for the local instance. Each stored message counts once, including its decoded data, attribute keys and values, and ordering key. Usage is point-in-time and carries no charges. |
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
| PostgreSQL wire endpoint | Yes | Listens on `127.0.0.1:15439` by default for local Redshift-style SQL clients. |
| PostgreSQL simple query protocol | Yes | Covers `psql` smoke workflows. |
| TLS on the SQL endpoint | Yes | Off by default. Set `services.redshift.tls.certFile` and `services.redshift.tls.keyFile` (PEM; certificate chain leaf first, key in PKCS#8, PKCS#1, or SEC1; paths relative to where `devcloud up` runs) and clients that send an SSLRequest get TLS 1.2 or 1.3 with that certificate, so `sslmode=verify-full` works when the client trusts the issuing CA and the certificate names the host it connects to. devcloud does not issue certificates. Plain connections are still accepted unless `services.redshift.tls.require: true`. The standalone binary reads `DEVCLOUD_REDSHIFT_TLS_CERT_FILE`, `DEVCLOUD_REDSHIFT_TLS_KEY_FILE`, and `DEVCLOUD_REDSHIFT_TLS_REQUIRE`. |
| PostgreSQL extended query protocol | Partial | Supports Parse, Bind, Describe, Execute, Sync, Close, text bind parameters, portal resume, and safe unsupported errors for binary formats. |
| PostgreSQL execution backend | Yes | Default backend is managed local PostgreSQL; explicit memory fallback remains available for development continuity. Each SQL client connection gets its own PostgreSQL session, so temp tables, `SET`s, and transactions stay with that client and end when it disconnects. See TLS below. |
| Redshift SQL translation | Partial | Handles local subsets for DDL/DML, Redshift table attributes, CTAS/views/materialized-view metadata, function rewrites, COPY, and UNLOAD. |
| COPY from local S3 | Partial | `COPY <table> FROM 's3://bucket/prefix'` loads every object under the prefix from the local S3 store into the PostgreSQL backend, on the client's own session (temp tables and open transactions work): default text format with `DELIMITER`, `CSV` / `FORMAT AS CSV`, `JSON 'auto'` (or `'auto ignorecase'`), `GZIP`, `IGNOREHEADER`, `NULL AS`, `EMPTYASNULL`, `BLANKSASNULL`; an empty field is NULL except in character columns, as on Redshift. `IAM_ROLE` / `CREDENTIALS` / `REGION` are accepted and ignored. Column lists, `MANIFEST`, JSONPaths, other compressions, and columnar formats are refused. The memory backend keeps its own CSV/JSON COPY. |
| UNLOAD to local S3 | Partial | `UNLOAD ('<select>') TO 's3://bucket/prefix'` translates and executes the query on the PostgreSQL backend (on the client's own session, so temp tables and open transactions work) and writes the result to the local S3 store (`<prefix>000`, `<prefix>000.gz`, `<prefix>000.json`, or `<prefix>000.json.gz`): default pipe-delimited text format with `DELIMITER`, `CSV` / `FORMAT AS CSV`, `JSON` / `FORMAT AS JSON`, `GZIP`, `HEADER`, `NULL AS`, `ALLOWOVERWRITE`, `CLEANPATH`, and `PARALLEL ON\|OFF` (accepted as single-slice output). A non-empty target prefix is refused unless `ALLOWOVERWRITE` or `CLEANPATH` is set. `IAM_ROLE` / `CREDENTIALS` / `REGION` are accepted and ignored; `MANIFEST`, `PARQUET`, `PARTITION BY`, `MAXFILESIZE`, and `BZIP2` are refused. The memory backend keeps its own CSV UNLOAD. |
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
| External Redis mode | Yes | Set `services.redis.mode: external` and `services.redis.externalUrl` to `redis://host[:port][/db]` or `rediss://host[:port][/db]`. For TLS, set `services.redis.externalTlsCaFile` to a PEM trust bundle. Devcloud verifies the certificate chain and URL hostname or IP. Startup validates the connection, authentication, and selected database. The dashboard uses the same target and displays the URL without credentials. |
| Managed persistence | Yes | Runtime data is kept under `.devcloud/data/redis` by default. |
| String/hash/list/set/zset inspection | Partial | `/dashboard/redis` lists keys with SCAN and shows type-specific previews. |
| Command runner | Partial | Dashboard commands are restricted to the Redis allowlist in `docs/design-redis-compat.md`. |
| Destructive actions | Partial | Key delete, expire, and guarded `FLUSHDB` are supported; `FLUSHALL` and unsafe commands are rejected. |
| Redis AUTH | Yes | In managed mode, strict auth configures `requirepass` and relaxed mode leaves it unset. For external Redis, put `:password@` (or `default:password@`) before the host for password-only AUTH, or `username:password@` for a non-default ACL user. Percent-encode special characters in URL credentials. Upstream credentials apply in both HTTP auth modes. |
| HTTP API authentication | Partial | Strict mode protects the Redis control API with the configured Redis Basic credentials; the dashboard forwards those credentials for reads and mutations. |
| Browser guards | Yes | Direct introspection/control HTTP endpoints reject public DNS Host names and cross-site mutations, using the Dashboard API host allowlist. SDK/CLI requests without Origin remain supported. |
| Cluster, Sentinel, modules, RedisJSON, RediSearch | No | Out of local compatibility scope. |

### AWS Lambda-Compatible API

| Feature | Status | Notes |
| --- | --- | --- |
| Function CRUD | Yes | `CreateFunction`, `GetFunction`, `GetFunctionConfiguration`, `ListFunctions` (Marker/MaxItems), `UpdateFunctionConfiguration` (RevisionId precondition), `UpdateFunctionCode`, `DeleteFunction` on the `/2015-03-31/functions` REST API. Name, partial ARN, and full ARN identifiers are accepted. |
| Deployment packages | Partial | Zip (`ZipFile` base64, or `S3Bucket`/`S3Key` from the local S3 store when S3 is enabled; stored + DEFLATE entries, no zip64) and container images (below). As on AWS, a zip package over 250 MB unzipped is rejected (50 MB for a direct `ZipFile` upload). Unix symlinks in the zip (`zip -y`, e.g. `node_modules/.bin`) are kept as symlinks (pointing anywhere, as on AWS); an entry stored beneath a link rejects the package, since extraction never writes through a link. |
| Container images | Partial | `PackageType: Image` with `Code.ImageUri` and `ImageConfig` (`EntryPoint`, `Command`, `WorkingDirectory`) on create/update/get; `GetFunction` returns `RepositoryType: ECR`. Invoking needs `services.lambda.docker: true` (`DEVCLOUD_LAMBDA_DOCKER=true` for `devcloud-lambda`) and the `docker` CLI: each environment is a `docker run` of the image (any local tag or pullable reference; the first run may pull it, within a 120 s start budget) with the Runtime Interface Emulator port published on loopback — or, with `services.lambda.dockerNetwork` (`DEVCLOUD_LAMBDA_DOCKER_NETWORK`) set because devcloud itself runs in a container sharing the Docker socket, joined to that network and reached by container name — so the image must start the RIE — every AWS base image (`public.ecr.aws/lambda/*`) does through its entrypoint; a custom `EntryPoint` must too. Environments stay warm like zip functions. Containers are labelled `devcloud.lambda.owner=<instance id>` (the id is kept in the Lambda storage directory as `instance-id`); on startup devcloud removes containers carrying its own id that a killed previous run left behind, and never touches other instances' containers. `MemorySize` becomes the container's `--memory` limit; `Architectures` is not mapped to `--platform` (the image's own platform runs). Variables reach the container through an owner-only `--env-file`, so a value with a line break cannot be passed. The RIE answers 200 for function errors too, so a response that is exactly a Lambda error document (`errorType`, `errorMessage`, and only error fields) is reported as `X-Amz-Function-Error: Unhandled`. `Init Duration` is the RIE's runtime init time, and a cold start's log opens with `INIT_START Image: <ImageUri>`. REPORT's `Max Memory Used` is the container's cgroup memory peak since it started (page cache included; read with `docker exec <container> cat`, so it is omitted for images without `cat`), with the same warning above `MemorySize`. The standalone `devcloud-lambda` Docker image does not include Docker (its `docker` build target does): invoking an image function without it fails with an error saying so (and `DEVCLOUD_LAMBDA_DOCKER=true` without a `docker` CLI on `PATH` is warned about at startup). |
| Invoke | Partial | `RequestResponse`, `Event` (async, 202), and `DryRun`; `X-Amz-Function-Error: Unhandled` on handler errors; `X-Amz-Log-Type: Tail` returns the last 4 KB of the START/END/REPORT-framed log; per-function `Timeout` is enforced. **Execution environments are reused (warm starts)**: an environment is one `python3` / `node` process that loads the handler module once and then serves one invocation at a time, so module-level state and one-time initialization carry over, as on AWS. A cold start adds an `INIT_START` line, the init-phase output, and `Init Duration` in REPORT; init gets its own budget of one `Timeout`. Concurrent invocations get separate environments. An environment is stopped after `services.lambda.idleTimeoutSeconds` (default 300) without invocations, when the function's configuration or code changes or the function is deleted, after a timeout, crash, or init failure, and on shutdown; `idleTimeoutSeconds: 0` makes every invocation a cold start. Unlike AWS, an idle environment is not frozen: threads a Python handler left running keep running (Node handlers are paused between invocations). `MemorySize` is reported to the handler and in REPORT but does not limit the process's memory. On Linux, REPORT adds `Max Memory Used` (the interpreter process's peak resident memory during the invocation, init included on a cold start), and the log warns when it exceeds `MemorySize`. With `services.lambda.logInvocations: true` (`DEVCLOUD_LAMBDA_LOG_INVOCATIONS`, on by default for `devcloud-lambda`) every invocation's framed log is also printed to stdout, each line tagged `[<function>]`. |
| Runtimes | Partial | `python3.x` and `nodejs*` handlers run as local `python3` / `node` processes (CommonJS, ESM `.mjs`, async and callback handlers). A Python runtime uses the matching `python3.<minor>` on devcloud's `PATH` when there is one (e.g. `python3.12`), otherwise `python3`; Node uses `node`. When the interpreter's version differs from the runtime (`python3.12` on Python 3.11, `nodejs20.x` on Node 22), the invocation log gets a `[WARNING] devcloud: runtime ... is running on ...` line before the handler's output (on a cold start), and devcloud's stderr repeats it once per function and runtime. The AWS-bundled SDKs (`boto3` for Python, AWS SDK v3 for Node) are not provided; ship them in the package or under the `/opt` directory. Other runtimes (`java*`, `dotnet*`, `ruby*`, `go1.x`) can be deployed but invoking them returns `InvalidRuntimeException`. |
| Custom runtimes (`provided*`) | Partial | The package's `bootstrap` (in the code root, else in the `/opt` directory) runs as a local process against a per-environment Lambda Runtime API on loopback (`AWS_LAMBDA_RUNTIME_API`): `invocation/next`, `invocation/{id}/response` (including chunked bodies and the error trailers of streamed responses), `invocation/{id}/error`, and `init/error`. Environments are reused like interpreter runtimes; output is framed per invocation. The bootstrap must be built for the host devcloud runs on (a Go runner for macOS needs `GOOS=darwin`); a missing or foreign-OS bootstrap fails the invocation with `Runtime.InvalidEntrypoint`. Extensions may register (so `aws-lambda-go`'s `WithEnableSIGTERM` starts cleanly) but receive no events, and stopped environments are killed without SIGTERM. |
| Handler environment | Partial | Cleared environment: `PATH`, Lambda's reserved variables (`AWS_LAMBDA_FUNCTION_NAME`, `AWS_REGION`, `LAMBDA_TASK_ROOT`, ...), and the function's `Environment.Variables`. Host credentials are never passed through. To stand in for the execution role, set `services.lambda.functionAccessKeyId` / `functionSecretAccessKey` (and optionally `functionSessionToken`) and every handler receives them as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`. |
| `/opt` (layer contents) | Partial | As on AWS, the default `PYTHONPATH` is `/opt/python/lib/python<version>/site-packages:/opt/python` and the default `NODE_PATH` is `/opt/nodejs/node<major>/node_modules:/opt/nodejs/node_modules`, so dependencies placed (or mounted) there are importable without extra settings. `services.lambda.optDir` points these paths at another directory. A function that sets its own `PYTHONPATH` / `NODE_PATH` replaces the default, as on AWS. |
| Tags / account settings | Yes | `/2017-03-31/tags/{arn}` list/tag/untag and `GetAccountSettings`. |
| Function URLs | Partial | `CreateFunctionUrlConfig` / `GetFunctionUrlConfig` / `UpdateFunctionUrlConfig` / `DeleteFunctionUrlConfig` / `ListFunctionUrlConfigs` under `/2021-10-31/functions/{name}/url` (`$LATEST` and aliases). The returned `FunctionUrl` is `http://<url-id>.lambda-url.<region>.localhost:<lambdaPort>/`, where `<url-id>` is derived from the account, region, and function name (or `<name>:<alias>`), so it stays the same when the URL, the function, or the data directory is recreated. Clients that cannot use that host can call `http://127.0.0.1:<lambdaPort>/_url/<url-id>/<path>`, or address the URL by function name: `http://127.0.0.1:<lambdaPort>/urls/<function>/<path>`. Requests reach the handler as a payload format 2.0 event through the same synchronous invoke path as `Invoke` (recorded in recent invocations), and the result is mapped like AWS: an object with `statusCode` sets status, `headers`, `multiValueHeaders`, `cookies` (`Set-Cookie`), `body`, and `isBase64Encoded`; any other JSON value becomes a `200` `application/json` body; a function error is `502`. `Cors` answers preflight requests itself and replaces the function's `Access-Control-*` headers. `AuthType: AWS_IAM` refuses unsigned requests (`403`) in every auth mode; `strict` verifies the SigV4 signature (service `lambda`) against `auth.lambda` credentials, `relaxed` / `signed-relaxed` only check that it is well formed. The mode is `auth.lambda.mode` unless `services.lambda.urlAuthMode` (`DEVCLOUD_LAMBDA_URL_AUTH_MODE`) sets one for URLs alone, e.g. `strict` URLs with a `relaxed` API. `AuthType: NONE` is public: resource-based policies are not evaluated, so no `AddPermission` is needed, and URLs bypass the API's browser (CSRF) guard. `InvokeMode: RESPONSE_STREAM` is accepted but served buffered. `sourceIp` is always `127.0.0.1`. |
| Versions | Yes | `PublishVersion` (idempotent when unchanged; `RevisionId`/`CodeSha256` checks), `ListVersionsByFunction`, `ListFunctions?FunctionVersion=ALL`, `Publish: true` on `CreateFunction`/`UpdateFunctionCode`, and `DeleteFunction?Qualifier=<version>`. Published versions snapshot code, configuration, and attached layers, run in isolated warm environments, and report their version in `X-Amz-Executed-Version`, `AWS_LAMBDA_FUNCTION_VERSION`, handler context, and logs. |
| Aliases | Yes | `CreateAlias`, `GetAlias`, `UpdateAlias` (`RevisionId` precondition), `DeleteAlias`, and `ListAliases` (`FunctionVersion` filter). Aliases resolve in `GetFunction`, `GetFunctionConfiguration`, `Invoke`, and Function URLs, with deterministic weighted traffic splitting via `RoutingConfig.AdditionalVersionWeights`. |
| Layers | Partial | `PublishLayerVersion` (`ZipFile` or local `S3Bucket`/`S3Key`), `GetLayerVersion`, `GetLayerVersionByArn`, `ListLayers`, `ListLayerVersions` (runtime/architecture filters), `DeleteLayerVersion`, and up to 5 `Layers` on Zip functions. Attached layers are overlaid in order into a per-environment `/opt` stand-in (`PYTHONPATH`, `NODE_PATH`, `PATH` `bin/`, and `provided*` `bootstrap` lookup) ahead of `services.lambda.optDir`. `AddLayerVersionPermission` (action `lambda:GetLayerVersion`; principal `*`, an account id, or `arn:aws:iam::<account>:root`; `OrganizationId` with `*`; `RevisionId` precondition), `GetLayerVersionPolicy`, and `RemoveLayerVersionPermission` store and return the layer version's resource policy, which persists and is deleted with the version. The policy is not consulted on access: every caller is the instance's own account, and layers of other accounts cannot be referenced. |
| Event source mappings | Partial | `CreateEventSourceMapping`, `GetEventSourceMapping`, `ListEventSourceMappings` (`EventSourceArn`/`FunctionName` filters and pagination), `UpdateEventSourceMapping`, and `DeleteEventSourceMapping` with field and `FilterCriteria` JSON pattern validation and persistence. Under `devcloud up`, enabled mappings for SQS queues and DynamoDB streams are polled every second from the local SQS / DynamoDB services (when those are enabled; requests are signed with `auth.sqs` / `auth.dynamodb` credentials, so `strict` services work) and invoke the function synchronously with Lambda-shaped `Records`. `FilterCriteria` patterns (literals, `prefix`, `suffix`, `equals-ignore-case`, `anything-but`, `exists`, `numeric`; SQS bodies match as JSON when they are objects) drop non-matching records — for SQS they are deleted. SQS: up to `BatchSize` messages per batch; a successful invocation deletes them except those named in `batchItemFailures` (`FunctionResponseTypes: [ReportBatchItemFailures]`); a function error deletes none, so they return after the queue's visibility timeout. DynamoDB Streams: `StartingPosition` `LATEST` (from the first poll) or `TRIM_HORIZON` (`AT_TIMESTAMP` reads from `TRIM_HORIZON`); a failed batch is retried until `MaximumRetryAttempts` is exceeded and then skipped, and `batchItemFailures` resume from the first failed sequence number; shard positions are kept in memory, so after a restart a mapping reads from its `StartingPosition` again. Get/List report `LastProcessingResult` (`OK` or `PROBLEM: ...`) after a mapping's first batch or polling error. `MaximumBatchingWindowInSeconds`, `ParallelizationFactor`, `BisectBatchOnFunctionError`, `MaximumRecordAgeInSeconds`, `ScalingConfig`, and `DestinationConfig` are stored but do not change polling. Kinesis and Kafka/MSK mappings are not polled (devcloud has no local Kinesis or Kafka), nor are any mappings of the standalone `devcloud-lambda` binary. |
| Concurrency | Partial | `PutFunctionConcurrency`, `GetFunctionConcurrency`, and `DeleteFunctionConcurrency` update `AccountLimit.UnreservedConcurrentExecutions` and enforce per-function reserved concurrency on `Invoke` and Function URLs with `429 TooManyRequestsException`. `PutProvisionedConcurrencyConfig`, `GetProvisionedConcurrencyConfig`, `ListProvisionedConcurrencyConfigs`, and `DeleteProvisionedConcurrencyConfig` persist configs for published versions and aliases (validated against reserved/account limits) and pre-start that many execution environments of the version (an alias's target version; a version and an alias to it share them): `Put` answers `IN_PROGRESS`, `Get`/`List` report `READY` once `AvailableProvisionedConcurrentExecutions` reaches the request, or `FAILED` with `StatusReason` when an environment fails to start (a crash or timeout during init; a Python/Node import error is reported by the first invocation instead, as for on-demand environments). Pre-started environments serve invocations of that version without a cold start, never expire with `idleTimeoutSeconds`, are replaced after a crash or timeout, are stopped when the config is lowered or deleted (or the version or function is deleted), and start again when devcloud restarts. Container image functions are not pre-started (their configs report `READY`). Invocations beyond the provisioned environments use on-demand environments as usual. |
| Auth | Partial | `relaxed`, `signed-relaxed`, and `strict` SigV4 and SigV4a (`AWS4-ECDSA-P256-SHA256`; service `lambda`), like Application Auto Scaling, including `aws-chunked` chunk and trailer signature verification for both (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`, `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD[-TRAILER]`). `strict` requires the credential scope's date to match `X-Amz-Date` and accepts a signature over the path either normalized as most SDKs sign it (`//x` and dot segments collapsed) or exactly as sent; there is no clock-skew check, so requests signed with fixed timestamps keep working. |
| Browser (CSRF) guard | Yes | Non-GET requests carrying a non-loopback `Origin` (or `Sec-Fetch-Site: cross-site`) are rejected with 403, so a web page cannot create or invoke functions; SDKs/CLIs (no `Origin`) and loopback front-ends are unaffected. The dashboard's test invoke applies the same rule. Requests whose `Host` is a public DNS name are refused too (DNS rebinding; see the Dashboard API notes). |

### SSM Parameter Store-Compatible API

| Feature | Status | Notes |
| --- | --- | --- |
| Parameters | Partial | `PutParameter` (`String`, `StringList`, `SecureString`; `Overwrite`; `Standard`/`Advanced`/`Intelligent-Tiering` value limits; `KeyId`, `Tags`, `Policies`), `GetParameter` (with `:<version>` and `:<label>` selectors), `GetParameters`, `GetParametersByPath` (`Recursive`, `WithDecryption`, `MaxResults`/`NextToken`, `Type`/`KeyId`/`Label` filters), `GetParameterHistory`, `DeleteParameter`, `DeleteParameters`, and `DescribeParameters` (`MaxResults`/`NextToken`; filters are rejected). AWS JSON 1.1 under `X-Amz-Target: AmazonSSM.<Op>`. Persisted to `.devcloud/data/ssm/state.json`. |
| Labels & version history | Yes | `LabelParameterVersion`, `UnlabelParameterVersion`, `GetParameterHistory`, and `:<version>` / `:<label>` selectors on `GetParameter` and `GetParameters` (up to 100 versions retained per parameter and up to 10 labels per version). |
| Tags & policies | Partial | `AddTagsToResource`, `RemoveTagsFromResource`, `ListTagsForResource` (`ResourceType: Parameter`) and initial `Tags` on `PutParameter`. The SSM server evaluates Advanced-tier policies on startup and every second. `Expiration` deletes the parameter and its history at the earliest RFC 3339 deadline. `ExpirationNotification` fires before that deadline while the parameter still exists, and `NoChangeNotification` fires after the configured time since the last successful `PutParameter`. Notifications support hours and days, use live in-process delivery through `Server::subscribe_policy_events`, and have no EventBridge integration or replay. |
| `SecureString` | Partial | No KMS: values are stored in plaintext in `state.json` and `KeyId` (default `alias/aws/ssm`) is persisted as metadata. `GetParameter`, `GetParameters`, `GetParametersByPath`, and `GetParameterHistory` return the value only with `WithDecryption: true`; without it the value is an opaque placeholder, never the plaintext. |
| Auth | Partial | `relaxed` / `signed-relaxed` / `strict` SigV4 and SigV4a (`AWS4-ECDSA-P256-SHA256`; service `ssm`), including `aws-chunked` chunk and trailer signature verification for both (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`, `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD[-TRAILER]`). `strict` only accepts `auth.ssm` credentials, so STS temporary credentials are rejected there. |
| Dashboard | Partial | `/dashboard/ssm` lists names, types, and versions through `DescribeParameters`; values never reach the dashboard. |

### STS-Compatible API

| Feature | Status | Notes |
| --- | --- | --- |
| `AssumeRole` | Partial | AWS Query protocol (form `POST /` or `GET /?Action=`), XML responses. Validates `RoleArn`, `RoleSessionName`, and `DurationSeconds` (900–43200), and issues new `ASIA…` temporary credentials per call. An inline session `Policy` must be JSON of at most 2048 characters. In `strict` mode the STS server keeps each issued session in memory (lost on restart) and accepts requests signed with it (signature checked with the session secret, `X-Amz-Security-Token` must match → `InvalidClientTokenId`, expiry → `ExpiredToken`). Those requests are limited by the session policy, evaluated like the S3 IAM registry (`Allow`/`Deny`, `Action`/`NotAction`, `Resource`/`NotResource` wildcards, `StringEquals`/`StringNotEquals`/`StringLike`/`StringNotLike`/`Bool` over `aws:PrincipalArn`, `aws:PrincipalAccount`, `aws:SecureTransport`, `sts:RoleSessionName`): role chaining (`AssumeRole` with session credentials) needs an `Allow` for `sts:AssumeRole` on the target `RoleArn` and no `Deny` (else `403 AccessDenied`), and is capped at `DurationSeconds` 3600; `GetCallerIdentity` needs no permission. Role identity and trust policies are not emulated, so a session without a session policy is unrestricted. Other devcloud services do not accept STS credentials in `strict` mode, so session policies limit only STS requests. `relaxed` / `signed-relaxed` stay stateless and evaluate nothing. |
| `GetCallerIdentity` | Partial | Always reports the configured account's root identity, not the assumed role. |
| Auth | Partial | `relaxed` / `signed-relaxed` / `strict` SigV4 and SigV4a (`AWS4-ECDSA-P256-SHA256`; service `sts`), including `aws-chunked` chunk and trailer signature verification for both (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`, `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD[-TRAILER]`). |

### Cloud Run-Compatible API

| Feature | Status | Notes |
| --- | --- | --- |
| Admin API v2 services | Yes | Create (`serviceId`, `validateOnly`), get, list (including location `-`), patch (`updateMask`, `allowMissing`, etag check), delete under `/v2/projects/{p}/locations/{l}/services`. |
| Revisions | Yes | A template change creates a `<svc>-<generation>-<suffix>` revision; list/get/delete (the latest revision cannot be deleted). |
| Long-running operations | Partial | Every mutation completes synchronously and returns a `done: true` Operation; operations are kept in memory (get/list/delete/`:wait`). |
| IAM policy | Partial | `getIamPolicy`, `setIamPolicy`, `testIamPermissions` per service. Strict mode enforces a bearer token on the Admin API and on service requests unless the service grants `allUsers` `roles/run.invoker` or sets `invokerIamDisabled`. |
| Serving requests | Partial | Requests with `Host: <svc>.<location>.<project>.run.localhost[:port]` (the service `uri`) or under `/_run/<project>/<location>/<svc>/` (in `urls`) are reverse-proxied to the latest revision. Traffic splitting is not emulated: the latest revision always serves. |
| Container execution | Partial | `containers[0].command` + `args` run as a local process with `PORT` set to a free loopback port plus `K_SERVICE`/`K_REVISION`/`K_CONFIGURATION` and the container `env` values (`valueSource` secrets are not resolved). Image-only containers run with `docker run` when `services.cloudRun.docker: true`, otherwise requests return 503. One instance per service, started on first request, replaced on a new revision, stopped on delete/shutdown. |
| Browser (CSRF) guard | Yes | Non-GET Admin API requests with a non-loopback `Origin` (or `Sec-Fetch-Site: cross-site`) are rejected with 403; service (data-plane) requests are not restricted. Requests on either plane whose `Host` is a public DNS name are refused (DNS rebinding; see the Dashboard API notes). In `strict` mode `/_introspect/*` needs the bearer token. |
| Jobs, domain mappings, v1 (Knative) API | No | Out of local compatibility scope. |

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
| Application Auto Scaling dashboard API | Yes | Status, scalable targets, scaling policies, scheduled actions, and guarded register/deregister/put/delete mutations over the AWS JSON 1.1 provider protocol. |
| Lambda dashboard API | Yes | Status, functions, recent invocations with log tails, and test invoke through the Invoke API. |
| Cloud Run dashboard API | Yes | Status, services, revisions, running instances, instance logs, and guarded create/update/delete service, delete revision, and IAM invoker policy mutations through the Admin API v2. |
| Browser guards | Yes | `/api/*` mutations from another site (a non-loopback `Origin` that is not the dashboard's own, or `Sec-Fetch-Site: cross-site`) are refused with 403; scripts and CLIs send no `Origin` and are unaffected. Every request whose `Host` is a public DNS name is refused (a DNS-rebinding page keeps its own domain as `Host`); IP addresses, single-label names (`localhost`, docker-compose service names) and `.localhost`, `.internal`, `.local`, `.test`, `.example`, `.invalid`, `.home.arpa` names are accepted. Set `DEVCLOUD_ALLOWED_HOSTS` (comma-separated; `.suffix` for a domain, `*` for any) to reach devcloud through another name. Lambda and Cloud Run apply the same Host rule. WebSocket handshakes (`/api/events`, and the event relay itself) are refused from other sites' origins too, since WebSockets get no CORS protection. |
| Common React dashboard shell | Yes | All service pages are served under `/dashboard/<svc>` from the shared React shell; compatibility `/mail`, `/s3`, `/gcs`, `/dynamodb`, `/bigquery`, `/redis` paths return 301 redirects. |

## Verification

### Local build and test

Install a current stable Rust toolchain, Node.js 22 or later, and the Protocol
Buffers compiler (`protoc`, including its standard `.proto` files). Pub/Sub's
build script uses `protoc`; on Debian/Ubuntu, install `protobuf-compiler`.

```bash
cd web/dashboard
npm ci
npm test
npm run build
cd ../..
cargo build --locked --workspace
cargo test --locked --workspace
```

The dashboard build writes the assets embedded by the Rust dashboard crate.
Commit the generated bundle with dashboard changes. `npm test` exercises the
event relay frame contract, reconnects, resynchronization, subscription cleanup,
and stale React responses without requiring a browser.

To exercise the built CLI and dashboard together in Chromium:

```bash
cargo build --locked -p devcloud-orchestrator
cd web/dashboard
npx playwright install --with-deps chromium
npm run test:browser
```

The browser test starts an isolated GCS workspace and verifies that provider
mutations appear through the live event stream, including a snapshot refresh
after a disconnected interval. Its screenshot and result are written to
`target/dashboard-browser/` and uploaded as a CI artifact.

Pull requests run `.github/workflows/ci.yml`: Rust formatting, individual service
builds, workspace tests and Clippy, dashboard tests and bundle consistency,
the Chromium smoke test, and Rust/npm dependency audits. Individual builds catch missing Cargo features that
can otherwise be hidden by workspace-wide feature unification.

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
| Application Auto Scaling MVP | `VERIFY_STAGE=full bash scripts/applicationautoscaling-autoloop/verify.sh` |
| Lambda MVP | `VERIFY_STAGE=full bash scripts/lambda-autoloop/verify.sh` |
| Cloud Run MVP | `VERIFY_STAGE=full bash scripts/cloudrun-autoloop/verify.sh` |
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
| Redshift managed PostgreSQL | `scripts/redshift-managed-postgres-e2e.sh` | `initdb`, `postgres`, `psql` on `PATH` |
| Redshift SQL translator | `scripts/redshift-translator-e2e.sh` | `psql`, `aws`, and `postgres` server binary on `PATH` |
| Redis | `scripts/redis-e2e.sh` | `redis-cli` |
| Application Auto Scaling | `scripts/applicationautoscaling-e2e.sh` | none |
| SSM Parameter Store | `scripts/ssm-e2e.sh` | none |
| STS | `scripts/sts-e2e.sh` | none |
| Lambda | `scripts/lambda-e2e.sh` | `python3` (handler runtime) |
| Cloud Run | `scripts/cloudrun-e2e.sh` | `python3` (sample service) |

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
| `services/applicationautoscaling` | Application Auto Scaling-compatible JSON API service. |
| `services/ssm` | SSM Parameter Store-compatible JSON API service. |
| `services/sts` | STS-compatible Query API service (`AssumeRole`, `GetCallerIdentity`). |
| `services/lambda` | AWS Lambda-compatible REST API with local python/node handler execution. |
| `services/cloudrun` | Cloud Run Admin API v2 plus a reverse proxy to locally run service processes. |
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
