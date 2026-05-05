# BigQuery Compatibility Design

## Summary

`devcloud` に Google BigQuery 互換のローカル analytical database server を追加する。

目標は「Google Cloud BigQuery client libraries、`bq` CLI、REST API 利用アプリケーションが endpoint override / API endpoint override だけで主要 workflow を実行できる」ことである。BigQuery の完全互換は REST resources、GoogleSQL、jobs、streaming insert、load/extract/copy、partitioning、IAM、routines、models、Storage API まで非常に広いため、実装は段階化する。ただし内部設計は最初から catalog、table storage、job runner、SQL engine、streaming buffer、dashboard を分離し、完全互換へ拡張できる形にする。

## Document Control

| Field | Value |
| --- | --- |
| Audience | devcloud implementers, reviewers, future agent loops |
| Status | Draft |
| Owner | devcloud maintainers |
| Reviewer | TBD |
| Related docs | `docs/design-s3-compat.md`, `docs/design-gcs-compat.md`, `docs/design-dynamodb-compat.md`, `docs/design-dashboard-shell.md` |
| Primary references | BigQuery REST API v2, datasets, tables, tabledata, jobs, GoogleSQL reference, BigQuery Storage API |
| Reference check date | 2026-05-01 |

## Compatibility Goal

### Definition

ここでの BigQuery 互換は、以下を満たす状態を指す。

1. BigQuery client libraries / `bq` CLI / REST clients が local endpoint を指定するだけで接続できる。
2. BigQuery REST API v2 の URI、query parameter、resource schema、JSON response、JSON error response が主要操作で一致する。
3. Dataset、Table、TableData、Job の core workflow を filesystem backed state として保存・再実行できる。
4. `jobs.query` と `jobs.insert` query job が GoogleSQL subset を実行し、`jobs.getQueryResults` / pagination で結果を取得できる。
5. `tabledata.insertAll` による streaming insert、schema validation、insertErrors、best-effort dedup semantics を再現できる。
6. load / extract / copy job、partitioning、clustering、views、routines、models、row access policies、IAM surface を段階的に追加できる。
7. 実 Google Cloud、BigQuery service、Cloud Storage service への依存なしに local process 内で動作する。

### Compatibility Levels

| Level | Name | Purpose |
| --- | --- | --- |
| L0 | Client Smoke | client library / curl が REST v2 endpoint に疎通できる |
| L1 | Catalog Core | project、dataset、table metadata CRUD が通る |
| L2 | Row Core | `tabledata.insertAll`、`tabledata.list`、schema validation が通る |
| L3 | Query Core | `jobs.query` / query job / `getQueryResults` が GoogleSQL subset を実行できる |
| L4 | Job Workflows | load、extract、copy、dryRun、job list/cancel/delete が通る |
| L5 | BigQuery Features | partitioning、clustering、views、routines、DML/DDL、UDF、row access policies が通る |
| L6 | Operational Parity | IAM policy surface、models、BI Engine compatible metadata、Storage API read/write を追加 |

MVP は L1 + L2 + L3 の最小 subset を対象にする。最終的な「完全互換」は L6 までを含む長期目標とする。

## Goals

- REQ-001: `devcloud up` で BigQuery REST API endpoint を起動する。
- REQ-002: BigQuery client libraries / REST clients が endpoint override で datasets、tables、tabledata、jobs の主要操作を実行できる。
- REQ-003: Dataset / Table / Job / TableData resource の JSON shape、`kind`、`etag`、`id`、`selfLink`、reference fields を互換形式で返す。
- REQ-004: `tabledata.insertAll` で JSON row を schema validation し、table storage と streaming buffer に保存する。
- REQ-005: `jobs.query` と `jobs.insert` query job で select / filter / projection / aggregation の GoogleSQL subset を実行する。
- REQ-006: `jobs.get`, `jobs.list`, `jobs.getQueryResults`, `jobs.cancel`, `jobs.delete` で job lifecycle を確認できる。
- REQ-007: load / extract / copy job は local filesystem と devcloud GCS/S3 object core に接続できる設計にする。
- REQ-008: OAuth / bearer auth は `relaxed` / `bearer-dev` / future `strict` mode で扱う。
- REQ-009: dashboard/API から projects、datasets、tables、schemas、rows、jobs、query results を確認できる。
- REQ-010: stage based verification script で REST contract、client smoke、query smoke、dashboard smoke を実行できる。

## Non-Goals

- 実 Google Cloud IAM、OAuth token introspection、Service Account Credentials API とは連携しない。
- 実 BigQuery の distributed execution、slots、reservations、BI Engine、remote functions、continuous queries は初期対象外とする。
- billing、quota、organization policy、VPC Service Controls、audit logs、Data Catalog integration は再現しない。
- BigQuery ML の実学習・推論、Vector Search、Analytics Hub、Data Clean Rooms は初期対象外とする。metadata compatibility から始める。
- Cloud Storage への load/extract は devcloud GCS/S3 object core との local integration とし、実 GCS には接続しない。
- 完全な GoogleSQL optimizer / execution engine を最初から実装しない。互換 subset と明示的な unsupported error から始める。
- BigQuery Storage Read/Write API の gRPC surface は REST v2 MVP の後に扱う。

## User Experience

```bash
devcloud init
devcloud up
```

REST API with curl:

```bash
export DEV_BQ=http://127.0.0.1:9050
export DEV_PROJECT=devcloud

curl -sS -X POST \
  "${DEV_BQ}/bigquery/v2/projects/${DEV_PROJECT}/datasets" \
  -H "Content-Type: application/json" \
  -d '{"datasetReference":{"projectId":"devcloud","datasetId":"demo"}}'

curl -sS -X POST \
  "${DEV_BQ}/bigquery/v2/projects/${DEV_PROJECT}/datasets/demo/tables" \
  -H "Content-Type: application/json" \
  -d '{"tableReference":{"projectId":"devcloud","datasetId":"demo","tableId":"people"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"age","type":"INTEGER"}]}}'

curl -sS -X POST \
  "${DEV_BQ}/bigquery/v2/projects/${DEV_PROJECT}/datasets/demo/tables/people/insertAll" \
  -H "Content-Type: application/json" \
  -d '{"rows":[{"insertId":"1","json":{"id":"user#1","age":37}}]}'

curl -sS -X POST \
  "${DEV_BQ}/bigquery/v2/projects/${DEV_PROJECT}/queries" \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT id, age FROM `devcloud.demo.people` WHERE age >= 30","useLegacySql":false}'
```

Client libraries should use:

```txt
endpoint: http://127.0.0.1:9050
project: devcloud
credentials: local emulator credential or anonymous relaxed mode
```

## Scope

### v0.1 BigQuery MVP

```txt
Daemon:
  BigQuery REST API endpoint  http://127.0.0.1:9050
  Dashboard/API               http://127.0.0.1:8025

Project API:
  GET  /bigquery/v2/projects
  GET  /bigquery/v2/projects/{projectId}/serviceAccount

Dataset API:
  POST   /bigquery/v2/projects/{projectId}/datasets
  GET    /bigquery/v2/projects/{projectId}/datasets/{datasetId}
  GET    /bigquery/v2/projects/{projectId}/datasets
  PATCH  /bigquery/v2/projects/{projectId}/datasets/{datasetId}
  PUT    /bigquery/v2/projects/{projectId}/datasets/{datasetId}
  DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}

Table API:
  POST   /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables
  GET    /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}
  GET    /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables
  PATCH  /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}
  PUT    /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}
  DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}

TableData API:
  POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/insertAll
  GET  /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/data

Job API:
  POST   /bigquery/v2/projects/{projectId}/queries
  POST   /bigquery/v2/projects/{projectId}/jobs
  GET    /bigquery/v2/projects/{projectId}/jobs/{jobId}
  GET    /bigquery/v2/projects/{projectId}/queries/{jobId}
  GET    /bigquery/v2/projects/{projectId}/jobs
  POST   /bigquery/v2/projects/{projectId}/jobs/{jobId}/cancel
  DELETE /bigquery/v2/projects/{projectId}/jobs/{jobId}/delete

Compatibility:
  JSON success/error response
  project/dataset/table/job references
  TableSchema / TableFieldSchema scalar and repeated fields
  insertAll insertErrors
  pageToken / maxResults basics
  selectedFields basics
  query job statistics compatibility subset
  BigQuery-compatible error reason / location / message shape
```

### v0.2 Query and Job Core

```txt
GoogleSQL subset:
  SELECT list
  FROM `project.dataset.table` / dataset.table
  WHERE comparison / AND / OR / NOT
  ORDER BY
  LIMIT / OFFSET
  GROUP BY
  COUNT / SUM / AVG / MIN / MAX
  INNER / LEFT JOIN basics
  query parameters

DML subset:
  INSERT
  UPDATE
  DELETE
  MERGE later

Job workflows:
  dryRun
  destinationTable
  writeDisposition
  createDisposition
  copy job
  load job from newline-delimited JSON and CSV
  extract job to local devcloud GCS/S3 object core
```

### Later

```txt
Views and materialized view metadata
DDL statements CREATE SCHEMA / TABLE / VIEW
Partitioned tables
Clustered tables
External tables
Routines and UDF metadata
Row access policies
IAM get/set/testIamPermissions compatibility
Models metadata
BigQuery Storage Read API
BigQuery Storage Write API
Reservations / capacity commitments compatibility metadata
Information schema and system tables
Streaming buffer visibility semantics
Time travel / table snapshots / clones
```

## Architecture

```txt
BigQuery client library / bq / REST client
        |
        v
+--------------------------------+
| BigQuery HTTP Gateway          | :9050
| REST v2 + Upload routes        |
+--------------------------------+
        |
        v
+--------------------------------+
| BigQuery API Adapter           |
| request -> command             |
+--------------------------------+
        |
        +------------------+--------------------+------------------+
        v                  v                    v                  v
+---------------+   +---------------+   +---------------+   +---------------+
| Catalog       |   | Table Storage |   | Job Runner    |   | SQL Engine    |
| projects      |   | rows/segments |   | async state   |   | GoogleSQL sub |
| datasets      |   | streaming buf |   | results       |   | planner/eval  |
+---------------+   +---------------+   +---------------+   +---------------+
        |                  |                    |                  |
        +------------------+--------------------+------------------+
                           |
                           v
              +---------------------------+
              | Filesystem Store          |
              | catalog/jobs/tables/rows  |
              +---------------------------+
                           |
                           v
              +---------------------------+
              | Dashboard/API             | :8025
              +---------------------------+
```

## Repository Layout

```txt
internal/
  app/
    config.go
    daemon.go

  services/
    bigquery/
      server.go
      router.go
      handlers_projects.go
      handlers_datasets.go
      handlers_tables.go
      handlers_tabledata.go
      handlers_jobs.go
      handlers_upload.go
      auth.go
      errors.go
      model.go
      schema.go
      row.go
      job.go
      sql.go
      service.go

  storage/
    bigquery/
      catalog_store.go
      table_store.go
      row_store.go
      job_store.go
      result_store.go

  dashboard/
    bigquery_static.go

web/
  dashboard/
    src/app/services/bigquery/
```

`storage/bigquery` は BigQuery の catalog/table/job semantics を閉じ込める。load/extract job は将来的に `internal/services/gcs` / `internal/services/s3` の object core と連携する。

## Configuration

Default config:

```yaml
project: dev

server:
  smtpPort: 1025
  dashboardPort: 8025
  s3Port: 4566
  gcsPort: 4443
  dynamodbPort: 8000
  bigqueryPort: 9050

auth:
  bigquery:
    mode: relaxed
    project: devcloud
    bearerToken: dev

storage:
  path: .devcloud/data

services:
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
```

### Config Integration

- `ServerConfig` に `BigQueryPort` を追加する。
- `AuthConfig` に `BigQuery` を追加する。
- `ServicesConfig` に `BigQuery` を追加する。
- default config YAML と `applyConfigValue` に mapping を追加する。
- `InitWorkspace` で `.devcloud/data/bigquery` を作成する。
- `ResetWorkspace` は `storage.path` 配下削除で BigQuery catalog / table / job state も削除する。

## API Mapping

### Dataset Operations

| BigQuery method | HTTP route | Internal command |
| --- | --- | --- |
| `datasets.insert` | `POST /bigquery/v2/projects/{projectId}/datasets` | `CreateDataset` |
| `datasets.get` | `GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}` | `GetDataset` |
| `datasets.list` | `GET /bigquery/v2/projects/{projectId}/datasets` | `ListDatasets` |
| `datasets.patch` | `PATCH /bigquery/v2/projects/{projectId}/datasets/{datasetId}` | `PatchDataset` |
| `datasets.update` | `PUT /bigquery/v2/projects/{projectId}/datasets/{datasetId}` | `UpdateDataset` |
| `datasets.delete` | `DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}` | `DeleteDataset` |

### Table Operations

| BigQuery method | HTTP route | Internal command |
| --- | --- | --- |
| `tables.insert` | `POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables` | `CreateTable` |
| `tables.get` | `GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}` | `GetTable` |
| `tables.list` | `GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables` | `ListTables` |
| `tables.patch` | `PATCH /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}` | `PatchTable` |
| `tables.update` | `PUT /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}` | `UpdateTable` |
| `tables.delete` | `DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}` | `DeleteTable` |

### TableData Operations

| BigQuery method | HTTP route | Internal command |
| --- | --- | --- |
| `tabledata.insertAll` | `POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/insertAll` | `InsertRows` |
| `tabledata.list` | `GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/data` | `ListRows` |

### Job Operations

| BigQuery method | HTTP route | Internal command |
| --- | --- | --- |
| `jobs.query` | `POST /bigquery/v2/projects/{projectId}/queries` | `RunQuerySync` |
| `jobs.insert` | `POST /bigquery/v2/projects/{projectId}/jobs` | `CreateJob` |
| `jobs.insert` upload | `POST /upload/bigquery/v2/projects/{projectId}/jobs` | `CreateLoadJobWithUpload` |
| `jobs.get` | `GET /bigquery/v2/projects/{projectId}/jobs/{jobId}` | `GetJob` |
| `jobs.getQueryResults` | `GET /bigquery/v2/projects/{projectId}/queries/{jobId}` | `GetQueryResults` |
| `jobs.list` | `GET /bigquery/v2/projects/{projectId}/jobs` | `ListJobs` |
| `jobs.cancel` | `POST /bigquery/v2/projects/{projectId}/jobs/{jobId}/cancel` | `CancelJob` |
| `jobs.delete` | `DELETE /bigquery/v2/projects/{projectId}/jobs/{jobId}/delete` | `DeleteJobMetadata` |

## Resource Model

### Dataset

```go
type Dataset struct {
    ProjectID     string
    DatasetID     string
    Location      string
    FriendlyName  string
    Description   string
    Labels        map[string]string
    Access        []AccessEntry
    CreatedAt     time.Time
    UpdatedAt     time.Time
    ETag          string
}
```

### Table

```go
type Table struct {
    ProjectID       string
    DatasetID       string
    TableID         string
    Type            TableType
    Schema          TableSchema
    TimePartitioning *TimePartitioning
    RangePartitioning *RangePartitioning
    Clustering      *Clustering
    View            *ViewDefinition
    Labels          map[string]string
    CreatedAt       time.Time
    UpdatedAt       time.Time
    NumRows         int64
    NumBytes        int64
    ETag            string
}
```

### Row

```go
type Row struct {
    RowID       string
    InsertID    string
    Values      map[string]Value
    InsertedAt  time.Time
    SegmentID   string
}
```

### Job

```go
type Job struct {
    ProjectID    string
    JobID        string
    Location     string
    Type         JobType
    State        JobState
    Configuration JobConfiguration
    Statistics   JobStatistics
    Status       JobStatus
    ResultRef    *ResultReference
    CreatedAt    time.Time
    StartedAt    *time.Time
    EndedAt      *time.Time
}
```

## Type System

### Supported BigQuery Types

| BigQuery type | Internal representation | MVP |
| --- | --- | --- |
| `STRING` | Go `string` | Yes |
| `BYTES` | `[]byte` base64 JSON | Yes |
| `INTEGER` / `INT64` | decimal string + int64 parse when safe | Yes |
| `FLOAT` / `FLOAT64` | float64 | Yes |
| `NUMERIC` / `BIGNUMERIC` | decimal string | Partial |
| `BOOLEAN` / `BOOL` | bool | Yes |
| `TIMESTAMP` | microsecond timestamp + RFC3339 output | Yes |
| `DATE` | ISO date string | Yes |
| `TIME` | ISO time string | Yes |
| `DATETIME` | ISO datetime string | Yes |
| `GEOGRAPHY` | WKT string | Partial |
| `JSON` | raw JSON value | Yes |
| `RECORD` / `STRUCT` | nested map | Yes |
| `RANGE` | typed range object | Later |
| `INTERVAL` | duration-like string | Later |

`mode` は `NULLABLE`、`REQUIRED`、`REPEATED` を実装する。schema validation は write boundary で行い、`ignoreUnknownValues` と `skipInvalidRows` の behavior を `insertAll` に反映する。

## Storage Layout

```txt
.devcloud/
  data/
    bigquery/
      projects/
        {project}/
          project.json
          datasets/
            {dataset}/
              dataset.json
              tables/
                {table}/
                  table.json
                  rows/
                    segments/
                      00000001.jsonl
                    streaming-buffer.jsonl
                  results/
                    {job-id}/
                      schema.json
                      rows.jsonl
              routines/
              models/
          jobs/
            {location}/
              {job-id}.json
```

MVP は JSON file / JSONL segment store から始める。将来の query performance 改善では columnar segment、SQLite/DuckDB adapter、または provider-neutral local analytical store を検討する。ただし API compatibility と deterministic behavior を優先し、初期段階では外部 database daemon を必須にしない。

## Request Handling

### REST Dispatch

BigQuery endpoint は REST v2 path を route table で dispatch する。

```txt
GET  /bigquery/v2/projects/{projectId}/datasets
POST /bigquery/v2/projects/{projectId}/queries
POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/insertAll
```

処理手順:

1. request ID を生成する。
2. path / method / query parameter を route definition と照合する。
3. auth mode に応じて bearer token を検証する。
4. operation ごとの request struct に JSON decode する。
5. resource reference と body reference の project/dataset/table consistency を検証する。
6. service command を実行する。
7. BigQuery-compatible JSON response または error response を返す。

### Error Response

BigQuery error は `error.errors[]` と top-level `code` / `message` / `status` を返す。

```json
{
  "error": {
    "code": 404,
    "message": "Not found: Dataset devcloud:missing",
    "errors": [
      {
        "message": "Not found: Dataset devcloud:missing",
        "domain": "global",
        "reason": "notFound"
      }
    ],
    "status": "NOT_FOUND"
  }
}
```

代表 mapping:

| Condition | HTTP | reason | status |
| --- | --- | --- | --- |
| dataset/table/job missing | 404 | `notFound` | `NOT_FOUND` |
| duplicate resource | 409 | `duplicate` | `ALREADY_EXISTS` |
| invalid schema/request | 400 | `invalid` | `INVALID_ARGUMENT` |
| unsupported SQL/API | 400 / 501 | `invalidQuery` / `notImplemented` | `INVALID_ARGUMENT` / `NOT_IMPLEMENTED` |
| auth failed | 401 / 403 | `authError` / `accessDenied` | `UNAUTHENTICATED` / `PERMISSION_DENIED` |
| job cancelled | 200 with job status error | `stopped` | `CANCELLED` |

## SQL Engine Strategy

### Principle

BigQuery compatibility depends heavily on GoogleSQL. Query support must be explicit, testable, and fail closed for unsupported syntax.

### Phased Engine

| Phase | Engine | Scope |
| --- | --- | --- |
| E0 | Parser stub | Accept dryRun and reject unsupported SQL with BigQuery-style errors |
| E1 | Minimal planner/evaluator | Single-table `SELECT` / `WHERE` / `LIMIT` / `ORDER BY` |
| E2 | Relational core | Aggregation, grouping, joins, query parameters |
| E3 | DML/DDL | `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `CREATE VIEW` |
| E4 | GoogleSQL parity layer | functions, structs, arrays, window functions, subqueries |

Implementation options:

1. Hand-rolled parser for E1 only.
2. Embed an existing SQL parser/evaluator if dependency and license are acceptable.
3. Use a pluggable `QueryEngine` interface so E1 implementation can be replaced without changing REST/job APIs.

Recommended default: implement `QueryEngine` interface first, start with a conservative E1 evaluator, and keep all unsupported syntax covered by tests.

```go
type QueryEngine interface {
    DryRun(ctx context.Context, request QueryRequest) (QueryPlan, error)
    Execute(ctx context.Context, request QueryRequest, catalog CatalogReader) (QueryResult, error)
}
```

## Job Lifecycle

```txt
PENDING -> RUNNING -> DONE
                  \-> DONE with error
PENDING/RUNNING -> CANCELLED
```

- `jobs.query` creates an implicit query job unless `JOB_CREATION_OPTIONAL` behavior can be safely emulated.
- synchronous query returns the first result page and a `jobReference`.
- long-running jobs are represented by persisted job metadata and result files.
- `jobs.getQueryResults` reads persisted result pages by `pageToken`.
- `jobs.insert` accepts query/load/copy/extract configuration and runs it through a local worker queue.

## Streaming Insert Semantics

`tabledata.insertAll` behavior:

1. Validate destination table exists.
2. Validate each row against table schema.
3. Apply `ignoreUnknownValues` and `skipInvalidRows`.
4. Track `insertId` in a bounded dedup index per table.
5. Append accepted rows to `streaming-buffer.jsonl`.
6. Return `insertErrors[]` indexed by original row position.

Streaming buffer flush can be deterministic in local mode:

```txt
insertAll -> streaming buffer
flush interval / query boundary -> row segments
query reads committed segments + active buffer
```

## Load / Extract / Copy Jobs

### Load Job

Supported sources:

- local uploaded multipart body via `/upload/bigquery/v2/projects/{projectId}/jobs`
- `gs://` URI resolved through devcloud GCS object store
- `s3://` URI resolved through devcloud S3 object store in later phase

Formats:

| Format | MVP | Notes |
| --- | --- | --- |
| NEWLINE_DELIMITED_JSON | Yes | Direct row decode |
| CSV | Yes | Header rows and delimiter basics |
| AVRO / PARQUET / ORC | Later | Metadata stub first |

### Extract Job

Extract writes result/table rows to devcloud GCS object store. MVP supports JSONL and CSV.

### Copy Job

Copy duplicates table metadata and row segments inside local BigQuery storage. `writeDisposition` and `createDisposition` must be enforced.

## Auth Modes

| Mode | Behavior |
| --- | --- |
| `off` | No auth check. |
| `relaxed` | Accept anonymous or bearer token; never logs token contents. |
| `bearer-dev` | Require `Authorization: Bearer {configured token}`. |
| `strict` | Reserved for future local JWT/JWKS verification. |

`Authorization` headers and access tokens must not be logged. Dashboard APIs expose auth mode but not tokens.

## Dashboard Integration

### Service Registry

`/api/dashboard/services` adds:

```json
{
  "id": "bigquery",
  "name": "BigQuery",
  "path": "/dashboard/bigquery",
  "status": "running",
  "endpoint": "http://127.0.0.1:9050",
  "storagePath": ".devcloud/data/bigquery"
}
```

Compatibility entry routes:

```txt
GET /bigquery             legacy service page entry, returns the shared dashboard app
GET /dashboard/bigquery   shared React dashboard route
```

### Dashboard API

```txt
GET /api/bigquery/status
GET /api/bigquery/projects
GET /api/bigquery/projects/{project}/datasets
GET /api/bigquery/projects/{project}/datasets/{dataset}/tables
GET /api/bigquery/projects/{project}/datasets/{dataset}/tables/{table}/rows?limit=100
GET /api/bigquery/projects/{project}/jobs
GET /api/bigquery/projects/{project}/jobs/{job}
POST /api/bigquery/projects/{project}/queries
POST /api/bigquery/projects/{project}/datasets
POST /api/bigquery/projects/{project}/datasets/{dataset}/tables
POST /api/bigquery/projects/{project}/datasets/{dataset}/tables/{table}/insertAll
```

### UI Surface

- Project/dataset/table navigator.
- Schema inspector.
- Row preview table.
- SQL query runner for the local GoogleSQL subset with `useLegacySql=false`, dry run, max results, result table, selected result JSON, and job reference.
- Guarded BigQuery dashboard management controls for `datasets.insert`, `tables.insert`, and `tabledata.insertAll`.
- Guided create dataset/table fields plus raw JSON mode for local request shapes.
- Insert row JSON editor with client-side validation and partial insert error display.
- Job list, recent query metadata, selected job JSON, and job detail.
- Error panel showing BigQuery-style `reason`, `location`, and `message`.
- Disabled BigQuery state keeps mutation and query controls unavailable.

Dashboard safety boundaries:

- All mutations go through `/api/bigquery/*` or the local BigQuery REST API path; dashboard code does not call storage directly.
- Row payloads, credentials, Authorization headers, bearer tokens, and full request bodies are not persisted or logged by dashboard management flows.
- Selected job JSON shown in the dashboard redacts query parameter values from the UI detail view.

## Implementation Plan

### Phase 1: Foundation

| ID | Task | Output |
| --- | --- | --- |
| IMPL-001 | Add `docs/design-bigquery-compat.md` | Reviewable compatibility design |
| IMPL-002 | Add config shape: `server.bigqueryPort`, `auth.bigquery`, `services.bigquery` | Defaults and config parser support |
| IMPL-003 | Add daemon lifecycle wiring with disabled-by-config behavior | BigQuery server starts/stops with `devcloud up` |
| IMPL-004 | Add empty BigQuery HTTP server with status route and BigQuery-style error writer | REST gateway foundation |
| IMPL-005 | Add dashboard service registry entry and `/bigquery` compatibility route | Service appears in shared dashboard |

Acceptance:

```bash
go test ./...
go run ./cmd/devcloud help
curl -fsS http://127.0.0.1:9050/bigquery/v2/projects
```

### Phase 2: Catalog and TableData MVP

| ID | Task | Output |
| --- | --- | --- |
| IMPL-101 | Implement project/dataset/table stores | Filesystem-backed catalog |
| IMPL-102 | Implement datasets and tables CRUD | REST v2 catalog API |
| IMPL-103 | Implement schema validation | BigQuery type/mode validation |
| IMPL-104 | Implement `tabledata.insertAll` and `tabledata.list` | Row insert/list workflow |
| IMPL-105 | Add dashboard table/row inspection | UI/API row preview |

Acceptance:

```bash
curl create dataset
curl create table
curl insertAll rows
curl tabledata.list
```

### Phase 3: Query MVP

| ID | Task | Output |
| --- | --- | --- |
| IMPL-201 | Add query job model and job store | Persisted job lifecycle |
| IMPL-202 | Add `jobs.query`, `jobs.insert` query job, `jobs.get`, `jobs.getQueryResults` | Query job REST surface |
| IMPL-203 | Implement GoogleSQL E1 evaluator | Single-table query subset |
| IMPL-204 | Add query dashboard panel | Query editor and results view |

Acceptance:

```bash
curl jobs.query SELECT id FROM `devcloud.demo.people`
curl jobs.getQueryResults
```

### Phase 4: Job Workflows

| ID | Task | Output |
| --- | --- | --- |
| IMPL-301 | Add load job from upload body and devcloud GCS | Local load workflow |
| IMPL-302 | Add extract job to devcloud GCS | Local extract workflow |
| IMPL-303 | Add copy job | Table copy workflow |
| IMPL-304 | Add job cancellation and deletion semantics | Operational job lifecycle |

### Phase 5: Compatibility Expansion

| ID | Task | Output |
| --- | --- | --- |
| IMPL-401 | Add partitioning and clustering metadata | Table feature metadata |
| IMPL-402 | Add DML/DDL subset | Broader GoogleSQL workflows |
| IMPL-403 | Add views and routines metadata | Catalog parity expansion |
| IMPL-404 | Add IAM policy compatibility responses | Policy surface |
| IMPL-405 | Add BigQuery Storage API design and implementation loop | gRPC/API expansion plan |

## Verification Plan

### Unit Tests

- REST route parsing.
- Dataset/table ID validation.
- Table schema validation.
- Row encoding and type conversion.
- `insertAll` error indexing.
- BigQuery error response shape.
- Job state transitions.
- Query parser/evaluator subset.

### Contract Tests

```txt
datasets.insert/get/list/patch/delete
tables.insert/get/list/patch/delete
tabledata.insertAll/list
jobs.query
jobs.insert query
jobs.get
jobs.getQueryResults
jobs.list
jobs.cancel
```

### E2E Script

```bash
scripts/bigquery-e2e.sh
```

Test journey:

1. start `devcloud up` on free ports.
2. create dataset.
3. create table with schema.
4. insert rows through `insertAll`.
5. list rows through `tabledata.list`.
6. run `jobs.query`.
7. fetch results through `jobs.getQueryResults`.
8. inspect dashboard API.
9. optionally keep data with `E2E_INTERACTIVE=true E2E_DELETE_DATA=false`.

### Stage Gates

```txt
foundation: design doc, config shape, server starts, dashboard registry
catalog: datasets/tables CRUD
tabledata: insertAll/list
query: jobs.query/getQueryResults
jobs: insert load/copy/extract basics
dashboard: UI/API inspection
full: all of the above + client smoke
```

## Compatibility Matrix

| Area | v0.1 | v0.2 | Later |
| --- | --- | --- | --- |
| REST v2 datasets | CRUD | undelete metadata | access/IAM parity |
| REST v2 tables | CRUD | partition/clustering metadata | snapshots/clones/external tables |
| tabledata | insertAll/list | streaming buffer visibility | Storage Write API |
| jobs.query | SELECT subset | parameters/aggregation/join | broader GoogleSQL |
| jobs.insert | query | load/copy/extract | full job configs |
| load formats | JSONL/CSV | compression basics | Avro/Parquet/ORC |
| extract formats | JSONL/CSV | compression basics | Avro/Parquet |
| routines/models | metadata stub | CRUD metadata | executable UDF/ML later |
| IAM | relaxed auth | policy get/set/test stubs | local policy enforcement |
| dashboard | status/catalog/table rows | jobs/query editor | query plan/result visualization |

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| GoogleSQL compatibility is very large | High | Explicit query levels; unsupported syntax returns BigQuery-style errors |
| Client library endpoint override differs by language | Medium | Test Go / Python / Node client smoke where practical |
| `bq` CLI endpoint override may be awkward | Medium | Prioritize REST/client smoke first; document CLI limitations |
| Load/extract crosses GCS/S3 object semantics | Medium | Use adapter boundary and test with devcloud GCS first |
| JSON schema drift from official API | Medium | Keep resource structs focused; add contract fixtures from docs |
| Large local tables can be slow | Medium | Start with deterministic file store; add segment/index strategy later |
| Auth logs could leak tokens | High | Never log `Authorization`; redact request headers in errors |
| Dashboard query editor can imply full SQL support | Medium | Display supported subset and exact error reason from API |

## Open Questions

1. Should BigQuery endpoint default to `9050` or another port to avoid common local service conflicts?
2. Should query execution depend on an embedded SQL engine, or should MVP use a narrow internal evaluator?
3. Should load/extract jobs integrate with devcloud GCS only first, or also S3 from the start?
4. Which client library should be the first automated smoke target: Go, Python, or Node.js?
5. Should BigQuery Storage API be a separate service/port because it is gRPC, or share configuration with REST v2?

## Initial Acceptance Criteria

| ID | Criterion |
| --- | --- |
| AC-001 | `go run ./cmd/devcloud up` starts BigQuery on `127.0.0.1:9050`. |
| AC-002 | `GET /bigquery/v2/projects` returns local project metadata. |
| AC-003 | A dataset can be created, listed, fetched, patched, and deleted. |
| AC-004 | A table with schema can be created, listed, fetched, patched, and deleted. |
| AC-005 | `tabledata.insertAll` accepts valid rows and returns indexed errors for invalid rows. |
| AC-006 | `jobs.query` can run a simple `SELECT` over inserted rows. |
| AC-007 | `jobs.getQueryResults` returns paginated rows with `TableSchema`. |
| AC-008 | Dashboard shows BigQuery service, datasets, tables, rows, jobs, and query results. |
| AC-009 | `VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh` passes. |

## References

- BigQuery REST API v2: https://cloud.google.com/bigquery/docs/reference/rest/
- Datasets resource: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets
- Tables resource: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
- `tabledata.insertAll`: https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
- Jobs resource: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
- `jobs.query`: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
- `jobs.insert`: https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert

## Change History

| Date | Change |
| --- | --- |
| 2026-05-01 | Initial BigQuery compatibility design draft. |
