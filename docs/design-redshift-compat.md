# Amazon Redshift Compatibility Design

## Summary

`devcloud` に Amazon Redshift 互換の local analytical database server を追加する。

目標は「Redshift を使う開発者が endpoint override と local credentials だけで、SQL client、AWS CLI / SDK の Redshift Data API、Redshift 管理 API の主要 workflow を実行できる」ことである。Redshift の完全互換は PostgreSQL 系 SQL wire protocol、Redshift SQL dialect、MPP / columnar behavior、Data API、provisioned cluster 管理 API、Serverless 管理 API、COPY / UNLOAD、system catalog、IAM / Secrets Manager 連携、WLM、snapshot、Spectrum、datashare まで広いため、実装は段階化する。

初期実装では「local 開発とテストに必要な互換」を優先する。SQL endpoint は Redshift の既定 port である `5439` を使い、`psql`、JDBC / ODBC compatible clients、Redshift Data API の `ExecuteStatement` / `GetStatementResult` workflow を最短で成立させる。管理 API は cluster / namespace / workgroup を local metadata として返し、AWS SDK / CLI の起動確認、dashboard、e2e で使える control plane を提供する。

DB 実行部分は PostgreSQL backend を第一候補にする。`devcloud` は Redshift 互換 API / wire protocol / dialect translation / Data API lifecycle / control plane metadata を担当し、通常の SQL 実行、transaction、catalog の大部分は PostgreSQL に委譲する。`DISTKEY`、`SORTKEY`、`ENCODE`、`COPY FROM s3://...`、`UNLOAD TO s3://...`、Redshift 固有関数、`stl` / `stv` / `svv` system views は互換レイヤーで吸収し、PostgreSQL 向け SQL または devcloud side effect に変換する。

`docs/spec-v0.md` の方針通り、Redshift は PostgreSQL wire protocol server、Redshift dialect adapter、SQL backend の3層に分ける。BigQuery と Redshift が共有できる Query Engine boundary は残すが、Redshift の primary backend は PostgreSQL に寄せる。BigQuery は必要に応じて同じ logical plan interface を使い、Redshift 固有の API / protocol / dialect 差分は adapter 層で扱う。

## Document Control

| Field | Value |
| --- | --- |
| Audience | devcloud implementers, reviewers, future agent loops |
| Status | Draft |
| Owner | devcloud maintainers |
| Reviewer | TBD |
| Related docs | `docs/spec-v0.md`, `docs/design-s3-compat.md`, `docs/design-dynamodb-compat.md`, `docs/design-bigquery-compat.md`, `docs/design-dashboard-shell.md` |
| Primary references | Amazon Redshift API Reference, Amazon Redshift Data API Reference, Amazon Redshift SQL command reference, COPY / UNLOAD docs, Redshift JDBC / ODBC docs |
| Reference check date | 2026-05-04 |

## Compatibility Goal

### Definition

ここでの Redshift 互換は、以下を満たす状態を指す。

1. SQL client が `host=127.0.0.1 port=5439 sslmode=disable` で接続できる。
2. PostgreSQL startup / auth / query protocol のうち、Redshift client libraries と `psql` が使う主要 path を処理できる。
3. Redshift SQL dialect の DDL / DML / query / transaction / COPY / UNLOAD の主要 subset を PostgreSQL backend と devcloud side effects の組み合わせで実行できる。
4. AWS CLI / SDK の `redshift-data` client が endpoint override で `ExecuteStatement`、`BatchExecuteStatement`、`DescribeStatement`、`GetStatementResult`、`ListStatements`、metadata list 系を実行できる。
5. AWS CLI / SDK の `redshift` client が endpoint override で provisioned cluster metadata、snapshot、parameter group、tag、temporary credential 系の主要 API を実行できる。
6. Redshift Serverless の namespace / workgroup metadata API を local metadata として段階的に提供できる。
7. `pg_catalog`、`information_schema`、Redshift system views の代表 subset を返し、drivers / BI tools の introspection を通す。
8. 実 AWS、LocalStack、実 Redshift cluster には依存しない。PostgreSQL backend は devcloud 管理または明示 DSN の local dependency として扱い、`.devcloud/data/redshift` 配下の metadata / statement state と組み合わせて deterministic に動作する。

### Compatibility Levels

| Level | Name | Purpose |
| --- | --- | --- |
| L0 | Endpoint Smoke | SQL endpoint と HTTP API endpoint が起動し、health と basic auth が通る |
| L1 | SQL Client Core | `psql` / driver が接続し、simple query protocol で `select 1` と catalog introspection が通る |
| L2 | SQL Resource Core | PostgreSQL backend 経由で database / schema / table / view metadata、CREATE / DROP / INSERT / SELECT の基本 workflow が通る |
| L3 | Data API Core | `ExecuteStatement` から SQL を実行し、非同期 statement lifecycle と result paging が通る |
| L4 | Redshift Dialect | DISTKEY / SORTKEY / ENCODE / IDENTITY / CTAS / transaction / COPY / UNLOAD を変換層で吸収して通す |
| L5 | Control Plane | cluster、snapshot、parameter group、tag、credential、serverless workgroup / namespace metadata が通る |
| L6 | Advanced Parity | stored procedure、UDF、materialized view、Spectrum metadata、WLM、datashare、strict error compatibility を追加 |

MVP は L0 + L1 + L2 + L3 を対象にする。最終的な「完全互換」は L6 までを含む長期目標とする。

## Goals

- REQ-001: `devcloud up` で Redshift SQL listener を `127.0.0.1:5439` に起動する。
- REQ-002: `devcloud up` で Redshift HTTP API listener を `127.0.0.1:9099` に起動する。
- REQ-003: `psql` から password auth または relaxed auth で接続できる。
- REQ-004: PostgreSQL simple query protocol を実装し、extended query protocol は段階的に追加できる boundary を用意する。
- REQ-005: Redshift SQL dialect の parser / translator / backend boundary を定義し、SQL backend を service adapter から分離する。
- REQ-005a: Redshift 固有の syntax / catalog / error は dialect adapter に閉じ込め、PostgreSQL backend には変換済み SQL と安全な parameter だけを渡す。
- REQ-005b: PostgreSQL backend を primary SQL execution engine とし、現在の compatibility 製簡易 executor は migration 中の fallback / contract test fixture として扱えるようにする。
- REQ-006: database、schema、table、column、view、user、group、role、query history を catalog として永続化する。
- REQ-007: `CREATE SCHEMA`、`CREATE TABLE`、`DROP TABLE`、`INSERT`、`SELECT`、`UPDATE`、`DELETE`、`BEGIN`、`COMMIT`、`ROLLBACK` の MVP subset を実装できる。
- REQ-008: Redshift 固有 table attributes を metadata として保持する。初期は実行計画には反映せず、catalog / introspection / dashboard で確認できる状態から始める。
- REQ-009: `COPY` は local S3 service または local file から CSV / JSON を読み込み、PostgreSQL `COPY FROM STDIN` または parameterized insert に変換する。`UNLOAD` は PostgreSQL query result を読み取り、local S3 service または local file に書き出す。
- REQ-010: Data API の statement lifecycle を `SUBMITTED`、`STARTED`、`FINISHED`、`FAILED`、`ABORTED` として保持する。
- REQ-011: Data API の `ClientToken` idempotency、`SessionId`、`SessionKeepAliveSeconds` を local deterministic model で扱う。
- REQ-012: Redshift 管理 API の cluster metadata は local cluster descriptor として返し、endpoint、port、database、node type、status、tags を保持する。
- REQ-013: dashboard/API から clusters、databases、schemas、tables、query history、recent statements、COPY / UNLOAD jobs を確認できる。
- REQ-014: `devcloud reset` で Redshift catalog、table data、statement results、snapshots、session state を削除できる。
- REQ-015: stage based verification script で SQL client smoke、Data API smoke、management API contract、dashboard smoke、e2e を実行できる。

## Non-Goals

- 実 AWS Redshift、Redshift Serverless、LocalStack には依存しない。
- production PostgreSQL cluster には依存しない。PostgreSQL backend は devcloud 管理の local process、test container、または明示 DSN の local database として扱う。
- MPP execution、columnar compression、zone map、workload management、concurrency scaling、RA3 managed storage の実性能は再現しない。
- IAM、Secrets Manager、KMS、VPC、security group、subnet、billing、quota、CloudWatch、CloudTrail は初期対象外とする。
- 実 Redshift と同じ planner / optimizer / cost model は実装しない。
- Spectrum の外部 table に対する実 S3 / Glue / Lake Formation 権限連携は初期対象外とする。
- PL/pgSQL stored procedure、Python / Lambda UDF、ML、datashare、cross-account feature は初期対象外とする。
- TLS / SSL は初期では disabled local endpoint を前提にする。strict mode で後続対応する。
- production database としての durability / isolation / performance guarantee は提供しない。

## User Experience

```bash
devcloud init
devcloud up
```

SQL client:

```bash
psql "host=127.0.0.1 port=5439 dbname=dev user=dev password=dev sslmode=disable"
```

JDBC:

```txt
jdbc:redshift://127.0.0.1:5439/dev?ssl=false
```

AWS CLI Data API:

```bash
aws redshift-data execute-statement \
  --endpoint-url http://127.0.0.1:9099 \
  --region us-east-1 \
  --cluster-identifier devcloud \
  --database dev \
  --db-user dev \
  --sql "select 1"
```

```bash
aws redshift-data get-statement-result \
  --endpoint-url http://127.0.0.1:9099 \
  --region us-east-1 \
  --id "$STATEMENT_ID"
```

AWS CLI management API:

```bash
aws redshift describe-clusters \
  --endpoint-url http://127.0.0.1:9099 \
  --region us-east-1
```

COPY / UNLOAD with local S3:

```sql
create table events(id int, payload varchar(256));

copy events
from 's3://demo-bucket/events.csv'
iam_role default
csv;

unload ('select * from events')
to 's3://demo-bucket/exports/events_'
iam_role default
csv;
```

Dashboard:

```txt
http://127.0.0.1:8025/dashboard/redshift
```

## Scope

### In Scope

| Area | MVP | Target |
| --- | --- | --- |
| SQL listener | startup, relaxed/strict password auth, simple query protocol, extended query protocol subset | broader JDBC/ODBC matrix and binary protocol formats |
| SQL DDL | CREATE / DROP SCHEMA, CREATE / DROP TABLE, CTAS, views, materialized-view metadata | ALTER, temp tables, deeper materialized-view behavior |
| SQL DML | INSERT, SELECT, UPDATE, DELETE, MERGE local subset | stricter Redshift transaction and error semantics |
| Redshift dialect | DISTKEY / SORTKEY / ENCODE metadata accepted, COPY / UNLOAD side effects, common function rewrites | external schema metadata, broader SQL grammar |
| Data API | Execute, describe, get result, list statements, batch/session subset | cancel behavior, CSV edge cases, stricter async lifecycle |
| Management API | clusters, parameter groups, tags, credentials, snapshots, restore metadata | broader low-value network fields and event history |
| Serverless API | namespace / workgroup metadata | deeper SDK field parity |
| Catalog | database, schema, table, column, query history, system/catalog/workload metadata subset | users, roles, groups, privileges, broader system tables |
| Dashboard | status, tables, query history, query runner | COPY / UNLOAD inspector and advanced metadata editors |
| E2E | psql + AWS CLI + dashboard/API smoke | SDK matrix, Redshift JDBC/ODBC, BI tool introspection smoke |

### Out of Scope Until Advanced Phases

- Distributed query execution and parallel node simulation.
- Redshift optimizer hints and exact explain plans.
- Serializable isolation parity.
- Full SQL grammar and full PostgreSQL function library beyond what PostgreSQL backend already provides.
- Full IAM policy evaluation.
- Production-grade TLS certificate management.
- Cross-service AWS auth propagation beyond local relaxed / strict checks.

## Architecture

```txt
                         +-----------------------------+
 psql / JDBC / ODBC ---> | Redshift SQL Listener :5439 |
                         +-------------+---------------+
                                       |
                                       v
                         +-------------+---------------+
                         | Session / Auth / PgWire     |
                         +-------------+---------------+
                                       |
 AWS CLI / SDK ----------+             |
 redshift-data/redshift  |             v
                         | +-----------+---------------+
                         +>| Redshift HTTP API :9099   |
                           +-----------+---------------+
                                       |
 Dashboard API ----------+             v
                         | +-----------+---------------+
                         +>| Redshift Service Facade   |
                           +-----+-------------+-------+
                                 |             |
                                 v             v
                         +-------+----+  +-----+----------------+
                         | Metadata   |  | Redshift Translator |
                         | Store      |  | + Side Effect Router|
                         +-------+----+  +-----+----------------+
                                 |             |
                                 |             +----------------+
                                 |                              |
                                 v                              v
                         +-------+-------------+-------+  +-----+----------------+
                         | Filesystem Redshift State  |  | PostgreSQL Backend  |
                         | .devcloud/data/redshift    |  | local / configured  |
                         +---------------+------------+  +-----+----------------+
                                         |
                                         v
                         +---------------+-------------+
                         | Optional local S3 adapter   |
                         +-----------------------------+
```

### Components

| Component | Package | Responsibility |
| --- | --- | --- |
| Daemon wiring | `orchestrator` | config load, listener startup, reset integration |
| SQL listener | `services/redshift/pgwire` | PostgreSQL wire protocol startup, auth, query messages |
| HTTP API | `services/redshift/api` | AWS Redshift Data API / management API request parsing and responses |
| Service facade | `services/redshift` | shared business operations across SQL, Data API, dashboard |
| Metadata store | `services/redshift/catalog` | Redshift-only metadata, cluster metadata, dist/sort/encode attributes, statement history |
| PostgreSQL backend | `services/redshift/backend/postgres` | SQL execution, transaction behavior, base catalog, result sets |
| Fallback backend | `services/redshift/backend/memory` | migration-only compatibility fixture for tests and no-postgres development |
| Statement store | `services/redshift/statement` | Data API statement lifecycle, result pages, sessions |
| Redshift translator | `services/redshift/translator` | Redshift SQL parsing, function rewrite, attribute extraction, PostgreSQL SQL generation |
| Side effect router | `services/redshift/sideeffects` | COPY / UNLOAD / system view / metadata-only operations handled by devcloud |
| Dashboard API | `services/dashboard` | Redshift route, status, query history, resource operations |
| Dashboard UI | `web/dashboard/src/app/services/redshift` | service-specific management UI |
| Verification | `scripts/redshift-autoloop`, `scripts/redshift-e2e.sh` | stage gates and acceptance smoke |

## Repository Layout

```txt
services/
  app/
    config.rs
    daemon.rs
  services/
    redshift/
      service.rs
      errors.rs
      models.rs
      pgwire/
        listener.rs
        protocol.rs
        auth.rs
      api/
        data_api.rs
        management_api.rs
        serverless_api.rs
        aws_json.rs
        aws_query.rs
      backend/
        backend.rs
        postgres/
          backend.rs
          catalog.rs
          tx.rs
        memory/
          backend.rs
      translator/
        parser.rs
        rewrite.rs
        functions.rs
        metadata.rs
      sideeffects/
        copy.rs
        unload.rs
        system_views.rs
  storage/
    redshift/
      catalog/
        store.rs
      statement/
        store.rs
      snapshot/
        store.rs
web/
  dashboard/src/app/services/redshift/
    api.ts
    types.ts
    RedshiftDashboard.tsx
scripts/
  redshift-autoloop/
    README.md
    goal.md
    verify.sh
    run-loop.sh
    recover.sh
  redshift-e2e.sh
docs/
  design-redshift-compat.md
```

## Configuration

### Default Config

```yaml
server:
  redshiftPort: 5439
  redshiftAPIPort: 9099

auth:
  redshift:
    mode: relaxed
    user: dev
    password: dev
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"

services:
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
      externalDsn: ""
      managed: true
    dataApi:
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

### Config Integration

- `ServerConfig` に `RedshiftPort` と `RedshiftAPIPort` を追加する。
- `AuthConfig` に `RedshiftAuthConfig` を追加する。
- `ServicesConfig` に `RedshiftServiceConfig` を追加する。
- `DefaultConfig()` は local Redshift の一般的な接続を優先し、SQL port は `5439`、HTTP API port は `9099` とする。
- `services.redshift.backend.kind` は `postgres` を default backend とする。`backend.kind=memory` は明示設定された場合だけ使う開発継続用 fallback として残す。
- `services.redshift.backend.mode` は `managed` または `external` を持つ。`managed` は devcloud が local PostgreSQL process を起動する。`external` は `externalDsn` で指定された local PostgreSQL に接続する。
- managed PostgreSQL は bundled binary や container ではなく、`PATH` 上の system `initdb`、system `postgres`、system `psql` を利用する。見つからない場合は、PostgreSQL binaries のインストールまたは `services.redshift.backend.mode=external` と `externalDsn` の設定を促す actionable error を返す。
- `devcloud reset` は `.devcloud/data/redshift`、Redshift statement temp files、managed PostgreSQL data directory を削除する。`external` DSN の database は明示 opt-in なしに drop しない。
- disabled service では SQL listener と HTTP API listener の両方を起動しない。

## API Mapping

### SQL Wire Protocol

| Protocol Message | MVP | Target | Notes |
| --- | --- | --- | --- |
| StartupMessage | Yes | Yes | user, database, client encoding, application name を読む |
| SSLRequest | Reject | Optional | MVP は `N` を返す |
| AuthenticationCleartextPassword | Yes | Yes | relaxed / strict mode |
| AuthenticationMD5Password | No | Yes | strict mode の後続対応 |
| Query | Yes | Yes | simple query protocol |
| Parse / Bind / Describe / Execute / Sync | No | Yes | JDBC / BI tool compatibility phase |
| Terminate | Yes | Yes | session cleanup |
| CopyData / CopyDone | No | Optional | Redshift `COPY` SQL を優先し、protocol-level COPY は後続 |
| ParameterStatus | Yes | Yes | server_version, client_encoding, DateStyle など |
| RowDescription / DataRow / CommandComplete | Yes | Yes | text format first |
| ErrorResponse / NoticeResponse | Yes | Yes | SQLSTATE と Redshift-like message |

### Data API

| Operation | MVP | Target | Behavior |
| --- | --- | --- | --- |
| `ExecuteStatement` | Yes | Yes | one SQL statementを非同期 statement として登録し、local executor を起動 |
| `BatchExecuteStatement` | Partial | Yes | MVP は serial transaction または validation error、target は all-or-rollback |
| `DescribeStatement` | Yes | Yes | status, duration, result rows, error, query text metadata |
| `GetStatementResult` | Yes | Yes | JSON records, pagination |
| `GetStatementResultV2` | No | Yes | CSV format and new shape |
| `ListStatements` | Yes | Yes | session / status / pagination filter |
| `CancelStatement` | Partial | Yes | queued/running local statement を aborted にする |
| `ListDatabases` | Yes | Yes | catalog database list |
| `ListSchemas` | Yes | Yes | catalog schema list |
| `ListTables` | Yes | Yes | catalog table list |
| `DescribeTable` | Yes | Yes | column metadata |

### Provisioned Redshift Management API

| Operation Group | MVP | Target | Notes |
| --- | --- | --- | --- |
| Clusters | `DescribeClusters` | `CreateCluster`, `DeleteCluster`, `ModifyCluster` | local metadata only |
| Credentials | Partial `GetClusterCredentials` | IAM-like constraints | returns generated local db user/password in relaxed mode |
| Snapshots | No | create/describe/delete/restore | filesystem copy or manifest |
| Parameter groups | Describe default | create/modify/delete | metadata only |
| Tags | describe/create/delete | full filtering | common AWS tag model |
| Events | No | describe events | local operation log |
| Logging / HSM / subnet / security groups | No | metadata only | no real network enforcement |

### Redshift Serverless API

| Operation Group | MVP | Target | Notes |
| --- | --- | --- | --- |
| Workgroups | No | list/get/create/delete | maps to SQL endpoint metadata |
| Namespaces | No | list/get/create/delete | maps to database/catalog root |
| Credentials | No | get credentials | local relaxed credentials |
| Snapshots | No | list/create/restore | shared snapshot store |

## Resource Model

### Cluster

| Field | Source | Notes |
| --- | --- | --- |
| `ClusterIdentifier` | config / create API | default `devcloud` |
| `ClusterStatus` | service lifecycle | `available`, `creating`, `deleting` |
| `Endpoint.Address` | config | `127.0.0.1` by default |
| `Endpoint.Port` | config | `5439` |
| `DBName` | config / catalog | default `dev` |
| `MasterUsername` | auth config | default `dev` |
| `NodeType` | config | metadata only |
| `NumberOfNodes` | config | metadata only |
| `Tags` | management API | persisted |

### Catalog

| Entity | Key | Stored Data |
| --- | --- | --- |
| Database | database name | owner, encoding, created_at |
| Schema | database + schema name | owner, privileges, created_at |
| Table | database + schema + table name | columns, dist style, sort keys, backup flag, temp flag |
| Column | table + ordinal | name, type, nullable, default, identity, encode |
| View | database + schema + view name | SQL definition, late-binding flag |
| User | user name | auth metadata, groups, privileges |
| Query | query id | SQL text digest, status, timestamps, row count, error |
| Statement | Data API statement id | request, session, status, result pointer |

### SQL Data

SQL table data は PostgreSQL backend に保存する。`.devcloud/data/redshift` は Redshift 互換レイヤー固有の metadata、statement lifecycle、query history、snapshot manifest、managed PostgreSQL data directory を保持する。Redshift の columnar storage は再現しないが、table attributes は metadata として保持する。

```txt
.devcloud/data/redshift/
  clusters/
    devcloud.json
  catalog/
    databases/dev.json
    schemas/dev/public.json
    tables/dev/public/events.json
  postgres/
    base/...
    postgresql.conf
  statements/
    2026/05/04/<statement-id>.json
    2026/05/04/<statement-id>.results.jsonl
  sessions/
    <session-id>.json
  snapshots/
    <snapshot-id>/
      manifest.json
```

`external` PostgreSQL mode では `.devcloud/data/redshift/postgres` を使わず、metadata と statement state のみを保存する。

## Type System

| Redshift Type | Internal Type | MVP Notes |
| --- | --- | --- |
| `SMALLINT`, `INTEGER`, `BIGINT` | signed integer | range validation |
| `DECIMAL`, `NUMERIC` | decimal string + scale metadata | exact arithmetic later |
| `REAL`, `DOUBLE PRECISION` | float64 | NaN/Infinity policy documented |
| `BOOLEAN` | bool | Redshift literal aliases accepted |
| `CHAR`, `VARCHAR` | string | length validation |
| `DATE` | date string | ISO parse |
| `TIME`, `TIMETZ` | time string | timezone metadata |
| `TIMESTAMP`, `TIMESTAMPTZ` | timestamp string | UTC normalization optional |
| `VARBYTE` | bytes | base64 in JSON storage |
| `SUPER` | JSON value | PartiQL-like query support later |
| `GEOMETRY`, `GEOGRAPHY` | string/bytes metadata | advanced phase |

Data API result type mapping follows Redshift Data API の JDBC-like result shape を前提にし、MVP は `longValue`、`doubleValue`、`booleanValue`、`stringValue`、`blobValue`、`isNull` を返す。

## SQL Backend and Translation Strategy

### Boundary

SQL execution は protocol、API、metadata store から分離する。Redshift adapter は SQL を受け取り、必要に応じて devcloud side effect と PostgreSQL backend SQL に分解する。

```go
type SQLBackend interface {
    Exec(ctx context.Context, session Session, sql string, params []Parameter) (ResultSet, error)
    Begin(ctx context.Context, session Session) (Transaction, error)
    Catalog(ctx context.Context, database string) (CatalogSnapshot, error)
    Close() error
}

type RedshiftTranslator interface {
    Translate(ctx context.Context, session Session, sql string) (TranslationResult, error)
}

type TranslationResult struct {
    BackendSQL        string
    Parameters        []Parameter
    MetadataEffects   []MetadataEffect
    SideEffects       []SideEffect
    HandledByDevcloud bool
}
```

この boundary により、Redshift wire protocol / Data API / dashboard query runner は同じ `RedshiftService.ExecuteSQL` を呼び、実行先だけを `postgres` または migration-only `memory` に切り替えられる。production dependency を追加する場合は、license、binary portability、connection pooling、test determinism、local install path を確認する。

### PostgreSQL Backend

PostgreSQL backend は Redshift SQL の最終実行先である。

- `CREATE SCHEMA`、通常の `CREATE TABLE`、`INSERT`、`SELECT`、`UPDATE`、`DELETE`、transaction は PostgreSQL に委譲する。
- Redshift table attributes は PostgreSQL SQL から除去し、metadata effects として保存する。
- Data API result は PostgreSQL result rows から Redshift Data API field shape に変換する。
- PostgreSQL error は Redshift-compatible SQLSTATE / AWS error shape に map する。
- managed mode では devcloud が local PostgreSQL process lifecycle を管理する。
- external mode では user-provided DSN を使い、reset / cleanup は devcloud metadata に限定する。

### Redshift Translator

- statement splitter は Data API の single statement 制約と SQL client の semicolon 区切りを区別する。
- DDL / DML は Redshift syntax を受け付け、PostgreSQL に渡せる SQL へ normalize する。
- `DISTSTYLE`、`DISTKEY`、`SORTKEY`、`ENCODE`、`BACKUP`、`IDENTITY` は SQL から除去または PostgreSQL equivalent に変換し、metadata effects として保存する。
- 未実装 feature は PostgreSQL に流さず、Redshift-like SQLSTATE で返す。
- translator は単純な文字列置換ではなく、statement 分類、quote-aware tokenization、function rewrite、side-effect extraction を持つ。

### Function Rewrite

| Redshift Syntax | PostgreSQL Target | Notes |
| --- | --- | --- |
| `GETDATE()` | `CURRENT_TIMESTAMP` | transaction timestamp semantics は後続で調整 |
| `SYSDATE` | `CURRENT_TIMESTAMP` | bare keyword detection |
| `NVL(a, b)` | `COALESCE(a, b)` | nested expressions supported by parser phase |
| `DECODE(expr, ...)` | `CASE WHEN ... THEN ... ELSE ... END` | MVP は simple equality form |
| `DATEADD(part, n, ts)` | `ts + (n * interval '1 part')` | supported parts are explicit |
| `DATEDIFF(part, start, end)` | PostgreSQL date arithmetic | part-specific implementation |
| `LISTAGG(x, sep) WITHIN GROUP (ORDER BY y)` | `string_agg(x, sep ORDER BY y)` | overflow clause unsupported initially |

### Transaction Model

- Transaction は PostgreSQL backend に委譲する。
- Redshift metadata effects は backend transaction と同じ commit boundary に揃える。
- `ROLLBACK` は PostgreSQL transaction と pending metadata effects を破棄する。
- strict Redshift isolation parity は L6 の対象とする。

## COPY / UNLOAD

### COPY

`COPY` は Redshift の主要 data loading workflow であり、local S3 互換 service との連携価値が高い。

MVP:

- `COPY table FROM 's3://bucket/key' IAM_ROLE default CSV`
- `CSV`、`DELIMITER`、`IGNOREHEADER`、`NULL AS`、`REGION` を parse する。
- local S3 service が enabled の場合は internal S3 store から object を読む。
- PostgreSQL backend には `COPY table FROM STDIN WITH (FORMAT csv, ...)` または batched insert として渡す。
- local S3 service が disabled の場合は clear error を返す。
- input row size limit を config で制御する。

Target:

- JSON、MANIFEST、GZIP、fixed width、column list、date/time format。
- DynamoDB source は DynamoDB service との連携 phase で扱う。
- remote host / EMR source は metadata validation のみ。

### UNLOAD

MVP:

- `UNLOAD ('select ...') TO 's3://bucket/prefix' IAM_ROLE default CSV`
- SELECT は PostgreSQL backend で実行し、result stream を devcloud side effect として local S3 object に書き出す。
- `ALLOWOVERWRITE`、`HEADER`、`DELIMITER` を parse する。

Target:

- JSON、PARQUET metadata placeholder、manifest、partition output、gzip。
- KMS / encryption は metadata のみ。

## Auth Modes

| Mode | SQL Endpoint | HTTP API | Purpose |
| --- | --- | --- | --- |
| `off` | no password check | no SigV4 check | local smoke |
| `relaxed` | accepts configured user/password and common local values | accepts unsigned or signed local requests | default developer mode |
| `strict` | validates configured user/password; optional MD5/SCRAM later | validates SigV4 access key / region / service | contract and negative tests |

Security baseline:

- SQL text can contain sensitive values. Logs should include query id, status, duration, and optionally a redacted preview only.
- Passwords, authorization headers, signatures, bind values, COPY credentials, object payloads are never logged.
- Dashboard query history shows SQL text only from local state and should make truncation/redaction explicit in API fields.

## Request Handling

### SQL Endpoint Flow

1. Accept TCP connection.
2. Handle optional SSLRequest. MVP returns no SSL support.
3. Parse StartupMessage and create session.
4. Authenticate according to `auth.redshift.mode`.
5. Send ParameterStatus and ReadyForQuery.
6. For each Query message, execute SQL through `RedshiftService.ExecuteSQL`.
7. Send RowDescription / DataRow / CommandComplete or ErrorResponse.
8. Persist query history and close session on Terminate.

### Data API Flow

1. Parse AWS JSON request and operation target.
2. Validate auth mode, cluster/workgroup/database/user/secret parameter combination.
3. Apply `ClientToken` idempotency if present.
4. Create statement record and session record if needed.
5. Execute SQL synchronously for MVP while preserving asynchronous status shape.
6. Store result pages in statement store.
7. Return statement id, session id, and metadata.

### Management API Flow

1. Parse AWS Query API action and version.
2. Validate auth mode and region.
3. Route action to cluster metadata service.
4. Return AWS XML shape with `ResponseMetadata.RequestId`.
5. Persist operation events for dashboard and future `DescribeEvents`.

## Error Model

| Source | Error Shape | MVP Examples |
| --- | --- | --- |
| SQL endpoint | PostgreSQL ErrorResponse with SQLSTATE | `42601` syntax error, `42P01` undefined table, `42710` duplicate object |
| Data API | AWS JSON error | `ValidationException`, `ResourceNotFoundException`, `ExecuteStatementException` |
| Management API | AWS Query XML error | `ClusterNotFound`, `InvalidParameterValue`, `ClusterAlreadyExists` |
| Dashboard API | JSON error envelope | `invalid_request`, `not_found`, `conflict` |

Errors should be compatible enough for SDK retry / validation behavior while avoiding leakage of secrets or full payloads.

## Dashboard Integration

### Navigation

- Add `Redshift` to service registry and service switcher.
- Route: `/dashboard/redshift`.
- Status card shows SQL endpoint, API endpoint, cluster id, database, active sessions, recent statement status.

### Views

| View | Purpose |
| --- | --- |
| Overview | cluster status, endpoints, default connection strings |
| Query Runner | run SQL through dashboard API and inspect result |
| Databases | database / schema / table tree |
| Table Detail | columns, dist/sort metadata, sample rows |
| Statements | Data API statement history, status, duration, result row count |
| COPY / UNLOAD | recent load/export jobs and local S3 object links |
| Snapshots | snapshot metadata in later phase |

Dashboard actions must call the same service facade as SQL / Data API so behavior does not drift.

## Implementation Plan

### Phase 0: Design and Automation Skeleton

- IMPL-001: Add this design document.
- IMPL-002: Add `scripts/redshift-autoloop/verify.sh` with `foundation`, `config`, `pgwire`, `sql-core`, `data-api`, `management`, `dashboard`, `e2e`, `full` stages.
- IMPL-003: Add task prompts for codex engine loops.
- IMPL-004: Add `scripts/redshift-e2e.sh` smoke test placeholder.

### Phase 1: Config and Daemon

- IMPL-005: Add Redshift config fields in `orchestrator/config.rs`.
- IMPL-006: Start SQL and HTTP API listeners from `orchestrator/daemon.rs`.
- IMPL-007: Register Redshift in dashboard service registry.
- IMPL-008: Add reset handling for `.devcloud/data/redshift`.

### Phase 2: Catalog and Storage

- IMPL-009: Implement cluster metadata store.
- IMPL-010: Implement catalog store for databases, schemas, tables, columns, and views.
- IMPL-011: Implement table row store and result materialization.
- IMPL-012: Implement statement/session store for Data API.
- IMPL-013: Add deterministic tests for catalog and statement stores.

### Phase 3: SQL Endpoint Core

- IMPL-014: Implement PostgreSQL startup/auth/simple query protocol.
- IMPL-015: Implement SQL session model and query history.
- IMPL-016: Implement `select 1` and basic system parameter responses.
- IMPL-017: Add `psql` smoke test.

### Phase 4: SQL Resource Core

- IMPL-018: Implement CREATE / DROP SCHEMA.
- IMPL-019: Implement CREATE / DROP TABLE with Redshift table attribute metadata.
- IMPL-020: Implement INSERT and SELECT subset.
- IMPL-021: Implement basic transaction state.
- IMPL-022: Add SQL behavior tests and e2e fixture.

### Phase 5: Data API

- IMPL-023: Implement AWS JSON request routing for `redshift-data`.
- IMPL-024: Implement `ExecuteStatement`, `DescribeStatement`, `GetStatementResult`, `ListStatements`.
- IMPL-025: Implement metadata list operations.
- IMPL-026: Implement `ClientToken` idempotency and session retention.
- IMPL-027: Add AWS CLI / first-party SDK smoke tests.

### Phase 6: Management API

- IMPL-028: Implement AWS Query request routing for `redshift`.
- IMPL-029: Implement `DescribeClusters` and metadata-only create/delete cluster lifecycle.
- IMPL-030: Implement tags and parameter group metadata.
- IMPL-031: Implement temporary credential response in relaxed mode.
- IMPL-032: Add AWS CLI management smoke tests.

### Phase 7: COPY / UNLOAD and Dashboard

- IMPL-033: Implement COPY from local S3 CSV.
- IMPL-034: Implement UNLOAD to local S3 CSV.
- IMPL-035: Add Redshift dashboard route and API.
- IMPL-036: Add query runner, catalog browser, and statement history UI.
- IMPL-037: Add Playwright or dashboard API e2e.

### Phase 8: Advanced Compatibility

- IMPL-038: Implement extended query protocol.
- IMPL-039: Add CTAS, views, materialized views, UPDATE / DELETE / MERGE.
- IMPL-040: Add serverless namespace / workgroup metadata API.
- IMPL-041: Add snapshots and restore.
- IMPL-042: Add system views, WLM metadata, and BI introspection compatibility.
- IMPL-043: Add stored procedure / UDF metadata and limited execution.

### Phase 9: PostgreSQL Backend Migration

- IMPL-044: Introduce `SQLBackend` and `RedshiftTranslator` interfaces without changing external API behavior.
- IMPL-045: Keep the local memory SQL executor behind `backend/memory` as a fallback fixture.
- IMPL-046: Add `backend/postgres` with connection lifecycle, transaction handling, result mapping, and error mapping.
- IMPL-047: Add config for `services.redshift.backend.kind`, managed PostgreSQL data dir, and external DSN.
- IMPL-048: Implement translator extraction for `DISTSTYLE`, `DISTKEY`, `SORTKEY`, `ENCODE`, `IDENTITY`, and defaults.
- IMPL-049: Implement Redshift function rewrites for `GETDATE`, `SYSDATE`, `NVL`, `DECODE`, `DATEADD`, `DATEDIFF`, and `LISTAGG`.
- IMPL-050: Rework COPY to read local S3/file sources and stream into PostgreSQL.
- IMPL-051: Rework UNLOAD to query PostgreSQL and write local S3/file outputs.
- IMPL-052: Rework `pg_catalog`, `information_schema`, `stl`, `stv`, and `svv` views to combine PostgreSQL catalog data with devcloud metadata.
- IMPL-053: Add migration tests proving Data API, management API, dashboard, and E2E behavior remain compatible with `backend.kind=postgres`.
- IMPL-054: Make `postgres` the default backend with managed PostgreSQL as the generated config path; done when `VERIFY_STAGE=full-remaining` passes.

## PostgreSQL Backend Migration Plan

### Current State

The current Redshift implementation passes the local full gate with a local memory SQL executor and filesystem-backed state. This is acceptable as an early scaffold, but it should not remain the long-term database engine because SQL grammar, transaction semantics, catalog behavior, and client introspection will expand quickly.

### Target State

PostgreSQL becomes the primary execution backend. The Redshift layer remains responsible for:

- PostgreSQL wire compatibility on Redshift port `5439`.
- Redshift Data API and management API response shapes.
- Redshift SQL dialect translation.
- Redshift-only metadata such as dist/sort/encode and cluster descriptors.
- S3-backed COPY / UNLOAD side effects.
- System views that PostgreSQL does not provide.
- Redshift-like error normalization and log redaction.

### Migration Sequence

| Step | Change | Verification |
| --- | --- | --- |
| MIG-001 | Add backend interfaces and keep memory backend as explicit fallback | Done; `SQLBackend` boundary and memory backend tests exist |
| MIG-002 | Add PostgreSQL backend in external DSN mode | Done; external DSN mode remains supported |
| MIG-003 | Add managed PostgreSQL mode | Done; managed mode starts system PostgreSQL without user-provided DSN |
| MIG-004 | Move DDL/DML execution to PostgreSQL | Done for current core gate; broader SQL parity remains in advanced backlog |
| MIG-005 | Add Redshift attribute extraction | Done for current catalog metadata gate |
| MIG-006 | Add Redshift function rewrite suite | Done for current translator gate |
| MIG-007 | Move COPY / UNLOAD to side-effect router + PostgreSQL | Done for current COPY / UNLOAD gate |
| MIG-008 | Merge PostgreSQL catalog with devcloud system views | Done for current dashboard and catalog smoke gate |
| MIG-009 | Flip default backend to `postgres` | Done; default config targets managed PostgreSQL and `VERIFY_STAGE=full-remaining` passes |

### Rollback

Keep `backend.kind=memory` as an explicit documented fallback while `postgres` remains the default backend. If PostgreSQL process management or dependency installation fails, users can set:

```yaml
services:
  redshift:
    backend:
      kind: memory
```

The fallback is for development continuity only and should not be claimed as full Redshift SQL compatibility.

## Verification Plan

### Commands

```bash
cargo test --workspace
VERIFY_STAGE=foundation bash scripts/redshift-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/redshift-autoloop/verify.sh
VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh
VERIFY_STAGE=full-managed bash scripts/redshift-managed-postgres-autoloop/verify.sh
VERIFY_STAGE=full-remaining bash scripts/redshift-remaining-autoloop/verify.sh
scripts/redshift-e2e.sh
```

### Stage Gates

| Stage | Checks |
| --- | --- |
| `foundation` | design doc, config fields, service registry, no generated state committed |
| `config` | default ports, disabled service, custom dataDir, reset behavior |
| `pgwire` | startup/auth/simple query protocol and `psql select 1` smoke |
| `sql-core` | catalog DDL, table DDL, INSERT, SELECT, transaction subset |
| `postgres-backend` | PostgreSQL backend lifecycle, transaction mapping, result mapping, error mapping |
| `translator` | Redshift table attribute extraction and Redshift function rewrites |
| `data-api` | ExecuteStatement, DescribeStatement, GetStatementResult, ListStatements |
| `management` | DescribeClusters, cluster metadata, tags, AWS Query XML shape |
| `copy-unload` | COPY from local S3 and UNLOAD to local S3 |
| `dashboard` | dashboard service registry, API smoke, Redshift route |
| `e2e` | psql + AWS CLI Data API + management API workflow |
| `full` | all stages plus `cargo test --workspace` |

### Acceptance Criteria

- AC-001: `devcloud up` starts Redshift SQL server on `127.0.0.1:5439` by default.
- AC-002: `devcloud up` starts Redshift HTTP API server on `127.0.0.1:9099` by default.
- AC-003: `psql` can connect with local credentials and run `select 1`.
- AC-004: SQL client can create schema/table, insert rows, and query rows.
- AC-005: Redshift table attributes such as dist/sort/encode are stripped from PostgreSQL SQL, persisted as Redshift metadata, and visible in catalog metadata.
- AC-006: `aws redshift-data execute-statement` can run a query and return a statement id.
- AC-007: `aws redshift-data get-statement-result` can fetch result rows with correct field shape.
- AC-008: `aws redshift describe-clusters` returns local cluster metadata with endpoint port `5439`.
- AC-009: Dashboard shows Redshift endpoints, cluster status, catalog, and recent statements.
- AC-010: `devcloud reset` removes Redshift local state.
- AC-011: PostgreSQL backend mode passes SQL core, Data API, COPY / UNLOAD, dashboard, and e2e gates.
- AC-012: Managed PostgreSQL mode is the default backend, with explicit `memory` fallback kept for development continuity.
- AC-013: Dashboard query runner can create, publish SQL execution requests, show results, and preserve redacted statement history.
- AC-014: Advanced compatibility gate passes extended protocol, advanced SQL, serverless metadata, snapshots, introspection, procedures/UDF metadata, dashboard/E2E, and repository tests.

## Compatibility Matrix

| Capability | Amazon Redshift | devcloud Current | devcloud Target |
| --- | --- | --- | --- |
| SQL endpoint on 5439 | Yes | Yes | Yes |
| PostgreSQL simple query protocol | Yes | Yes | Yes |
| PostgreSQL extended query protocol | Yes | Partial | Broader JDBC/ODBC and binary format parity |
| Redshift JDBC / ODBC clients | Yes | Smoke only | Yes |
| CREATE SCHEMA / TABLE | Yes | Yes | Yes |
| DISTKEY / SORTKEY / ENCODE | Yes | Metadata only | Execution-aware metadata |
| INSERT / SELECT | Yes | PostgreSQL backend target | Broad subset with stricter Redshift errors |
| UPDATE / DELETE / MERGE | Yes | PostgreSQL backend target | Broader dialect parity |
| COPY from S3 | Yes | devcloud side effect into PostgreSQL | Broader formats |
| UNLOAD to S3 | Yes | PostgreSQL result into devcloud side effect | Broader formats |
| Data API ExecuteStatement | Yes | Yes | Yes |
| Data API BatchExecuteStatement | Yes | Partial | Yes |
| Data API session reuse | Yes | Partial | Yes |
| Management DescribeClusters | Yes | Yes | Yes |
| Create/Delete Cluster | Yes | Metadata only | Metadata lifecycle |
| Snapshots | Yes | Metadata lifecycle | Broader field parity |
| Serverless workgroups | Yes | Metadata | Broader field parity |
| System catalogs | Yes | Broad local subset | BI/client-driven expansion |
| WLM / workload metadata | Yes | Metadata | Broader workload introspection |
| Stored procedure / UDF metadata | Yes | Metadata and safe unsupported behavior | Limited safe execution where PostgreSQL-backed behavior is clear |
| IAM / Secrets Manager | Yes | Relaxed/strict local | Local compatibility |
| MPP / columnar performance | Yes | No | No |
| PostgreSQL execution backend | Internal detail | Yes | Yes |

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| PostgreSQL wire protocol scope expands quickly | JDBC / BI clients fail despite psql passing | Start with simple protocol, add extended protocol as separate phase with client matrix |
| SQL dialect compatibility becomes unbounded | MVP stalls | Define SQL levels and return explicit unsupported SQLSTATE for advanced syntax |
| Data API async shape hides synchronous executor limitations | SDK behavior diverges | Persist statement lifecycle and add polling tests even when execution is local and fast |
| COPY / UNLOAD can leak object payloads or credentials | Security issue | Redact SQL credentials, headers, object payloads, and bind values from logs |
| Management API has many low-value network fields | Large metadata burden | Return stable local metadata for fields SDKs commonly inspect, mark unsupported fields empty |
| System catalog gaps break drivers | Client compatibility issue | Add introspection e2e for psql, JDBC, and selected BI-style metadata queries early |
| Reusing generic SQL parser may not match Redshift | Incorrect syntax behavior | Keep parser behind interface and add Redshift-specific contract tests before broadening |
| PostgreSQL backend dependency complicates local setup | `devcloud up` may fail on machines without PostgreSQL | Support managed and external modes, add clear diagnostics, keep temporary memory fallback |
| Redshift-to-PostgreSQL rewrite corrupts SQL semantics | Silent query correctness bugs | Use parser-aware translation, golden tests, and reject unsupported constructs before backend execution |
| Metadata effects and PostgreSQL transaction commit drift | dist/sort/encode metadata differs from actual tables | Commit metadata effects only after backend transaction success; rollback pending effects on failure |

## Open Questions

1. Resolved for current implementation: the HTTP API listener shares one local port for `redshift-data`, `redshift`, and `redshift-serverless`; routing is based on AWS target/action shape.
2. Resolved for current implementation: managed PostgreSQL uses system `initdb`, `postgres`, and `psql` binaries discovered from `PATH`; bundled helper and container paths are out of scope for this phase.
3. Resolved for current implementation: PostgreSQL backend remains behind a local adapter boundary so the public Redshift API shape does not depend on a driver-specific API.
4. Which client matrix is mandatory before claiming broader L2 parity: Redshift JDBC, Redshift ODBC, common PostgreSQL drivers, or BI tools?
5. Resolved for current implementation: dashboard and statement history store redacted previews rather than leaking bind values or credential material.
6. Should COPY / UNLOAD require local S3 service to be enabled, or fall back to filesystem paths for developer convenience?
7. How strict should Redshift-specific SQLSTATE and error message matching be in the first full compatibility gate?

## References

- Amazon Redshift API Reference: https://docs.aws.amazon.com/redshift/latest/APIReference/
- DescribeClusters API: https://docs.aws.amazon.com/redshift/latest/APIReference/API_DescribeClusters.html
- Amazon Redshift Data API Reference: https://docs.aws.amazon.com/redshift-data/latest/APIReference/
- Redshift Data API operations: https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_Operations.html
- Using the Amazon Redshift Data API: https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html
- ExecuteStatement API: https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_ExecuteStatement.html
- Redshift SQL commands: https://docs.aws.amazon.com/redshift/latest/dg/c_SQL_commands.html
- COPY command: https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
- UNLOAD command: https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
- Redshift JDBC / ODBC notes: https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-postgres-jdbc.html
- devcloud core spec: `docs/spec-v0.md`
- BigQuery compatibility design: `docs/design-bigquery-compat.md`

## Change History

| Date | Change | Author |
| --- | --- | --- |
| 2026-05-04 | Initial Amazon Redshift compatibility design | Nexus |
| 2026-05-04 | Reframed SQL execution around PostgreSQL backend with Redshift translation and side-effect layer | Scribe |
