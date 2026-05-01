# DynamoDB Compatibility Design

## Summary

`devcloud` に Amazon DynamoDB 互換のローカル NoSQL database server を追加する。

目標は「AWS SDK / AWS CLI / DynamoDB client が endpoint override だけで主要 workflow を実行できる」ことである。完全互換は API、式評価、index、streams、transactions、PartiQL、運用系 API まで広いため、実装は段階化する。ただし内部設計は最初から table metadata、item store、secondary index、mutation log、expression engine を分離し、完全互換へ拡張できる形にする。

## Document Control

| Field | Value |
| --- | --- |
| Audience | devcloud implementers, reviewers, future agent loops |
| Status | Draft |
| Owner | devcloud maintainers |
| Reviewer | TBD |
| Related docs | `docs/spec-v0.md`, `docs/design-s3-compat.md`, `docs/design-gcs-compat.md`, `docs/design-dashboard-shell.md` |
| Primary references | Amazon DynamoDB API Reference, DynamoDB low-level API, expressions, indexes, streams, transactions, PartiQL |

## Compatibility Goal

### Definition

ここでの DynamoDB 互換は、以下を満たす状態を指す。

1. AWS SDK / AWS CLI が endpoint override だけで接続できる。
2. DynamoDB low-level JSON protocol の `POST /`、`Content-Type: application/x-amz-json-1.0`、`X-Amz-Target: DynamoDB_20120810.{Operation}`、JSON request / response shape が主要操作で一致する。
3. SigV4 signed request と local credential を検証できる。
4. `AttributeValue` の `S`、`N`、`B`、`BOOL`、`NULL`、`M`、`L`、`SS`、`NS`、`BS` を保存、比較、返却できる。
5. table、primary key、GSI、LSI、item CRUD、query、scan、condition expression、update expression、projection、filter の observable behavior を再現する。
6. streams、transactions、TTL、backup/restore、PartiQL、resource policy などを段階的に追加できる。
7. 実 AWS への接続や DynamoDB Local への依存なしに、local filesystem backed state として動作する。

### Compatibility Levels

| Level | Name | Purpose |
| --- | --- | --- |
| L0 | SDK Smoke | AWS CLI / SDK が endpoint override で疎通できる |
| L1 | Table and Item Core | table CRUD、item CRUD、basic Scan / Query が通る |
| L2 | Expression Core | condition、update、projection、filter、pagination が通る |
| L3 | Index Core | GSI / LSI 作成、更新、Query / Scan が通る |
| L4 | Streams and TTL | DynamoDB Streams、TTL、mutation log replay が通る |
| L5 | Transactions and PartiQL | TransactGet/Write、ExecuteStatement、BatchExecuteStatement が通る |
| L6 | Operational Parity | backup/restore、global tables compatibility response、resource policy、contributor insights など周辺 API を追加 |

MVP は L1 + L2 の一部を対象にする。最終的な「完全互換」は L6 までを含む長期目標とする。

## Goals

- REQ-001: `devcloud up` で DynamoDB endpoint を起動する。
- REQ-002: AWS CLI / SDK が `endpoint-url` または endpoint override で table 作成、item put/get/update/delete、query、scan を実行できる。
- REQ-003: DynamoDB JSON protocol の request header、target operation、JSON request / response shape を実装する。
- REQ-004: SigV4 を `relaxed` / `strict` mode で扱い、strict mode では local credential で署名検証する。
- REQ-005: `AttributeValue` を DynamoDB 互換の型ルールで保存し、key comparison と expression evaluation に使う。
- REQ-006: primary key、GSI、LSI の index state を item write と同一 atomic boundary で更新する。
- REQ-007: `ConditionalCheckFailedException`、`ValidationException`、`ResourceNotFoundException` などの error name、HTTP status、message shape を SDK が解釈できる形で返す。
- REQ-008: dashboard/API から tables、items、indexes、streams、TTL 状態を確認できる。
- REQ-009: `devcloud reset` で DynamoDB data、indexes、streams log、TTL queue を削除できる。
- REQ-010: stage based verification script で SDK smoke と protocol contract tests を実行できる。

## Non-Goals

- 実 AWS IAM、STS、CloudWatch、CloudTrail、KMS とは連携しない。
- DynamoDB Local、LocalStack、外部 emulator には依存しない。
- multi-AZ durability、adaptive capacity、actual provisioned billing、AWS managed auto scaling は再現しない。
- global tables の実リージョン間 replication は初期対象外とする。API response compatibility から始める。
- DAX protocol は対象外とする。
- Contributor Insights、Kinesis streaming destination、import/export to S3 の実処理は初期対象外とする。
- TTL の削除時刻は AWS と同じ非決定的遅延を完全再現しない。local scheduler による deterministic cleanup から始める。

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

aws --endpoint-url http://127.0.0.1:8000 dynamodb create-table \
  --table-name Demo \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST

aws --endpoint-url http://127.0.0.1:8000 dynamodb put-item \
  --table-name Demo \
  --item '{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}'

aws --endpoint-url http://127.0.0.1:8000 dynamodb get-item \
  --table-name Demo \
  --key '{"pk":{"S":"user#1"},"sk":{"S":"profile"}}'
```

SDK clients should use:

```txt
endpoint: http://127.0.0.1:8000
region: us-east-1
accessKeyId: dev
secretAccessKey: dev
```

## Scope

### v0.1 DynamoDB MVP

```txt
Daemon:
  DynamoDB JSON endpoint  http://127.0.0.1:8000
  Dashboard/API           http://127.0.0.1:8025

Table API:
  CreateTable
  DescribeTable
  ListTables
  DeleteTable
  UpdateTable              limited billing mode / stream / TTL metadata updates
  DescribeLimits           local fixed response

Item API:
  PutItem
  GetItem
  DeleteItem
  UpdateItem               SET / REMOVE basics first
  BatchGetItem
  BatchWriteItem
  Query                    primary key equality + sort key comparison basics
  Scan

Compatibility:
  JSON 1.0 request / response
  X-Amz-Target dispatch
  SigV4 relaxed and strict modes
  AttributeValue scalar/document/set types
  consistent read flag accepted
  ReturnValues basics
  ConditionExpression basics
  ProjectionExpression basics
  LastEvaluatedKey / ExclusiveStartKey pagination
  DynamoDB-compatible JSON errors
```

### v0.2 Expression and Index Core

```txt
ConditionExpression:
  attribute_exists
  attribute_not_exists
  attribute_type
  contains
  begins_with
  size
  comparison operators
  BETWEEN / IN
  AND / OR / NOT

UpdateExpression:
  SET
  REMOVE
  ADD number / set
  DELETE set
  if_not_exists
  list_append

Query / Scan:
  KeyConditionExpression
  FilterExpression
  ProjectionExpression
  ExpressionAttributeNames
  ExpressionAttributeValues
  ScanIndexForward
  Limit
  ConsumedCapacity compatibility response

Indexes:
  GSI create at table creation
  LSI create at table creation
  GSI/LSI Query and Scan
  projection types ALL / KEYS_ONLY / INCLUDE
```

### Later

```txt
UpdateTable add/delete GSI
DynamoDB Streams
TimeToLive
TransactGetItems
TransactWriteItems
ExecuteStatement
ExecuteTransaction
BatchExecuteStatement
Backup / Restore compatibility metadata
GlobalTable compatibility metadata
ResourcePolicy get/put/delete
TagResource / UntagResource / ListTagsOfResource
ImportTable / ExportTable compatibility stubs or local file workflows
```

## Architecture

```txt
AWS CLI / SDK / DynamoDB client
        |
        v
+-----------------------------+
| DynamoDB HTTP Gateway       | :8000
| JSON 1.0 + X-Amz-Target     |
| SigV4 verifier              |
+-----------------------------+
        |
        v
+-----------------------------+
| DynamoDB API Adapter        |
| request -> command          |
+-----------------------------+
        |
        v
+-----------------------------+
| DynamoDB Service            |
| table/item/index rules      |
+-----------------------------+
        |             |              |
        v             v              v
+-------------+ +-------------+ +-------------+
| Table Store | | Item Store  | | Index Store |
+-------------+ +-------------+ +-------------+
        |             |              |
        v             v              v
+---------------------------------------------+
| KV Engine / Filesystem-backed transaction log |
+---------------------------------------------+
        |
        v
+-----------------------------+
| Dashboard/API               | :8025
+-----------------------------+
```

## Repository Layout

```txt
internal/
  app/
    config.go
    daemon.go

  protocol/
    aws/
      sigv4/
        verifier.go
        canonical_request.go
      jsonrpc/
        target.go
        response.go
        error.go

  services/
    dynamodb/
      server.go
      router.go
      handlers_table.go
      handlers_item.go
      handlers_query.go
      handlers_batch.go
      handlers_streams.go
      handlers_transactions.go
      attribute_value.go
      expressions.go
      errors.go
      model.go
      service.go

  storage/
    kv/
      engine.go
      wal.go
      snapshot.go
      transaction.go
    dynamodb/
      store.go
      table_store.go
      item_store.go
      index_store.go
      stream_store.go
      ttl_queue.go

  dashboard/
    dynamodb_static.go
```

`storage/kv` は DynamoDB 専用に閉じず、将来 SQS / PubSub / BigQuery catalog / cache metadata でも使える provider-neutral core とする。`storage/dynamodb` は DynamoDB の table、item、index、stream semantics を `kv` 上に実装する。

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

auth:
  dynamodb:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev

storage:
  path: .devcloud/data

services:
  dynamodb:
    enabled: true
    region: us-east-1
    billingMode: PAY_PER_REQUEST
    maxItemBytes: 400000
    maxTables: 256
    streams:
      enabled: false
    ttl:
      schedulerIntervalSeconds: 60
```

### Config Integration

- `ServerConfig` に `DynamoDBPort` を追加する。
- `AuthConfig` に `DynamoDB` を追加する。
- `ServicesConfig` に `DynamoDB` を追加する。
- `defaultConfigYAML` と `applyConfigValue` に明示的 mapping を追加する。
- `InitWorkspace` で `.devcloud/data/dynamodb` と `.devcloud/data/kv` を作成する。
- `ResetWorkspace` は既存方針通り `storage.path` 配下削除で DynamoDB state も削除する。

## API Mapping

### Table Operations

| DynamoDB operation | Target | Internal command |
| --- | --- | --- |
| `CreateTable` | `DynamoDB_20120810.CreateTable` | `CreateTable` |
| `DescribeTable` | `DynamoDB_20120810.DescribeTable` | `GetTable` |
| `ListTables` | `DynamoDB_20120810.ListTables` | `ListTables` |
| `UpdateTable` | `DynamoDB_20120810.UpdateTable` | `UpdateTableMetadata` / `UpdateIndexes` |
| `DeleteTable` | `DynamoDB_20120810.DeleteTable` | `DeleteTable` |
| `DescribeLimits` | `DynamoDB_20120810.DescribeLimits` | `DescribeLocalLimits` |

### Item Operations

| DynamoDB operation | Target | Internal command |
| --- | --- | --- |
| `PutItem` | `DynamoDB_20120810.PutItem` | `PutItem` |
| `GetItem` | `DynamoDB_20120810.GetItem` | `GetItem` |
| `UpdateItem` | `DynamoDB_20120810.UpdateItem` | `UpdateItem` |
| `DeleteItem` | `DynamoDB_20120810.DeleteItem` | `DeleteItem` |
| `BatchGetItem` | `DynamoDB_20120810.BatchGetItem` | `BatchGetItem` |
| `BatchWriteItem` | `DynamoDB_20120810.BatchWriteItem` | `BatchWriteItem` |
| `Query` | `DynamoDB_20120810.Query` | `QueryTableOrIndex` |
| `Scan` | `DynamoDB_20120810.Scan` | `ScanTableOrIndex` |

### Advanced Operations

| DynamoDB operation | Target | Internal command |
| --- | --- | --- |
| `TransactGetItems` | `DynamoDB_20120810.TransactGetItems` | `ReadTransaction` |
| `TransactWriteItems` | `DynamoDB_20120810.TransactWriteItems` | `WriteTransaction` |
| `ExecuteStatement` | `DynamoDB_20120810.ExecuteStatement` | `PartiQLStatement` |
| `ExecuteTransaction` | `DynamoDB_20120810.ExecuteTransaction` | `PartiQLTransaction` |
| `DescribeStream` | streams endpoint target | `DescribeStream` |
| `GetRecords` | streams endpoint target | `GetStreamRecords` |
| `GetShardIterator` | streams endpoint target | `CreateShardIterator` |
| `ListStreams` | streams endpoint target | `ListStreams` |

## Resource Model

### Table

```go
type Table struct {
    Name              string
    ARN               string
    Region            string
    Status            TableStatus
    AttributeDefs     []AttributeDefinition
    KeySchema         KeySchema
    BillingMode       BillingMode
    Provisioned       ProvisionedThroughput
    GSIs              []GlobalSecondaryIndex
    LSIs              []LocalSecondaryIndex
    StreamSpec        StreamSpecification
    TTLSpec           TimeToLiveSpecification
    CreatedAt         time.Time
    UpdatedAt         time.Time
    ItemCount         int64
    SizeBytes         int64
}
```

### Item

```go
type Item struct {
    Table       string
    Key         EncodedKey
    Attributes  map[string]AttributeValue
    Version     int64
    SizeBytes   int64
    CreatedAt   time.Time
    UpdatedAt   time.Time
    ExpiresAt   *time.Time
}
```

### AttributeValue

```go
type AttributeValue struct {
    S    *string
    N    *NumberString
    B    []byte
    BOOL *bool
    NULL *bool
    M    map[string]AttributeValue
    L    []AttributeValue
    SS   []string
    NS   []NumberString
    BS   [][]byte
}
```

`N` と `NS` は JSON number ではなく文字列として保存する。比較と arithmetic は decimal-compatible parser を通して行い、元の文字列表現は response compatibility のため保持する。

## Storage Layout

```txt
.devcloud/
  data/
    kv/
      wal/
        00000001.log
      snapshots/
        current/
    dynamodb/
      tables/
        {table}/
          table.json
          items/
            {partition-key-hash}/
              {encoded-key}.json
          indexes/
            gsi/
              {index}/
                partitions/
            lsi/
              {index}/
                partitions/
          streams/
            stream.json
            shards/
              {shard-id}.jsonl
          ttl/
            queue.jsonl
```

MVP は単純な filesystem store から始める。ただし書き込み順序は `wal append -> table/item/index atomic rewrite -> commit marker` とし、process crash 後に recovery できる設計にする。

## Request Handling

### JSON Protocol Dispatch

DynamoDB endpoint は原則として `POST /` のみを受ける。

```txt
POST /
Content-Type: application/x-amz-json-1.0
X-Amz-Target: DynamoDB_20120810.PutItem
```

処理手順:

1. request ID を生成する。
2. `Content-Type` と `X-Amz-Target` を検証する。
3. auth mode に応じて SigV4 を検証する。
4. operation ごとの request struct に JSON decode する。
5. validation を行い、DynamoDB-compatible error に変換する。
6. service command を実行する。
7. response struct を JSON encode する。

### Error Response

DynamoDB error は JSON body と AWS error headers を返す。

```json
{
  "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
  "message": "Requested resource not found"
}
```

代表 mapping:

| Condition | HTTP | Error name |
| --- | --- | --- |
| table missing | 400 | `ResourceNotFoundException` |
| duplicate table | 400 | `ResourceInUseException` |
| condition false | 400 | `ConditionalCheckFailedException` |
| invalid request shape | 400 | `ValidationException` |
| item too large | 400 | `ValidationException` |
| throughput compatibility throttle | 400 | `ProvisionedThroughputExceededException` |
| auth failed | 400 / 403 | `UnrecognizedClientException` / `AccessDeniedException` |
| internal store failure | 500 | `InternalServerError` |

## Expression Engine

Expression support は DynamoDB 互換性の中核であるため、API handler から分離する。

```txt
Expression string
        |
        v
Lexer / Parser
        |
        v
AST
        |
        v
Evaluator
        |
        v
Condition / Update / Projection / Filter result
```

### MVP Grammar

| Expression | MVP support |
| --- | --- |
| `ConditionExpression` | equality, comparison, `attribute_exists`, `attribute_not_exists`, `AND` |
| `KeyConditionExpression` | partition key equality, sort key comparison, `begins_with`, `BETWEEN` |
| `ProjectionExpression` | top-level attributes and expression attribute names |
| `FilterExpression` | same subset as condition expression |
| `UpdateExpression` | `SET`, `REMOVE`, simple arithmetic |

後続 phase で nested document path、list index、set operation、function coverage を拡張する。

## Index Design

### Primary Index

Primary key は `HASH` のみ、または `HASH` + `RANGE` をサポートする。

```txt
partitionKey = encodeAttributeValue(table.KeySchema.Hash)
sortKey      = encodeAttributeValue(table.KeySchema.Range)
itemKey      = partitionKey + "\x00" + sortKey
```

`S`、`N`、`B` の sort order は DynamoDB の Query 結果と一致させる。`N` は numeric ordering、`S` は UTF-8 byte ordering、`B` は unsigned byte ordering として扱う。

### Global Secondary Index

GSI は table と異なる partition/sort key を持てる。item write 時に projected attributes を index entry として更新する。

### Local Secondary Index

LSI は table と同じ partition key を持ち、別 sort key を持つ。MVP では table creation 時のみ定義可能にする。

### Projection

| ProjectionType | Behavior |
| --- | --- |
| `KEYS_ONLY` | table key と index key のみ保存 |
| `INCLUDE` | `NonKeyAttributes` を追加保存 |
| `ALL` | item 全体を index entry に保存 |

## Authentication

### Modes

| Mode | Purpose |
| --- | --- |
| `relaxed` | local development default。Authorization なしを許可 |
| `signed-relaxed` | SigV4 header の形式だけ検証し、credential mismatch は許容 |
| `strict` | configured local access key / secret で SigV4 を検証 |

SigV4 service name は `dynamodb`、region は config の `services.dynamodb.region` を使う。Authorization header、secret、canonical request、payload body は log に出さない。

## Streams and TTL

Streams は item mutation log から構築する。

```txt
PutItem / UpdateItem / DeleteItem
        |
        v
Mutation event
        |
        v
stream shard append
        |
        v
DescribeStream / GetShardIterator / GetRecords
```

MVP では streams は無効化し、L4 で `NEW_IMAGE`、`OLD_IMAGE`、`NEW_AND_OLD_IMAGES`、`KEYS_ONLY` を追加する。TTL は `.devcloud/data/dynamodb/.../ttl/queue.jsonl` に候補を保存し、scheduler が削除と stream event を生成する。

## Transactions

Transactions は L5 で追加する。初期設計では以下を前提にする。

- 単一 process 内の table-level lock から始める。
- `ConditionCheck`、`Put`、`Update`、`Delete` を同一 validation phase で評価する。
- 全 validation 成功後に WAL へ transaction record を append する。
- `ClientRequestToken` による idempotency を保存する。
- cancellation reasons は SDK compatibility を優先して返す。

## PartiQL

PartiQL は parser 導入範囲が広いため、`ExecuteStatement` / `BatchExecuteStatement` の限定文法から始める。

```txt
SELECT ... FROM table WHERE pk = ?
INSERT INTO table VALUE {...}
UPDATE table SET attr = ? WHERE pk = ?
DELETE FROM table WHERE pk = ?
```

PartiQL parser は `services/dynamodb/partiql` として隔離し、内部 command へ変換する。SQL parser の依存追加は security / licensing review 後に行う。

## Dashboard

DynamoDB dashboard は共通 dashboard shell に追加する。

```txt
GET /dynamodb
GET /api/dynamodb/status
GET /api/dynamodb/tables
POST /api/dynamodb/tables
GET /api/dynamodb/tables/{table}
DELETE /api/dynamodb/tables/{table}
GET /api/dynamodb/tables/{table}/items?limit=&exclusiveStartKey=
GET /api/dynamodb/tables/{table}/indexes
GET /api/dynamodb/tables/{table}/streams
GET /api/dynamodb/tables/{table}/ttl
```

UI は operational console として、tables list、key schema、index health、item explorer、raw AttributeValue JSON、recent mutations を表示する。DynamoDB API の代替操作面ではなく、SDK/CLI が書き込んだ状態を確認する observability layer とする。

## Implementation Plan

### Phase 0: Design and Test Harness

- IMPL-001: `docs/design-dynamodb-compat.md` を確定する。
- IMPL-002: `scripts/dynamodb-autoloop/verify.sh` を追加し、stage based verification を用意する。
- IMPL-003: AWS CLI / SDK smoke fixtures と golden JSON error fixtures を追加する。

### Phase 1: Config and Daemon

- IMPL-010: config に `server.dynamodbPort`、`services.dynamodb`、`auth.dynamodb` を追加する。
- IMPL-011: `.devcloud/config.yaml` initialization と reset 対象に DynamoDB storage を追加する。
- IMPL-012: `devcloud up` で DynamoDB endpoint を起動する。
- IMPL-013: `/api/dashboard/services` に DynamoDB を追加する。

### Phase 2: Protocol and Table Core

- IMPL-020: JSON 1.0 protocol dispatcher と `X-Amz-Target` router を実装する。
- IMPL-021: DynamoDB-compatible JSON error helper を実装する。
- IMPL-022: `CreateTable`、`DescribeTable`、`ListTables`、`DeleteTable` を実装する。
- IMPL-023: relaxed / strict SigV4 mode を実装する。

### Phase 3: Item Core

- IMPL-030: `AttributeValue` decode / encode / validation を実装する。
- IMPL-031: primary key encoding と item storage を実装する。
- IMPL-032: `PutItem`、`GetItem`、`DeleteItem` を実装する。
- IMPL-033: `Scan` と basic `Query` を実装する。
- IMPL-034: `BatchGetItem` と `BatchWriteItem` を実装する。

### Phase 4: Expression Core

- IMPL-040: expression lexer/parser/AST を実装する。
- IMPL-041: `ConditionExpression` と conditional write を実装する。
- IMPL-042: `UpdateExpression` の `SET` / `REMOVE` / arithmetic を実装する。
- IMPL-043: `ProjectionExpression` と `FilterExpression` を実装する。
- IMPL-044: pagination と `LastEvaluatedKey` を実装する。

### Phase 5: Index Core

- IMPL-050: GSI / LSI metadata validation を実装する。
- IMPL-051: item write 時の index projection update を実装する。
- IMPL-052: index `Query` / `Scan` を実装する。
- IMPL-053: `UpdateTable` による GSI add/delete を実装する。

### Phase 6: Streams, TTL, and Dashboard

- IMPL-060: mutation log と streams store を実装する。
- IMPL-061: `ListStreams`、`DescribeStream`、`GetShardIterator`、`GetRecords` を実装する。
- IMPL-062: TTL scheduler と `UpdateTimeToLive` / `DescribeTimeToLive` を実装する。
- IMPL-063: `/dynamodb` dashboard と `/api/dynamodb/*` を実装する。

### Phase 7: Transactions and PartiQL

- IMPL-070: `TransactGetItems` を実装する。
- IMPL-071: `TransactWriteItems` と idempotency token を実装する。
- IMPL-072: limited `ExecuteStatement` を実装する。
- IMPL-073: `BatchExecuteStatement` と `ExecuteTransaction` を実装する。

## Verification Plan

### Unit Tests

- `X-Amz-Target` parsing and operation dispatch
- DynamoDB JSON error encoding
- AttributeValue decode / encode / validation
- key encoding and sort ordering for `S` / `N` / `B`
- item size calculation
- condition expression parser and evaluator
- update expression parser and evaluator
- projection and filter behavior
- GSI / LSI projection updates
- stream record generation
- transaction validation and rollback
- SigV4 canonical request verification without logging secrets

### Integration Tests

```bash
go test ./...
VERIFY_STAGE=foundation bash scripts/dynamodb-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/dynamodb-autoloop/verify.sh
```

### E2E Tests

AWS CLI:

```bash
aws --endpoint-url http://127.0.0.1:8000 dynamodb create-table ...
aws --endpoint-url http://127.0.0.1:8000 dynamodb put-item ...
aws --endpoint-url http://127.0.0.1:8000 dynamodb get-item ...
aws --endpoint-url http://127.0.0.1:8000 dynamodb update-item ...
aws --endpoint-url http://127.0.0.1:8000 dynamodb query ...
aws --endpoint-url http://127.0.0.1:8000 dynamodb scan ...
aws --endpoint-url http://127.0.0.1:8000 dynamodb delete-table ...
```

SDK matrix:

- AWS SDK for Go v2
- boto3
- AWS SDK for JavaScript v3

Contract fixtures:

- `CreateTable` / `DescribeTable` response
- `PutItem` / `GetItem` / `UpdateItem` response
- `Query` pagination response
- common validation errors
- conditional check failure
- SigV4 strict success/failure

### Acceptance Criteria

- AC-001: Given `devcloud up` is running, when `ListTables` is called through AWS CLI, then a DynamoDB-compatible JSON response is returned.
- AC-002: Given a valid `CreateTable` request, when the table is created, then `DescribeTable` returns `ACTIVE` and the configured key schema.
- AC-003: Given a table exists, when `PutItem` then `GetItem` are called, then the original `AttributeValue` JSON is returned.
- AC-004: Given a conditional put with false condition, when `PutItem` is called, then `ConditionalCheckFailedException` is returned.
- AC-005: Given an item exists, when `UpdateItem` with `SET` is called, then the updated attributes are persisted.
- AC-006: Given multiple items share a partition key, when `Query` is called with key condition and limit, then sorted items and `LastEvaluatedKey` are returned.
- AC-007: Given a GSI exists, when an indexed attribute is updated, then index `Query` reflects the new state.
- AC-008: Given strict auth is enabled, when a signed SDK request is sent, then SigV4 validation succeeds without exposing secrets in logs.
- AC-009: Given DynamoDB dashboard is opened, when table data exists, then `/dynamodb` shows tables, key schema, item counts, and recent mutations.
- AC-010: Given `devcloud reset` is run, when DynamoDB endpoint restarts, then tables and items are cleared.

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| "complete DynamoDB compatibility" is too broad | Scope explosion | compatibility levels and phase gates |
| Expression grammar drift | SDK/app behavior mismatch | parser tests from AWS CLI examples and golden fixtures |
| AttributeValue numeric precision mismatch | incorrect conditions and sort order | store number strings and compare with decimal parser |
| GSI/LSI update is not atomic with item writes | stale query results | WAL transaction boundary for item + index updates |
| SigV4 canonicalization bugs | SDK incompatibility | SDK-generated request fixtures and strict-mode tests |
| Error shape mismatch | SDK retry/error handling breaks | golden JSON error fixtures |
| filesystem store corruption | local data loss | WAL, atomic rewrite, recovery tests |
| PartiQL parser scope expands too early | implementation stall | isolate PartiQL and start with limited statements |
| TTL timing differs from AWS | flaky tests | deterministic local scheduler and documented compatibility note |

## Open Questions

1. Should DynamoDB use dedicated port `8000`, or share a unified AWS gateway port `4566` with S3/SQS later?
2. Should MVP implement `UpdateItem` before GSI, or stabilize GSI read paths first?
3. Which compatibility target should gate v0.1: AWS CLI, AWS SDK for Go v2, boto3, or JavaScript v3?
4. Should `storage/kv` be introduced before DynamoDB, or start with `storage/dynamodb` and extract later?
5. Should DynamoDB dashboard allow item mutation, or remain read-only observability in v0.1?

## References

- Amazon DynamoDB API Reference: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/Welcome.html
- DynamoDB API actions: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations.html
- DynamoDB low-level API: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.LowLevelAPI.html
- DynamoDB expressions: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.html
- DynamoDB secondary indexes: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html
- DynamoDB PartiQL: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html

## Change History

| Date | Change |
| --- | --- |
| 2026-05-01 | Initial draft |
