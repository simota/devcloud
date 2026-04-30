
```txt
AWS SDK / CLI        GCP SDK / CLI        SMTP / SQL Client
     |                    |                    |
     v                    v                    v
+------------------------------------------------------+
|                    Edge Gateway                      |
|  SigV4 / OAuth / REST / XML / JSON / gRPC / SMTP     |
+------------------------------------------------------+
        |              |              |
        v              v              v
+---------------+ +-------------+ +-------------------+
| Object Core   | | Queue Core  | | Query / SQL Core  |
| S3 / GCS      | | SQS/PubSub  | | BigQuery/Redshift |
+---------------+ +-------------+ +-------------------+
        |              |              |
        v              v              v
+------------------------------------------------------+
|                 DevCloud Kernel                      |
| resource registry / storage / scheduler / logs / UI  |
+------------------------------------------------------+
```

つまり、自作する対象はこうです。

```txt
メールサーバー
S3 API adapter
GCS API adapter
BigQuery API adapter
DynamoDB API adapter
SQS API adapter
Pub/Sub API adapter
Redshift API / PostgreSQL wire adapter
CloudFront API adapter
CloudFront data-plane proxy/cache
共通 object store
共通 message broker
共通 SQL/query engine
共通 KV/index engine
共通 dashboard
共通 CLI
```

かなり大きいですが、設計を分ければ成立します。

---

## まず決めるべき前提

「全部自作」には段階があります。

| 方針 | 内容                                          | おすすめ度   |
| -- | ------------------------------------------- | ------- |
| A  | 既存エミュレータは使わない。ライブラリは使う                      | 現実的     |
| B  | 既存エミュレータは使わない。ライブラリは vendoring / pinning する | かなり良い   |
| C  | 標準ライブラリ以外ほぼ使わない                             | かなり重い   |
| D  | HTTP、gRPC、SQL parser、TLS、DB engine まで全自作    | 研究開発レベル |

おすすめは **B** です。

外部の影響を避けたい理由が、LocalStack、Mailpit、DynamoDB Local、fake-gcs-server などの **外部プロダクト都合に振り回されたくない** という意味なら、B が最もバランス良いです。

一方で、gRPC、protobuf、HTTP/2、TLS、PostgreSQL wire protocol、SQL parser、MIME parser、XML parser まで完全に自作すると、互換性よりも基盤実装に時間を取られます。

---

## 製品コンセプト

名前は仮に `devcloud` とします。

```bash
devcloud init
devcloud up
devcloud seed
devcloud reset
devcloud snapshot save fixture-001
devcloud snapshot restore fixture-001
devcloud dashboard
```

ユーザー側の体験はこうです。

```bash
export AWS_ACCESS_KEY_ID=dev
export AWS_SECRET_ACCESS_KEY=dev
export AWS_REGION=ap-northeast-1
export AWS_ENDPOINT_URL=http://localhost:4566

export PUBSUB_EMULATOR_HOST=localhost:8085
export STORAGE_EMULATOR_HOST=http://localhost:4443
export BIGQUERY_API_ENDPOINT=http://localhost:9050
```

ただし内部では、LocalStack などは一切使わず、すべて自前サーバーです。

---

## 全体アーキテクチャ

### 1. 単一バイナリ方式

自作を徹底するなら、Docker compose 集ではなく **単一プロセス / 単一バイナリ** が良いです。

```txt
devcloudd
  ├─ AWS gateway       : localhost:4566
  ├─ GCS endpoint      : localhost:4443
  ├─ BigQuery REST     : localhost:9050
  ├─ BigQuery gRPC     : localhost:9060
  ├─ Pub/Sub gRPC      : localhost:8085
  ├─ SMTP              : localhost:1025
  ├─ Dashboard/API     : localhost:8025
  ├─ Redshift wire     : localhost:5439
  └─ CDN proxy         : localhost:9090
```

データはローカルディレクトリに保存します。

```txt
.devcloud/
  config.yaml
  data/
    objects/
    kv/
    queues/
    sql/
    mail/
    cdn-cache/
  snapshots/
  logs/
```

---

## 推奨リポジトリ構成

```txt
devcloud/
  cmd/
    devcloud/
    devcloudd/

  internal/
    kernel/
      app.go
      config.go
      registry.go
      scheduler.go
      clock.go
      snapshot.go

    storage/
      wal/
      blob/
      kv/
      index/
      catalog/

    protocol/
      aws/
        sigv4/
        restxml/
        jsonrpc/
        queryapi/
      gcp/
        oauth/
        restjson/
        grpc/
      smtp/
      pgwire/

    services/
      mail/
      object/
        core/
        s3/
        gcs/
      kvdb/
        dynamodb/
      queue/
        core/
        sqs/
        pubsub/
      query/
        core/
        bigquery/
        redshift/
      cdn/
        core/
        cloudfront/

    dashboard/
    cli/
    testing/
      contract/
      fixtures/
```

重要なのは、**S3 と GCS を別々のストレージとして作らない** ことです。
同じように、**SQS と Pub/Sub を別々のキューとして作らない** ことです。
BigQuery と Redshift も、できるだけ同じ query engine を使います。

---

# 共通コア設計

## 1. Resource Registry

AWS/GCP の違いを吸収するため、内部リソースはクラウド非依存の形で持ちます。

```txt
Account
Project
Region
Namespace
Resource
```

例：

```yaml
accounts:
  - id: "000000000000"
    name: "local-aws"

projects:
  - id: "dev"
    name: "local-gcp"

regions:
  - ap-northeast-1
  - us-east-1
```

内部 resource ID はこうします。

```txt
object_bucket:dev:app-assets
queue:dev:events
topic:dev:user-created
dynamodb_table:dev:users
bigquery_dataset:dev:analytics
redshift_database:dev:warehouse
cdn_distribution:dev:dist-001
```

AWS API から来ても GCP API から来ても、最終的には内部 registry にマップします。

---

## 2. Storage Engine

全部自作するなら、まず最小限の永続化レイヤーを作ります。

```txt
storage/
  wal/       # write-ahead log
  blob/      # object body, attachments, query files
  kv/        # metadata, DynamoDB items, queue metadata
  index/     # secondary indexes
  catalog/   # schemas, tables, buckets, queues
```

最初は高性能な DB を作る必要はありません。

MVP はこれで十分です。

```txt
WAL + append-only files + in-memory index + periodic snapshot
```

イメージ：

```txt
write request
   |
   v
append WAL
   |
   v
apply in-memory state
   |
   v
periodic snapshot
```

これにより、DynamoDB、SQS、Pub/Sub、BigQuery catalog、S3 metadata、CloudFront cache metadata を同じ仕組みで扱えます。

---

## 3. Blob Store

S3/GCS、メール添付、BigQuery load file、CloudFront cache で使います。

```txt
blob/
  ab/
    cd/
      abcdef123456...blob
```

API：

```ts
interface BlobStore {
  put(stream, metadata): BlobID
  get(id): ReadStream
  delete(id): void
  stat(id): BlobStat
}
```

object 本体は直接ファイルに置き、metadata は KV に置くのが良いです。

---

## 4. Scheduler

SQS visibility timeout、Pub/Sub ack deadline、DynamoDB TTL、CloudFront invalidation、メール送信時刻などで使います。

```txt
scheduler
  - runAt
  - interval
  - retry
  - lease expiration
```

ここも共通化します。

---

# 各サービスの自作方針

## 1. メールサーバー

MailHog 風なら、最初は SMTP だけで十分です。

### 実装するもの

```txt
SMTP server
  HELO / EHLO
  MAIL FROM
  RCPT TO
  DATA
  RSET
  NOOP
  QUIT
  AUTH PLAIN
  AUTH LOGIN
  STARTTLS optional

Mail store
  raw RFC 5322 message
  parsed headers
  MIME parts
  attachments

Dashboard
  inbox
  search
  raw source
  attachment download
```

### 内部モデル

```ts
type MailMessage = {
  id: string
  from: string
  to: string[]
  subject: string
  headers: Record<string, string[]>
  raw: BlobID
  textBody?: string
  htmlBody?: string
  attachments: Attachment[]
  receivedAt: Time
}
```

最初は IMAP/POP3 は不要です。
開発用途では SMTP + Web UI + API があればほぼ足ります。

---

## 2. S3 / GCS 互換オブジェクトストレージ

ここは **Object Core** を中心にします。

```txt
S3 API  ---> Object Core <--- GCS API
```

### Object Core

```ts
type Bucket = {
  id: string
  name: string
  owner: string
  location: string
  versioning: boolean
  createdAt: Time
}

type ObjectEntry = {
  bucket: string
  key: string
  generation: string
  versionId?: string
  size: number
  etag: string
  contentType?: string
  metadata: Record<string, string>
  blob: BlobID
  createdAt: Time
  deletedAt?: Time
}
```

### S3 adapter で必要なもの

最初の MVP：

```txt
CreateBucket
DeleteBucket
ListBuckets
PutObject
GetObject
HeadObject
DeleteObject
ListObjectsV2
CopyObject
CreateMultipartUpload
UploadPart
CompleteMultipartUpload
AbortMultipartUpload
Presigned URL
```

後回し：

```txt
ACL
Bucket Policy
Lifecycle
Replication
Object Lock
SSE
Event notification
SelectObjectContent
```

### GCS adapter で必要なもの

MVP：

```txt
buckets.insert
buckets.get
buckets.list
buckets.delete

objects.insert
objects.get
objects.list
objects.delete
objects.copy
resumable upload
signed URL
```

### 重要な実装ポイント

S3 と GCS は似ていますが、完全には同じではありません。

| 項目             | S3               | GCS                         |
| -------------- | ---------------- | --------------------------- |
| object version | versionId        | generation / metageneration |
| metadata       | `x-amz-meta-*`   | object metadata             |
| signed URL     | AWS SigV4        | GCS V4 signing              |
| upload         | multipart upload | resumable upload            |
| listing        | ListObjectsV2    | objects.list                |
| auth           | SigV4            | OAuth / signed URL          |

なので内部では、S3 用語ではなく **中立的な Object Core 用語** にするべきです。

---

## 3. DynamoDB 完全互換を目指す KV Core

DynamoDB は自作しやすそうに見えて、実は **expression parser** が大変です。

### DynamoDB Core

```ts
type Table = {
  name: string
  partitionKey: Attribute
  sortKey?: Attribute
  gsis: GlobalSecondaryIndex[]
  lsis: LocalSecondaryIndex[]
  billingMode: "PAY_PER_REQUEST" | "PROVISIONED"
  status: "CREATING" | "ACTIVE" | "DELETING"
}

type Item = Record<string, AttributeValue>
```

### MVP API

```txt
CreateTable
DeleteTable
DescribeTable
ListTables

PutItem
GetItem
DeleteItem
UpdateItem

Query
Scan

BatchGetItem
BatchWriteItem
```

### 次に必要な API

```txt
ConditionExpression
UpdateExpression
ProjectionExpression
FilterExpression
ExpressionAttributeNames
ExpressionAttributeValues

GSI
LSI
TTL
Streams
TransactGetItems
TransactWriteItems
```

### 内部 index

```txt
primary index:
  pk -> item
  pk + sk -> item

GSI:
  gsi_pk -> item refs
  gsi_pk + gsi_sk -> item refs
```

DynamoDB 互換で重要なのは、単に key-value を保存することではなく、以下です。

```txt
AttributeValue の型表現
条件式
更新式
Query の KeyConditionExpression
Scan の pagination
LastEvaluatedKey
ConsumedCapacity 風レスポンス
ValidationException の再現
```

ここを先に実装方針として切り出した方が良いです。

---

## 4. SQS / Pub/Sub 互換メッセージング

ここも **Message Core** を作ります。

```txt
SQS API ---> Message Core <--- Pub/Sub API
```

### Message Core

```ts
type Topic = {
  id: string
  name: string
}

type Queue = {
  id: string
  name: string
  visibilityTimeout: Duration
  delaySeconds: Duration
  dlq?: string
  fifo: boolean
}

type Subscription = {
  id: string
  topic: string
  ackDeadline: Duration
  mode: "pull" | "push"
}

type Message = {
  id: string
  body: bytes
  attributes: Record<string, string>
  publishTime: Time
  availableAt: Time
  deliveryCount: number
  orderingKey?: string
  deduplicationId?: string
}
```

### SQS adapter MVP

```txt
CreateQueue
DeleteQueue
GetQueueUrl
GetQueueAttributes
SetQueueAttributes
ListQueues

SendMessage
SendMessageBatch
ReceiveMessage
DeleteMessage
DeleteMessageBatch
ChangeMessageVisibility
PurgeQueue
```

### Pub/Sub adapter MVP

```txt
CreateTopic
DeleteTopic
ListTopics
Publish

CreateSubscription
DeleteSubscription
ListSubscriptions
Pull
Acknowledge
ModifyAckDeadline
```

### 重要な違い

| 項目             | SQS                | Pub/Sub               |
| -------------- | ------------------ | --------------------- |
| message target | queue              | topic -> subscription |
| retry          | visibility timeout | ack deadline          |
| ack            | DeleteMessage      | Acknowledge           |
| ordering       | FIFO queue         | ordering key          |
| delay          | DelaySeconds       | 直接同等ではない              |
| DLQ            | redrive policy     | dead letter policy    |

内部では、SQS queue も Pub/Sub subscription も **delivery stream** として扱うと実装しやすいです。

```txt
Topic
  |
  +--> Subscription A
  +--> Subscription B

Queue
  |
  +--> implicit delivery stream
```

---

## 5. BigQuery 互換

一番重いです。
BigQuery は単なる REST API ではなく、SQL エンジン、ジョブ管理、スキーマ管理、ロードジョブ、ストリーミング insert、Storage API まであります。

自作するなら、分割します。

```txt
BigQuery API adapter
  |
  +-- Dataset / Table catalog
  +-- Job manager
  +-- Query engine
  +-- Storage engine
  +-- Result cache
```

### BigQuery MVP API

```txt
datasets.insert
datasets.get
datasets.list
datasets.delete

tables.insert
tables.get
tables.list
tables.delete

tabledata.insertAll
tabledata.list

jobs.insert
jobs.get
jobs.query
jobs.getQueryResults
```

### SQL engine の最小 subset

最初はこれで良いです。

```sql
SELECT
FROM
WHERE
GROUP BY
HAVING
ORDER BY
LIMIT
JOIN
UNION ALL
WITH
INSERT
CREATE TABLE
CREATE TABLE AS SELECT
DROP TABLE
```

BigQuery っぽさを出すには、この型を最初から意識します。

```txt
INT64
FLOAT64
NUMERIC
BOOL
STRING
BYTES
DATE
TIME
DATETIME
TIMESTAMP
ARRAY
STRUCT
JSON
```

ただし、ARRAY / STRUCT を最初から完璧にやると重いので、MVP では JSON 表現で内部保持して、SQL での操作は段階的に増やすのが良いです。

### Job manager

BigQuery は同期クエリだけではありません。
`jobs.insert` して、あとから `jobs.get` / `jobs.getQueryResults` する必要があります。

```ts
type Job = {
  id: string
  projectId: string
  type: "query" | "load" | "extract" | "copy"
  status: "PENDING" | "RUNNING" | "DONE"
  error?: ErrorProto
  statistics: JobStatistics
  resultTable?: TableRef
}
```

ローカルでは即時実行で良いですが、API 形式は非同期 job として持つべきです。

---

## 6. Redshift 互換

「中身は Postgres で OK」という条件を外して全自作にするなら、Redshift はこうなります。

```txt
Redshift JDBC / psql
   |
   v
PostgreSQL wire protocol server
   |
   v
Redshift dialect adapter
   |
   v
Query Engine
```

### 自作すべきもの

```txt
PostgreSQL wire protocol
  StartupMessage
  Authentication
  Query
  Parse
  Bind
  Describe
  Execute
  Sync
  RowDescription
  DataRow
  CommandComplete
  ErrorResponse

SQL dialect
  Redshift-ish DDL
  Redshift-ish functions
  system tables
```

### Redshift MVP

```sql
CREATE TABLE
DROP TABLE
INSERT
SELECT
UPDATE
DELETE
COPY local file subset
```

### Redshift 風 system schema

```txt
pg_catalog
information_schema
stl
stv
svv
```

最初は中身がダミーでも、JDBC/BI ツールが参照しがちな catalog は用意した方が良いです。

---

## 7. CloudFront 互換

CloudFront は **control plane** と **data plane** に分けます。

```txt
CloudFront API
  CreateDistribution
  GetDistribution
  UpdateDistribution
  DeleteDistribution
  CreateInvalidation
  GetInvalidation

CDN proxy
  Host routing
  Origin fetch
  Cache policy
  Header policy
  Signed URL
  Signed Cookie
  Range request
  Compression
```

### CloudFront Core

```ts
type Distribution = {
  id: string
  domainName: string
  origins: Origin[]
  defaultCacheBehavior: CacheBehavior
  cacheBehaviors: CacheBehavior[]
  enabled: boolean
}

type CacheBehavior = {
  pathPattern?: string
  targetOriginId: string
  viewerProtocolPolicy: string
  allowedMethods: string[]
  cachedMethods: string[]
  cachePolicy: CachePolicy
}

type CacheEntry = {
  cacheKey: string
  status: number
  headers: HeaderMap
  body: BlobID
  expiresAt: Time
  etag?: string
}
```

### ローカル CDN の挙動

```txt
client -> localhost:9090
  -> match distribution by Host header
  -> match behavior by path
  -> compute cache key
  -> cache hit: return
  -> cache miss: fetch origin
  -> store response
  -> return
```

CloudFront の完全な edge network は再現しなくてよいです。
ただし、アプリ開発で必要な以下は再現できます。

```txt
cache hit / miss
TTL
invalidation
origin headers
custom error response
signed URL
signed cookie
range request
compression
```

---

# 互換 API Gateway

## AWS 側

AWS 系は protocol がバラバラです。

| サービス              | 主な protocol  |
| ----------------- | ------------ |
| S3                | REST XML     |
| SQS               | Query API    |
| DynamoDB          | JSON RPC     |
| CloudFront        | REST XML     |
| Redshift Data API | JSON         |
| STS/IAM 風         | Query / JSON |

なので AWS gateway はこう分けます。

```txt
localhost:4566
  |
  +-- Host / path / x-amz-target で振り分け
      |
      +-- S3 REST XML router
      +-- SQS Query API router
      +-- DynamoDB JSON RPC router
      +-- CloudFront REST XML router
      +-- Redshift JSON router
```

### SigV4

全 AWS 互換の中心は SigV4 です。

MVP では 2 モード用意すると良いです。

```yaml
auth:
  aws:
    mode: relaxed
```

```txt
strict  : 署名を完全検証する
relaxed : access key / region / service は読むが、署名失敗でも通す
off     : 認証しない
```

開発用途では `relaxed` が便利です。
互換テストでは `strict` を使います。

---

## GCP 側

GCP は OAuth / REST JSON / gRPC が中心です。

```txt
GCP Gateway
  |
  +-- GCS JSON API
  +-- BigQuery REST API
  +-- BigQuery Storage gRPC
  +-- Pub/Sub gRPC
```

こちらも認証は 3 モードが良いです。

```yaml
auth:
  gcp:
    mode: relaxed
```

```txt
strict  : JWT / OAuth token を検証
relaxed : Authorization header は読むが通す
off     : 認証しない
```

---

# 設定ファイル案

```yaml
project: dev

server:
  awsGatewayPort: 4566
  gcsPort: 4443
  bigqueryRestPort: 9050
  bigqueryGrpcPort: 9060
  pubsubGrpcPort: 8085
  smtpPort: 1025
  dashboardPort: 8025
  redshiftPort: 5439
  cdnPort: 9090

auth:
  aws:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
  gcp:
    mode: relaxed
    projectId: dev

storage:
  path: .devcloud/data
  snapshotInterval: 30s

services:
  mail:
    enabled: true

  object:
    enabled: true
    buckets:
      - name: app-assets
      - name: uploads

  dynamodb:
    enabled: true
    tables:
      - name: users
        partitionKey:
          name: pk
          type: S
        sortKey:
          name: sk
          type: S

  queue:
    enabled: true
    sqs:
      queues:
        - name: events
    pubsub:
      topics:
        - name: user-created
          subscriptions:
            - name: user-created-sub

  bigquery:
    enabled: true
    datasets:
      - name: analytics

  redshift:
    enabled: true
    databases:
      - name: dev

  cloudfront:
    enabled: true
    distributions:
      - id: dist-local
        domainName: cdn.localhost
        origin:
          type: s3
          bucket: app-assets
```

---

# 実装順序

## Phase 0: Kernel

最初にこれを作ります。

```txt
config loader
resource registry
WAL
blob store
snapshot
scheduler
HTTP gateway
dashboard shell
CLI
```

ここを雑に作ると、後で全部が壊れます。

---

## Phase 1: メール + オブジェクトストレージ

最初の成功体験はこれです。

```bash
devcloud up
```

```bash
aws s3 mb s3://app-assets --endpoint-url http://localhost:4566
aws s3 cp ./hello.txt s3://app-assets/hello.txt --endpoint-url http://localhost:4566
aws s3 cp s3://app-assets/hello.txt - --endpoint-url http://localhost:4566
```

同時に SMTP も動かします。

```txt
localhost:1025 SMTP
localhost:8025 dashboard
```

この段階で dashboard に以下を出します。

```txt
Mail inbox
Buckets
Objects
Logs
```

---

## Phase 2: Queue Core + SQS

次に SQS を実装します。

```bash
aws sqs create-queue \
  --queue-name events \
  --endpoint-url http://localhost:4566

aws sqs send-message \
  --queue-url http://localhost:4566/queue/events \
  --message-body '{"hello":"world"}' \
  --endpoint-url http://localhost:4566

aws sqs receive-message \
  --queue-url http://localhost:4566/queue/events \
  --endpoint-url http://localhost:4566
```

この段階で、Message Core の設計を固めます。
Pub/Sub はこの後に同じ core に adapter を被せます。

---

## Phase 3: DynamoDB

次は DynamoDB です。

最低限：

```txt
CreateTable
PutItem
GetItem
Query
Scan
UpdateItem
DeleteItem
```

ここで重要なのは、`ConditionExpression` と `UpdateExpression` を避けないことです。
アプリ側ではかなり使われるため、ここがないと実用性が落ちます。

実装順序：

```txt
1. AttributeValue encoder/decoder
2. table schema
3. primary key index
4. Put/Get/Delete
5. Query/Scan
6. expression tokenizer
7. expression parser
8. condition evaluation
9. update expression
10. GSI
11. transaction
```

---

## Phase 4: Pub/Sub

Pub/Sub は gRPC が絡みます。
完全なクライアント互換を目指すなら gRPC server が必要です。

実装順序：

```txt
1. topics
2. subscriptions
3. publish
4. pull
5. acknowledge
6. modifyAckDeadline
7. orderingKey
8. dead letter policy
9. push subscription
10. streaming pull
```

最初は REST/JSON も用意しておくと dashboard やテストが楽です。

---

## Phase 5: Query Engine

BigQuery と Redshift のために、共通 query engine を作ります。

```txt
SQL text
  |
  v
lexer
  |
  v
parser
  |
  v
AST
  |
  v
logical plan
  |
  v
optimizer
  |
  v
physical plan
  |
  v
executor
```

MVP の executor は row-based で十分です。

```txt
scan
filter
project
join
aggregate
sort
limit
```

その後、必要に応じて columnar storage にします。

---

## Phase 6: BigQuery

Query Engine ができてから BigQuery adapter を作ります。

```txt
BigQuery REST API
  |
  +-- jobs.query
  +-- jobs.insert
  +-- jobs.get
  +-- jobs.getQueryResults
  +-- datasets.*
  +-- tables.*
  +-- tabledata.*
```

BigQuery は API レスポンス形式が重要です。
SQL の完全性より先に、SDK が期待する JSON 構造を丁寧に返すべきです。

---

## Phase 7: Redshift

Redshift は PostgreSQL wire protocol を作ります。

最初は simple query protocol だけでよいです。

```txt
StartupMessage
AuthenticationOk
ReadyForQuery
Query
RowDescription
DataRow
CommandComplete
ReadyForQuery
```

次に prepared statement 系を入れます。

```txt
Parse
Bind
Describe
Execute
Sync
```

JDBC/BI ツールを使うなら prepared statement が必要になりやすいです。

---

## Phase 8: CloudFront

最後で良いです。
CloudFront は API 互換よりも data-plane 挙動が大事です。

```txt
distribution config
origin
cache behavior
cache key
TTL
invalidation
signed URL
signed cookie
custom error response
```

最初の使い方：

```txt
S3 bucket app-assets
  |
  v
CloudFront local distribution
  |
  v
http://cdn.localhost:9090/logo.png
```

---

# 互換性の定義

自作で一番大事なのは、**互換性を宣言できる形にすること** です。

```yaml
compatibility:
  s3:
    CreateBucket: supported
    PutObject: supported
    GetObject: supported
    MultipartUpload: partial
    BucketPolicy: unsupported

  gcs:
    objects.insert: supported
    resumableUpload: partial
    iam: unsupported

  dynamodb:
    PutItem: supported
    ConditionExpression: supported
    TransactWriteItems: partial
    Streams: unsupported

  bigquery:
    jobs.query: supported
    standardSql: partial
    loadJob: partial
    storageApi: unsupported

  cloudfront:
    CreateDistribution: supported
    Invalidation: supported
    EdgeLocation: simulated
    LambdaAtEdge: unsupported
```

「完全互換」と言い切るより、**どの API がどの状態かを機械可読にする** 方が強いです。

---

# Contract Test の設計

外部プロダクトに依存したくない場合でも、互換性を保つにはテストが必要です。

ただし、常に本物の AWS/GCP を叩く必要はありません。
以下の 3 層に分けます。

```txt
1. Unit tests
   自作 core のテスト

2. SDK compatibility tests
   AWS SDK / GCP SDK が devcloud に接続できるか

3. Golden response tests
   期待するレスポンス JSON/XML を fixture として固定
```

本物のクラウドとの比較は、必要なときだけ手動または専用 CI で実行します。

```txt
contract/
  s3/
    put_object.yaml
    list_objects_v2.yaml
    multipart_upload.yaml

  dynamodb/
    condition_expression.yaml
    update_expression.yaml
    query_pagination.yaml

  bigquery/
    jobs_query.yaml
    tabledata_insert_all.yaml

  sqs/
    visibility_timeout.yaml
    dlq.yaml

  pubsub/
    ack_deadline.yaml
    redelivery.yaml
```

---

# 最初の MVP 範囲

全部を一気にやると巨大なので、最初の MVP はこれに絞るのが良いです。

```txt
devcloudd single binary

対応:
  - SMTP mail server
  - S3 basic API
  - SQS basic API
  - DynamoDB basic API
  - dashboard
  - seed/reset/snapshot

未対応:
  - BigQuery
  - Redshift
  - Pub/Sub
  - GCS
  - CloudFront
```

理由は、最初に作るべき共通コアが以下だからです。

```txt
blob store
kv store
message scheduler
AWS gateway
dashboard
snapshot
```

この基盤ができれば、GCS、Pub/Sub、BigQuery、Redshift、CloudFront を追加しやすくなります。

---

# 最小 MVP の機能表

## v0.1

```txt
devcloud up
devcloud down
devcloud reset
devcloud dashboard

SMTP
S3:
  CreateBucket
  ListBuckets
  PutObject
  GetObject
  HeadObject
  DeleteObject
  ListObjectsV2

SQS:
  CreateQueue
  SendMessage
  ReceiveMessage
  DeleteMessage

DynamoDB:
  CreateTable
  PutItem
  GetItem
  DeleteItem
  Query
  Scan
```

## v0.2

```txt
S3:
  multipart upload
  presigned URL

SQS:
  visibility timeout
  delay
  DLQ
  batch API

DynamoDB:
  ConditionExpression
  UpdateExpression
  GSI

Dashboard:
  object browser
  queue browser
  dynamodb table browser
```

## v0.3

```txt
GCS:
  buckets
  objects
  signed URL
  resumable upload

Pub/Sub:
  topic
  subscription
  publish
  pull
  ack
  modifyAckDeadline
```

## v0.4

```txt
Query Engine:
  SQL parser
  SELECT
  WHERE
  JOIN
  GROUP BY
  ORDER BY
  LIMIT

BigQuery:
  datasets
  tables
  jobs.query
  tabledata.insertAll
```

## v0.5

```txt
Redshift:
  PostgreSQL wire protocol
  simple query
  prepared statement
  system catalog subset
```

## v0.6

```txt
CloudFront:
  distribution
  origin
  cache
  invalidation
  signed URL
```

---

# 技術選定

## 実装言語

おすすめは **Go** です。

理由：

```txt
単一バイナリにしやすい
HTTP server が書きやすい
TCP protocol server が書きやすい
並行処理が扱いやすい
CLI と daemon を同じ言語で書ける
クロスコンパイルしやすい
```

Rust も良いですが、開発速度とチーム拡大を考えると Go の方が無難です。

---

## ライブラリ方針

外部影響を避けるなら、こういうポリシーが良いです。

```txt
1. 既存エミュレータは禁止
2. 重要な protocol 実装は自作
3. 汎用 parser / grpc / protobuf は vendoring 可能
4. 依存ライブラリは lockfile で固定
5. 依存ライブラリの license を CI で検査
6. 将来置き換えられるよう interface を切る
```

たとえば：

```txt
使わない:
  LocalStack
  MailHog
  Mailpit
  DynamoDB Local
  fake-gcs-server
  BigQuery emulator
  ElasticMQ
  Pub/Sub emulator
  MinIO

使ってもよい候補:
  protobuf / grpc
  yaml parser
  testing framework
  frontend build tools
```

厳密に全部自作したいなら、gRPC/protobuf も自作対象になります。
ただし、その場合 Pub/Sub と BigQuery Storage API の実装難度がかなり上がります。

---

# 最初に実装する core interface

## Object Core

```ts
interface ObjectCore {
  createBucket(input: CreateBucketInput): Bucket
  deleteBucket(name: string): void
  listBuckets(): Bucket[]

  putObject(input: PutObjectInput): ObjectVersion
  getObject(bucket: string, key: string, options?: GetObjectOptions): ObjectData
  headObject bucket: string, key: string): ObjectMetadata
  deleteObject(bucket: string, key: string): void
  listObjects(input: ListObjectsInput): ListObjectsResult

  createMultipartUpload(input: CreateMultipartUploadInput): UploadID
  uploadPart(input: UploadPartInput): PartETag
  completeMultipartUpload(input: CompleteMultipartUploadInput): ObjectVersion
}
```

## Message Core

```ts
interface MessageCore {
  createQueue(input: CreateQueueInput): Queue
  deleteQueue(name: string): void

  send(input: SendMessageInput): MessageID
  receive(input: ReceiveInput): ReceivedMessage[]
  ack(input: AckInput): void
  changeVisibility(input: ChangeVisibilityInput): void
  purge(queue: string): void

  createTopic(input: CreateTopicInput): Topic
  publish(input: PublishInput): MessageID
  createSubscription(input: CreateSubscriptionInput): Subscription
}
```

## KV Core

```ts
interface KvTableCore {
  createTable(input: CreateTableInput): Table
  putItem(input: PutItemInput): void
  getItem(input: GetItemInput): Item?
  updateItem(input: UpdateItemInput): Item?
  deleteItem(input: DeleteItemInput): Item?
  query(input: QueryInput): QueryResult
  scan(input: ScanInput): ScanResult
}
```

## Query Core

```ts
interface QueryCore {
  createDatabase(name: string): void
  createSchema(name: string): void
  createTable(input: CreateSqlTableInput): void
  insert(input: InsertInput): void
  query(sql: string, params?: Value[]): QueryResult
}
```

---

# 一番危険なところ

## 1. BigQuery SQL

BigQuery は SQL 方言がかなり大きいです。
ここを完璧にやろうとすると、プロジェクト全体の大半が SQL エンジン開発になります。

対策：

```txt
まず jobs.query の API 互換
次に SELECT subset
次に DDL/DML
次に ARRAY/STRUCT
次に window functions
次に BigQuery 独自関数
```

---

## 2. DynamoDB Expression

DynamoDB の `ConditionExpression` / `UpdateExpression` は避けられません。

対策：

```txt
expression lexer/parser を独立 module にする
AST を作る
評価器を作る
エラー文言 fixture を持つ
```

---

## 3. Pub/Sub gRPC

GCP クライアント互換には gRPC が必要になりがちです。

対策：

```txt
REST API adapter を先に作る
gRPC adapter は後で追加
protobuf 定義は vendoring
service implementation は自作
```

---

## 4. PostgreSQL wire protocol

Redshift 互換のために psql/JDBC を通したいなら、wire protocol が必要です。

対策：

```txt
Simple Query protocol から始める
Extended Query protocol は後で追加
SSL negotiation は最初は disable
system catalog は fixture から増やす
```

---

## 5. CloudFront

CloudFront は「API 互換」と「CDN 挙動互換」が別物です。

対策：

```txt
control plane:
  distribution / invalidation を実装

data plane:
  local reverse proxy cache を実装

edge network:
  simulated と明記
```

---

# 外部影響を避けるための運用設計

## Compatibility Profile

AWS/GCP の仕様変更に振り回されないよう、互換対象を固定します。

```yaml
compatibilityProfile:
  aws: "aws-local-2026-04"
  gcp: "gcp-local-2026-04"
```

将来の変更は profile を増やします。

```yaml
compatibilityProfile:
  aws: "aws-local-2026-10"
```

---

## Golden Files

レスポンス形式は fixture 化します。

```txt
fixtures/
  aws/
    s3/
      list_objects_v2.response.xml
    dynamodb/
      put_item.validation_error.json
    sqs/
      receive_message.response.xml
  gcp/
    bigquery/
      jobs_query.response.json
    pubsub/
      pull.response.json
```

API の互換性は、コードの雰囲気ではなく fixture で守ります。

---

## 自作 dashboard

管理 UI はかなり重要です。

```txt
Dashboard
  - mail inbox
  - object browser
  - queue browser
  - topic/subscription browser
  - dynamodb table browser
  - bigquery query console
  - redshift query console
  - cdn cache viewer
  - logs
  - compatibility matrix
```

開発者はここで「何が入っているか」「何が詰まっているか」を見たいです。

---

# 現実的な初期開発チーム規模

かなり大きいので、目安としてはこうです。

```txt
1人:
  v0.1 までは可能
  BigQuery/Redshift/CloudFront 完成度を上げるのは長期戦

2〜3人:
  v0.1〜v0.3 が現実的
  object / queue / kv を並行開発できる

5人以上:
  BigQuery/Redshift/CloudFront も本格的に進められる
```

一人で始めるなら、最初に BigQuery へ行かず、**S3 + SQS + DynamoDB + Mail + Dashboard** から始めるのが良いです。

---

# まとめ

全自作にするなら、方針はこれです。

```txt
既存エミュレータは使わない
単一バイナリで動かす
共通 kernel を作る
Object Core に S3/GCS adapter を載せる
Message Core に SQS/PubSub adapter を載せる
Query Core に BigQuery/Redshift adapter を載せる
KV Core に DynamoDB adapter を載せる
CloudFront は control plane と data plane を分ける
互換性は profile と fixture で固定する
```

最初の到達点はこれが良いです。

```txt
devcloud up

起動するもの:
  SMTP
  S3 basic
  SQS basic
  DynamoDB basic
  dashboard

提供するもの:
  seed
  reset
  snapshot
  AWS SDK 接続
  互換性 matrix
```


