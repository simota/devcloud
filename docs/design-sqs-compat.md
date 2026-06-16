# SQS Compatibility Design

## Summary

`devcloud` に Amazon SQS 互換のローカル message queue server を追加する。

目標は「AWS SDK / AWS CLI / SQS client が endpoint override だけで主要 workflow を実行できる」ことである。SQS の完全互換は Query protocol、AWS JSON protocol、standard queue、FIFO queue、visibility timeout、long polling、dead-letter queue、redrive、policy、tagging、SSE、fair queue、CloudWatch 互換 metrics まで広いため、実装は段階化する。ただし内部設計は最初から SQS と将来の Pub/Sub adapter が共有できる Message Core を中心に置き、API adapter、delivery scheduler、receipt handle、deduplication、dashboard を分離して拡張できる形にする。

## Document Control

| Field | Value |
| --- | --- |
| Audience | devcloud implementers, reviewers, future agent loops |
| Status | Draft |
| Owner | devcloud maintainers |
| Reviewer | TBD |
| Related docs | `docs/spec-v0.md`, `docs/design-s3-compat.md`, `docs/design-gcs-compat.md`, `docs/design-dynamodb-compat.md`, `docs/design-dashboard-shell.md` |
| Primary references | Amazon SQS API Reference, Amazon SQS Developer Guide, SQS queue types, visibility timeout, FIFO queues, dead-letter queues |
| Reference check date | 2026-05-02 |

## Compatibility Goal

### Definition

ここでの SQS 互換は、以下を満たす状態を指す。

1. AWS SDK / AWS CLI が endpoint override だけで接続できる。
2. SQS Query protocol の `Action` / `Version=2012-11-05` と XML response / error response を主要操作で再現できる。
3. SQS AWS JSON protocol の `POST /`、`Content-Type: application/x-amz-json-1.0`、`X-Amz-Target: AmazonSQS.{Operation}`、JSON request / response shape を主要操作で再現できる。
4. SigV4 signed request と local credential を `relaxed` / `strict` mode で扱える。
5. standard queue の at-least-once delivery、best-effort ordering、visibility timeout、delay、retention、long polling の observable behavior を local deterministic state で再現できる。
6. FIFO queue の `MessageGroupId` ordering、`MessageDeduplicationId` / content-based deduplication、sequence number を段階的に追加できる。
7. DLQ / redrive policy、batch operations、message attributes、system attributes、tags、queue policies を段階的に追加できる。
8. 実 AWS、LocalStack、ElasticMQ などの外部 emulator に依存せず、local filesystem backed state として動作する。

### Compatibility Levels

| Level | Name | Purpose |
| --- | --- | --- |
| L0 | SDK Smoke | AWS CLI / SDK が endpoint override で疎通できる |
| L1 | Queue Core | CreateQueue、ListQueues、GetQueueUrl、Get/SetQueueAttributes、DeleteQueue が通る |
| L2 | Message Core | SendMessage、ReceiveMessage、DeleteMessage、ChangeMessageVisibility、PurgeQueue が通る |
| L3 | Batch and Long Polling | Send/Receive/Delete/Visibility batch、WaitTimeSeconds、message attributes が通る |
| L4 | FIFO and Dedup | FIFO queue、message group ordering、deduplication、sequence number が通る |
| L5 | DLQ and Redrive | RedrivePolicy、RedriveAllowPolicy、ListDeadLetterSourceQueues、message move task が通る |
| L6 | Operational Parity | policy、permissions、tagging、SSE metadata、fair queue、metrics compatibility を追加 |

MVP は L1 + L2 + L3 の一部を対象にする。最終的な「完全互換」は L6 までを含む長期目標とする。

## Goals

- REQ-001: `devcloud up` で SQS endpoint を起動する。
- REQ-002: AWS CLI / SDK が `endpoint-url` または endpoint override で queue 作成、send、receive、delete、visibility timeout 変更を実行できる。
- REQ-003: Query protocol と AWS JSON protocol の request dispatch、response shape、error shape を実装する。
- REQ-004: SigV4 を `relaxed` / `strict` mode で扱い、strict mode では local credential で署名検証する。
- REQ-005: message body、message attributes、system attributes、MD5 digest、receipt handle、receive count を SQS 互換の型ルールで保存・返却する。
- REQ-006: visibility timeout、delay seconds、retention period、long polling を deterministic scheduler で制御する。
- REQ-007: FIFO queue の metadata と validation は最初から保持し、L4 で ordering / dedup を追加できる storage model にする。
- REQ-008: `QueueDoesNotExist`、`ReceiptHandleIsInvalid`、`InvalidMessageContents`、`InvalidAttributeValue` などの error name、HTTP status、XML/JSON shape を SDK が解釈できる形で返す。
- REQ-009: dashboard/API から queues、attributes、visible/in-flight/delayed messages、recent receives、DLQ relationship を確認できる。
- REQ-010: `devcloud reset` で SQS data、receipt handles、delay/visibility schedule、dedup cache、redrive task state を削除できる。
- REQ-011: stage based verification script で SDK smoke、protocol contract、scheduler behavior、dashboard smoke を実行できる。
- REQ-012: S3 event notification や将来の Pub/Sub adapter が同じ Message Core を使える境界を設計する。

## Non-Goals

- 実 AWS IAM、STS、CloudWatch、CloudTrail、KMS とは連携しない。
- LocalStack、ElasticMQ、実 SQS service には依存しない。
- multi-AZ durability、SQS internal replication、AWS service quota、actual throughput billing は再現しない。
- CloudWatch metrics / alarms は初期対象外とする。dashboard 用の local counters から始める。
- SSE-KMS / SSE-SQS の実暗号処理は初期対象外とする。attribute compatibility と metadata preservation から始める。
- queue policy、AddPermission / RemovePermission の実 authorization は初期対象外とする。互換 response と validation から始める。
- Lambda event source mapping、EventBridge pipes、SNS subscription delivery などの周辺 AWS integration は初期対象外とする。
- SQS と Pub/Sub の完全な意味論一致は目標にしない。共有するのは Message Core と delivery stream の内部モデルであり、外部 API behavior は adapter ごとに分ける。

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

aws --endpoint-url http://127.0.0.1:9324 sqs create-queue \
  --queue-name demo

QUEUE_URL=$(aws --endpoint-url http://127.0.0.1:9324 sqs get-queue-url \
  --queue-name demo \
  --query QueueUrl \
  --output text)

aws --endpoint-url http://127.0.0.1:9324 sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-body '{"type":"demo","id":"msg-1"}'

aws --endpoint-url http://127.0.0.1:9324 sqs receive-message \
  --queue-url "$QUEUE_URL" \
  --wait-time-seconds 1 \
  --attribute-names All \
  --message-attribute-names All
```

SDK clients should use:

```txt
endpoint: http://127.0.0.1:9324
region: us-east-1
accessKeyId: dev
secretAccessKey: dev
```

Query protocol with curl:

```bash
curl -sS http://127.0.0.1:9324/ \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "Action=CreateQueue" \
  --data-urlencode "Version=2012-11-05" \
  --data-urlencode "QueueName=demo"
```

AWS JSON protocol with curl:

```bash
curl -sS http://127.0.0.1:9324/ \
  -H "Content-Type: application/x-amz-json-1.0" \
  -H "X-Amz-Target: AmazonSQS.CreateQueue" \
  -d '{"QueueName":"demo"}'
```

## Scope

### v0.1 SQS MVP

```txt
Daemon:
  SQS endpoint        http://127.0.0.1:9324
  Dashboard/API       http://127.0.0.1:8025

Queue API:
  CreateQueue
  DeleteQueue
  GetQueueUrl
  ListQueues
  GetQueueAttributes
  SetQueueAttributes
  PurgeQueue

Message API:
  SendMessage
  ReceiveMessage
  DeleteMessage
  ChangeMessageVisibility

Compatibility:
  Query protocol XML success/error response
  AWS JSON protocol success/error response
  SigV4 relaxed and strict modes
  queue URL shape and ARN shape
  standard queue only delivery semantics
  DelaySeconds and VisibilityTimeout
  MessageRetentionPeriod cleanup
  ReceiveMessage WaitTimeSeconds basics
  receipt handle invalidation
  MD5OfMessageBody
  ApproximateReceiveCount
  ApproximateFirstReceiveTimestamp
  SentTimestamp
  SQS-compatible errors
```

### v0.2 Batch, Attributes, and Long Polling

```txt
Batch:
  SendMessageBatch
  DeleteMessageBatch
  ChangeMessageVisibilityBatch

Receive:
  MaxNumberOfMessages up to 10
  long polling wait up to 20 seconds
  ReceiveRequestAttemptId accepted for FIFO future compatibility

Attributes:
  MessageAttributes
  MessageSystemAttributes
  AWSTraceHeader
  MD5OfMessageAttributes
  MD5OfMessageSystemAttributes
  ApproximateNumberOfMessages
  ApproximateNumberOfMessagesNotVisible
  ApproximateNumberOfMessagesDelayed

Queue metadata:
  tags at create
  TagQueue
  UntagQueue
  ListQueueTags
```

### v0.3 FIFO and Deduplication

```txt
FIFO:
  FifoQueue
  .fifo queue name validation
  MessageGroupId required
  MessageDeduplicationId
  ContentBasedDeduplication
  SequenceNumber
  per-message-group ordering
  one in-flight message per group delivery guard
  5-minute deduplication window

High throughput metadata:
  DeduplicationScope
  FifoThroughputLimit
```

### v0.4 DLQ and Redrive

```txt
Dead-letter queues:
  RedrivePolicy
  RedriveAllowPolicy
  maxReceiveCount
  DLQ type validation
  ListDeadLetterSourceQueues

Redrive tasks:
  StartMessageMoveTask
  ListMessageMoveTasks
  CancelMessageMoveTask
```

### Later

```txt
AddPermission / RemovePermission policy compatibility
queue policy condition parsing
ListQueueTags / TagQueue / UntagQueue full validation
SSE-SQS / SSE-KMS metadata compatibility
fair queue behavior for MessageGroupId on standard queues
CloudWatch-compatible local metrics endpoint
SNS -> SQS local subscription integration
S3 event notification -> SQS integration
Pub/Sub adapter over Message Core
message move task rate controls
large payload offload helper metadata
```

## Architecture

```txt
AWS CLI / SDK / SQS client
        |
        v
+-----------------------------+
| SQS HTTP Gateway            | :9324
| Query + AWS JSON protocol   |
| SigV4 verifier              |
+-----------------------------+
        |
        v
+-----------------------------+
| SQS API Adapter             |
| operation dispatch          |
| request/response codec      |
| SQS validation/errors       |
+-----------------------------+
        |
        v
+-----------------------------+
| Message Core                |
| queue registry              |
| delivery stream             |
| scheduler                   |
| receipt handles             |
| dedup cache                 |
+-----------------------------+
        |
        v
+-----------------------------+
| Filesystem Store            |
| metadata + messages + WAL   |
+-----------------------------+
        |
        +---- Dashboard API / UI
```

Message Core は SQS adapter から SQS 用語を受け取るが、内部では `Queue`、`DeliveryStream`、`Message`、`Lease`、`DeadLetterRoute` で表現する。将来 Pub/Sub adapter は topic / subscription を同じ `DeliveryStream` に割り当てる。

## Repository Layout

```txt
services/sqs/
  server.rs              # HTTP server and route dispatch
  protocol.rs            # Query and AWS JSON protocol codecs
  operations.rs          # operation handlers
  errors.rs              # SQS-compatible error responses
  sigv4.rs               # AWS SigV4 verifier, shared shape with S3/DynamoDB
  model.rs               # SQS request/response/resource structs
  attributes.rs          # queue/message attribute validation
  md5.rs                 # SQS MD5 calculation helpers
  fifo.rs                # FIFO ordering and dedup helpers
  server_test.rs
  protocol_test.rs
  scheduler_test.rs

services/message/
  store.rs               # queue and message persistence interface
  fs.rs                  # filesystem-backed implementation
  wal.rs                 # append/replay crash recovery
  lease.rs               # visibility timeout receipt leases
  scheduler.rs           # delay/visibility/retention timers
  store_test.rs

orchestrator/
  config.rs              # server.sqsPort, auth.sqs, services.sqs
  daemon.rs              # SQS lifecycle wiring

services/dashboard/
  server.rs              # service registry + /api/sqs/*
  services.rs            # SQS dashboard status metadata

web/dashboard/src/app/services/sqs/
  SqsDashboard.tsx
  api.ts
  types.ts

scripts/sqs-autoloop/
  README.md
  bootstrap.sh
  goal.md
  recover.sh
  run-loop.sh
  verify.sh

scripts/sqs-e2e.sh
docs/design-sqs-compat.md
```

## Configuration

Default config:

```yaml
server:
  sqsPort: 9324

auth:
  sqs:
    mode: relaxed
    accessKeyId: dev
    secretAccessKey: dev
    accountId: "000000000000"

services:
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
    fifo:
      enabled: true
      deduplicationWindowSeconds: 300
    redrive:
      enabled: true
      maxMoveTaskMessages: 10000
```

### Config Integration

- `DefaultConfig()` に SQS defaults を追加する。
- `.devcloud/config.yaml` generation に `server.sqsPort`、`auth.sqs`、`services.sqs` を追加する。
- `LoadConfig` は duration / size / count の下限と上限を検証する。
- `Workspace` 初期化で `.devcloud/data/sqs` を作成する。
- `devcloud reset` は `.devcloud/data/sqs` を削除対象に含める。
- disabled mode では SQS endpoint と dashboard API を登録しない。

## API Mapping

### Protocol Dispatch

| Protocol | Request shape | Operation source | Response |
| --- | --- | --- | --- |
| AWS JSON | `POST /`, `Content-Type: application/x-amz-json-1.0` | `X-Amz-Target: AmazonSQS.{Operation}` | JSON |
| Query | `POST /` or queue path, `application/x-www-form-urlencoded` | `Action` parameter | XML |
| Query GET | `GET /?Action=...` | query string | XML |

AWS JSON protocol を優先実装しつつ、AWS CLI / SDK の設定差を吸収するため Query protocol も MVP に含める。内部 handler は protocol に依存しない command struct を受け取り、codec が request/response 変換だけを担当する。

### Queue Operations

| SQS operation | Target | Internal command | MVP |
| --- | --- | --- | --- |
| `CreateQueue` | `AmazonSQS.CreateQueue` / `Action=CreateQueue` | `CreateQueue` | Yes |
| `DeleteQueue` | `AmazonSQS.DeleteQueue` / `Action=DeleteQueue` | `DeleteQueue` | Yes |
| `GetQueueUrl` | `AmazonSQS.GetQueueUrl` / `Action=GetQueueUrl` | `GetQueueURL` | Yes |
| `ListQueues` | `AmazonSQS.ListQueues` / `Action=ListQueues` | `ListQueues` | Yes |
| `GetQueueAttributes` | `AmazonSQS.GetQueueAttributes` / `Action=GetQueueAttributes` | `GetQueueAttributes` | Yes |
| `SetQueueAttributes` | `AmazonSQS.SetQueueAttributes` / `Action=SetQueueAttributes` | `SetQueueAttributes` | Yes |
| `PurgeQueue` | `AmazonSQS.PurgeQueue` / `Action=PurgeQueue` | `PurgeQueue` | Yes |
| `ListDeadLetterSourceQueues` | `AmazonSQS.ListDeadLetterSourceQueues` / `Action=ListDeadLetterSourceQueues` | `ListDLQSources` | Later |

### Message Operations

| SQS operation | Target | Internal command | MVP |
| --- | --- | --- | --- |
| `SendMessage` | `AmazonSQS.SendMessage` / `Action=SendMessage` | `SendMessage` | Yes |
| `ReceiveMessage` | `AmazonSQS.ReceiveMessage` / `Action=ReceiveMessage` | `ReceiveMessages` | Yes |
| `DeleteMessage` | `AmazonSQS.DeleteMessage` / `Action=DeleteMessage` | `DeleteMessage` | Yes |
| `ChangeMessageVisibility` | `AmazonSQS.ChangeMessageVisibility` / `Action=ChangeMessageVisibility` | `ChangeVisibility` | Yes |
| `SendMessageBatch` | `AmazonSQS.SendMessageBatch` / `Action=SendMessageBatch` | `SendMessageBatch` | v0.2 |
| `DeleteMessageBatch` | `AmazonSQS.DeleteMessageBatch` / `Action=DeleteMessageBatch` | `DeleteMessageBatch` | v0.2 |
| `ChangeMessageVisibilityBatch` | `AmazonSQS.ChangeMessageVisibilityBatch` / `Action=ChangeMessageVisibilityBatch` | `ChangeVisibilityBatch` | v0.2 |

### Policy and Tag Operations

| SQS operation | Target | Internal command | MVP |
| --- | --- | --- | --- |
| `AddPermission` | `AmazonSQS.AddPermission` / `Action=AddPermission` | `AddPermission` | Later |
| `RemovePermission` | `AmazonSQS.RemovePermission` / `Action=RemovePermission` | `RemovePermission` | Later |
| `TagQueue` | `AmazonSQS.TagQueue` / `Action=TagQueue` | `TagQueue` | v0.2 |
| `UntagQueue` | `AmazonSQS.UntagQueue` / `Action=UntagQueue` | `UntagQueue` | v0.2 |
| `ListQueueTags` | `AmazonSQS.ListQueueTags` / `Action=ListQueueTags` | `ListQueueTags` | v0.2 |

## Resource Model

### Queue

```go
type Queue struct {
	ID              string
	Name            string
	URL             string
	ARN             string
	Region          string
	AccountID       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Attributes      QueueAttributes
	Tags            map[string]string
	DeletedAt       *time.Time
}
```

Queue identity は `region/account/name` で一意にする。Queue URL は `http://127.0.0.1:{port}/{accountId}/{queueName}` を基本形にし、path-style endpoint と root endpoint の両方を受ける。

### QueueAttributes

```go
type QueueAttributes struct {
	VisibilityTimeout              time.Duration
	MaximumMessageSize             int64
	MessageRetentionPeriod         time.Duration
	DelaySeconds                   time.Duration
	ReceiveMessageWaitTimeSeconds  time.Duration
	Policy                         string
	RedrivePolicy                  *RedrivePolicy
	RedriveAllowPolicy             *RedriveAllowPolicy
	FifoQueue                      bool
	ContentBasedDeduplication      bool
	DeduplicationScope             string
	FifoThroughputLimit            string
	SqsManagedSseEnabled           bool
}
```

Attribute values は SQS と同じく string map として受け取り、内部で typed value に変換する。unknown attribute は `InvalidAttributeName` として返す。

### Message

```go
type Message struct {
	ID                 string
	QueueID            string
	Body               string
	Attributes         map[string]MessageAttribute
	SystemAttributes   map[string]MessageAttribute
	MD5OfBody          string
	MD5OfAttributes    string
	SentAt             time.Time
	AvailableAt        time.Time
	ExpiresAt          time.Time
	ReceiveCount       int
	FirstReceivedAt    *time.Time
	MessageGroupID     string
	DeduplicationID    string
	SequenceNumber     string
	State              MessageState
}
```

### Receipt Lease

```go
type ReceiptLease struct {
	Handle       string
	MessageID    string
	QueueID      string
	IssuedAt     time.Time
	VisibleAfter time.Time
	Generation   int64
	Deleted      bool
}
```

Receipt handle は receive ごとに新しく発行する。古い handle は message が再表示された時点で invalid になり、`ReceiptHandleIsInvalid` または `InvalidParameterValue` に変換する。

### MessageState

```txt
delayed      SendMessage DelaySeconds or queue delay is active
visible      ReceiveMessage can lease the message
in_flight    receipt lease exists until visibility timeout expires
deleted      DeleteMessage completed
expired      retention cleanup completed
dead_letter  moved to DLQ
```

## Storage Layout

```txt
.devcloud/data/sqs/
  queues/
    {queue-id}/
      queue.json
      messages/
        {message-id}.json
      leases/
        {receipt-hash}.json
      dedup/
        {dedup-key}.json
      wal.log
  indexes/
    queue-name.json
    queue-url.json
  scheduler/
    delayed.json
    visibility.json
    retention.json
    redrive.json
```

MVP は filesystem store から始める。ただし message state mutation は `wal append -> entity atomic rewrite -> index atomic rewrite -> commit marker` の順にし、process crash 後に replay できる設計にする。

`ReceiveMessage` は visible candidates を store lock 内で lease し、lease の発行と message state の `in_flight` 化を同一 atomic boundary に置く。これにより local process 内では同じ receipt window で二重 lease されない。

## Request Handling

### Dispatch Flow

```txt
HTTP request
  -> request id assignment
  -> protocol detection
  -> SigV4 relaxed/strict validation
  -> operation decode
  -> input validation
  -> command execution
  -> response encoding
```

SQS endpoint は root `POST /` と queue URL path の両方を受ける。Query protocol では queue path から `QueueUrl` を補完できるが、operation parameter に明示された `QueueUrl` を優先する。

### Visibility and Scheduling

`ReceiveMessage` は message を返すときに receipt lease を発行し、`VisibleAfter = now + VisibilityTimeout` を設定する。scheduler は以下を周期実行する。

1. delayed message の `AvailableAt <= now` を visible にする。
2. in-flight message の `VisibleAfter <= now` を visible に戻し、old receipt handle を invalid にする。
3. retention を超えた message を expired にする。
4. receive count が redrive threshold を超えた message を DLQ に move する。
5. FIFO dedup cache の expired entries を削除する。

Long polling は `ReceiveMessage` handler 内で deadline まで visible message を待つ。busy wait ではなく condition channel + ticker の併用とし、server shutdown context で即時終了する。

### Standard Queue Semantics

AWS の standard queue は at-least-once delivery と best-effort ordering である。local implementation は deterministic FIFO-ish order で visible messages を返すが、互換 contract として strict ordering は保証しない。duplicate delivery は forced compatibility mode では再現可能にするが、MVP の通常運用では local tests の安定性を優先し、同一 visibility window 内の重複 lease は避ける。

### FIFO Queue Semantics

FIFO queue は L4 で有効化する。設計上は v0.1 から以下の metadata を保存する。

- queue name must end with `.fifo` when `FifoQueue=true`
- `MessageGroupId` is required on send to FIFO queue
- `MessageDeduplicationId` or content-based dedup key is required
- sequence number is monotonically increasing per queue or per message group
- same message group has at most one active delivery group

MVP で未実装の FIFO behavior は `UnsupportedOperation` ではなく、queue 作成時に `FifoQueue` を拒否するか、明示的な compatibility level error を返す。中途半端な FIFO delivery は SDK users を誤解させるため避ける。

## Response Compatibility

### Headers

All successful responses should include:

```txt
x-amzn-RequestId: {request-id}
Date: {http-date}
Content-Type: application/x-amz-json-1.0
```

Query protocol response:

```txt
Content-Type: text/xml
```

### Query XML Shape

```xml
<SendMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <SendMessageResult>
    <MessageId>...</MessageId>
    <MD5OfMessageBody>...</MD5OfMessageBody>
  </SendMessageResult>
  <ResponseMetadata>
    <RequestId>...</RequestId>
  </ResponseMetadata>
</SendMessageResponse>
```

### AWS JSON Shape

```json
{
  "MessageId": "...",
  "MD5OfMessageBody": "..."
}
```

## Error Model

SQS error は protocol ごとに shape が異なるが、内部では共通の `SQSError` を使う。

| Condition | HTTP | SQS code |
| --- | --- | --- |
| unknown operation | 400 | `InvalidAction` |
| missing queue | 400 | `QueueDoesNotExist` |
| duplicate queue with different attributes | 400 | `QueueNameExists` |
| recently deleted queue name | 400 | `QueueDeletedRecently` |
| invalid queue name | 400 | `InvalidAddress` |
| invalid or unknown attribute | 400 | `InvalidAttributeName` / `InvalidAttributeValue` |
| invalid receipt handle | 400 | `ReceiptHandleIsInvalid` |
| message body has invalid characters | 400 | `InvalidMessageContents` |
| message too large | 400 | `InvalidParameterValue` |
| missing required parameter | 400 | `MissingParameter` |
| strict auth failure | 400 | `InvalidSecurity` |
| unsupported staged feature | 400 | `UnsupportedOperation` |
| throttling compatibility mode | 400 | `RequestThrottled` |

Query protocol error:

```xml
<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <Error>
    <Type>Sender</Type>
    <Code>QueueDoesNotExist</Code>
    <Message>The specified queue does not exist.</Message>
  </Error>
  <RequestId>...</RequestId>
</ErrorResponse>
```

AWS JSON error:

```json
{
  "__type": "com.amazonaws.sqs#QueueDoesNotExist",
  "message": "The specified queue does not exist."
}
```

## Authentication

### Modes

| Mode | Behavior | Use |
| --- | --- | --- |
| `relaxed` | Accept unsigned and signed local requests | default developer ergonomics |
| `strict` | Require SigV4 and configured local credentials | SDK compatibility tests |
| `disabled` | Reject all requests except health checks | operational toggle |

Strict mode validates:

- `Authorization` credential scope service is `sqs`
- region matches configured `services.sqs.region`
- access key matches `auth.sqs.accessKeyId`
- canonical request signature matches `auth.sqs.secretAccessKey`
- `X-Amz-Date` skew is within configured tolerance

Do not log authorization headers, canonical request, message body, or full queue policy. Debug logs may include request id, operation, queue name, status code, and SQS error code.

## Message Attributes

Message attributes follow SQS shape:

```json
{
  "AttributeName": {
    "DataType": "String",
    "StringValue": "value"
  }
}
```

Supported MVP types:

| DataType | MVP | Notes |
| --- | --- | --- |
| `String` | Yes | UTF-8 string, included in MD5OfMessageAttributes |
| `Number` | Yes | stored as original string, numeric validation only |
| `Binary` | v0.2 | base64 JSON value or Query binary field |
| `String.List` | Later | accepted only after MD5 compatibility tests |
| `Binary.List` | Later | accepted only after MD5 compatibility tests |

System attribute MVP:

| Name | MVP | Notes |
| --- | --- | --- |
| `AWSTraceHeader` | v0.2 | preserve and return when requested |
| `SentTimestamp` | Yes | returned as message system attribute |
| `ApproximateFirstReceiveTimestamp` | Yes | returned after first receive |
| `ApproximateReceiveCount` | Yes | incremented on each receive |
| `MessageDeduplicationId` | v0.3 | FIFO |
| `MessageGroupId` | v0.3 | FIFO / fair queue |
| `SequenceNumber` | v0.3 | FIFO |

## Dashboard Integration

### Service Registry

`/api/dashboard/services` に SQS を追加する。

```json
{
  "id": "sqs",
  "name": "SQS",
  "status": "running",
  "endpoint": "http://127.0.0.1:9324",
  "dashboardPath": "/dashboard/sqs"
}
```

### Dashboard API

```txt
GET /api/sqs/status
GET /api/sqs/queues
GET /api/sqs/queues/{queueName}
GET /api/sqs/queues/{queueName}/messages
GET /api/sqs/queues/{queueName}/leases
GET /api/sqs/queues/{queueName}/dlq
POST /api/sqs/queues/{queueName}/purge
```

Dashboard API は observability layer とする。SDK/CLI が書き込んだ状態を確認するための読み取り中心 UI であり、MVP では message mutation は `purge` のみに限定する。

### UI Surface

SQS dashboard は共通 dashboard shell に追加する。

```txt
+----------------------------------------------------------------------------+
| devcloud  [Mail] [S3] [DynamoDB] [BigQuery] [SQS]              Refresh      |
+----------------------------------------------------------------------------+
| Queues                  | Queue detail                                      |
| - demo                  | URL, ARN, attributes, counts                    |
| - jobs.fifo             | visible / in-flight / delayed / DLQ            |
|                         |                                                  |
|                         | Messages                                         |
|                         | body preview, attributes, receive count, state   |
+----------------------------------------------------------------------------+
```

表示項目:

- queue name / URL / ARN
- queue attributes and tags
- approximate counts
- message body preview with safe escaping
- message attributes / system attributes
- visible / delayed / in-flight / dead-letter state
- receipt lease expiration
- recent operations
- DLQ relationship and redrive counters

Message body は user data なので logs には出さない。UI では local dashboard user の明示操作として表示するが、HTML escaping と size limit を必須にする。

## Implementation Plan

### Phase 0: Design and Test Harness

- IMPL-001: `docs/design-sqs-compat.md` を確定する。
- IMPL-002: SQS API contract fixtures を `services/sqs/testdata` に追加する。
- IMPL-003: `scripts/sqs-autoloop/verify.sh` の stage skeleton を用意する。
- IMPL-004: `scripts/sqs-e2e.sh` の CLI/curl smoke skeleton を用意する。

### Phase 1: Config and Daemon

- IMPL-005: `server.sqsPort`、`auth.sqs`、`services.sqs` を config に追加する。
- IMPL-006: default config、config validation、config tests を更新する。
- IMPL-007: `.devcloud/data/sqs` workspace と reset 対象を追加する。
- IMPL-008: `devcloud up` で SQS endpoint を起動する。
- IMPL-009: `/api/dashboard/services` に SQS service registry を追加する。

### Phase 2: Protocol and Queue Core

- IMPL-010: `services/sqs` package を追加する。
- IMPL-011: AWS JSON protocol dispatch を実装する。
- IMPL-012: Query protocol decode / XML encode を実装する。
- IMPL-013: SQS-compatible error helper を実装する。
- IMPL-014: CreateQueue、ListQueues、GetQueueUrl、GetQueueAttributes、SetQueueAttributes、DeleteQueue を実装する。
- IMPL-015: queue attribute validation と URL / ARN generation を実装する。

### Phase 3: Message Core

- IMPL-016: filesystem-backed Message Core store を実装する。
- IMPL-017: SendMessage、ReceiveMessage、DeleteMessage、ChangeMessageVisibility、PurgeQueue を実装する。
- IMPL-018: receipt handle generation / invalidation を実装する。
- IMPL-019: delay、visibility timeout、retention scheduler を実装する。
- IMPL-020: MD5OfMessageBody と core system attributes を実装する。

### Phase 4: Batch, Attributes, and Long Polling

- IMPL-021: SendMessageBatch、DeleteMessageBatch、ChangeMessageVisibilityBatch を実装する。
- IMPL-022: MessageAttributes と MD5OfMessageAttributes を実装する。
- IMPL-023: long polling と WaitTimeSeconds を実装する。
- IMPL-024: TagQueue、UntagQueue、ListQueueTags を実装する。
- IMPL-025: approximate queue counts を実装する。

### Phase 5: FIFO

- IMPL-026: FIFO queue creation validation を実装する。
- IMPL-027: MessageGroupId ordering と group lock を実装する。
- IMPL-028: MessageDeduplicationId と content-based deduplication を実装する。
- IMPL-029: SequenceNumber を実装する。
- IMPL-030: FIFO SDK smoke を追加する。

### Phase 6: DLQ, Redrive, and Dashboard

- IMPL-031: RedrivePolicy と RedriveAllowPolicy を実装する。
- IMPL-032: DLQ move scheduler を実装する。
- IMPL-033: ListDeadLetterSourceQueues を実装する。
- IMPL-034: message move task operations を実装する。
- IMPL-035: SQS dashboard API と React page を追加する。

### Phase 7: Operational Surface

- IMPL-036: AddPermission / RemovePermission compatibility response を実装する。
- IMPL-037: policy validation subset を実装する。
- IMPL-038: SSE attributes compatibility を実装する。
- IMPL-039: S3 event notification integration を追加する。
- IMPL-040: Pub/Sub adapter のため Message Core boundary を抽出・安定化する。

## Verification Plan

### Unit Tests

- protocol detection and operation dispatch
- Query protocol parameter parsing including repeated batch fields
- XML response and XML error encoding
- AWS JSON response and JSON error encoding
- queue name validation including `.fifo`
- queue attribute defaulting and validation
- queue URL and ARN generation
- message body character and size validation
- MD5OfMessageBody and MD5OfMessageAttributes
- receipt handle generation and invalidation
- visibility timeout expiration
- delayed message availability
- retention cleanup
- long polling cancellation
- FIFO deduplication and group ordering
- DLQ maxReceiveCount move
- SigV4 relaxed and strict modes

### Contract Tests

Contract tests should use golden fixtures generated from AWS SDK requests:

- AWS CLI v2 `create-queue`
- AWS CLI v2 `send-message`
- AWS CLI v2 `receive-message`
- AWS SDK v2 client smoke
- boto3 client smoke
- JavaScript v3 client smoke
- Query protocol curl fixtures
- AWS JSON protocol curl fixtures

### E2E Tests

`scripts/sqs-e2e.sh` should:

1. create temporary devcloud workspace
2. choose free SQS / dashboard / SMTP ports
3. write `.devcloud/config.yaml`
4. start `devcloud up`
5. create queue through AWS CLI or curl
6. send message
7. receive message
8. assert message becomes invisible before timeout
9. delete message with receipt handle
10. assert deleted message is not received again
11. check dashboard service registry
12. check `/dashboard/sqs`
13. check `/api/sqs/queues`
14. cleanup by default, preserve data when `E2E_DELETE_DATA=false`

Interactive mode:

```bash
E2E_INTERACTIVE=true E2E_DELETE_DATA=false scripts/sqs-e2e.sh
```

### Stage Gates

| Stage | Command | Required checks |
| --- | --- | --- |
| foundation | `VERIFY_STAGE=foundation bash scripts/sqs-autoloop/verify.sh` | config, compile, protocol unit tests |
| queue | `VERIFY_STAGE=queue bash scripts/sqs-autoloop/verify.sh` | queue CRUD and attributes |
| message | `VERIFY_STAGE=message bash scripts/sqs-autoloop/verify.sh` | send/receive/delete/visibility |
| dashboard | `VERIFY_STAGE=dashboard bash scripts/sqs-autoloop/verify.sh` | service registry and UI route |
| full | `VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh` | all tests + e2e |

## Compatibility Matrix

| Capability | v0.1 | v0.2 | v0.3 | v0.4+ |
| --- | --- | --- | --- | --- |
| AWS JSON protocol | Yes | Yes | Yes | Yes |
| Query protocol | Yes | Yes | Yes | Yes |
| SigV4 relaxed mode | Yes | Yes | Yes | Yes |
| SigV4 strict mode | Partial | Yes | Yes | Yes |
| Create/List/Get/Delete queue | Yes | Yes | Yes | Yes |
| Get/Set queue attributes | Yes | Yes | Yes | Yes |
| Send/Receive/Delete message | Yes | Yes | Yes | Yes |
| ChangeMessageVisibility | Yes | Yes | Yes | Yes |
| PurgeQueue | Yes | Yes | Yes | Yes |
| Long polling | Partial | Yes | Yes | Yes |
| Message attributes | Partial | Yes | Yes | Yes |
| Batch operations | No | Yes | Yes | Yes |
| FIFO queue | No | No | Yes | Yes |
| Content-based deduplication | No | No | Yes | Yes |
| DLQ redrive policy | No | No | Partial | Yes |
| Message move tasks | No | No | No | Yes |
| Queue tags | No | Yes | Yes | Yes |
| Queue policy permissions | No | Partial | Partial | Yes |
| SSE attributes | No | No | Partial | Partial |
| CloudWatch metrics | No | No | No | Partial local counters |
| S3 event notification integration | No | No | No | Yes |
| Pub/Sub adapter shared core | Design only | Design only | Partial | Yes |

## Initial Acceptance Criteria

- AC-001: Given `devcloud up` is running, when AWS CLI calls `create-queue` with endpoint override, then a SQS-compatible QueueUrl is returned.
- AC-002: Given a queue exists, when `send-message` is called, then MessageId and MD5OfMessageBody are returned.
- AC-003: Given a visible message exists, when `receive-message` is called, then MessageId, ReceiptHandle, Body, and requested attributes are returned.
- AC-004: Given a message has been received, when another receive occurs before visibility timeout expires, then the same message is not returned.
- AC-005: Given visibility timeout expires, when receive is called again, then the message can be returned with a new ReceiptHandle and incremented ApproximateReceiveCount.
- AC-006: Given DeleteMessage is called with the latest receipt handle, when receive is called after timeout, then the message is not returned.
- AC-007: Given Query protocol is used with `Action=SendMessage`, then XML success response matches SQS shape.
- AC-008: Given AWS JSON protocol is used with `X-Amz-Target: AmazonSQS.SendMessage`, then JSON success response matches SQS shape.
- AC-009: Given invalid QueueUrl is used, then SQS-compatible `QueueDoesNotExist` error is returned.
- AC-010: Given SQS dashboard is opened, when queues and messages exist, then `/dashboard/sqs` shows queue counts and message state.
- AC-011: Given `devcloud reset` is run, when SQS endpoint restarts, then queues and messages are cleared.
- AC-012: Given `VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh` is run, then unit, contract, dashboard, and e2e checks pass.

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| "complete SQS compatibility" is too broad | Scope explosion | compatibility levels and stage gates |
| Query protocol parser differs from AWS SDK expectations | SDK incompatibility | golden fixtures from AWS CLI / SDK requests |
| AWS JSON protocol support changes SDK defaults | missing modern client compatibility | implement JSON and Query codecs from the start |
| receipt handle invalidation differs from SQS | consumers behave incorrectly | dedicated lease model and timeout tests |
| visibility timeout tests become flaky | unstable CI | deterministic fake clock in unit tests; real clock only in e2e smoke |
| FIFO partial implementation misleads users | data ordering bugs | reject FIFO until L4 behavior is complete enough |
| message body leaks through logs | privacy issue | never log body or attributes; UI escaping and size limits |
| filesystem rewrite corrupts queue state | data loss in local dev | WAL append/replay and atomic rename |
| SQS/Pub/Sub common core over-abstracts too early | complexity | start SQS-first but keep Message Core names neutral |
| dashboard mutation causes accidental data loss | user surprise | MVP dashboard is read-mostly; purge is explicit |

## Open Questions

1. Should SQS use dedicated port `9324`, or join an AWS unified gateway port `4566` with S3 later?
2. Should Query protocol or AWS JSON protocol be the first SDK compatibility gate?
3. Should `services/s3/sigv4.rs` and `services/dynamodb/sigv4.rs` be extracted before SQS, or should SQS start with a local copy and refactor later?
4. Should FIFO queue creation be accepted before full FIFO delivery is implemented, or rejected until L4?
5. Should dashboard allow sending/deleting messages, or remain read-only observability in v0.1?
6. Should Message Core be introduced as `services/message` immediately, or start under `services/sqs` and extract before Pub/Sub?

## References

- Amazon SQS API Reference - Actions: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_Operations.html
- Amazon SQS API Reference - CreateQueue: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html
- Amazon SQS API Reference - SendMessage: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
- Amazon SQS API Reference - ReceiveMessage: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
- Amazon SQS API Reference - Common Parameters: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonParameters.html
- Amazon SQS API Reference - Common Errors: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonErrors.html
- Amazon SQS Developer Guide - queue types: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-types.html
- `docs/spec-v0.md`
- `docs/design-s3-compat.md`
- `docs/design-dynamodb-compat.md`
- `docs/design-dashboard-shell.md`

## Change History

| Date | Change |
| --- | --- |
| 2026-05-02 | Initial SQS compatibility design draft. |
