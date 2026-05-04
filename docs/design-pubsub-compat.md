# Google Cloud Pub/Sub Compatibility Design

## Summary

`devcloud` に Google Cloud Pub/Sub 互換の local messaging server を追加する。

目標は「Google Cloud Pub/Sub client libraries / gcloud / REST clients が emulator endpoint override だけで主要 workflow を実行できる」ことである。Pub/Sub の完全互換は gRPC API、REST v1 API、topic / subscription 管理、publish、pull、ack、ack deadline、StreamingPull、ordering key、dead letter policy、retry policy、push subscription、snapshot / seek、schema、filter、exactly-once delivery、IAM まで広いため、実装は段階化する。

内部設計は SQS 互換 server と別々の queue engine を作らず、`docs/spec-v0.md` と `docs/design-sqs-compat.md` の方針通り Message Core を共有する。Pub/Sub adapter は topic fan-out、subscription lease、ack ID、ordering key、dead letter policy など Pub/Sub 固有の意味論を adapter 層で扱い、永続化、lease scheduler、dashboard、e2e、自動化 script は既存の devcloud service pattern に合わせる。

## Document Control

| Field | Value |
| --- | --- |
| Audience | devcloud implementers, reviewers, future agent loops |
| Status | Draft |
| Owner | devcloud maintainers |
| Reviewer | TBD |
| Related docs | `docs/spec-v0.md`, `docs/design-sqs-compat.md`, `docs/design-dashboard-shell.md`, `docs/design-gcs-compat.md`, `docs/design-bigquery-compat.md` |
| Primary references | Google Cloud Pub/Sub REST reference, Pub/Sub emulator docs, pull subscriptions, ordering, dead-letter topics |
| Reference check date | 2026-05-02 |

## Compatibility Goal

### Definition

ここでの Pub/Sub 互換は、以下を満たす状態を指す。

1. Google Cloud Pub/Sub client libraries が `PUBSUB_EMULATOR_HOST` だけで接続できる。
2. gcloud Pub/Sub emulator workflow が local endpoint に対して topic / subscription / publish / pull / ack を実行できる。
3. gRPC `google.pubsub.v1.Publisher`、`google.pubsub.v1.Subscriber`、将来 `SchemaService` を段階的に実装できる。
4. REST v1 の `projects.topics`、`projects.subscriptions`、`projects.snapshots`、`projects.schemas` の主要 endpoint を段階的に実装できる。
5. pull subscription の at-least-once delivery、ack deadline、redelivery、delivery attempt、message retention を local deterministic state で再現できる。
6. ordering key、dead letter policy、retry policy、push subscription、StreamingPull、snapshot / seek、filter、schema、exactly-once delivery を段階的に追加できる。
7. 実 Google Cloud、Cloud Pub/Sub emulator、LocalStack などの外部 emulator に依存せず、local filesystem backed state として動作する。

### Compatibility Levels

| Level | Name | Purpose |
| --- | --- | --- |
| L0 | Emulator Smoke | `PUBSUB_EMULATOR_HOST=127.0.0.1:8085` で SDK が接続できる |
| L1 | Resource Core | Create/Get/List/Delete Topic と Subscription が通る |
| L2 | Publish and Pull | Publish、Pull、Acknowledge、ModifyAckDeadline が通る |
| L3 | Lease and Retry | ack deadline expiration、redelivery、delivery attempt、message retention が通る |
| L4 | Streaming and Ordering | StreamingPull、ordering key、per-key delivery blocking が通る |
| L5 | DLQ and Push | dead letter policy、retry policy、push subscription が通る |
| L6 | Advanced Parity | snapshot / seek、filter、schema、exactly-once delivery、IAM compatibility を追加 |

MVP は L0 + L1 + L2 + L3 の一部を対象にする。最終的な「完全互換」は L6 までを含む長期目標とする。

## Goals

- REQ-001: `devcloud up` で Pub/Sub gRPC endpoint を起動する。
- REQ-002: `PUBSUB_EMULATOR_HOST` と `PUBSUB_PROJECT_ID` で Google Cloud Pub/Sub client libraries が local server に接続できる。
- REQ-003: gRPC Publisher service の topic CRUD、list topic subscriptions、publish を実装する。
- REQ-004: gRPC Subscriber service の subscription CRUD、pull、acknowledge、modify ack deadline を実装する。
- REQ-005: REST v1 endpoint を dashboard / curl / e2e 用に提供し、公式 REST resource path と request / response shape に寄せる。
- REQ-006: topic への publish を全 subscription stream に fan-out し、subscription ごとの delivery cursor、ack state、lease state を保持する。
- REQ-007: message `data`、`attributes`、`messageId`、`publishTime`、`orderingKey` を Pub/Sub 互換 shape で保存・返却する。
- REQ-008: `ackId` は subscription delivery lease と結びつけ、ack deadline expiration 後は再利用不可にする。
- REQ-009: `ModifyAckDeadline` で個別 message lease を延長・短縮でき、`ackDeadlineSeconds=0` で即時 redelivery 可能にする。
- REQ-010: `NOT_FOUND`、`ALREADY_EXISTS`、`INVALID_ARGUMENT`、`FAILED_PRECONDITION` など gRPC status と REST error shape を SDK が解釈できる形で返す。
- REQ-011: dashboard/API から topics、subscriptions、backlog、in-flight messages、dead-letter relationship、recent deliveries を確認できる。
- REQ-012: `devcloud reset` で Pub/Sub data、leases、delivery cursors、push retry state、snapshot state を削除できる。
- REQ-013: stage based verification script で SDK smoke、REST contract、scheduler behavior、dashboard smoke、e2e を実行できる。
- REQ-014: SQS adapter と Message Core boundary を共有しつつ、Pub/Sub 固有の fan-out と ack semantics は adapter に閉じ込める。

## Non-Goals

- 実 Google Cloud IAM、OAuth、service account、quota project、billing とは連携しない。
- 実 Pub/Sub service、Google Pub/Sub emulator、LocalStack には依存しない。
- global replication、multi-region persistence、Google internal scheduler、throughput quota、billing は再現しない。
- Cloud Monitoring / Cloud Logging / Audit Logs は初期対象外とする。dashboard 用 local counters から始める。
- push subscription の外部 HTTPS endpoint への production-grade delivery guarantee は初期対象外とする。local retry model と test server 連携から始める。
- exactly-once delivery の Google Cloud と同等の distributed guarantee は初期対象外とする。ack ID versioning と duplicate suppression の local approximation から始める。
- schema registry と Avro / Protocol Buffer validation は初期対象外とする。request validation と metadata preservation から始める。
- SQS と Pub/Sub の外部意味論を統一しない。共有するのは internal Message Core と delivery stream abstraction であり、API behavior は adapter ごとに分ける。

## User Experience

```bash
devcloud init
devcloud up
```

Client libraries:

```bash
export PUBSUB_EMULATOR_HOST=127.0.0.1:8085
export PUBSUB_PROJECT_ID=devcloud
```

gcloud:

```bash
gcloud config set project devcloud

gcloud pubsub topics create demo-topic \
  --project devcloud

gcloud pubsub subscriptions create demo-sub \
  --topic demo-topic \
  --project devcloud

gcloud pubsub topics publish demo-topic \
  --message '{"type":"demo","id":"msg-1"}' \
  --attribute source=devcloud \
  --project devcloud

gcloud pubsub subscriptions pull demo-sub \
  --auto-ack \
  --limit 1 \
  --project devcloud
```

REST:

```bash
curl -sS -X PUT http://127.0.0.1:8086/v1/projects/devcloud/topics/demo-topic \
  -H "Content-Type: application/json" \
  -d '{}'

curl -sS -X PUT http://127.0.0.1:8086/v1/projects/devcloud/subscriptions/demo-sub \
  -H "Content-Type: application/json" \
  -d '{"topic":"projects/devcloud/topics/demo-topic","ackDeadlineSeconds":10}'

curl -sS -X POST http://127.0.0.1:8086/v1/projects/devcloud/topics/demo-topic:publish \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"data":"eyJ0eXBlIjoiZGVtbyJ9","attributes":{"source":"curl"}}]}'

curl -sS -X POST http://127.0.0.1:8086/v1/projects/devcloud/subscriptions/demo-sub:pull \
  -H "Content-Type: application/json" \
  -d '{"maxMessages":1}'
```

Dashboard:

```txt
http://127.0.0.1:8025/dashboard/pubsub
```

## Scope

### v0.1 Pub/Sub Foundation

- Config schema, daemon wiring, health endpoint, service registry.
- gRPC listener on `server.pubsubGrpcPort`, default `8085`.
- REST listener on `server.pubsubRestPort`, default `8086`.
- Filesystem store under `.devcloud/data/pubsub`.
- Empty service starts and reports ready in dashboard.
- `scripts/pubsub-autoloop/verify.sh` foundation stage.

### v0.2 Resource Core

- CreateTopic, GetTopic, ListTopics, DeleteTopic.
- CreateSubscription, GetSubscription, ListSubscriptions, DeleteSubscription.
- Topic name and subscription name validation.
- Topic deletion behavior for attached subscriptions is configurable but defaults to Google-compatible `FAILED_PRECONDITION` for destructive ambiguity in strict mode.
- REST + gRPC parity for resource CRUD.

### v0.3 Publish and Pull

- Publish with base64 `data`, attributes, ordering key preservation.
- Fan-out published messages to every attached subscription.
- Pull with `maxMessages`, generated `ackId`, `deliveryAttempt`, ack deadline lease.
- Acknowledge removes the message from the subscription backlog.
- ModifyAckDeadline updates lease expiration or releases immediately.
- At-least-once redelivery after ack deadline expiration.

### v0.4 Scheduler and Retention

- Deterministic clock boundary for tests.
- Subscription `ackDeadlineSeconds`, topic / subscription message retention, expiration policy metadata.
- Redelivery count and `deliveryAttempt` semantics.
- Backlog counters and in-flight counters.
- Long-poll style pull wait for REST test clients if needed.

### v0.5 StreamingPull and Ordering

- Bidirectional StreamingPull RPC.
- Flow control fields accepted and partially enforced.
- Ordering key per subscription stream.
- Redelivery of an unacked ordering key blocks subsequent messages with the same key.
- SDK smoke tests for Go, Node.js, and Python where dependency footprint is acceptable.

### v0.6 Dead Letter, Push, and Retry

- DeadLetterPolicy with `deadLetterTopic` and `maxDeliveryAttempts`.
- RetryPolicy with minimum / maximum backoff.
- PushConfig accepted and exposed in GetSubscription.
- Local push worker for HTTP endpoints with retry state.
- Delivery attempt transfer to dead-letter topic.

### v1.0 Advanced Compatibility

- Snapshot create / get / list / delete.
- Seek by timestamp or snapshot.
- Subscription filter expression subset.
- SchemaService metadata and validation subset.
- Exactly-once delivery local approximation.
- IAM method compatibility: getIamPolicy, setIamPolicy, testIamPermissions as local no-op / metadata operations.

## Architecture

```txt
Google Pub/Sub SDK / gcloud
        |
        | gRPC :8085
        v
+-------------------------------+
| internal/services/pubsub/grpc |
| Publisher / Subscriber APIs   |
+-------------------------------+
        |
        | adapter DTOs
        v
+-------------------------------+
| internal/services/pubsub      |
| Pub/Sub adapter semantics     |
+-------------------------------+
        |
        | neutral message calls
        v
+-------------------------------+
| internal/storage/message      |
| Message Core                  |
+-------------------------------+
        |
        v
.devcloud/data/message + .devcloud/data/pubsub

REST / dashboard / e2e
        |
        | HTTP :8086
        v
+-------------------------------+
| internal/services/pubsub/rest |
+-------------------------------+
```

### Boundaries

| Layer | Responsibility |
| --- | --- |
| gRPC adapter | protobuf service implementation, gRPC status, metadata, StreamingPull |
| REST adapter | REST v1 path dispatch, JSON request / response, REST error shape |
| Pub/Sub service | topic fan-out, subscription semantics, validation, compatibility defaults |
| Message Core | durable messages, delivery streams, leases, ack, redelivery schedule |
| Pub/Sub store | Pub/Sub resource metadata, push config, retry policy, schema / snapshot metadata |
| Dashboard API | local inspection, destructive dev actions, counters |

## Repository Layout

```txt
internal/services/pubsub/
  server.go              # service composition and lifecycle
  config.go              # Pub/Sub service config normalization
  grpc_server.go         # gRPC listener and registration
  rest_server.go         # REST v1 endpoint dispatch
  publisher.go           # topic operations and publish
  subscriber.go          # subscription operations and pull/ack
  streaming_pull.go      # StreamingPull implementation
  push_worker.go         # push subscription worker
  validation.go          # resource names, field ranges, request validation
  errors.go              # gRPC and REST error mapping
  types.go               # adapter-level domain types

internal/storage/message/
  core.go                # Message Core interfaces shared by SQS and Pub/Sub
  fs_store.go            # filesystem-backed message storage
  scheduler.go           # lease expiration and redelivery scheduler
  clock.go               # deterministic test clock

internal/storage/pubsub/
  store.go               # Pub/Sub resource metadata store
  fs_store.go
  snapshots.go
  schemas.go

internal/dashboard/
  pubsub_api.go
  pubsub_page.go

scripts/pubsub-e2e.sh
scripts/pubsub-autoloop/
  verify.sh
  tasks/
```

Generated protobuf code should be added only if needed. Prefer importing official `google.golang.org/genproto/googleapis/pubsub/v1` and implementing the generated interfaces instead of checking in generated source.

## Configuration

`.devcloud/config.yaml`:

```yaml
server:
  dashboardPort: 8025
  pubsubGrpcPort: 8085
  pubsubRestPort: 8086

auth:
  pubsub:
    mode: relaxed
    projectID: devcloud
    bearerToken: dev

services:
  pubsub:
    enabled: true
    dataDir: .devcloud/data/pubsub
    messageDataDir: .devcloud/data/message
    defaultAckDeadlineSeconds: 10
    messageRetentionSeconds: 604800
    maxAckDeadlineSeconds: 600
    maxPullMessages: 1000
    enableREST: true
    enableStreamingPull: true
    enablePush: false
```

### Defaults

| Field | Default | Notes |
| --- | --- | --- |
| `server.pubsubGrpcPort` | `8085` | Google Pub/Sub emulator default port |
| `server.pubsubRestPort` | `8086` | REST/dashboard/e2e convenience endpoint |
| `auth.pubsub.mode` | `relaxed` | Accept emulator-style unauthenticated requests |
| `auth.pubsub.projectID` | `devcloud` | Used when request omits project in local helpers |
| `auth.pubsub.bearerToken` | `dev` | Local bearer token for strict REST auth |
| `services.pubsub.enabled` | `true` | Disabled service is hidden from running status |
| `services.pubsub.defaultAckDeadlineSeconds` | `10` | Pub/Sub default-compatible practical local value |
| `services.pubsub.messageRetentionSeconds` | `604800` | Seven days |
| `services.pubsub.maxAckDeadlineSeconds` | `600` | Ten minutes |
| `services.pubsub.maxPullMessages` | `1000` | Guardrail for local memory and response size |

## API Mapping

### gRPC Publisher

| RPC | Level | Behavior |
| --- | --- | --- |
| `CreateTopic` | L1 | Create topic metadata; duplicate returns `ALREADY_EXISTS` |
| `UpdateTopic` | L6 | Update labels, message retention, schema settings |
| `GetTopic` | L1 | Return topic metadata |
| `ListTopics` | L1 | Page topics by project |
| `ListTopicSubscriptions` | L1 | Return attached subscription names |
| `ListTopicSnapshots` | L6 | Return snapshot names |
| `DeleteTopic` | L1 | Delete topic after compatibility validation |
| `DetachSubscription` | L6 | Detach subscription from topic |
| `Publish` | L2 | Persist message and fan out to subscriptions |

### gRPC Subscriber

| RPC | Level | Behavior |
| --- | --- | --- |
| `CreateSubscription` | L1 | Create subscription and delivery stream |
| `GetSubscription` | L1 | Return subscription metadata |
| `UpdateSubscription` | L4-L6 | Patch ack deadline, retention, retry, DLQ, filter, push config |
| `ListSubscriptions` | L1 | Page subscriptions by project |
| `DeleteSubscription` | L1 | Delete metadata and delivery stream |
| `ModifyAckDeadline` | L2 | Update lease expiration for ack IDs |
| `Acknowledge` | L2 | Ack messages by ack IDs |
| `Pull` | L2 | Lease and return messages |
| `StreamingPull` | L5 | Bidirectional streaming delivery and ack |
| `ModifyPushConfig` | L6 | Update push endpoint config |
| `GetSnapshot` / `ListSnapshots` / `CreateSnapshot` / `DeleteSnapshot` / `Seek` | L6 | Snapshot and replay support |

### REST v1

| Resource | Method | Level |
| --- | --- | --- |
| `PUT /v1/{name=projects/*/topics/*}` | Create topic | L1 |
| `GET /v1/{topic=projects/*/topics/*}` | Get topic | L1 |
| `GET /v1/{project=projects/*}/topics` | List topics | L1 |
| `DELETE /v1/{topic=projects/*/topics/*}` | Delete topic | L1 |
| `POST /v1/{topic=projects/*/topics/*}:publish` | Publish | L2 |
| `PUT /v1/{name=projects/*/subscriptions/*}` | Create subscription | L1 |
| `GET /v1/{subscription=projects/*/subscriptions/*}` | Get subscription | L1 |
| `GET /v1/{project=projects/*}/subscriptions` | List subscriptions | L1 |
| `DELETE /v1/{subscription=projects/*/subscriptions/*}` | Delete subscription | L1 |
| `POST /v1/{subscription=projects/*/subscriptions/*}:pull` | Pull | L2 |
| `POST /v1/{subscription=projects/*/subscriptions/*}:acknowledge` | Acknowledge | L2 |
| `POST /v1/{subscription=projects/*/subscriptions/*}:modifyAckDeadline` | Modify ack deadline | L2 |
| `PATCH /v1/{subscription.name=projects/*/subscriptions/*}` | Update subscription | L4-L6 |
| `POST /v1/{subscription=projects/*/subscriptions/*}:seek` | Seek | L6 |
| `GET /v1/{topic=projects/*/topics/*}/subscriptions` | List topic subscriptions | L1 |
| `GET /v1/{topic=projects/*/topics/*}/snapshots` | List topic snapshots | L6 |
| `projects.schemas.*` | Schema service REST | L6 |

## Resource Model

### Project

```go
type Project struct {
	ID        string
	CreatedAt time.Time
	UpdatedAt time.Time
}
```

Project is mostly a namespace. Local relaxed mode auto-creates a project namespace when a request references it.

### Topic

```go
type Topic struct {
	Name                    string
	ProjectID               string
	TopicID                 string
	Labels                  map[string]string
	MessageRetentionDuration time.Duration
	SchemaSettings          *SchemaSettings
	KMSKeyName              string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}
```

### Subscription

```go
type Subscription struct {
	Name                         string
	ProjectID                    string
	SubscriptionID               string
	Topic                        string
	AckDeadline                  time.Duration
	RetainAckedMessages          bool
	MessageRetentionDuration     time.Duration
	ExpirationPolicy             *ExpirationPolicy
	RetryPolicy                  *RetryPolicy
	DeadLetterPolicy             *DeadLetterPolicy
	PushConfig                   *PushConfig
	EnableMessageOrdering        bool
	Filter                       string
	ExactlyOnceDeliveryEnabled   bool
	DeliveryStreamID             string
	CreatedAt                    time.Time
	UpdatedAt                    time.Time
}
```

### Published Message

```go
type PublishedMessage struct {
	MessageID   string
	Data        []byte
	Attributes  map[string]string
	OrderingKey string
	PublishTime time.Time
}
```

REST uses base64-encoded `data`; gRPC uses bytes. The storage boundary should store raw bytes and let adapters encode or decode.

### Delivery

```go
type Delivery struct {
	AckID           string
	MessageID       string
	Subscription    string
	DeliveryAttempt int
	LeaseExpiresAt  time.Time
	OrderingKey      string
	State            DeliveryState
}
```

`AckID` is a lease token, not a stable message ID. Each redelivery gets a new `AckID`.

## Message Core Mapping

| Pub/Sub Concept | Message Core Concept | Notes |
| --- | --- | --- |
| Topic | Topic | Fan-out source |
| Subscription | DeliveryStream | Independent backlog and ack state |
| Published message | Message | Raw bytes + attributes + publish time |
| Ack ID | Lease token | Versioned per delivery |
| Ack deadline | Lease expiration | Same scheduler family as SQS visibility timeout |
| Delivery attempt | Delivery count | Subscription scoped |
| DeadLetterPolicy | DeadLetterRoute | Routes failed delivery to another topic |
| Ordering key | Ordering group | Per-stream ordering gate |

SQS queue は implicit delivery stream を 1 つ持つ。Pub/Sub topic は複数 subscription delivery stream に fan-out する。この差は adapter 層で扱い、Message Core は stream ごとの message availability、lease、ack、redelivery に集中する。

## Storage Layout

```txt
.devcloud/data/pubsub/
  projects/
    devcloud/
      topics/
        demo-topic.json
      subscriptions/
        demo-sub.json
      snapshots/
      schemas/
  push/
    attempts.jsonl
  metrics/
    counters.json

.devcloud/data/message/
  topics/
    pubsub-devcloud-demo-topic/
      messages/
        2026/05/02/<message-id>.json
  streams/
    pubsub-devcloud-demo-sub/
      state.json
      pending/
      leased/
      acked/
      deadlettered/
  leases/
    <ack-id>.json
```

Metadata files use atomic write (`write temp + rename`) and append-only event logs where history is useful. Message payloads must not be logged.

## Request Handling

### gRPC

1. Accept connection on `server.pubsubGrpcPort`.
2. Decode protobuf request using official Pub/Sub v1 generated types.
3. Extract project, topic, subscription names with strict resource-name parser.
4. Validate field ranges and unsupported options.
5. Call Pub/Sub service methods.
6. Map domain errors to gRPC status codes.
7. Return official response messages.

### REST

1. Accept request on `server.pubsubRestPort`.
2. Match official REST v1 paths.
3. Decode JSON body and URL path variables.
4. Convert REST shape to service input.
5. Return official JSON shape.
6. Map errors to Google JSON error envelope.

### StreamingPull

StreamingPull is stateful and must not block daemon shutdown.

- Each stream owns a subscriber session ID.
- Initial request establishes subscription and client flow-control settings.
- Server pulls available messages from Message Core and sends `StreamingPullResponse`.
- Client ack / modify deadline frames update leases.
- Keepalive and context cancellation release stream resources.
- Flow-control support starts with conservative per-stream outstanding message limits.

## Error Model

| Condition | gRPC | REST HTTP | Notes |
| --- | --- | --- | --- |
| invalid resource name | `INVALID_ARGUMENT` | `400` | Include field name when possible |
| duplicate topic/subscription | `ALREADY_EXISTS` | `409` | Match SDK retry expectations |
| missing topic/subscription | `NOT_FOUND` | `404` | Stable message text |
| subscription references missing topic | `NOT_FOUND` | `404` | CreateSubscription |
| invalid ack ID | `INVALID_ARGUMENT` | `400` | Do not leak lease internals |
| expired ack ID on ack | `INVALID_ARGUMENT` or no-op compatibility mode | `400` or `200` | Strictness controlled by config |
| unsupported advanced field | `UNIMPLEMENTED` or metadata-preserving accept | `501` or `200` | Stage dependent |
| storage failure | `INTERNAL` | `500` | Redact local paths when not useful |

REST error envelope:

```json
{
  "error": {
    "code": 404,
    "message": "Resource not found",
    "status": "NOT_FOUND"
  }
}
```

## Auth

Pub/Sub emulator workflow normally uses local unauthenticated traffic. devcloud should support:

| Mode | Behavior |
| --- | --- |
| `relaxed` | Accept unauthenticated local requests. Default for emulator compatibility. |
| `strict` | Require configured bearer token for REST; gRPC metadata validation for SDK smoke. |

No mode should contact Google Cloud IAM. Do not log authorization metadata.

## Semantics

### Publish

- `Publish` accepts one or more messages.
- Empty message list is invalid.
- Message `data` may be empty bytes if attributes are present.
- REST `data` must be base64 encoded.
- `messageId` is generated per published message and is stable across subscription deliveries.
- `publishTime` is server-side local clock time.
- Publish fan-out creates a pending delivery entry for every active subscription attached to the topic.

### Pull

- Pull leases up to `maxMessages` available messages.
- Leased messages are invisible to other pulls for the same subscription until ack deadline expiration.
- Pull response includes `ackId`, `message`, and `deliveryAttempt` when known.
- `returnImmediately` is deprecated upstream; accept it for client compatibility but prefer immediate response unless long-poll support is explicitly enabled.

### Acknowledge

- Acknowledge removes messages from the subscription backlog unless `retainAckedMessages` is enabled.
- Unknown ack IDs should be handled with compatibility mode. Default strict behavior returns invalid argument; relaxed mode may ignore them to match client retries.
- Ack must not delete the underlying published message while other subscriptions still reference it.

### ModifyAckDeadline

- Deadline range is `0..maxAckDeadlineSeconds`.
- `0` releases the lease immediately.
- Expired ack IDs are rejected or ignored according to compatibility mode.

### Ordering

- Ordering is per subscription and per `orderingKey`.
- If `enableMessageOrdering` is true, a later message for the same ordering key is not delivered while an earlier message is unacked or awaiting redelivery.
- Dead-letter transfer can break ordering; dashboard should surface this caveat.

### Dead Letter

- Dead letter policy is configured on subscription.
- `maxDeliveryAttempts` accepts the Google-compatible range `5..100`.
- When attempts exceed policy, the adapter publishes a wrapped message to the configured dead-letter topic.
- Delivery attempt count is approximate in full cloud Pub/Sub; local mode should be deterministic for tests but document the difference.

### Push

- PushConfig is accepted in early phases as metadata.
- Push worker is enabled only when `services.pubsub.enablePush=true`.
- Push attempts must redact headers and payloads in logs.
- Retry policy controls local scheduling.

### Snapshots and Seek

- Snapshot captures subscription cursor, acked retained messages, and pending lease state at a logical time.
- Seek by timestamp replays messages published after that timestamp, subject to retention.
- Seek by snapshot restores snapshot cursor state.

## Dashboard Integration

### Routes

| Route | Purpose |
| --- | --- |
| `/dashboard/pubsub` | Pub/Sub console |
| `/api/pubsub/health` | Service health |
| `/api/pubsub/projects` | Local project namespaces |
| `/api/pubsub/topics` | Topic list and create |
| `/api/pubsub/topics/{topic}` | Topic detail and delete |
| `/api/pubsub/subscriptions` | Subscription list and create |
| `/api/pubsub/subscriptions/{subscription}` | Subscription detail and delete |
| `/api/pubsub/subscriptions/{subscription}/pull` | Development pull helper |
| `/api/pubsub/subscriptions/{subscription}/ack` | Development ack helper |
| `/api/pubsub/messages/{message}` | Message metadata viewer |

### UI Requirements

- Show service status, gRPC endpoint, REST endpoint, and configured project ID.
- Topic table: name, subscriptions, publish count, backlog total, created time.
- Subscription table: topic, ack deadline, backlog, in-flight, delivery attempts, DLQ policy.
- Detail view: recent messages, in-flight leases, dead-letter target, push config.
- Publish form for local testing with data, attributes, ordering key.
- Pull helper that can pull without auto-ack, copy ack IDs, and acknowledge selected messages.
- Destructive actions require confirmation and affect only local dev data.

## Implementation Plan

### Phase 0: Design and Automation Skeleton

- IMPL-001: Add this design document.
- IMPL-002: Add `scripts/pubsub-autoloop/verify.sh` with `foundation`, `config`, `resource`, `message`, `dashboard`, `e2e`, `full` stages.
- IMPL-003: Add task prompts for codex engine loops.
- IMPL-004: Add `scripts/pubsub-e2e.sh` smoke test placeholder.

### Phase 1: Config and Daemon

- IMPL-005: Add Pub/Sub config fields in `internal/app/config.go`.
- IMPL-006: Start gRPC and REST listeners from `internal/app/daemon.go`.
- IMPL-007: Register service in dashboard service registry.
- IMPL-008: Add reset handling for `.devcloud/data/pubsub` and message data owned by Pub/Sub.

### Phase 2: Resource CRUD

- IMPL-009: Implement Pub/Sub resource store.
- IMPL-010: Implement topic CRUD.
- IMPL-011: Implement subscription CRUD.
- IMPL-012: Implement REST v1 topic/subscription endpoints.
- IMPL-013: Implement gRPC Publisher/Subscriber resource RPCs.

### Phase 3: Message Core Boundary

- IMPL-014: Extract or finalize `internal/storage/message` interfaces from SQS work.
- IMPL-015: Add Pub/Sub topic fan-out operation.
- IMPL-016: Add subscription stream state and ack ID lease operation.
- IMPL-017: Add deterministic scheduler tests for ack deadline expiration.

### Phase 4: Publish, Pull, Ack

- IMPL-018: Implement Publish with message ID generation and attributes.
- IMPL-019: Implement Pull with lease creation.
- IMPL-020: Implement Acknowledge and ModifyAckDeadline.
- IMPL-021: Add Google Cloud Go client smoke test.
- IMPL-022: Add REST curl smoke test.

### Phase 5: Dashboard and E2E

- IMPL-023: Add Pub/Sub dashboard route and API.
- IMPL-024: Add topic/subscription tables and detail views.
- IMPL-025: Add publish/pull/ack helper UI.
- IMPL-026: Add `scripts/pubsub-e2e.sh`.
- IMPL-027: Add Playwright or dashboard API smoke when dashboard frontend test infrastructure is available.

### Phase 6: Advanced Compatibility

- IMPL-028: Implement StreamingPull.
- IMPL-029: Implement ordering key gates.
- IMPL-030: Implement retry policy and dead letter policy.
- IMPL-031: Implement push worker.
- IMPL-032: Implement snapshots and seek.
- IMPL-033: Implement schema metadata and validation subset.
- IMPL-034: Implement exactly-once local approximation.

## Verification Plan

### Commands

```bash
go test ./...
VERIFY_STAGE=foundation bash scripts/pubsub-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/pubsub-autoloop/verify.sh
scripts/pubsub-e2e.sh
```

### Stage Gates

| Stage | Checks |
| --- | --- |
| `foundation` | config fields, daemon startup, service health, no generated state committed |
| `config` | default ports, disabled service, custom dataDir, reset behavior |
| `resource` | topic/subscription CRUD over REST and gRPC |
| `message` | publish, pull, ack, modify ack deadline, redelivery |
| `scheduler` | lease expiration, delivery attempt, retention |
| `dashboard` | dashboard service registry and Pub/Sub API smoke |
| `e2e` | SDK or gcloud-compatible full workflow |
| `full` | all stages plus `go test ./...` |

### Acceptance Criteria

- AC-001: `devcloud up` starts Pub/Sub gRPC server on `127.0.0.1:8085` by default.
- AC-002: `PUBSUB_EMULATOR_HOST=127.0.0.1:8085` client can create topic and subscription.
- AC-003: Client can publish a message and receive it from a pull subscription.
- AC-004: Acknowledged messages are not delivered again.
- AC-005: Unacknowledged messages are delivered again after ack deadline expiration.
- AC-006: `ModifyAckDeadline` can extend lease and can release immediately with `0`.
- AC-007: REST v1 endpoints support equivalent MVP workflow.
- AC-008: Dashboard shows topics, subscriptions, backlog, in-flight messages, and endpoints.
- AC-009: `devcloud reset` removes Pub/Sub local state.
- AC-010: `go test ./...` and `VERIFY_STAGE=full bash scripts/pubsub-autoloop/verify.sh` pass.

## Compatibility Matrix

| Capability | Google Pub/Sub | Google Emulator | devcloud MVP | devcloud Target |
| --- | --- | --- | --- | --- |
| gRPC endpoint | Yes | Yes | Yes | Yes |
| REST v1 endpoint | Yes | Limited by emulator tooling | Yes | Yes |
| Topic CRUD | Yes | Yes | Yes | Yes |
| Subscription CRUD | Yes | Yes | Yes | Yes |
| Publish | Yes | Yes | Yes | Yes |
| Pull | Yes | Yes | Yes | Yes |
| Acknowledge | Yes | Yes | Yes | Yes |
| ModifyAckDeadline | Yes | Yes | Yes | Yes |
| StreamingPull | Yes | Yes | No | Yes |
| Ordering key | Yes | Partial | Metadata only | Yes |
| Dead letter policy | Yes | Partial | No | Yes |
| Push subscription | Yes | Partial | Metadata only | Yes |
| Snapshot / Seek | Yes | Partial | No | Yes |
| Schema | Yes | Partial | No | Yes |
| Exactly-once delivery | Yes | Cloud-specific | No | Local approximation |
| IAM | Yes | No cloud IAM | No-op metadata | Local compatibility |

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| gRPC/protobuf implementation diverges from official clients | SDKs fail despite REST passing | Use official genproto interfaces and SDK smoke tests early |
| Message Core over-abstracts SQS/PubSub differences | Hard-to-maintain core | Keep fan-out, ordering, and API-specific validation in Pub/Sub adapter |
| StreamingPull complexity delays MVP | Core workflow blocked | Ship unary Pull first; stage StreamingPull separately |
| Ack ID semantics are too permissive | Hidden compatibility bugs | Add strict/relaxed mode and contract tests |
| Dead-letter ordering interactions are subtle | Incorrect delivery behavior | Document deterministic local model and isolate DLQ phase |
| Push subscription can leak payloads in logs | Security/privacy issue | Redact payloads and sensitive headers by default |
| REST and gRPC drift | Inconsistent behavior | Route both adapters through the same service methods |

## Open Questions

1. Should Pub/Sub REST run on a separate port (`8086`) or share the gRPC port through h2c / grpc-gateway?
2. Should default topic deletion reject attached subscriptions or delete topic metadata while subscriptions remain detached?
3. Should invalid or expired ack IDs be strict errors by default, or ignored in relaxed emulator mode?
4. Which SDK languages are mandatory for e2e before claiming L2 compatibility?
5. Should Message Core live in `internal/storage/message` immediately, or remain under SQS until Pub/Sub Phase 3 forces extraction?
6. Should push subscription be implemented before StreamingPull if dashboard demo value is higher?

## References

- Google Cloud Pub/Sub REST API: https://cloud.google.com/pubsub/docs/reference/rest
- Pub/Sub emulator: https://cloud.google.com/pubsub/docs/emulator
- Pull subscriptions: https://cloud.google.com/pubsub/docs/pull
- Message ordering: https://cloud.google.com/pubsub/docs/ordering
- Dead-letter topics: https://cloud.google.com/pubsub/docs/dead-letter-topics
- devcloud core spec: `docs/spec-v0.md`
- SQS compatibility design: `docs/design-sqs-compat.md`

## Change History

| Date | Change | Author |
| --- | --- | --- |
| 2026-05-02 | Initial Google Cloud Pub/Sub compatibility design | Nexus |
