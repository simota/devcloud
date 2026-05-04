# Google Cloud Pub/Sub Server Autoloop Goal

## Goal

Implement a Google Cloud Pub/Sub compatible local server for `devcloud`, following `docs/design-pubsub-compat.md`.

## Why

`devcloud` already has local Mail, S3, GCS, DynamoDB, BigQuery, SQS design, and dashboard foundations. Pub/Sub compatibility should add a local Google Cloud SDK / gcloud / REST target for topic, subscription, publish, pull, ack, ack deadline, StreamingPull, ordering, dead-letter, push, snapshot, schema, and dashboard workflows without reaching Google Cloud or depending on the official emulator.

## Acceptance Criteria

1. `devcloud up` starts a Pub/Sub gRPC endpoint on the configured local port, defaulting to `127.0.0.1:8085`.
2. `devcloud up` starts a Pub/Sub REST v1 endpoint on the configured local port, defaulting to `127.0.0.1:8086`.
3. `PUBSUB_EMULATOR_HOST=127.0.0.1:8085` clients can create topics and subscriptions through the gRPC Publisher and Subscriber services.
4. REST v1 endpoints work for `projects.topics` and `projects.subscriptions` create, get, list, and delete workflows.
5. `Publish` stores data, attributes, ordering key, server publish time, and stable message IDs.
6. Publishing to a topic fans out messages to every attached subscription without cross-subscription ack interference.
7. `Pull` returns leased messages with `ackId`, message payload, attributes, publish time, and delivery attempt.
8. `Acknowledge` removes messages from the subscription backlog and does not remove messages for other subscriptions.
9. `ModifyAckDeadline` extends leases and releases messages immediately when the deadline is `0`.
10. Ack deadline expiration redelivers unacked messages deterministically in tested fixtures.
11. StreamingPull, ordering key metadata, dead letter policy metadata, push config metadata, and retry policy metadata are represented without blocking the MVP.
12. Existing Mail, S3, GCS, DynamoDB, BigQuery, SQS, and dashboard behavior remains compatible; `go test ./...` passes.
13. Dashboard service registry exposes Pub/Sub, and `/api/pubsub/*` can inspect topics, subscriptions, backlog, in-flight leases, and recent deliveries.
14. `VERIFY_STAGE=full bash scripts/pubsub-autoloop/verify.sh` passes.

## Out of Scope for This Loop

- Real Google Cloud IAM, OAuth, service accounts, Cloud Monitoring, Cloud Logging, Audit Logs, quota, billing, or external Google Cloud calls.
- Dependency on the official Google Pub/Sub emulator, LocalStack, or other external emulator.
- Multi-region durability, Google internal replication, actual production throughput guarantees, and billing behavior.
- Full exactly-once delivery guarantee; local approximation can come after core ack ID versioning is stable.
- Full schema validation for Avro and Protocol Buffer; metadata and validation stubs can precede full validation.
- Production-grade push delivery; push metadata and local test worker can be staged after pull compatibility.

## Implementation Guidance

- Preserve existing Mail, S3, GCS, DynamoDB, BigQuery, SQS, and dashboard behavior before broad storage refactors.
- Prefer small vertical slices with tests.
- Keep runtime data under `.devcloud/`.
- Use Go standard library unless a dependency is clearly justified.
- Prefer official `google.golang.org/genproto/googleapis/pubsub/v1` service interfaces for gRPC compatibility.
- Keep gRPC adapter, REST adapter, Pub/Sub service logic, Message Core, scheduler, resource metadata store, and dashboard boundaries separate.
- Reuse the SQS-compatible Message Core direction from `docs/spec-v0.md` and `docs/design-sqs-compat.md`; do not duplicate a second queue engine.
- Use filesystem-backed persistence with atomic writes and WAL/replay where message state changes need crash recovery.
- Do not log authorization metadata, request payloads, message data, attributes, ack IDs, push headers, or sensitive local paths.
- Treat `docs/design-pubsub-compat.md` as the implementation contract.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Pub/Sub compatibility loop contract is ready.
