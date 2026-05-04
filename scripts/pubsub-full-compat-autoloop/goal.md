# Pub/Sub Full Compatibility Remaining Tasks

## Goal

Complete the remaining Google Cloud Pub/Sub compatibility work for `devcloud` after the Pub/Sub MVP.

## Why

The current Pub/Sub implementation passes the local MVP gate, including REST topic/subscription/message workflows and unary gRPC publish/pull/ack smoke. It is not yet a full Pub/Sub-compatible emulator because advanced gRPC surfaces and client-library behavior remain incomplete.

## Acceptance Criteria

1. Existing MVP remains green: `VERIFY_STAGE=full bash scripts/pubsub-autoloop/verify.sh` passes.
2. gRPC `StreamingPull` supports bidirectional receive, ack, modify-ack-deadline, stream cancellation, and bounded flow-control behavior.
3. gRPC snapshot APIs work: `CreateSnapshot`, `GetSnapshot`, `ListSnapshots`, `DeleteSnapshot`, and `Seek` by snapshot.
4. gRPC seek by timestamp works for retained messages.
5. gRPC SchemaService-compatible APIs cover create/get/list/delete/validate for metadata and local validation subset.
6. Push subscriptions can deliver to a local HTTP endpoint, retry failures, and avoid logging push payloads or sensitive headers.
7. Ordering-key behavior is covered across unary Pull and StreamingPull with per-key blocking.
8. Dead-letter and retry policies are exercised through gRPC and REST with deterministic delivery-attempt behavior.
9. Google Pub/Sub Go client smoke covers create topic, create subscription, publish, receive, ack, and cleanup using `PUBSUB_EMULATOR_HOST`.
10. Existing Mail, S3, GCS, DynamoDB, BigQuery, SQS, dashboard, and Pub/Sub MVP behavior remains compatible; `go test ./...` passes.
11. `VERIFY_STAGE=full-compat bash scripts/pubsub-full-compat-autoloop/verify.sh` passes.

## Out of Scope

- Real Google Cloud IAM, OAuth, billing, quota, global replication, and Cloud Monitoring integration.
- Production-grade exactly-once delivery guarantees beyond local deterministic approximation.
- Full Avro and Protocol Buffer schema validation beyond a documented local subset.
- Replacing the existing MVP autoloop; this loop only tracks remaining full-compatibility work.

## Implementation Guidance

- Preserve current MVP behavior before adding advanced compatibility.
- Keep REST and gRPC behavior routed through shared service methods where practical.
- Prefer focused tests for each protocol surface before broad refactors.
- Use official `cloud.google.com/go/pubsub/apiv1/pubsubpb` and gRPC server interfaces.
- Keep message payloads, attributes, ack IDs, Authorization metadata, and push request bodies out of logs.
- Treat `docs/design-pubsub-compat.md` as the compatibility contract and update it only when the contract changes.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Pub/Sub remaining full-compatibility loop contract is ready.
