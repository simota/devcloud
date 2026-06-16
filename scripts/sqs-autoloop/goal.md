# SQS Server Autoloop Goal

## Goal

Implement an Amazon SQS compatible local server for `devcloud`, following `docs/design-sqs-compat.md`.

## Why

`devcloud` already has local Mail, S3, GCS, DynamoDB, BigQuery, and dashboard foundations. SQS compatibility should add a local AWS SDK / CLI target for queue, message, visibility timeout, long polling, FIFO, DLQ, and dashboard workflows without reaching AWS or depending on LocalStack or ElasticMQ.

## Acceptance Criteria

1. `devcloud up` starts an SQS endpoint on the configured local port, defaulting to `127.0.0.1:9324`.
2. AWS JSON protocol works with `POST /`, `Content-Type: application/x-amz-json-1.0`, and `X-Amz-Target: AmazonSQS.{Operation}`.
3. Query protocol works with `Action`, `Version=2012-11-05`, form parameters, and SQS-compatible XML success/error responses.
4. Queue operations work: `CreateQueue`, `DeleteQueue`, `GetQueueUrl`, `ListQueues`, `GetQueueAttributes`, `SetQueueAttributes`, and `PurgeQueue`.
5. Message operations work: `SendMessage`, `ReceiveMessage`, `DeleteMessage`, and `ChangeMessageVisibility`.
6. Message delivery supports visibility timeout, delay seconds, retention cleanup, receipt handle invalidation, and long polling for tested fixtures.
7. Batch operations and attributes work for the documented MVP: `SendMessageBatch`, `DeleteMessageBatch`, `ChangeMessageVisibilityBatch`, `MessageAttributes`, `MessageSystemAttributes`, and SQS MD5 fields.
8. FIFO queues support `.fifo` validation, `MessageGroupId`, `MessageDeduplicationId`, content-based deduplication, sequence numbers, and per-group ordering for tested fixtures.
9. DLQ/redrive support covers `RedrivePolicy`, `RedriveAllowPolicy`, `ListDeadLetterSourceQueues`, and local message move behavior for tested fixtures.
10. Strict SigV4 mode validates signed AWS SDK / CLI requests without logging credentials, signatures, canonical requests, message bodies, attributes, or receipt handles.
11. Existing Mail, S3, GCS, DynamoDB, BigQuery, and dashboard behavior remains compatible; `cargo test --workspace` passes.
12. Dashboard service registry exposes SQS, and `/api/sqs/*` can inspect queues, attributes, visible/in-flight/delayed messages, leases, and DLQ relationships.
13. `VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh` passes.

## Out of Scope for This Loop

- Real AWS IAM, STS, CloudWatch, CloudTrail, KMS, SNS, Lambda, EventBridge, or external AWS service calls.
- LocalStack, ElasticMQ, or other external emulator dependency.
- Multi-AZ durability, AWS internal replication, actual AWS quota enforcement, and billing behavior.
- Real SSE-KMS / SSE-SQS encryption; preserve compatibility attributes before cryptographic behavior.
- Full queue policy authorization; compatibility response and validation can precede enforcement.
- Pub/Sub API implementation; keep Message Core boundaries ready for future Pub/Sub work.

## Implementation Guidance

- Preserve existing Mail, S3, GCS, DynamoDB, BigQuery, and dashboard behavior before broad storage refactors.
- Prefer small vertical slices with tests.
- Keep runtime data under `.devcloud/`.
- Use Rust standard library unless a dependency is clearly justified.
- Keep SQS protocol codecs, service logic, scheduler, receipt lease storage, FIFO/dedup logic, and dashboard boundaries separate.
- Use filesystem-backed persistence with atomic writes and WAL/replay where message state changes need crash recovery.
- Do not log credentials, Authorization headers, signatures, canonical requests, message bodies, message attributes, receipt handles, or queue policy payloads.
- Treat `docs/design-sqs-compat.md` as the implementation contract.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: SQS compatibility loop contract is ready.
