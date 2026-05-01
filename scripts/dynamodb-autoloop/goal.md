# DynamoDB Server Autoloop Goal

## Goal

Implement an Amazon DynamoDB compatible local server for `devcloud`, following `docs/design-dynamodb-compat.md`.

## Why

`devcloud` already has local Mail, S3, GCS, and dashboard foundations. DynamoDB compatibility should add a local AWS SDK / CLI target for table, item, expression, index, stream, transaction, and PartiQL workflows without reaching AWS or depending on DynamoDB Local.

## Acceptance Criteria

1. `devcloud up` starts a DynamoDB JSON API endpoint on the configured local port, defaulting to `127.0.0.1:8000`.
2. DynamoDB low-level JSON protocol works with `POST /`, `Content-Type: application/x-amz-json-1.0`, and `X-Amz-Target: DynamoDB_20120810.{Operation}`.
3. Table operations work: `CreateTable`, `DescribeTable`, `ListTables`, `UpdateTable` for supported metadata, and `DeleteTable`.
4. Item operations work: `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem`, `BatchGetItem`, and `BatchWriteItem`.
5. `AttributeValue` supports `S`, `N`, `B`, `BOOL`, `NULL`, `M`, `L`, `SS`, `NS`, and `BS` with DynamoDB-compatible JSON response shapes.
6. Expressions work for the documented MVP: `ConditionExpression`, `UpdateExpression`, `ProjectionExpression`, `FilterExpression`, and `KeyConditionExpression`.
7. `Query` and `Scan` support `Limit`, `ExclusiveStartKey`, `LastEvaluatedKey`, sort ordering, filters, and projection for tested fixtures.
8. GSI and LSI metadata, projection updates, and index `Query` / `Scan` work for tested fixtures.
9. Strict SigV4 mode validates signed AWS SDK / CLI requests without logging credentials, signatures, canonical requests, or payloads.
10. Existing Mail, S3, GCS, and dashboard behavior remains compatible; `go test ./...` passes.
11. Dashboard service registry exposes DynamoDB, and `/api/dynamodb/*` can inspect tables, indexes, items, streams, and TTL state.
12. `VERIFY_STAGE=full bash scripts/dynamodb-autoloop/verify.sh` passes.

## Out of Scope for This Loop

- Real AWS IAM, STS, CloudWatch, CloudTrail, KMS, or external AWS service calls.
- DynamoDB Local, LocalStack, or other external emulator dependency.
- DAX protocol.
- Multi-region global table replication.
- Real provisioned billing, adaptive capacity, and AWS-managed auto scaling.
- Full Contributor Insights, Kinesis streaming destination, and S3 import/export behavior.

## Implementation Guidance

- Preserve existing Mail, S3, GCS, and dashboard behavior before broad storage refactors.
- Prefer small vertical slices with tests.
- Keep runtime data under `.devcloud/`.
- Use Go standard library unless a dependency is clearly justified.
- Keep DynamoDB protocol, service logic, expression parsing, and storage boundaries separate.
- Do not log credentials, Authorization headers, signatures, canonical requests, item payloads, or other sensitive request bodies.
- Treat `docs/design-dynamodb-compat.md` as the implementation contract.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: DynamoDB compatibility loop contract is ready.
