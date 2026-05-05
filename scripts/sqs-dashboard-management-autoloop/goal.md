# SQS Dashboard Management UI Goal

## Goal

Upgrade the SQS React dashboard from queue/message inspection plus purge into a safer local management console for common SQS developer workflows.

## Acceptance Criteria

1. Existing SQS compatibility gate remains green:
   - `VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh`
2. The existing `/dashboard/sqs` React route remains available and keeps queue, message, lease, DLQ, and attribute inspection.
3. The dashboard exposes guarded local management flows for `CreateQueue`, `SendMessage`, `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`, and `PurgeQueue` through `/api/sqs/*`, not by mutating storage directly.
4. CreateQueue supports standard and FIFO queue setup, key attributes, and raw JSON/attributes mode where useful.
5. SendMessage supports body, delay, message attributes, FIFO group/deduplication fields, and JSON validation for attributes.
6. ReceiveMessage supports max messages, visibility timeout, wait time, attribute selection, and displays receipt handles safely without persisting sensitive payloads.
7. DeleteMessage and ChangeMessageVisibility require selected receipt handle or explicit pasted handle and clear confirmation.
8. DLQ/redrive or DLQ inspection gets a clearer operational path, including source queue and dead-letter metadata.
9. Disabled SQS state renders read-only/disabled controls and clear status.
10. E2E or dashboard tests verify create queue, send/receive/delete, visibility change, purge confirmation, validation errors, and disabled-state behavior.
11. README or design docs describe SQS dashboard management capabilities and safety boundaries.
12. `VERIFY_STAGE=full-management-ui bash scripts/sqs-dashboard-management-autoloop/verify.sh` passes.

## Stages

- `foundation`: loop contract and existing `sqs-autoloop` full gate.
- `queue-send`: CreateQueue and SendMessage UI/API helpers.
- `receive-delete`: ReceiveMessage, DeleteMessage, and receipt-handle workflow.
- `visibility-purge-dlq`: ChangeMessageVisibility, safer purge confirmation, and DLQ operational detail.
- `e2e-docs`: tests/E2E and docs updates.
- `full-management-ui`: all stages plus repository tests.

## Out of Scope

- Real AWS calls, IAM policy enforcement, billing, quotas, CloudWatch, or production durability.
- Implementing new SQS protocol behavior beyond dashboard needs.
- Persisting message bodies, receipt handles, credentials, Authorization headers, SigV4 material, or full request bodies in browser history.

## Implementation Guidance

- Keep the UI compact, operational, and consistent with the existing dashboard shell.
- Mutations must call existing local dashboard/API endpoints or route through the local SQS protocol path.
- Prefer guided controls for common flows and raw JSON/attribute inputs for advanced request shapes.
- Destructive flows must require explicit confirmation and must be disabled when SQS is unavailable.
- Never log credentials, Authorization headers, SigV4 material, message bodies, receipt handles, or full request payloads.
- Update generated dashboard assets with `npm run build` after frontend changes.
- Preserve existing Mail, S3, GCS, DynamoDB, BigQuery, Pub/Sub, and Redshift dashboard routes.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: SQS dashboard management UI loop contract is ready.
