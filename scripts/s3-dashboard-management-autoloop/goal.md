# S3 Dashboard Management UI Goal

## Goal

Upgrade the S3 React dashboard from bucket/object inspection into a safer local management console for common S3 developer workflows while preserving existing S3 compatibility behavior.

## Acceptance Criteria

1. Existing S3 compatibility gate remains green:
   - `VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh`
2. The existing `/s3` React route remains available and keeps bucket, prefix, object, metadata, and download inspection.
3. `web/dashboard/src/app/services/s3/` contains typed API helpers and UI flows for bucket creation, guarded bucket deletion, object upload, guarded object deletion, and object copy.
4. S3 management mutations go through `/api/s3/*` or the local S3-compatible endpoint path, not by mutating storage directly.
5. Upload flows support content type and user metadata input without persisting object body payloads in durable UI state or logs.
6. The UI exposes object detail including key, size, ETag, Last-Modified, content type, user metadata, download link, and copy source/destination controls.
7. Multipart upload state is visible when available, with guarded abort controls if dashboard/API support exists.
8. Presigned URL or signed-download helper behavior is visible through a safe local-only UI affordance when supported by the existing S3 implementation.
9. Destructive actions require explicit confirmation and are disabled when S3 is unavailable.
10. E2E or dashboard tests verify bucket create/delete, object upload/delete, copy/download, metadata display, disabled-state behavior, and safety confirmations.
11. README or design docs describe S3 dashboard management capabilities and safety boundaries.
12. `VERIFY_STAGE=full-management-ui bash scripts/s3-dashboard-management-autoloop/verify.sh` passes.

## Stages

- `foundation`: loop contract and existing `s3-autoloop` full gate.
- `bucket-object-management`: bucket create/delete and object delete controls call safe APIs.
- `upload-copy-download`: upload, copy, object detail, metadata, and download flows work from the dashboard.
- `multipart-presign`: multipart upload visibility/actions and presigned URL affordances are covered where supported.
- `e2e-docs`: dashboard tests/E2E/docs cover S3 management behavior.
- `full-management-ui`: all stages plus repository tests.

## Out of Scope

- Real AWS calls, IAM policy enforcement, billing, quotas, object lock, lifecycle policies, replication, or production durability.
- Implementing new S3 protocol behavior beyond dashboard needs unless required to expose an already-planned local workflow.
- Persisting object body payloads, credentials, Authorization headers, SigV4 material, presigned secrets, or full request bodies in UI state/history.
- Replacing the shared Rust-served React dashboard architecture.

## Implementation Guidance

- Keep the UI compact, operational, and consistent with the shared dashboard shell.
- Reuse existing S3 object explorer components where helpful, but add management controls without nesting UI cards.
- Mutations must call dashboard APIs or the local S3-compatible endpoint; do not bypass service logic by writing storage files.
- Destructive flows must require explicit confirmation and must be disabled when S3 is unavailable.
- Update generated dashboard assets with `npm run build` after frontend changes.
- Preserve existing Mail, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, and Redshift dashboard routes.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: S3 dashboard management UI loop contract is ready.
