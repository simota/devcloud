# S3 Feature Parity Autoloop Goal

## Objective

Implement the missing S3-compatible feature surface in bounded, reviewable slices without breaking the existing S3 core, dashboard, GCS, BigQuery, DynamoDB, SQS, Pub/Sub, Redshift, or Mail workflows.

This loop targets the 1-10 missing S3 features identified from the current compatibility matrix:

1. Versioning
2. Bucket policy and ACL metadata
3. Lifecycle expiration
4. Event notifications
5. SSE/KMS metadata
6. Virtual-host style routes
7. Object Lock
8. S3 Select
9. Inventory and analytics metadata/reporting
10. Replication metadata and local side effects

## Scope

### Feature Targets

#### FT-001 Versioning
- Add bucket versioning configuration APIs.
- Store object versions when versioning is enabled.
- Return version IDs on mutating object operations.
- Support local listing/get/delete behavior for versions.

#### FT-002 Bucket Policy and ACL Metadata
- Add bucket policy put/get/delete compatibility endpoints.
- Add bucket/object ACL put/get compatibility endpoints.
- Persist metadata locally.
- Do not implement real AWS IAM enforcement unless explicitly scoped by tests.

#### FT-003 Lifecycle Expiration
- Add lifecycle configuration put/get/delete endpoints.
- Apply deterministic local expiration for supported rules.
- Keep unsupported transitions safely rejected or metadata-only.

#### FT-004 Notifications
- Add notification configuration put/get endpoints.
- Persist S3 event notification metadata.
- Emit local event records for object create/delete flows where feasible.

#### FT-005 SSE/KMS Metadata
- Accept and expose supported SSE headers and metadata.
- Support SSE-S3 and SSE-KMS metadata compatibility without real cryptographic KMS.
- Never log encryption keys or customer-provided sensitive headers.

#### FT-006 Virtual-Host Style Routes
- Route `{bucket}.localhost` / Host-header bucket requests to the same bucket handlers where safe.
- Preserve path-style compatibility.
- Avoid broad host parsing that could conflict with dashboard or other services.

#### FT-007 Object Lock
- Add object lock configuration, retention, and legal hold metadata endpoints.
- Enforce local delete guards for retained/legal-hold objects where deterministic.

#### FT-008 S3 Select
- Add a minimal `SelectObjectContent` compatibility path for CSV and JSON local objects.
- Support a narrow SQL subset and safe errors for unsupported expressions.

#### FT-009 Inventory and Analytics
- Add inventory and analytics configuration put/get/list/delete metadata endpoints.
- Generate local inventory report metadata or deterministic report files where feasible.

#### FT-010 Replication
- Add replication configuration put/get/delete endpoints.
- Persist replication metadata.
- Implement local same-process bucket replication for object writes where feasible.

## Non-Goals

- Real AWS IAM, KMS, CloudWatch, billing, cross-region networking, or production-grade security.
- Perfect AWS error message parity for every unsupported edge case.
- Large rewrites of the S3 storage core unless a feature requires a small, well-tested data model change.
- Replacing existing S3/GCS shared object storage behavior without compatibility tests.

## Implementation Order

1. Versioning data model and endpoint slice.
2. Policy/ACL metadata endpoints.
3. SSE/KMS metadata acceptance and response headers.
4. Virtual-host routing.
5. Lifecycle and object lock local enforcement.
6. Notifications and replication side effects.
7. S3 Select narrow evaluator.
8. Inventory/analytics metadata and report generation.
9. Dashboard/API visibility for new metadata.
10. Documentation and final compatibility matrix cleanup.

## Acceptance Criteria

- AC-001: `VERIFY_STAGE=foundation bash scripts/s3-feature-parity-autoloop/verify.sh` passes before implementation work starts.
- AC-002: Existing S3 core gate still passes: `VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh`.
- AC-003: `go test ./...` passes.
- AC-004: Versioning endpoints and object version behavior are covered by focused tests.
- AC-005: Bucket policy and ACL metadata endpoints are covered by focused tests.
- AC-006: Lifecycle expiration behavior and unsupported transition handling are covered by focused tests.
- AC-007: Notification configuration and local event recording are covered by focused tests.
- AC-008: SSE/KMS metadata request/response behavior is covered by focused tests and does not log secrets.
- AC-009: Virtual-host style routing is covered by HTTP handler tests.
- AC-010: Object Lock retention/legal hold metadata and delete guards are covered by focused tests.
- AC-011: S3 Select narrow CSV/JSON workflow and unsupported-query errors are covered by focused tests.
- AC-012: Inventory/analytics configuration endpoints are covered by focused tests.
- AC-013: Replication configuration and local replication side effect are covered by focused tests.
- AC-014: README S3 compatibility matrix reflects the implemented feature status.
- AC-015: Full verification passes: `VERIFY_STAGE=full bash scripts/s3-feature-parity-autoloop/verify.sh`.

## Done Criteria

The loop may report `DONE` only when:

- AC-001 through AC-015 are satisfied.
- The existing `scripts/s3-autoloop/verify.sh` full gate passes.
- New tests document the supported subset and unsupported boundaries.
- No runtime loop state files are staged.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: S3 feature parity autoloop goal is ready.
