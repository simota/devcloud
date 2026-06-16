# S3 Test Coverage Goal

## Goal

Strengthen S3 test coverage after the S3 dashboard management UI expansion. The loop should add focused tests for high-risk S3 behavior without changing product behavior.

## Acceptance Criteria

1. Existing S3 compatibility and dashboard management gates remain green:
   - `VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh`
   - `VERIFY_STAGE=full-management-ui bash scripts/s3-dashboard-management-autoloop/verify.sh`
2. `services/s3/server_test.rs` gains targeted tests for S3 edge/error behavior across bucket, object, copy, metadata, range, multipart, and presigned URL flows.
3. `services/dashboard/server_test.rs` gains targeted tests for `/api/s3/*` dashboard API validation, disabled-state behavior, mutation safety, metadata propagation, and destructive confirmation behavior where applicable.
4. `scripts/s3-e2e.sh` or docs describe and/or verify S3 management workflows: bucket create/delete, object upload/delete/copy, metadata, multipart abort, and presigned URL behavior.
5. New tests assert behavior and error contracts, not private implementation details.
6. Tests remain deterministic: no arbitrary sleeps, no external AWS calls, no shared mutable state between tests, and all temporary data is isolated.
7. Test additions do not log or persist object payloads, credentials, Authorization headers, SigV4 material, presigned secrets, or full request bodies.
8. S3 service package coverage is at least `72.0%`.
9. Dashboard package coverage is at least `68.5%`.
10. `VERIFY_STAGE=full-test-coverage bash scripts/s3-test-coverage-autoloop/verify.sh` passes.

## Stages

- `foundation`: loop contract plus existing S3 full and S3 dashboard management gates.
- `service-edge`: add S3 service edge/error tests.
- `dashboard-api`: add dashboard S3 API validation and safety tests.
- `e2e-docs`: extend E2E/docs coverage for S3 management workflows.
- `coverage`: enforce S3 service and dashboard coverage thresholds.
- `full-test-coverage`: all stages plus repository tests.

## Out of Scope

- Product behavior changes unrelated to making tests pass.
- New test frameworks, Testcontainers, mutation testing infrastructure, or CI workflow changes.
- Real AWS calls, production credentials, IAM policy enforcement, billing, quotas, or lifecycle policies.
- Broad refactors of S3 service or dashboard code.

## Implementation Guidance

- Prefer small, behavior-focused Rust tests matching the existing `testing` style.
- Add regression/edge tests before touching production code; only fix production code when a test exposes a real defect.
- Keep setup isolated with `t.TempDir`, in-memory stores, or existing local helpers.
- Use table tests for boundary matrices only when they stay readable.
- Keep S3 tests scoped to `services/s3`, `services/dashboard`, `scripts/s3-e2e.sh`, README/docs, and this loop directory.
- Preserve existing Mail, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, and Redshift behavior.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: S3 test coverage loop contract is ready.
