# GCS Test Coverage Goal

## Goal

Strengthen GCS test coverage after the GCS React dashboard integration. The loop should add focused tests for high-risk GCS behavior without changing product behavior.

## Acceptance Criteria

1. Existing GCS compatibility and React dashboard gates remain green:
   - `VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh`
   - `VERIFY_STAGE=full-react-gcs bash scripts/gcs-dashboard-react-autoloop/verify.sh`
2. `services/gcs/server_test.rs` gains targeted tests for GCS edge/error behavior across bucket, object, metadata, generation/metageneration, resumable upload sessions, pagination, and delete flows.
3. `services/dashboard/server_test.rs` gains targeted tests for `/api/gcs/*` dashboard API validation, disabled-state behavior, mutation safety, metadata propagation, upload session listing/deletion, and destructive behavior where applicable.
4. `scripts/gcs-e2e.sh` or docs describe and/or verify GCS management workflows: bucket create/delete, object listing/detail/delete, metadata, generation/metageneration, and upload session behavior.
5. New tests assert behavior and error contracts, not private implementation details.
6. Tests remain deterministic: no arbitrary sleeps, no external Google Cloud calls, no shared mutable state between tests, and all temporary data is isolated.
7. Test additions do not log or persist object payloads, credentials, Authorization headers, bearer tokens, upload URLs containing secrets, or full request bodies.
8. GCS service package coverage is at least `72.0%`.
9. Dashboard package coverage is at least `70.0%`.
10. `VERIFY_STAGE=full-test-coverage bash scripts/gcs-test-coverage-autoloop/verify.sh` passes.

## Stages

- `foundation`: loop contract plus existing GCS full and GCS React dashboard gates.
- `service-edge`: add GCS service edge/error tests.
- `dashboard-api`: add dashboard GCS API validation and safety tests.
- `e2e-docs`: extend E2E/docs coverage for GCS management workflows.
- `coverage`: enforce GCS service and dashboard coverage thresholds.
- `full-test-coverage`: all stages plus repository tests.

## Out of Scope

- Product behavior changes unrelated to making tests pass.
- New test frameworks, Testcontainers, mutation testing infrastructure, or CI workflow changes.
- Real Google Cloud calls, production credentials, IAM policy enforcement, billing, quotas, object retention, or lifecycle policies.
- Broad refactors of GCS service or dashboard code.

## Implementation Guidance

- Prefer small, behavior-focused Rust tests matching the existing `testing` style.
- Add regression/edge tests before touching production code; only fix production code when a test exposes a real defect.
- Keep setup isolated with `t.TempDir`, in-memory stores, or existing local helpers.
- Use table tests for boundary matrices only when they stay readable.
- Keep GCS tests scoped to `services/gcs`, `services/dashboard`, `scripts/gcs-e2e.sh`, README/docs, and this loop directory.
- Preserve existing Mail, S3, DynamoDB, BigQuery, SQS, Pub/Sub, and Redshift behavior.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: GCS test coverage loop contract is ready.
