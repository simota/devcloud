# Redshift Managed PostgreSQL Mode Goal

## Goal

Implement managed PostgreSQL mode for the Redshift-compatible server so `devcloud up` can run `services.redshift.backend.kind=postgres` without a user-provided external DSN.

## Acceptance Criteria

1. Existing Redshift PostgreSQL backend migration gate remains green: `VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh`.
2. `services.redshift.backend.kind=postgres` with `mode=managed` starts a local PostgreSQL process owned by devcloud.
3. Managed mode initializes the PostgreSQL data directory under `.devcloud/data/redshift/postgres` or the configured Redshift data directory.
4. Managed mode creates or reuses a local database/user safely without logging credentials or DSNs with secrets.
5. The daemon stops the managed PostgreSQL process during shutdown and does not leave stale lock files in normal exit paths.
6. External DSN mode continues to work unchanged.
7. `backend.kind=memory` remains available as a development fallback until the default backend is intentionally flipped.
8. `scripts/redshift-managed-postgres-e2e.sh` verifies `devcloud up`, `psql select 1`, Redshift SQL DDL/DML, Data API, management API, and dashboard API with managed PostgreSQL mode.
9. `VERIFY_STAGE=full-managed bash scripts/redshift-managed-postgres-autoloop/verify.sh` passes.

## Out of Scope

- Bundling PostgreSQL binaries.
- Running production PostgreSQL clusters.
- Docker/container orchestration unless used only as an explicit opt-in helper.
- Flipping the default backend from `memory` to `postgres`.
- Removing external DSN mode.

## Implementation Guidance

- Prefer system PostgreSQL binaries (`initdb`, `postgres` or `pg_ctl`) discovered from `PATH`, with clear actionable errors when missing.
- Use deterministic local ports and data paths from devcloud config; avoid hard-coded production defaults.
- Keep managed process lifecycle in `internal/app` or a narrowly scoped internal package, not in dashboard/API handlers.
- Redact credentials in errors, logs, and test output.
- Preserve all existing behavior for external DSN mode and memory fallback.
- Add focused tests for config, lifecycle command construction, startup failure, shutdown cleanup, and redaction.
- Treat the E2E script as optional-skippable only when PostgreSQL binaries are unavailable; unit and contract tests must still pass.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Redshift managed PostgreSQL mode loop contract is ready.
