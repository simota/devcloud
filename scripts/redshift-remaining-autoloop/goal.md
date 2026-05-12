# Redshift Remaining Tasks Goal

## Goal

Finish the remaining Redshift PostgreSQL-backed compatibility work after managed PostgreSQL mode.

## Acceptance Criteria

1. `services.redshift.backend.kind` defaults to `postgres` in generated/default config while `backend.kind=memory` remains an explicit fallback.
2. Default PostgreSQL mode uses managed PostgreSQL when no external DSN is configured, with actionable diagnostics when PostgreSQL binaries are missing.
3. Existing external DSN mode remains compatible.
4. Redshift docs reflect that system `initdb`, `postgres`, and `psql` are the managed PostgreSQL strategy.
5. The design migration sequence marks or describes MIG-001 through MIG-009 status accurately.
6. `VERIFY_STAGE=full bash scripts/redshift-autoloop/verify.sh` passes after the default backend change.
7. `VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh` passes.
8. `VERIFY_STAGE=full-managed bash scripts/redshift-managed-postgres-autoloop/verify.sh` passes.
9. `go test ./...` passes.

## Remaining Advanced Compatibility Backlog

These are not required for this loop to finish unless they become necessary for the default-backend gates:

- Extended PostgreSQL query protocol.
- CTAS, views, materialized views, UPDATE, DELETE, and MERGE broadening.
- Redshift Serverless namespace/workgroup metadata API.
- Snapshots and restore.
- WLM and workload metadata.
- Stored procedure/UDF metadata and limited execution.
- JDBC/ODBC/BI introspection matrix beyond current smoke tests.

## Out of Scope

- Removing the `memory` backend fallback.
- Bundling PostgreSQL binaries.
- Production Redshift, IAM, KMS, Secrets Manager, CloudWatch, billing, quotas, or real AWS calls.
- Full SQL parity beyond the documented advanced backlog.

## Implementation Guidance

- Make the smallest default-backend change first.
- Keep fallback behavior explicit and documented.
- Do not hide missing PostgreSQL binaries behind unrelated startup errors.
- Keep generated runtime data under `.devcloud/`.
- Do not log passwords, DSNs with credentials, authorization headers, SQL bind values, COPY credentials, object payloads, or statement results unless explicitly redacted.
- Update tests and docs in the same slice as behavior changes.

NEXUS_LOOP_STATUS: DONE
NEXUS_LOOP_SUMMARY: All ACs satisfied: default backend is postgres/managed (config.go DefaultConfig), actionable diagnostics for missing postgres binaries, MIG-001-009 marked Done in docs/design-redshift-compat.md, and all three verify gates (redshift-autoloop full, redshift-postgres-backend-autoloop full-postgres, redshift-managed-postgres-autoloop full-managed) plus go test ./... pass.
