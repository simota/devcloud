# Redshift Advanced Compatibility Goal

## Goal

Implement Redshift advanced compatibility beyond the completed PostgreSQL-backed MVP.

## Acceptance Criteria

1. Existing Redshift MVP and PostgreSQL-backed gates remain green:
   - `VERIFY_STAGE=full bash scripts/redshift-autoloop/verify.sh`
   - `VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh`
   - `VERIFY_STAGE=full-managed bash scripts/redshift-managed-postgres-autoloop/verify.sh`
   - `VERIFY_STAGE=full-remaining bash scripts/redshift-remaining-autoloop/verify.sh`
   - `VERIFY_STAGE=full-query-runner-ui bash scripts/redshift-query-runner-ui-autoloop/verify.sh`
2. PostgreSQL extended query protocol supports prepared-statement clients through Parse, Bind, Describe, Execute, Sync, Close, and ErrorResponse behavior.
3. SQL compatibility broadens to CTAS, views, materialized-view metadata, UPDATE, DELETE, and MERGE where PostgreSQL can execute safely.
4. Redshift Serverless namespace/workgroup metadata API is available as local metadata without real AWS calls.
5. Snapshot create/describe/delete/restore metadata workflow is available for provisioned clusters and serverless metadata where applicable.
6. WLM/workload metadata and BI introspection catalog coverage is broadened for common JDBC/ODBC/BI client probes.
7. Stored procedure and UDF metadata is represented, with execution limited to safe PostgreSQL-backed behavior or explicit unsupported errors.
8. Dashboard and E2E coverage is updated for the advanced metadata surfaces that become user-visible.
9. `VERIFY_STAGE=full-advanced bash scripts/redshift-advanced-compat-autoloop/verify.sh` passes.

## Stages

- `extended-protocol`: PostgreSQL extended query protocol and prepared statement smoke tests.
- `sql-advanced`: CTAS, views, materialized-view metadata, UPDATE, DELETE, and MERGE.
- `serverless`: Redshift Serverless namespace/workgroup metadata API.
- `snapshots`: provisioned/serverless snapshot metadata lifecycle.
- `introspection`: WLM/workload metadata and broader system catalog/BI probe coverage.
- `procedures`: stored procedure/UDF metadata and limited execution or safe unsupported behavior.
- `dashboard-e2e`: dashboard and E2E surfaces for advanced metadata.
- `full-advanced`: all stages plus repository tests and existing full Redshift gates.

## Out of Scope

- Real AWS Redshift, Redshift Serverless, IAM, KMS, Secrets Manager, CloudWatch, billing, quotas, or production calls.
- MPP execution, RA3 storage, columnar performance, concurrency scaling, Spectrum execution, or datashare execution.
- Unbounded SQL parser parity.
- Removing the PostgreSQL backend or memory fallback.

## Implementation Guidance

- Preserve current Redshift MVP behavior before adding advanced compatibility.
- Prefer PostgreSQL-backed execution when syntax can be translated safely.
- Return explicit unsupported SQLSTATE/API errors for unsafe or unsupported constructs.
- Do not log passwords, DSNs with credentials, authorization headers, SQL bind values, COPY credentials, object payloads, or result payloads unless explicitly redacted.
- Keep advanced metadata local and deterministic.
- Add focused tests before broadening behavior; update E2E only after unit/contract tests are green.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Redshift advanced compatibility loop contract is ready.
