# Redshift PostgreSQL Backend Migration Goal

## Goal

Migrate the Redshift-compatible local server from the current Go memory executor to a PostgreSQL-backed execution engine, following the updated `docs/design-redshift-compat.md`.

## Why

The current Redshift MVP passes the local full gate, but long-term SQL compatibility should not be reimplemented in a custom in-memory executor. PostgreSQL should own SQL execution, transactions, broad catalog behavior, and driver compatibility. `devcloud` should own the Redshift compatibility layer: wire/API shape, Data API lifecycle, management metadata, Redshift SQL translation, COPY / UNLOAD side effects, Redshift system views, and safe logging.

## Acceptance Criteria

1. Existing Redshift MVP remains green: `VERIFY_STAGE=full bash scripts/redshift-autoloop/verify.sh` passes.
2. `internal/services/redshift/backend` defines a `SQLBackend` boundary and keeps the current executor behind a temporary memory fallback.
3. `internal/services/redshift/backend/postgres` executes SQL against PostgreSQL with transaction, result mapping, catalog snapshot, timeout, and error mapping support.
4. Redshift config supports `services.redshift.backend.kind=postgres`, managed PostgreSQL mode, and external DSN mode.
5. Redshift SQL is routed through a translator before backend execution.
6. Translator extracts `DISTSTYLE`, `DISTKEY`, `SORTKEY`, `ENCODE`, `BACKUP`, `IDENTITY`, and default metadata before sending PostgreSQL-compatible DDL.
7. Translator rewrites or rejects Redshift-specific expressions including `GETDATE`, `SYSDATE`, `NVL`, `DECODE`, `DATEADD`, `DATEDIFF`, and `LISTAGG`.
8. `COPY` loads local S3/file CSV into PostgreSQL through devcloud side effects.
9. `UNLOAD` exports PostgreSQL query results to local S3/file through devcloud side effects.
10. `pg_catalog`, `information_schema`, `stl`, `stv`, and `svv` queries combine PostgreSQL catalog data with devcloud metadata for client introspection.
11. Dashboard exposes Redshift backend mode, cluster status, catalog, statements, query runner, and table details without leaking SQL bind values or credentials.
12. Existing Mail, S3, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, dashboard, and Redshift API behavior remains compatible; `go test ./...` passes.
13. `VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh` passes.

## Out of Scope

- Real AWS Redshift, Redshift Serverless, IAM, KMS, Secrets Manager, CloudWatch, billing, quotas, and production Redshift calls.
- Production PostgreSQL cluster management; this loop targets local managed PostgreSQL or explicit local DSN.
- MPP execution, RA3 storage, columnar compression, WLM, concurrency scaling, and Redshift optimizer parity.
- Full Redshift function coverage beyond the migration acceptance list.
- Removing the memory fallback before PostgreSQL backend has passed `full-postgres`.

## Implementation Guidance

- Preserve current Redshift MVP behavior before switching defaults.
- Introduce backend interfaces first; keep external API behavior stable.
- Use a PostgreSQL driver or process dependency only when justified in code and tests.
- Never send raw Redshift-only SQL directly to PostgreSQL; route through the translator.
- Keep Data API statement lifecycle and management API metadata in devcloud, not PostgreSQL.
- Commit metadata effects only after backend transaction success.
- Do not log passwords, authorization headers, DSNs with credentials, SQL bind values, COPY credentials, object payloads, or statement results unless explicitly redacted.
- Treat `docs/design-redshift-compat.md` as the migration contract.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Redshift PostgreSQL backend migration loop contract is ready.
