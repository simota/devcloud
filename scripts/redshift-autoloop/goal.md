# Amazon Redshift Server Autoloop Goal

## Goal

Implement an Amazon Redshift compatible local server for `devcloud`, following `docs/design-redshift-compat.md`.

## Why

`devcloud` already has local Mail, S3, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, and dashboard foundations. Redshift compatibility should add a local analytical database target for SQL clients, Redshift Data API users, AWS CLI / SDK workflows, COPY / UNLOAD integration, catalog introspection, and dashboard visibility without reaching AWS or depending on a real Redshift cluster. The DB execution backend should be PostgreSQL, with devcloud absorbing Redshift-specific syntax, APIs, metadata, S3 import/export, and system views in the compatibility layer.

## Acceptance Criteria

1. `devcloud up` starts a Redshift SQL endpoint on the configured local port, defaulting to `127.0.0.1:5439`.
2. `devcloud up` starts a Redshift HTTP API endpoint on the configured local port, defaulting to `127.0.0.1:9099`.
3. `psql "host=127.0.0.1 port=5439 dbname=dev user=dev password=dev sslmode=disable"` can connect and run `select 1`.
4. SQL clients can create schema/table resources, insert rows, update/delete rows, and query rows through the SQL endpoint using the PostgreSQL backend.
5. Redshift table attributes such as `DISTKEY`, `SORTKEY`, `ENCODE`, and identity/default metadata are stripped from backend SQL, persisted as Redshift metadata, and visible through catalog metadata.
6. Redshift Data API supports `ExecuteStatement`, `DescribeStatement`, `GetStatementResult`, `ListStatements`, `ListDatabases`, `ListSchemas`, `ListTables`, and `DescribeTable` for the MVP workflow.
7. Redshift management API supports `DescribeClusters` with local cluster metadata and endpoint port `5439`; metadata-only cluster lifecycle, tags, parameters, and credentials can be staged after the MVP.
8. `COPY` can load CSV data from local S3 or local file fixtures into PostgreSQL, and `UNLOAD` can export PostgreSQL query results to local S3 or local file fixtures.
9. Redshift-specific functions and expressions such as `GETDATE`, `SYSDATE`, `NVL`, `DECODE`, `DATEADD`, `DATEDIFF`, and `LISTAGG` are translated to PostgreSQL equivalents or rejected with Redshift-like errors when unsupported.
10. `pg_catalog`, `information_schema`, and representative `stl` / `stv` / `svv` system schema queries combine PostgreSQL catalog data with devcloud metadata enough for client introspection.
11. Dashboard service registry exposes Redshift, and `/api/redshift/*` plus `/dashboard/redshift` can inspect cluster status, backend mode, catalog, tables, recent statements, and query results.
12. Existing Mail, S3, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, and dashboard behavior remains compatible; `cargo test --workspace` passes.
13. `VERIFY_STAGE=full bash scripts/redshift-autoloop/verify.sh` passes with `services.redshift.backend.kind=postgres`.

## Out of Scope for This Loop

- Real AWS IAM, Secrets Manager, KMS, VPC, CloudWatch, CloudTrail, billing, quota, or production Redshift calls.
- MPP execution, RA3 managed storage, real columnar compression, WLM, concurrency scaling, production optimizer parity, and production durability guarantees.
- Full PL/pgSQL, external functions, ML, datashare, and Spectrum permission integration beyond local metadata and staged fixtures.
- Production-grade TLS certificate management; the MVP can require `sslmode=disable`.
- Depending on a production PostgreSQL cluster; managed local PostgreSQL or explicit local DSN are acceptable.
- Treating PostgreSQL as the public API. Redshift API shape, SQL dialect, Data API lifecycle, and management metadata must remain devcloud-owned.

## Implementation Guidance

- Preserve existing Mail, S3, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, and dashboard behavior before backend refactors.
- Prefer small vertical slices with tests.
- Keep runtime data under `.devcloud/`.
- Use Rust standard library unless a PostgreSQL driver or process-management dependency is clearly justified.
- Keep PostgreSQL wire protocol, Redshift dialect adapter, PostgreSQL backend, Data API, management API, metadata store, statement store, COPY / UNLOAD side effects, and dashboard boundaries separate.
- Route all SQL through a Redshift translator before backend execution; do not send raw Redshift-only syntax directly to PostgreSQL.
- Keep the current memory executor only as a migration fallback until PostgreSQL backend passes full verification.
- Start with simple query protocol before prepared statement / extended query protocol.
- Keep SQL text, bind values, passwords, authorization headers, COPY credentials, object payloads, and statement results out of logs unless explicitly redacted.
- Treat `docs/design-redshift-compat.md` as the implementation contract.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Redshift compatibility loop contract is ready.
