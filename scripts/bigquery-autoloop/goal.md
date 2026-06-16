# BigQuery Server Autoloop Goal

## Goal

Implement a Google BigQuery REST v2 compatible local server for `devcloud`, following `docs/design-bigquery-compat.md`.

## Why

`devcloud` already has local Mail, S3, GCS, DynamoDB, and shared dashboard foundations. BigQuery compatibility should add a local Google Cloud client / REST target for dataset, table, row ingestion, query job, load, extract, copy, and dashboard workflows without reaching Google Cloud or depending on an external emulator.

## Acceptance Criteria

1. `devcloud up` starts a BigQuery-compatible REST endpoint on the configured local port, defaulting to `127.0.0.1:9050`.
2. REST v2 project discovery works for `GET /bigquery/v2/projects`.
3. Dataset operations work: `datasets.insert`, `datasets.get`, `datasets.list`, `datasets.patch`, and `datasets.delete`.
4. Table operations work: `tables.insert`, `tables.get`, `tables.list`, `tables.patch`, and `tables.delete` for supported schema and metadata.
5. `tabledata.insertAll` accepts JSON rows with `insertId`, validates known schema types, persists rows under `.devcloud/`, and returns BigQuery-shaped partial errors when applicable.
6. `tabledata.list` returns persisted rows with pagination-compatible response fields for tested fixtures.
7. Query jobs work for the documented GoogleSQL subset: `jobs.query`, `jobs.insert` query jobs, `jobs.get`, `jobs.getQueryResults`, and job history.
8. Load, extract, and copy job skeletons work for local GCS-backed fixtures described in the design document.
9. Relaxed and strict bearer-token modes are supported without logging Authorization headers or row payloads.
10. Existing Mail, S3, GCS, DynamoDB, and dashboard behavior remains compatible; `cargo test --workspace` passes.
11. Dashboard service registry exposes BigQuery, and `/api/bigquery/*` can inspect projects, datasets, tables, rows, schemas, and jobs.
12. `VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh` passes.

## Out of Scope for This Loop

- Real Google Cloud IAM, OAuth token exchange, Cloud Audit Logs, billing, reservations, or external Google Cloud service calls.
- BigQuery Storage API, BI Engine, BigQuery ML, remote functions, Analytics Hub, Data Transfer Service, Dataform, and authorized views.
- Full GoogleSQL compatibility. Implement the staged subset from `docs/design-bigquery-compat.md`.
- Distributed execution, slot scheduling, production-scale columnar storage, and multi-region replication.
- External emulator dependencies.

## Implementation Guidance

- Preserve existing Mail, S3, GCS, DynamoDB, and dashboard behavior before broad storage refactors.
- Prefer small vertical slices with tests.
- Keep runtime data under `.devcloud/`.
- Use Rust standard library unless a dependency is clearly justified.
- Keep BigQuery REST routing, service logic, schema handling, SQL/query execution, job lifecycle, and storage boundaries separate.
- Do not log credentials, Authorization headers, bearer tokens, request bodies, row payloads, or query text unless explicitly sanitized.
- Treat `docs/design-bigquery-compat.md` as the implementation contract.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: BigQuery compatibility loop contract is ready.
