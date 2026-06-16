# BigQuery Server Refactor Autoloop Goal

## Objective

Refactor `services/bigquery/server.rs` into smaller, behavior-preserving files while keeping the BigQuery-compatible REST API stable.

The current `server.rs` combines HTTP routing, datasets, tables, routines, IAM policy handlers, tabledata, jobs, copy/load/extract workflows, GCS import/export helpers, query execution, SQL parsing/evaluation, storage helpers, dashboard snapshots, validation, response helpers, and resource types. This loop must split those responsibilities without changing behavior.

## Scope

### Target File Layout

Move code from `server.rs` into focused files in the same `services/bigquery` package:

1. `routes.rs`: `Run`, `routes`, `ServeHTTP`, top-level dispatch, path parsing, auth, and request decoding.
2. `dataset_handlers.rs`: dataset create/get/list/patch/delete handlers and dataset helpers.
3. `table_handlers.rs`: table create/get/list/patch/delete handlers and table schema/stat helpers.
4. `routine_handlers.rs`: routine create/get/list/patch/delete handlers and routine validation.
5. `iam_handlers.rs`: dataset/table IAM policy handlers and IAM policy helpers.
6. `tabledata_handlers.rs`: insertAll, tabledata list, row validation, row formatting, paging helpers.
7. `job_handlers.rs`: jobs insert/list/cancel/delete/getQueryResults and completed job helpers.
8. `job_load_extract.rs`: load jobs, upload load jobs, copy jobs, extract jobs, GCS URI parsing, source row loading, and extract formatting.
9. `query_engine.rs`: query job creation, dry-run/execute query flows, view execution, destination table writes.
10. `sql_parser.rs`: simple SELECT parsing, parameter binding, aggregate parsing, table identifiers, selected fields, conditions, and SQL token helpers.
11. `sql_eval.rs`: parsed query execution, aggregate/group aggregate execution, row matching, comparison, and result shaping.
12. `storage.rs`: storage path helpers and read/write helpers for datasets, tables, routines, rows, IAM policies, and jobs.
13. `dashboard.rs`: dashboard snapshot structs and snapshot helpers.
14. `responses.rs`: JSON writers, error writers, response structs, ETags/time helpers, default helpers.
15. `types.rs`: BigQuery resource/request/response types that are not tightly coupled to a single handler file.

## Non-Goals

- Do not change BigQuery behavior, HTTP routes, JSON response shape, error reason strings, persistence format, dashboard API, or compatibility matrix.
- Do not rename exported APIs unless required by the compiler.
- Do not introduce new dependencies.
- Do not add feature work.
- Do not split tests until source movement is stable.

## Implementation Order

1. Move pure types and response helpers first.
2. Move storage and dashboard helpers.
3. Move SQL parser/evaluator helpers.
4. Move job load/extract/copy helpers.
5. Move tabledata, table, dataset, routine, and IAM handlers.
6. Move routing and request decoding once handlers are stable.
7. Keep `server.rs` as a small entrypoint for `Config`, `Server`, `NewServer`, and any top-level orchestration that remains clearer.
8. Run `rustfmt` and focused tests after every meaningful move.
9. Split tests only after source movement is passing.

## Acceptance Criteria

- AC-001: `VERIFY_STAGE=foundation bash scripts/bigquery-server-refactor-autoloop/verify.sh` passes before source movement starts.
- AC-002: `services/bigquery/server.rs` is reduced to 800 lines or fewer.
- AC-003: At least eight focused BigQuery source files exist beyond `server.rs`.
- AC-004: `cargo test --workspace` passes.
- AC-005: `cargo test --workspace` passes.
- AC-006: `VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh` passes.
- AC-007: `rustfmt` is applied to changed BigQuery Rust files.
- AC-008: The refactor keeps all code in package `bigquery`.
- AC-009: Runtime loop state files are not staged.

## Done Criteria

The loop may report `DONE` only when AC-001 through AC-009 pass. If any behavior test fails, the loop must report `CONTINUE` and fix the regression instead of widening scope.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: BigQuery server refactor loop goal is ready.
