# BigQuery Server Refactor Autoloop Goal

## Objective

Refactor `internal/services/bigquery/server.go` into smaller, behavior-preserving files while keeping the BigQuery-compatible REST API stable.

The current `server.go` combines HTTP routing, datasets, tables, routines, IAM policy handlers, tabledata, jobs, copy/load/extract workflows, GCS import/export helpers, query execution, SQL parsing/evaluation, storage helpers, dashboard snapshots, validation, response helpers, and resource types. This loop must split those responsibilities without changing behavior.

## Scope

### Target File Layout

Move code from `server.go` into focused files in the same `internal/services/bigquery` package:

1. `routes.go`: `Run`, `routes`, `ServeHTTP`, top-level dispatch, path parsing, auth, and request decoding.
2. `dataset_handlers.go`: dataset create/get/list/patch/delete handlers and dataset helpers.
3. `table_handlers.go`: table create/get/list/patch/delete handlers and table schema/stat helpers.
4. `routine_handlers.go`: routine create/get/list/patch/delete handlers and routine validation.
5. `iam_handlers.go`: dataset/table IAM policy handlers and IAM policy helpers.
6. `tabledata_handlers.go`: insertAll, tabledata list, row validation, row formatting, paging helpers.
7. `job_handlers.go`: jobs insert/list/cancel/delete/getQueryResults and completed job helpers.
8. `job_load_extract.go`: load jobs, upload load jobs, copy jobs, extract jobs, GCS URI parsing, source row loading, and extract formatting.
9. `query_engine.go`: query job creation, dry-run/execute query flows, view execution, destination table writes.
10. `sql_parser.go`: simple SELECT parsing, parameter binding, aggregate parsing, table identifiers, selected fields, conditions, and SQL token helpers.
11. `sql_eval.go`: parsed query execution, aggregate/group aggregate execution, row matching, comparison, and result shaping.
12. `storage.go`: storage path helpers and read/write helpers for datasets, tables, routines, rows, IAM policies, and jobs.
13. `dashboard.go`: dashboard snapshot structs and snapshot helpers.
14. `responses.go`: JSON writers, error writers, response structs, ETags/time helpers, default helpers.
15. `types.go`: BigQuery resource/request/response types that are not tightly coupled to a single handler file.

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
7. Keep `server.go` as a small entrypoint for `Config`, `Server`, `NewServer`, and any top-level orchestration that remains clearer.
8. Run `gofmt` and focused tests after every meaningful move.
9. Split tests only after source movement is passing.

## Acceptance Criteria

- AC-001: `VERIFY_STAGE=foundation bash scripts/bigquery-server-refactor-autoloop/verify.sh` passes before source movement starts.
- AC-002: `internal/services/bigquery/server.go` is reduced to 800 lines or fewer.
- AC-003: At least eight focused BigQuery source files exist beyond `server.go`.
- AC-004: `go test ./internal/services/bigquery` passes.
- AC-005: `go test ./...` passes.
- AC-006: `VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh` passes.
- AC-007: `gofmt` is applied to changed BigQuery Go files.
- AC-008: The refactor keeps all code in package `bigquery`.
- AC-009: Runtime loop state files are not staged.

## Done Criteria

The loop may report `DONE` only when AC-001 through AC-009 pass. If any behavior test fails, the loop must report `CONTINUE` and fix the regression instead of widening scope.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: BigQuery server refactor loop goal is ready.
