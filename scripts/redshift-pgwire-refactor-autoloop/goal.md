# Redshift PG Wire Refactor Autoloop Goal

## Objective

Refactor `internal/services/redshift/pgwire.go` into smaller, behavior-preserving files while keeping the PostgreSQL wire protocol and Redshift-compatible SQL behavior stable.

The current `pgwire.go` combines frontend/backend protocol handling, startup/authentication, extended query protocol, SQL execution dispatch, in-memory fallback SQL, COPY/UNLOAD, S3 I/O, catalog result shaping, SQL parsing helpers, and wire message codecs. This loop must split those responsibilities without changing behavior.

## Scope

### Target File Layout

Move code from `pgwire.go` into focused files in the same `internal/services/redshift` package:

1. `pgwire_conn.go`: connection lifecycle, startup/authentication, simple query handling, SQL history recording.
2. `pgwire_extended.go`: extended query session, Parse/Bind/Describe/Execute/Close/Sync, bind parameter substitution.
3. `pgwire_codec.go`: message payload readers, startup parsing, C-string/int readers, auth/status/row/error/message writers, `pgField`.
4. `sql_execute.go`: query result structs/conversion, `executeSQL`, backend dispatch, batch execution, statement size validation.
5. `sql_memory.go`: in-memory SQL operations for CREATE/DROP/INSERT/UPDATE/DELETE/SELECT and literal selects.
6. `sql_copy_unload.go`: COPY/UNLOAD, CSV/JSON record reading, S3 URI parsing, S3 object read/write, copy option parsing.
7. `sql_parse.go`: qualified names, identifiers, columns, table attributes, comma/CSV-ish/value tuple parsing, literals, clauses.
8. `sql_predicates.go`: where predicates, assignments, selected columns, order/limit, comparison and row sorting helpers.
9. `catalog.go`: catalog SELECT detection, catalog result shaping, information_schema/pg_catalog/SVV/STL/STV result builders.
10. `sql_types.go`: Redshift/PostgreSQL type OID/size inference and literal type inference helpers.

## Non-Goals

- Do not change PG wire protocol behavior, SQL result shape, command tags, SQLSTATE codes, Redshift catalog output, COPY/UNLOAD behavior, or storage semantics.
- Do not rename exported APIs unless required by the compiler.
- Do not introduce new dependencies.
- Do not add feature work.
- Do not split tests until source movement is stable.

## Implementation Order

1. Move pure codec/types helpers first.
2. Move catalog and SQL parsing helpers.
3. Move predicate/assignment/selection helpers.
4. Move COPY/UNLOAD and S3 helpers.
5. Move SQL memory execution and backend dispatch.
6. Move extended query protocol.
7. Keep `pgwire.go` as a small entrypoint for `handleSQLConn` if that remains clearer, or reduce it to protocol constants.
8. Run `gofmt` and focused tests after every meaningful move.
9. Split tests only after source movement is passing.

## Acceptance Criteria

- AC-001: `VERIFY_STAGE=foundation bash scripts/redshift-pgwire-refactor-autoloop/verify.sh` passes before source movement starts.
- AC-002: `internal/services/redshift/pgwire.go` is reduced to 700 lines or fewer.
- AC-003: At least six focused Redshift source files exist matching `internal/services/redshift/pgwire*.go`, `sql_*.go`, or `catalog.go` beyond `pgwire.go`.
- AC-004: `go test ./internal/services/redshift` passes.
- AC-005: `go test ./...` passes.
- AC-006: `VERIFY_STAGE=full bash scripts/redshift-autoloop/verify.sh` passes.
- AC-007: `gofmt` is applied to changed Redshift Go files.
- AC-008: The refactor keeps all code in package `redshift`.
- AC-009: Runtime loop state files are not staged.

## Done Criteria

The loop may report `DONE` only when AC-001 through AC-009 pass. If any behavior test fails, the loop must report `CONTINUE` and fix the regression instead of widening scope.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Redshift pgwire refactor loop goal is ready.
