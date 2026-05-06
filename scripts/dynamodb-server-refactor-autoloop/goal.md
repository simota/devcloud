# DynamoDB Server Refactor Autoloop Goal

## Objective

Refactor `internal/services/dynamodb/server.go` into smaller, behavior-preserving files while keeping the DynamoDB-compatible API stable.

The current `server.go` combines request/response types, HTTP target dispatch, table lifecycle APIs, item APIs, query/scan, batch and transaction flows, streams, TTL, backups, tags, resource policies, persistence, PartiQL, condition/update expression evaluation, and dashboard snapshots. This loop must split those responsibilities without changing behavior.

## Scope

### Target File Layout

Move code from `server.go` into focused files in the same `internal/services/dynamodb` package:

1. `types.go`: request/response and persisted state types.
2. `routes.go`: `Run`, `routes`, `ServeHTTP`, target dispatch, request decoding, and JSON/error writers.
3. `dashboard.go`: dashboard snapshot helpers.
4. `table_handlers.go`: table lifecycle, indexes, limits, endpoints, and table description helpers.
5. `item_handlers.go`: PutItem, GetItem, DeleteItem, UpdateItem, item sizing, item keys, projections, and item clones.
6. `query_scan.go`: Query, Scan, sorting, pagination, filtering, select/projection response shaping.
7. `batch_handlers.go`: BatchGetItem, BatchWriteItem, BatchExecuteStatement, and batch consumed capacity helpers.
8. `transaction_handlers.go`: ExecuteTransaction, TransactGetItems, TransactWriteItems, transaction validation, rollback helpers, and transaction errors.
9. `streams.go`: stream APIs, stream records, iterators, stream descriptions, and stream append helpers.
10. `ttl_backups.go`: TTL, continuous backups, backup create/list/restore/delete helpers.
11. `tags_policy.go`: TagResource, ListTagsOfResource, UntagResource, resource policy handlers.
12. `persistence.go`: load/persist and table lookup helpers.
13. `partiql.go`: ExecuteStatement and PartiQL parsing/evaluation helpers.
14. `expressions.go`: condition expression, filter expression, update expression, comparison, arithmetic, set/list helpers.

## Non-Goals

- Do not change DynamoDB behavior, HTTP targets, JSON response shape, error names, persistence format, dashboard API, or compatibility matrix.
- Do not rename exported APIs unless the compiler requires it.
- Do not introduce new dependencies.
- Do not add feature work.
- Do not split tests until source movement is stable.

## Implementation Order

1. Move pure types and response helpers first.
2. Move expression and PartiQL helpers.
3. Move query/scan and item helpers.
4. Move batch and transaction handlers.
5. Move streams, TTL, backups, tags, and resource policies.
6. Move table lifecycle and persistence helpers.
7. Keep `server.go` as a small entrypoint for `Config`, `Server`, `NewServer`, and any top-level orchestration that remains clearer.
8. Run `gofmt` and focused tests after every meaningful move.
9. Split tests only after source movement is passing.

## Acceptance Criteria

- AC-001: `VERIFY_STAGE=foundation bash scripts/dynamodb-server-refactor-autoloop/verify.sh` passes before source movement starts.
- AC-002: `internal/services/dynamodb/server.go` is reduced to 800 lines or fewer.
- AC-003: At least eight focused DynamoDB source files exist beyond `server.go`.
- AC-004: `go test ./internal/services/dynamodb` passes.
- AC-005: `go test ./...` passes.
- AC-006: `VERIFY_STAGE=full bash scripts/dynamodb-autoloop/verify.sh` passes.
- AC-007: `gofmt` is applied to changed DynamoDB Go files.
- AC-008: The refactor keeps all code in package `dynamodb`.
- AC-009: Runtime loop state files are not staged.

## Done Criteria

The loop may report `DONE` only when AC-001 through AC-009 pass. If any behavior test fails, the loop must report `CONTINUE` and fix the regression instead of widening scope.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: DynamoDB server refactor loop goal is ready.
