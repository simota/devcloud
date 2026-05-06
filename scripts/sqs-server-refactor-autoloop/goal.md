# SQS Server Refactor Autoloop Goal

## Objective

Refactor `internal/services/sqs/server.go` into smaller, behavior-preserving files while keeping the SQS-compatible API stable.

The current `server.go` combines HTTP routing, protocol detection, queue management, queue attributes/tags/policies, permissions, DLQ redrive and move tasks, message send/receive/delete/visibility, FIFO deduplication, persistence, request parsing, dashboard snapshots, XML/JSON response types, validation, and low-level helpers. This loop must split those responsibilities without changing behavior.

## Scope

### Target File Layout

Move code from `server.go` into focused files in the same `internal/services/sqs` package:

1. `types.go`: queue/message/move task state, request/result types, persisted state types, and protocol error types.
2. `routes.go`: `Run`, `routes`, `ServeHTTP`, top-level dispatch, protocol detection, and query API version validation.
3. `dashboard.go`: dashboard snapshot helpers and purge-by-name support.
4. `queue_handlers.go`: ListQueues, CreateQueue, GetQueueURL, DeleteQueue, PurgeQueue, queue URL/ARN lookup helpers.
5. `queue_attributes.go`: Get/SetQueueAttributes, computed attributes, attribute validation, redrive/redrive allow policy parsing.
6. `tags_policy.go`: TagQueue, UntagQueue, ListQueueTags, AddPermission, RemovePermission, queue policy helpers.
7. `deadletter_move_tasks.go`: ListDeadLetterSourceQueues, Start/List/CancelMessageMoveTask, DLQ movement helpers.
8. `message_handlers.go`: SendMessage, SendMessageBatch, ReceiveMessage, DeleteMessage, DeleteMessageBatch, ChangeMessageVisibility, ChangeMessageVisibilityBatch.
9. `message_core.go`: message lifecycle, leases, visibility, retention cleanup, FIFO deduplication, wait channels, attribute filtering.
10. `request_parsing.go`: Query/JSON request parsing helpers, form extraction, queue URL extraction, attributes/tags/message attributes parsing.
11. `responses.go`: JSON/XML writers, response XML structs, protocol errors, XML conversion helpers.
12. `persistence.go`: load/persist, clone helpers, defaults, limits, queue state persistence conversions.
13. `validation.go`: queue name, batch entry ID, message body, message attributes, system attributes, and common validation helpers.
14. `hashing.go`: MD5 helpers, binary attribute decoding, opaque ID generation, and small scalar helpers.

## Non-Goals

- Do not change SQS behavior, HTTP protocol handling, JSON/XML response shape, error codes, persistence format, dashboard API, or compatibility matrix.
- Do not rename exported APIs unless required by the compiler.
- Do not introduce new dependencies.
- Do not add feature work.
- Do not split tests until source movement is stable.

## Implementation Order

1. Move pure types and response helpers first.
2. Move request parsing and validation helpers.
3. Move hashing, cloning, defaults, persistence, and dashboard helpers.
4. Move queue attributes, tags, policies, and DLQ/move-task logic.
5. Move message handlers and message core helpers.
6. Move queue handlers and routing once dependencies are stable.
7. Keep `server.go` as a small entrypoint for `Config`, `Server`, `NewServer`, and any top-level orchestration that remains clearer.
8. Run `gofmt` and focused tests after every meaningful move.
9. Split tests only after source movement is passing.

## Acceptance Criteria

- AC-001: `VERIFY_STAGE=foundation bash scripts/sqs-server-refactor-autoloop/verify.sh` passes before source movement starts.
- AC-002: `internal/services/sqs/server.go` is reduced to 700 lines or fewer.
- AC-003: At least eight focused SQS source files exist beyond `server.go`.
- AC-004: `go test ./internal/services/sqs` passes.
- AC-005: `go test ./...` passes.
- AC-006: `VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh` passes.
- AC-007: `gofmt` is applied to changed SQS Go files.
- AC-008: The refactor keeps all code in package `sqs`.
- AC-009: Runtime loop state files are not staged.

## Done Criteria

The loop may report `DONE` only when AC-001 through AC-009 pass. If any behavior test fails, the loop must report `CONTINUE` and fix the regression instead of widening scope.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: SQS server refactor loop goal is ready.
