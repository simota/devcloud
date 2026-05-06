# S3 Server Refactor Autoloop Goal

## Objective

Refactor `internal/services/s3/server.go` into smaller, behavior-preserving files while keeping the S3 compatibility surface stable.

The current `server.go` combines routing, bucket configuration handlers, object handlers, multipart flows, S3 Select, validation, side effects, listing builders, and XML response types. This loop must split those responsibilities without changing public behavior.

## Scope

### Target File Layout

Move code from `server.go` into focused files in the same `internal/services/s3` package:

1. `routes.go`: route construction and path/host dispatch helpers.
2. `bucket_config_handlers.go`: bucket policy, ACL, versioning, lifecycle, notification, inventory, analytics, replication, and object lock configuration handlers.
3. `object_handlers.go`: object put/get/delete/copy, object ACL, retention, legal hold, list objects, and list versions handlers.
4. `multipart_handlers.go`: multipart create/upload/list/complete/abort/list-uploads handlers and multipart helpers.
5. `select.go`: SelectObjectContent handler, evaluator, eventstream encoding, and select XML request types.
6. `validation.go`: validation helpers for lifecycle, object lock, notification, inventory, analytics, replication, configuration IDs, ACLs, SSE, and copy source parsing where appropriate.
7. `effects.go`: lifecycle application, replication side effects, notification event recording, and related match helpers.
8. `responses.go`: XML writers, error writers, object headers, ACL XML responses, range parsing, and response structs.
9. `listing.go`: object/version listing builders, pagination helpers, continuation tokens, and listing XML structs.
10. Optional test split: split `server_test.go` into feature-focused test files only after source movement is stable.

## Non-Goals

- Do not change S3 behavior, HTTP routes, XML output shape, error codes, compatibility matrix, or storage semantics.
- Do not rename public API or exported types unless required by Go compiler conflicts.
- Do not introduce new dependencies.
- Do not add feature work.
- Do not rewrite tests for style before source file movement is complete.

## Implementation Order

1. Move pure response/listing/select/helper code first.
2. Move multipart handlers.
3. Move bucket configuration handlers.
4. Move object handlers.
5. Move validation and side effects.
6. Keep `server.go` as the small entrypoint for `Config`, `Server`, `NewServer`, `Run`, and the top-level dispatcher if that remains clearer.
7. Run format and tests after every meaningful move.
8. Split tests only after all source moves are passing.

## Acceptance Criteria

- AC-001: `VERIFY_STAGE=foundation bash scripts/s3-server-refactor-autoloop/verify.sh` passes before source movement starts.
- AC-002: `internal/services/s3/server.go` is reduced to 700 lines or fewer.
- AC-003: At least five focused S3 source files exist beyond `server.go`, `store.go`, and `sigv4.go`.
- AC-004: `go test ./internal/services/s3` passes.
- AC-005: `go test ./...` passes.
- AC-006: `VERIFY_STAGE=full bash scripts/s3-feature-parity-autoloop/verify.sh` passes.
- AC-007: `VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh` passes.
- AC-008: `gofmt` is applied to changed Go files.
- AC-009: The refactor keeps all code in package `s3`.
- AC-010: Runtime loop state files are not staged.

## Done Criteria

The loop may report `DONE` only when AC-001 through AC-010 pass. If any behavior test fails, the loop must report `CONTINUE` and fix the regression instead of widening scope.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: S3 server refactor loop goal is ready.
