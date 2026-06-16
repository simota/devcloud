# Pub/Sub Server Refactor Autoloop Goal

## Objective

Refactor `services/pubsub/server.rs` into smaller, behavior-preserving files while keeping the Pub/Sub-compatible REST API stable.

The current `server.rs` combines HTTP routing, REST handlers, topics, subscriptions, snapshots, schemas, IAM compatibility, publish/pull/ack flows, push delivery, leases, retry/dead-letter logic, retention cleanup, path parsing, validation, persistence, dashboard snapshots, and JSON response helpers. This loop must split those responsibilities without changing behavior.

## Scope

### Target File Layout

Move code from `server.rs` into focused files in the same `services/pubsub` package:

1. `types.rs`: topic/subscription/snapshot/schema/message/delivery resource types and persisted file types.
2. `routes.rs`: `Run`, REST listener, `routes`, `ServeHTTP`, top-level dispatch, auth, and path/action dispatch helpers.
3. `dashboard.rs`: dashboard snapshot helpers and public snapshot shaping.
4. `topic_handlers.rs`: topic collection, topic get/create/update/delete, topic subscriptions/snapshots, and topic name helpers.
5. `subscription_handlers.rs`: subscription collection, get/create/update/delete, detach, seek, push config, and subscription name helpers.
6. `schema_handlers.rs`: schema collection, create/get/list/delete/validate message, schema validation, and schema view helpers.
7. `snapshot_handlers.rs`: snapshot collection, get/create/delete, snapshot expiration, and snapshot delivery helpers.
8. `iam_handlers.rs`: topic/subscription IAM compatibility handlers and IAM action helpers.
9. `message_handlers.rs`: publish, pull, acknowledge, modifyAckDeadline, and delivery response shaping.
10. `push_delivery.rs`: push worker, push delivery selection, completion, endpoint helpers, and safe push config snapshots.
11. `retention_leases.rs`: lease expiration, retry backoff, dead-letter delivery, message retention, snapshot cleanup, and unreferenced message cleanup.
12. `path_parsing.rs`: REST path predicates and path part parsing helpers.
13. `validation.rs`: resource ID/project/name validation, metadata validation, filters, retry policy, dead-letter policy, push config, publish message validation.
14. `patch_masks.rs`: topic/subscription patch decoding and update mask handling.
15. `pagination.rs`: list pagination, page bounds, generated names, and small copy/default helpers.
16. `persistence.rs`: load/save resources, atomic JSON writes, resource file path helpers, and persisted state conversion.
17. `responses.rs`: JSON/error writers, method-not-allowed helper, bearer token extraction if not kept in routes.

## Non-Goals

- Do not change Pub/Sub behavior, REST routes, JSON response shape, error codes, persistence format, dashboard API, or compatibility matrix.
- Do not change `services/pubsub/grpc.rs` unless strictly required by compiler dependencies.
- Do not rename exported APIs unless required by the compiler.
- Do not introduce new dependencies.
- Do not add feature work.
- Do not split tests until source movement is stable.

## Implementation Order

1. Move pure types and response helpers first.
2. Move path parsing, validation, patch masks, pagination, and persistence helpers.
3. Move retention/lease and push delivery helpers.
4. Move message, schema, snapshot, subscription, topic, and IAM handlers.
5. Move REST routing once handlers are stable.
6. Keep `server.rs` as a small entrypoint for `Config`, `Server`, `NewServer`, and any top-level orchestration that remains clearer.
7. Run `rustfmt` and focused tests after every meaningful move.
8. Split tests only after source movement is passing.

## Acceptance Criteria

- AC-001: `VERIFY_STAGE=foundation bash scripts/pubsub-server-refactor-autoloop/verify.sh` passes before source movement starts.
- AC-002: `services/pubsub/server.rs` is reduced to 700 lines or fewer.
- AC-003: At least eight focused Pub/Sub source files exist beyond `server.rs` and `grpc.rs`.
- AC-004: `cargo test --workspace` passes.
- AC-005: `cargo test --workspace` passes.
- AC-006: `VERIFY_STAGE=full bash scripts/pubsub-autoloop/verify.sh` passes.
- AC-007: `rustfmt` is applied to changed Pub/Sub Rust files.
- AC-008: The refactor keeps all code in package `pubsub`.
- AC-009: Runtime loop state files are not staged.

## Done Criteria

The loop may report `DONE` only when AC-001 through AC-009 pass. If any behavior test fails, the loop must report `CONTINUE` and fix the regression instead of widening scope.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Pub/Sub server refactor loop goal is ready.
