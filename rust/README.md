# devcloud — Rust services (Go→Rust transmute)

This directory holds the in-progress **Go→Rust strangler-fig migration** of the
devcloud service packages. The Go daemon under `../internal` remains the source
of truth and the running system; each Rust crate here is a behavior-preserving,
**differential-parity-verified** reimplementation of one service, migrated one
increment at a time (leaf services first, shared-store/protocol-heavy services
last).

## Layout

```
rust/
  Cargo.toml          # workspace root (shared target/)
  services/
    mail/                    # increment #1 — SMTP (parity of internal/services/mail)
    applicationautoscaling/  # increment #2 — AWS JSON 1.1 + SigV4 (parity of
                             #   internal/services/applicationautoscaling)
    sqs/                     # increment #3 — AWS JSON 1.0 + SigV4 + FIFO + DLQ
                             #   (parity of internal/services/sqs)
```

## Migration order

Leaf → hub, per the Phase 1 dependency analysis:

1. **mail** ✅ — SMTP, no shared-store coupling
2. **applicationautoscaling** ✅ — AWS JSON 1.1 (13 ops), SigV4, state.json
3. **sqs** ✅ — AWS JSON 1.0 (23 ops), SigV4, FIFO dedup, DLQ redrive,
   visibility timeouts, move-tasks, state.json. JSON protocol only; the legacy
   Query/XML protocol stays on the Go engine.
4. dynamodb — leaf HTTP service
5. pubsub — leaf, but gRPC + REST (tonic/prost)
6. redis — passthrough proxy
7. s3 — **hub**: owns the `BucketStore` boundary
8. gcs, bigquery, redshift — depend on s3 / pgwire / managed Postgres

## Parity discipline

Each crate ports the corresponding Go test suite 1:1 and pins additional
**golden-oracle** cases captured from the Go implementation (`ParseMessage`,
protocol responses, etc.). A crate is not considered done until every ported
test and every oracle case passes.

```
cd rust && cargo test          # run all migrated crates
cd rust && cargo test -p devcloud-mail
```

## Daemon seam (mail, applicationautoscaling, sqs)

Each migrated service is wired into the Go daemon behind an **opt-in, dev-only**
environment seam — the default path and the YAML config are unchanged. When the
`DEVCLOUD_<SVC>_ENGINE=rust` variable is set, `Daemon.Run` launches the Rust
binary as a subprocess on the same `127.0.0.1:<port>` the Go server would have
used, pointed at the same storage dir; otherwise the Go server runs as before.
The Rust stores write a byte-compatible `state.json` (and, for mail, the same
JSONL + blob layout), so state survives switching engines.

```
DEVCLOUD_MAIL_ENGINE=rust DEVCLOUD_MAIL_RUST_BIN=rust/target/debug/devcloud-mail \
DEVCLOUD_AAS_ENGINE=rust  DEVCLOUD_AAS_RUST_BIN=rust/target/debug/devcloud-applicationautoscaling \
DEVCLOUD_SQS_ENGINE=rust  DEVCLOUD_SQS_RUST_BIN=rust/target/debug/devcloud-sqs \
  go run ./cmd/devcloud up
```

On Ctrl-C the subprocess gets SIGTERM (graceful) then SIGKILL after a grace
period. Known gaps: the SQS Rust engine serves only the AWS JSON 1.0 protocol
(modern SDK default) — a request without an `X-Amz-Target` (legacy Query/XML)
gets a documented `501 NotImplemented`; and the in-process `events.Bus` SSE feed
is not bridged to subprocesses (live dashboard events absent under a Rust engine;
lists still update on refresh).
