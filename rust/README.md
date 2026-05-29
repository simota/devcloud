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
```

## Migration order

Leaf → hub, per the Phase 1 dependency analysis:

1. **mail** ✅ — SMTP, no shared-store coupling
2. **applicationautoscaling** ✅ — AWS JSON 1.1 (13 ops), SigV4, state.json
3. sqs, dynamodb — leaf HTTP services
3. pubsub — leaf, but gRPC + REST (tonic/prost)
4. redis — passthrough proxy
5. s3 — **hub**: owns the `BucketStore` boundary
6. gcs, bigquery, redshift — depend on s3 / pgwire / managed Postgres

## Parity discipline

Each crate ports the corresponding Go test suite 1:1 and pins additional
**golden-oracle** cases captured from the Go implementation (`ParseMessage`,
protocol responses, etc.). A crate is not considered done until every ported
test and every oracle case passes.

```
cd rust && cargo test          # run all migrated crates
cd rust && cargo test -p devcloud-mail
```

The Rust binaries are **not yet wired into the daemon seam** — that flip
(launching a Rust service as a subprocess on the same `127.0.0.1:<port>` and
proxying the dashboard) is a later increment, gated on parity being airtight.
