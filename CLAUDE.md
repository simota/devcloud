# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`devcloud` is a local cloud service emulator: a single Rust binary that runs compatible development endpoints for Mail (SMTP), S3, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, Redshift, Redis, and a React dashboard. It targets deterministic local tests and manual inspection — not production parity. New provider behavior should be added deliberately, backed by tests in the relevant Rust crate and usually an acceptance gate under `scripts/*-autoloop/`.

## Commands

### Rust build / test
- `cargo test --workspace` — run all unit/integration tests. Required to pass before claiming work is done.
- `cargo test -p devcloud-s3` — narrow test runs by crate.
- `cargo run -p devcloud-orchestrator -- init` — write `.devcloud/config.yaml` with default ports/auth/services.
- `cargo run -p devcloud-orchestrator -- up` — start all enabled services + dashboard (Ctrl-C to stop).
- `cargo run -p devcloud-orchestrator -- reset` — wipe `.devcloud/data` for the configured workspace.
- `cargo build -p devcloud-orchestrator` — build the CLI binary.

### Dashboard (React)
The dashboard lives in `web/dashboard/` (Vite + React 18 + TS) and is embedded into the Rust dashboard crate at `services/dashboard/assets/react` via `include_dir`.
- `cd web/dashboard && npm install`
- `npm run dev` — Vite dev server (proxies `/api` to a running `devcloud up` on `:8025`).
- `npm run build` — typecheck and emit static assets into `services/dashboard/assets/react/`. Run before testing Rust dashboard changes that depend on the latest UI bundle.
- `npm run typecheck` — TypeScript check only.

### Acceptance gates (per-service)
Each service has a bounded autoloop folder under `scripts/<service>-autoloop/`. The relevant entry points are:
- `VERIFY_STAGE=full bash scripts/<service>-autoloop/verify.sh` — final acceptance gate for that service (`mail`, `s3`, `gcs`, `dynamodb`, `bigquery`, `sqs`, `pubsub`). Stages such as `foundation`, `<svc>-core`, `dashboard-static`, `hardening` exist for faster partial checks.
- SDK / advanced gates: `scripts/gcs-sdk-compat-autoloop`, `scripts/bigquery-sdk-compat-autoloop`, `scripts/pubsub-full-compat-autoloop`, `scripts/redshift-advanced-compat-autoloop`.
- The autoloop folders also contain runner state (`progress.md`, `state.env`, `runner.log`). Treat these as generated; do not mix them with source commits.

### End-to-end smoke
`scripts/<service>-e2e.sh` boots `devcloud up`, exercises the service, and tears down. Useful env vars: `E2E_INTERACTIVE=true` keeps the daemon running for browser inspection; `E2E_DELETE_DATA=false` preserves storage; `E2E_<SVC>_PORT` / `E2E_DASHBOARD_PORT` override defaults when the standard ports are busy.

## Architecture

### Single Rust workspace, one orchestrator process
`Cargo.toml` declares the workspace. The `devcloud-orchestrator` crate owns the CLI and supervisor; it links service crates directly and runs enabled services as tokio tasks inside one process. Any new service must be wired through `orchestrator/src/config.rs`, `orchestrator/src/services`, and `orchestrator/src/supervisor.rs`.

### Service packages
Each provider lives under `services/<svc>/` and exposes crate-local config/server APIs used by the orchestrator. Files are split by concern (`server.rs`, `http.rs`, protocol handlers, stores, types, and focused tests). Cross-service contracts:
- **Object stores are shared.** S3 and GCS use the Rust S3 file-backed object store. BigQuery load/extract and Redshift `COPY`/`UNLOAD` read/write local `gs://` and `s3://` URIs without leaving the process.
- **Redshift backend is pluggable.** `services/redshift/src/backend.rs` defines `SqlBackend`; memory and PostgreSQL implementations live in `backend_memory.rs` and `backend_postgres.rs`. Managed PostgreSQL lifecycle lives in the Rust orchestrator.
- **Pub/Sub serves both gRPC and REST** from the Rust Pub/Sub crate. The gRPC and REST handlers share in-memory state and persistence.

### Dashboard
`services/dashboard` is the HTTP entry point users hit at `:8025`. It serves:
- The React SPA from `assets/react` (embedded). `assets.rs` mounts `/dashboard/` and falls back to `index.html` for client-side routes.
- A set of `/api/*` JSON endpoints that forward to each service's introspection/control or provider-protocol surface.
- **Route convention:** every service page lives under `/dashboard/<svc>` (`mail`, `s3`, `gcs`, `dynamodb`, `bigquery`, `sqs`, `pubsub`, `redshift`). The compatibility short paths `/mail`, `/s3`, `/gcs`, `/dynamodb`, `/bigquery` return 301 redirects to their `/dashboard/<svc>` counterpart — never add new functionality to the compatibility redirects.
- **Safety rule:** dashboard mutations MUST go through the provider-protocol path (`/api/<svc>/*` forwarding into the in-process service) — never directly through storage. Never log credentials, Authorization headers, signatures, message bodies, or object payloads. See `AGENTS.md` and the per-service notes in `README.md`.

### Auth modes
Every service supports a `relaxed` mode (default, used by all local tooling) and a stricter mode that validates configured credentials. Relaxed mode is what tests and autoloops assume; if you add credential checks, gate them on the mode string so the existing scripts still pass.

### Configuration & storage
Config lives at `.devcloud/config.yaml` (custom YAML-ish parser in `orchestrator/src/config.rs`). Runtime data is rooted at `Storage.Path` (default `.devcloud/data`) with per-service subdirectories (`mail`, `s3`, `dynamodb`, `bigquery`, `sqs`, `pubsub`, `redshift`, `gcs/upload_sessions`). `.devcloud/` is gitignored and must not be committed.

## Conventions

- Idiomatic Rust, `cargo fmt`, standard-library-first where practical.
- Crate/module names: short, lowercase (`mail`, `s3`, `pubsub`, `dashboard`).
- Tests sit next to the module they cover or in the crate's `tests/` directory. Keep new tests in the matching category file rather than a monolithic catch-all.
- Conventional commit style: `feat(s3): ...`, `fix(pubsub): ...`, `refactor(bigquery): ...`, `test: ...`, `docs: ...`. Do NOT add Claude Code signatures or `Co-Authored-By` lines.
- Do not commit `runner.log`, `state.env`, `progress.md`, or anything under `.devcloud/`.
