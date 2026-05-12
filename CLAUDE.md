# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`devcloud` is a local cloud service emulator: a single Go binary that runs compatible development endpoints for Mail (SMTP), S3, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, and Redshift, plus a React dashboard. It targets deterministic local tests and manual inspection — not production parity. New provider behavior should be added deliberately, backed by tests in the relevant package and (usually) an acceptance gate under `scripts/*-autoloop/`.

## Commands

### Go build / test
- `go test ./...` — run all unit/integration tests. Required to pass before claiming work is done.
- `go test ./internal/services/s3/...` — narrow test runs by package; use `-run TestName` for a single test.
- `go run ./cmd/devcloud init` — write `.devcloud/config.yaml` with default ports/auth/services.
- `go run ./cmd/devcloud up` — start all enabled services + dashboard (Ctrl-C to stop).
- `go run ./cmd/devcloud reset` — wipe `.devcloud/data` for the configured workspace.
- `go build -o /tmp/devcloud ./cmd/devcloud` — build the CLI binary.

### Dashboard (React)
The dashboard lives in `web/dashboard/` (Vite + React 18 + TS) and is embedded into the Go binary at `internal/dashboard/assets/react` via `//go:embed`.
- `cd web/dashboard && npm install`
- `npm run dev` — Vite dev server (proxies `/api` to a running `devcloud up` on `:8025`).
- `npm run build` — typecheck and emit static assets into `internal/dashboard/assets/react/`. Run before testing Go changes that depend on the latest UI bundle.
- `npm run typecheck` — TypeScript check only.

### Acceptance gates (per-service)
Each service has a bounded autoloop folder under `scripts/<service>-autoloop/`. The relevant entry points are:
- `VERIFY_STAGE=full bash scripts/<service>-autoloop/verify.sh` — final acceptance gate for that service (`mail`, `s3`, `gcs`, `dynamodb`, `bigquery`, `sqs`, `pubsub`). Stages such as `foundation`, `<svc>-core`, `dashboard-static`, `hardening` exist for faster partial checks.
- SDK / advanced gates: `scripts/gcs-sdk-compat-autoloop`, `scripts/bigquery-sdk-compat-autoloop`, `scripts/pubsub-full-compat-autoloop`, `scripts/redshift-advanced-compat-autoloop`.
- The autoloop folders also contain runner state (`progress.md`, `state.env`, `runner.log`). Treat these as generated; do not mix them with source commits.

### End-to-end smoke
`scripts/<service>-e2e.sh` boots `devcloud up`, exercises the service, and tears down. Useful env vars: `E2E_INTERACTIVE=true` keeps the daemon running for browser inspection; `E2E_DELETE_DATA=false` preserves storage; `E2E_<SVC>_PORT` / `E2E_DASHBOARD_PORT` override defaults when the standard ports are busy.

## Architecture

### Single Go module, one daemon process
`go.mod` declares one module (`devcloud`, Go 1.22). The CLI in `cmd/devcloud/main.go` dispatches to `internal/app`. `internal/app/daemon.go` is the central wiring point: it constructs every service server, the dashboard server, and any shared stores, then launches each enabled server on its own goroutine and fans errors back through a single channel. Any new service must be plumbed through `Daemon.Run`, `enabledServerCount`, and `Config` (see `internal/app/config.go`).

### Service packages
Each provider lives under `internal/services/<svc>/` and exposes a `NewServer(Config) *Server` constructor plus a `Run(ctx) error` method. Files are split by concern (e.g., `server.go`, `routes.go`, `*_handlers.go`, `store*.go`, `types.go`, `responses.go`, `<feature>_test.go`). Cross-service contracts:
- **Object stores are shared.** S3 and GCS both use `s3svc.BucketStore` backed by `s3svc.NewFileBucketStore` on disk. BigQuery `jobs.insert` load/extract and Redshift `COPY`/`UNLOAD` accept the same store to read/write `gs://` and `s3://` URIs locally without leaving the process.
- **Redshift backend is pluggable.** `internal/services/redshift/backend` defines `SQLBackend`; implementations live in `backend/memory` and `backend/postgres`. The default `managed` mode starts a local PostgreSQL under `.devcloud/data/redshift` (`internal/app/managed_postgres.go`); `external` mode connects to a user-supplied DSN; `memory` is a development fallback. Redshift SQL is rewritten before execution via `internal/services/redshift/translator`.
- **Pub/Sub serves both gRPC and REST** from one `Server` (different addrs). The gRPC and REST handlers share the in-memory state (`topics`, `subscriptions`, `messages`, `deliveries`); persistence lives in `persistence.go` rooted at `StoragePath`/`MessageStoragePath`.

### Dashboard
`internal/dashboard` is the only HTTP entry point users hit at `:8025`. It serves:
- The React SPA from `assets/react` (embedded). `assets.go` mounts `/dashboard/` and falls back to `index.html` for client-side routes.
- A set of `/api/*` JSON endpoints (`mail_handlers.go`, `s3_handlers.go`, `dynamodb_handlers.go`, `bigquery_handlers.go`, `sqs_handlers.go`, `pubsub_handlers.go`, `redshift_handlers.go`, plus the service registry in `services.go`).
- **Safety rule:** dashboard mutations MUST go through the provider-protocol path (`/api/<svc>/*` forwarding into the in-process service) — never directly through storage. Never log credentials, Authorization headers, signatures, message bodies, or object payloads. See `AGENTS.md` and the per-service notes in `README.md`.

### Auth modes
Every service supports a `relaxed` mode (default, used by all local tooling) and a stricter mode that validates configured credentials. Relaxed mode is what tests and autoloops assume; if you add credential checks, gate them on the mode string so the existing scripts still pass.

### Configuration & storage
Config lives at `.devcloud/config.yaml` (custom YAML-ish parser in `internal/app/config.go`); defaults come from `app.DefaultConfig()`. Runtime data is rooted at `Storage.Path` (default `.devcloud/data`) with per-service subdirectories (`mail`, `s3`, `dynamodb`, `bigquery`, `sqs`, `pubsub`, `redshift`, `gcs/upload_sessions`). `.devcloud/` is gitignored and must not be committed.

## Conventions

- Idiomatic Go, `gofmt`, standard-library-first. Few external dependencies (only `cloud.google.com/go/pubsub`, gRPC, protobuf currently).
- Package names: short, lowercase (`mail`, `s3`, `pubsub`, `dashboard`, `blob`).
- Tests sit next to the package they cover. Behavior-named tests (e.g., `TestSMTPRejectsOversizeMessage`). Service packages additionally have category-split test files (`object_test.go`, `multipart_test.go`, `versioning_test.go`, etc.) — keep new tests in the matching category file rather than a monolithic `server_test.go`.
- Conventional commit style: `feat(s3): ...`, `fix(pubsub): ...`, `refactor(bigquery): ...`, `test: ...`, `docs: ...`. Do NOT add Claude Code signatures or `Co-Authored-By` lines.
- Do not commit `runner.log`, `state.env`, `progress.md`, or anything under `.devcloud/`.
