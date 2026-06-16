# ROADMAP - Rust Workspace

> Status: **COMPLETE**.
> Revised: 2026-06-16.

## Current Source Of Truth

devcloud is a root-level Rust workspace. The production runtime is built from
`Cargo.toml`, `orchestrator/`, and `services/**`.

The runtime shape is:

- `orchestrator/`: CLI, config loading, supervisor, managed service lifecycle, and service wiring.
- `services/<svc>/`: local cloud-compatible service crates.
- `services/dashboard/`: Rust-served dashboard API and embedded React assets.
- `web/dashboard/`: React/Vite dashboard source.
- `scripts/*-e2e.sh` and `scripts/*-autoloop/verify.sh`: acceptance gates.

## Completion Criteria

- [x] Root `Cargo.toml` is the workspace source of truth.
- [x] `cargo build --workspace` and `cargo test --workspace` are the default build/test gates.
- [x] All service implementations live under Rust crates.
- [x] Dashboard runtime is served from the Rust dashboard crate.
- [x] Acceptance and e2e scripts build and launch the Rust orchestrator.
- [x] Documentation and scripts describe the repository as Rust-first.

## Runtime Components

| Area | Rust implementation | Location |
|---|---|---|
| CLI and supervisor | `devcloud-orchestrator` | `orchestrator/src/main.rs`, `orchestrator/src/supervisor.rs` |
| Config parser | Rust config loader and writer | `orchestrator/src/config.rs` |
| Managed PostgreSQL | Redshift managed PostgreSQL lifecycle | `orchestrator/src/services/managed_postgres.rs` |
| Managed Redis | Redis managed lifecycle | `orchestrator/src/services/redis.rs` |
| Event relay and control | Rust event/control services | `services/event-relay`, `services/redis-control` |
| Dashboard | Rust API server with embedded React assets | `services/dashboard` |
| Service crates | Mail, S3, GCS, DynamoDB, BigQuery, SQS, Pub/Sub, Redshift, Redis | `services/**` |

## Verification Surface

Use these commands as the normal local gates:

```sh
cargo fmt --all
cargo test --workspace
cargo build --workspace
bash -n scripts/*-e2e.sh scripts/*-autoloop/verify.sh scripts/lib/devcloud-engine.sh
```

Service-specific acceptance gates remain under `scripts/` and should be run with the
appropriate `VERIFY_STAGE` when touching that service.

## Repository Layout Policy

- Keep runtime Rust code in `orchestrator/` and `services/**`.
- Keep dashboard frontend source in `web/dashboard/`.
- Keep embedded dashboard build artifacts under `services/dashboard/assets/react`.
- Keep generated local runtime state under `.devcloud/`.
- Treat generated autoloop transcripts as historical logs, not current implementation guidance.

## Notes

Completed loop outputs under `scripts/**` may describe older task contracts. They should not
override this file, `README.md`, `AGENTS.md`, or `CLAUDE.md` when deciding how to build,
test, or extend the repository.
