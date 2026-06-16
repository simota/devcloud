# Repository Guidelines

## Project Structure & Module Organization

`orchestrator` contains the main CLI and in-process supervisor (`init`, `up`, `reset`, `dashboard`). Runtime service crates live under `services/*`, including the local dashboard and every provider-compatible service. The React dashboard source lives in `web/dashboard/`, and its built assets are embedded from `services/dashboard/assets/react`. Product and UI designs are in `docs/`. Design mocks live in `mock/mail` and `mock/s3`; these are reference artifacts, not production runtime code. Automation and acceptance gates live in `scripts/`, including `mail-autoloop` and `s3-autoloop`.

## Build, Test, and Development Commands

- `cargo test --workspace`: run all Rust unit and integration tests.
- `cargo run -p devcloud-orchestrator -- help`: verify CLI command routing.
- `cargo run -p devcloud-orchestrator -- up`: start local services using `.devcloud/config.yaml`.
- `cargo build -p devcloud-orchestrator`: build the CLI binary.
- `VERIFY_STAGE=full bash scripts/mail-autoloop/verify.sh`: run the Mail MVP acceptance gate.
- `VERIFY_STAGE=foundation bash scripts/s3-autoloop/verify.sh`: run the current S3 foundation gate.
- `scripts/mail-e2e.sh`: run the Mail browser/API smoke test.

For mocks, run commands inside the mock directory, for example `cd mock/s3 && npm install && npm run build`.

## Coding Style & Naming Conventions

Use idiomatic Rust with `cargo fmt` formatting and standard-library-first implementations where practical. Keep crate/module names short and lowercase (`mail`, `dashboard`, `s3`). Tests should sit beside the module or under the crate's `tests/` directory and use clear behavior names. Keep runtime data under `.devcloud/` and avoid adding production dependencies unless justified.

## Testing Guidelines

Add focused Rust tests for new behavior and keep `cargo test --workspace` passing. Acceptance scripts should remain deterministic and stage-based. Use `VERIFY_STAGE=foundation` for fast checks and `VERIFY_STAGE=full` before claiming a service MVP is complete.

## Commit & Pull Request Guidelines

Follow the existing Conventional Commit style: `feat(mail): ...`, `docs(s3): ...`, `test: ...`, `fix: ...`. Keep commits scoped and avoid mixing generated loop state with source changes. PRs should include a concise summary, verification commands run, linked issue or design reference when relevant, and screenshots for Web UI changes.

## Security & Configuration Tips

Do not log credentials, authorization headers, signatures, message bodies, or object payloads. Keep generated state files such as `runner.log`, `state.env`, and `.devcloud/` out of commits. The production dashboard is served by the Rust dashboard crate from embedded Vite assets.
