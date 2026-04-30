# Repository Guidelines

## Project Structure & Module Organization

`cmd/devcloud` contains the main CLI (`init`, `up`, `reset`, `dashboard`), and `cmd/devcloudd` contains daemon entry-point wiring. Core Go packages live under `internal/`: `app` for config and daemon orchestration, `dashboard` for the local Web UI/API, `services/mail` for SMTP behavior, and `storage/*` for filesystem-backed persistence. Product and UI designs are in `docs/`. Design mocks live in `mock/mail` and `mock/s3`; these are reference artifacts, not production runtime code. Automation and acceptance gates live in `scripts/`, including `mail-autoloop` and `s3-autoloop`.

## Build, Test, and Development Commands

- `go test ./...`: run all Go unit and integration tests.
- `go run ./cmd/devcloud help`: verify CLI command routing.
- `go run ./cmd/devcloud up`: start local services using `.devcloud/config.yaml`.
- `go build -o /tmp/devcloud ./cmd/devcloud`: build the CLI binary.
- `VERIFY_STAGE=full bash scripts/mail-autoloop/verify.sh`: run the Mail MVP acceptance gate.
- `VERIFY_STAGE=foundation bash scripts/s3-autoloop/verify.sh`: run the current S3 foundation gate.
- `scripts/mail-e2e.sh`: run the Mail browser/API smoke test.

For mocks, run commands inside the mock directory, for example `cd mock/s3 && npm install && npm run build`.

## Coding Style & Naming Conventions

Use idiomatic Go with `gofmt` formatting and standard-library-first implementations. Keep package names short and lowercase (`mail`, `dashboard`, `blob`). Test files should sit beside the package they cover and use clear behavior names such as `TestLoadConfig` or `TestSMTPRejectsOversizeMessage`. Keep runtime data under `.devcloud/` and avoid adding production dependencies unless justified.

## Testing Guidelines

Add focused Go tests for new behavior and keep `go test ./...` passing. Acceptance scripts should remain deterministic and stage-based. Use `VERIFY_STAGE=foundation` for fast checks and `VERIFY_STAGE=full` before claiming a service MVP is complete.

## Commit & Pull Request Guidelines

Follow the existing Conventional Commit style: `feat(mail): ...`, `docs(s3): ...`, `test: ...`, `fix: ...`. Keep commits scoped and avoid mixing generated loop state with source changes. PRs should include a concise summary, verification commands run, linked issue or design reference when relevant, and screenshots for Web UI changes.

## Security & Configuration Tips

Do not log credentials, authorization headers, signatures, message bodies, or object payloads. Keep generated state files such as `runner.log`, `state.env`, and `.devcloud/` out of commits. Production dashboard code should remain Go-served static HTML/CSS/JS unless an architecture decision changes that.
