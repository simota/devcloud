# GCS Test Coverage Autoloop

Codex loop for strengthening GCS service, dashboard API, and E2E tests without changing product behavior.

## Usage

```bash
bash scripts/gcs-test-coverage-autoloop/bootstrap.sh
bash scripts/gcs-test-coverage-autoloop/run-loop.sh
```

Useful variants:

```bash
MAX_ITERATIONS=40 bash scripts/gcs-test-coverage-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/gcs-test-coverage-autoloop/run-loop.sh
VERIFY_STAGE=service-edge bash scripts/gcs-test-coverage-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-test-coverage bash scripts/gcs-test-coverage-autoloop/run-loop.sh
VERIFY_STAGE=full-test-coverage bash scripts/gcs-test-coverage-autoloop/verify.sh
```

## Stages

- `foundation`: loop scripts, existing GCS compatibility gate, and GCS React dashboard gate.
- `service-edge`: GCS service edge/error tests for bucket, object, metadata, generation, and upload session behavior.
- `dashboard-api`: dashboard GCS API tests for validation, disabled state, mutation safety, metadata, and upload sessions.
- `e2e-docs`: E2E/docs coverage for GCS management workflows and test intent.
- `coverage`: coverage thresholds for GCS service and dashboard packages.
- `full-test-coverage`: all gates plus `cargo test --workspace`.

Runtime files such as `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `done.md`, and `iteration-*.out` are ignored by git.
