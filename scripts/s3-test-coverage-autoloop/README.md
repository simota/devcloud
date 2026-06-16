# S3 Test Coverage Autoloop

Codex loop for strengthening S3 service, dashboard API, and E2E tests without changing product behavior.

## Usage

```bash
bash scripts/s3-test-coverage-autoloop/bootstrap.sh
bash scripts/s3-test-coverage-autoloop/run-loop.sh
```

Useful variants:

```bash
MAX_ITERATIONS=40 bash scripts/s3-test-coverage-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/s3-test-coverage-autoloop/run-loop.sh
VERIFY_STAGE=service-edge bash scripts/s3-test-coverage-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-test-coverage bash scripts/s3-test-coverage-autoloop/run-loop.sh
VERIFY_STAGE=full-test-coverage bash scripts/s3-test-coverage-autoloop/verify.sh
```

## Stages

- `foundation`: loop scripts, existing S3 compatibility gate, and S3 dashboard management gate.
- `service-edge`: S3 service edge/error tests for bucket/object/multipart/presign behavior.
- `dashboard-api`: dashboard S3 API tests for validation, disabled state, mutation safety, and metadata.
- `e2e-docs`: E2E/docs coverage for S3 management workflows and test intent.
- `coverage`: coverage thresholds for S3 service and dashboard packages.
- `full-test-coverage`: all gates plus `cargo test --workspace`.

Runtime files such as `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `done.md`, and `iteration-*.out` are ignored by git.
