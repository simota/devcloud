# S3 Dashboard Management Autoloop

Codex loop for upgrading the S3 React dashboard from object browsing into a safer local management console.

## Usage

```bash
bash scripts/s3-dashboard-management-autoloop/bootstrap.sh
bash scripts/s3-dashboard-management-autoloop/run-loop.sh
```

Useful variants:

```bash
MAX_ITERATIONS=40 bash scripts/s3-dashboard-management-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/s3-dashboard-management-autoloop/run-loop.sh
VERIFY_STAGE=bucket-object-management bash scripts/s3-dashboard-management-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-management-ui bash scripts/s3-dashboard-management-autoloop/run-loop.sh
VERIFY_STAGE=full-management-ui bash scripts/s3-dashboard-management-autoloop/verify.sh
```

## Stages

- `foundation`: loop scripts and existing S3 full compatibility gate.
- `bucket-object-management`: typed dashboard API helpers and guarded bucket/object create-delete flows.
- `upload-copy-download`: upload, copy, object detail, metadata, and safe download flows.
- `multipart-presign`: multipart upload visibility/actions and presigned URL helper coverage.
- `e2e-docs`: dashboard tests or E2E plus docs.
- `full-management-ui`: all gates plus `cargo test --workspace`.

Runtime files such as `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `done.md`, and `iteration-*.out` are ignored by git.
