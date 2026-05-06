# S3 Server Refactor Autoloop

This loop refactors `internal/services/s3/server.go` into smaller files without changing S3 behavior.

## Usage

Initialize state:

```bash
bash scripts/s3-server-refactor-autoloop/bootstrap.sh
```

Run bounded iterations:

```bash
MAX_ITERATIONS=10 VERIFY_STAGE=foundation bash scripts/s3-server-refactor-autoloop/run-loop.sh
```

Before claiming completion:

```bash
VERIFY_STAGE=full bash scripts/s3-server-refactor-autoloop/verify.sh
```

## Stages

- `foundation`: validates loop contract, syntax, package tests, and current `server.go` size reporting.
- `shape`: validates the refactored file layout and `server.go` line budget.
- `s3-full`: runs the existing S3 compatibility gates.
- `full`: runs all checks.

## Constraints

- Behavior-preserving movement only.
- No feature work.
- No new dependencies.
- Keep package name `s3`.
- Do not stage loop runtime state files.
