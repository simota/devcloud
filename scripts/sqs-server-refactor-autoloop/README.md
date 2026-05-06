# SQS Server Refactor Autoloop

This loop refactors `internal/services/sqs/server.go` into smaller files without changing SQS behavior.

## Usage

Initialize state:

```bash
bash scripts/sqs-server-refactor-autoloop/bootstrap.sh
```

Run bounded iterations:

```bash
MAX_ITERATIONS=10 VERIFY_STAGE=foundation bash scripts/sqs-server-refactor-autoloop/run-loop.sh
```

Before claiming completion:

```bash
VERIFY_STAGE=full bash scripts/sqs-server-refactor-autoloop/verify.sh
```

## Stages

- `foundation`: validates loop contract, syntax, package tests, and formatting.
- `shape`: validates the refactored file layout and `server.go` line budget.
- `sqs-full`: runs the existing SQS compatibility gate.
- `full`: runs all checks.

## Constraints

- Behavior-preserving movement only.
- No feature work.
- No new dependencies.
- Keep package name `sqs`.
- Do not stage loop runtime state files.
