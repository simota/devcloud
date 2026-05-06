# BigQuery Server Refactor Autoloop

This loop refactors `internal/services/bigquery/server.go` into smaller files without changing BigQuery behavior.

## Usage

Initialize state:

```bash
bash scripts/bigquery-server-refactor-autoloop/bootstrap.sh
```

Run bounded iterations:

```bash
MAX_ITERATIONS=10 VERIFY_STAGE=foundation bash scripts/bigquery-server-refactor-autoloop/run-loop.sh
```

Before claiming completion:

```bash
VERIFY_STAGE=full bash scripts/bigquery-server-refactor-autoloop/verify.sh
```

## Stages

- `foundation`: validates loop contract, syntax, package tests, and formatting.
- `shape`: validates the refactored file layout and `server.go` line budget.
- `bigquery-full`: runs the existing BigQuery compatibility gate.
- `full`: runs all checks.

## Constraints

- Behavior-preserving movement only.
- No feature work.
- No new dependencies.
- Keep package name `bigquery`.
- Do not stage loop runtime state files.
