# BigQuery Dashboard Management Autoloop

Codex loop for upgrading the BigQuery React dashboard from read-only catalog browsing into a local management console.

## Usage

```bash
bash scripts/bigquery-dashboard-management-autoloop/bootstrap.sh
bash scripts/bigquery-dashboard-management-autoloop/run-loop.sh
```

Useful variants:

```bash
MAX_ITERATIONS=40 bash scripts/bigquery-dashboard-management-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/bigquery-dashboard-management-autoloop/run-loop.sh
VERIFY_STAGE=query-runner bash scripts/bigquery-dashboard-management-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-management-ui bash scripts/bigquery-dashboard-management-autoloop/run-loop.sh
VERIFY_STAGE=full-management-ui bash scripts/bigquery-dashboard-management-autoloop/verify.sh
```

## Stages

- `foundation`: loop scripts and existing BigQuery full compatibility gate.
- `query-runner`: SQL query runner and result table.
- `management`: create dataset/table and insert row flows.
- `jobs-validation`: job detail, recent query metadata, and JSON validation.
- `e2e-docs`: tests/E2E and docs.
- `full-management-ui`: all gates plus `cargo test --workspace`.

Runtime files such as `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `done.md`, and `iteration-*.out` are ignored by git.
