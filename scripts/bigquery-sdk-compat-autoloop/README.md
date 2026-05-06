# BigQuery SDK Compatibility Autoloop

Autonomous loop for adding Google BigQuery SDK compatibility E2E coverage.

## Run

```bash
VERIFY_STAGE=foundation bash scripts/bigquery-sdk-compat-autoloop/verify.sh
MAX_ITERATIONS=20 VERIFY_STAGE=foundation DONE_VERIFY_STAGE=full-sdk-compat bash scripts/bigquery-sdk-compat-autoloop/run-loop.sh
```

## Stages

- `foundation`: loop contract and script syntax.
- `sdk-go`: Go BigQuery client E2E contract.
- `sdk-e2e`: runnable SDK E2E script contract.
- `compat-docs`: README/docs mention SDK compatibility workflow.
- `full-sdk-compat`: all stages plus existing BigQuery gates and repository tests.

## Expected Output

The loop should converge by adding a deterministic local BigQuery SDK E2E path using `cloud.google.com/go/bigquery` with endpoint override only.
