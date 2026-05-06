# GCS SDK Compatibility Autoloop

Autonomous loop for adding Google Cloud Storage SDK compatibility E2E coverage.

## Run

```bash
VERIFY_STAGE=foundation bash scripts/gcs-sdk-compat-autoloop/verify.sh
MAX_ITERATIONS=20 VERIFY_STAGE=foundation DONE_VERIFY_STAGE=full-sdk-compat bash scripts/gcs-sdk-compat-autoloop/run-loop.sh
```

## Stages

- `foundation`: loop contract and script syntax.
- `sdk-go`: Go Storage client E2E contract.
- `sdk-e2e`: runnable SDK E2E script contract.
- `compat-docs`: README/docs mention SDK compatibility workflow.
- `full-sdk-compat`: all stages plus repository tests.

## Expected Output

The loop should converge by adding a deterministic local GCS SDK E2E path using `cloud.google.com/go/storage` with endpoint override only.
