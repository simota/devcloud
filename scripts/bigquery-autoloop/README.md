# BigQuery Server Autoloop

Codex-driven autonomous loop for completing the `devcloud` Google BigQuery REST v2 compatible local server.

## Usage

```bash
bash scripts/bigquery-autoloop/bootstrap.sh
bash scripts/bigquery-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=40 bash scripts/bigquery-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/bigquery-autoloop/run-loop.sh
CODEX_ARGS="--full-auto" bash scripts/bigquery-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/bigquery-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full bash scripts/bigquery-autoloop/run-loop.sh
VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh
```

## Verification Stages

- `foundation`: checks the BigQuery design contract, repository build, CLI help, and current Go test suite.
- `config`: checks BigQuery config, daemon wiring, and workspace initialization once implemented.
- `server`: checks BigQuery REST v2 endpoint startup and project discovery.
- `catalog`: checks dataset and table CRUD.
- `tabledata`: checks `tabledata.insertAll` and `tabledata.list` for persisted rows.
- `query`: checks `jobs.query` and `jobs.getQueryResults` for the GoogleSQL subset.
- `dashboard`: checks dashboard service registry and `/api/bigquery/*` visibility.
- `full`: final BigQuery compatibility acceptance gate.

Runtime files are ignored by git: `progress.md`, `state.env`, `state.env.sha256`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, and lock/circuit files.
