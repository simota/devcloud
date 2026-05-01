# GCS Server Autoloop

Codex-driven autonomous loop for completing the `devcloud` Google Cloud Storage compatible server.

## Usage

```bash
bash scripts/gcs-autoloop/bootstrap.sh
bash scripts/gcs-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=30 bash scripts/gcs-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/gcs-autoloop/run-loop.sh
CODEX_ARGS="--full-auto" bash scripts/gcs-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/gcs-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full bash scripts/gcs-autoloop/run-loop.sh
VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh
```

## Verification Stages

- `foundation`: checks docs, repository build, CLI help, and current Go test suite.
- `config`: checks GCS config, daemon wiring, and workspace initialization once implemented.
- `gcs-core`: checks JSON API bucket/object CRUD, list, metadata, and range download.
- `resumable`: checks resumable upload session creation, chunk upload, and final object commit.
- `dashboard`: checks dashboard service registry and `/api/gcs/*` visibility.
- `full`: final GCS compatibility acceptance gate.

Runtime files are ignored by git: `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, and lock/circuit files.
