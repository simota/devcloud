# Amazon Redshift Server Autoloop

Codex-driven autonomous loop for completing the `devcloud` Amazon Redshift compatible local server.

## Usage

```bash
bash scripts/redshift-autoloop/bootstrap.sh
bash scripts/redshift-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=50 bash scripts/redshift-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/redshift-autoloop/run-loop.sh
CODEX_ARGS="--full-auto" bash scripts/redshift-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/redshift-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full bash scripts/redshift-autoloop/run-loop.sh
VERIFY_STAGE=full bash scripts/redshift-autoloop/verify.sh
scripts/redshift-e2e.sh
```

## Verification Stages

- `foundation`: checks the Redshift design contract, runner contract, repository build, CLI help, and current Go test suite.
- `config`: checks Redshift config shape, defaults, and workspace initialization once implemented.
- `pgwire`: checks SQL listener startup and `psql select 1` smoke when `psql` is available.
- `sql-core`: checks CREATE SCHEMA/TABLE, INSERT, SELECT, and basic catalog behavior.
- `postgres-backend`: checks PostgreSQL backend lifecycle, transactions, result mapping, and error mapping.
- `translator`: checks Redshift table attribute extraction and function rewrites before backend execution.
- `data-api`: checks Redshift Data API statement lifecycle and result retrieval.
- `management`: checks Redshift management API cluster metadata shape.
- `copy-unload`: checks COPY from local S3 and UNLOAD to local S3 once implemented.
- `dashboard`: checks dashboard service registry, `/dashboard/redshift`, and `/api/redshift/*` visibility.
- `e2e`: runs `scripts/redshift-e2e.sh`.
- `full`: final Redshift compatibility acceptance gate.

Runtime files are ignored by git: `progress.md`, `state.env`, `state.env.sha256`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, prompts, lock, and circuit state.
