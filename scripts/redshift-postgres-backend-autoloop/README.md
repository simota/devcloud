# Redshift PostgreSQL Backend Autoloop

Codex-driven autonomous loop for moving the Redshift-compatible server from the local memory executor to a PostgreSQL-backed execution engine.

## Usage

```bash
bash scripts/redshift-postgres-backend-autoloop/bootstrap.sh
bash scripts/redshift-postgres-backend-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=40 bash scripts/redshift-postgres-backend-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/redshift-postgres-backend-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/redshift-postgres-backend-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/run-loop.sh
VERIFY_STAGE=full-postgres bash scripts/redshift-postgres-backend-autoloop/verify.sh
```

## Verification Stages

- `foundation`: checks the Redshift MVP gate, updated design contract, and script contract.
- `backend-interface`: checks `SQLBackend` / memory fallback boundaries.
- `postgres-backend`: checks PostgreSQL backend package, config, lifecycle, transaction, result, and error mapping.
- `translator`: checks Redshift table attribute extraction and function rewrite contracts.
- `copy-unload`: checks COPY / UNLOAD through the side-effect router into/out of PostgreSQL.
- `system-views`: checks `pg_catalog`, `information_schema`, `stl`, `stv`, and `svv` compatibility on PostgreSQL-backed metadata.
- `dashboard`: checks dashboard/backend-mode visibility and existing Redshift dashboard smoke.
- `e2e`: checks PostgreSQL backend E2E with SQL endpoint, Data API, management API, COPY/UNLOAD, and dashboard smoke.
- `full-postgres`: runs all PostgreSQL backend migration gates plus `cargo test --workspace`.

Runtime files are ignored by git: `progress.md`, `state.env`, `state.env.sha256`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, prompts, lock, and circuit state.
