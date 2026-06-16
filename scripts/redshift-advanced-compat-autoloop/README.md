# Redshift Advanced Compatibility Autoloop

Codex-driven loop for Redshift advanced compatibility after the PostgreSQL-backed MVP, managed backend, default backend, and dashboard query runner are complete.

## Usage

```bash
bash scripts/redshift-advanced-compat-autoloop/bootstrap.sh
bash scripts/redshift-advanced-compat-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=60 bash scripts/redshift-advanced-compat-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/redshift-advanced-compat-autoloop/run-loop.sh
VERIFY_STAGE=extended-protocol bash scripts/redshift-advanced-compat-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-advanced bash scripts/redshift-advanced-compat-autoloop/run-loop.sh
VERIFY_STAGE=full-advanced bash scripts/redshift-advanced-compat-autoloop/verify.sh
```

## Verification Stages

- `foundation`: loop contract and existing completed Redshift full gates.
- `extended-protocol`: Parse/Bind/Describe/Execute/Sync/Close protocol support and tests.
- `sql-advanced`: CTAS, views, materialized-view metadata, UPDATE, DELETE, and MERGE tests.
- `serverless`: Redshift Serverless namespace/workgroup metadata API tests.
- `snapshots`: snapshot create/describe/delete/restore metadata tests.
- `introspection`: WLM/workload metadata, catalog, and BI-style probe tests.
- `procedures`: stored procedure/UDF metadata and limited execution/safe unsupported tests.
- `dashboard-e2e`: dashboard and E2E coverage for advanced metadata.
- `full-advanced`: all stages plus `cargo test --workspace`.

Runtime files are ignored by git: `progress.md`, `state.env`, `state.env.sha256`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, prompts, lock, and circuit state.
