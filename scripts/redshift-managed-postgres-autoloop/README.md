# Redshift Managed PostgreSQL Autoloop

Codex-driven autonomous loop for implementing managed PostgreSQL mode for the Redshift-compatible server.

## Usage

```bash
bash scripts/redshift-managed-postgres-autoloop/bootstrap.sh
bash scripts/redshift-managed-postgres-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=30 bash scripts/redshift-managed-postgres-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/redshift-managed-postgres-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/redshift-managed-postgres-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-managed bash scripts/redshift-managed-postgres-autoloop/run-loop.sh
VERIFY_STAGE=full-managed bash scripts/redshift-managed-postgres-autoloop/verify.sh
```

## Verification Stages

- `foundation`: script contract, design contract, and existing PostgreSQL backend migration gate.
- `managed-config`: managed-mode config and daemon contract.
- `managed-lifecycle`: local PostgreSQL process lifecycle tests and removal of the temporary not-implemented guard.
- `managed-e2e`: managed PostgreSQL E2E script contract and execution when PostgreSQL binaries are available.
- `full-managed`: all managed-mode gates plus repository tests.

Runtime files are ignored by git: `progress.md`, `state.env`, `state.env.sha256`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, prompts, lock, and circuit state.
