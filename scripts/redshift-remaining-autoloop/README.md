# Redshift Remaining Tasks Autoloop

Codex-driven loop for finishing Redshift PostgreSQL default-backend work and documenting the remaining advanced compatibility backlog.

## Usage

```bash
bash scripts/redshift-remaining-autoloop/bootstrap.sh
bash scripts/redshift-remaining-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=20 bash scripts/redshift-remaining-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/redshift-remaining-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/redshift-remaining-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-remaining bash scripts/redshift-remaining-autoloop/run-loop.sh
VERIFY_STAGE=full-remaining bash scripts/redshift-remaining-autoloop/verify.sh
```

## Verification Stages

- `foundation`: script contract and existing Redshift PostgreSQL/managed gates.
- `default-backend`: checks default config and tests for `postgres` default plus explicit `memory` fallback.
- `docs`: checks design docs reflect managed PostgreSQL strategy and migration status.
- `full-remaining`: default-backend + docs + Redshift full gate + full-postgres + full-managed + `go test ./...`.

Runtime files are ignored by git: `progress.md`, `state.env`, `state.env.sha256`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, prompts, lock, and circuit state.
