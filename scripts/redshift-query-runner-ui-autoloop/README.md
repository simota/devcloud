# Redshift Query Runner UI Autoloop

Codex-driven loop for adding an interactive SQL query runner to the Redshift dashboard.

## Usage

```bash
bash scripts/redshift-query-runner-ui-autoloop/bootstrap.sh
bash scripts/redshift-query-runner-ui-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=20 bash scripts/redshift-query-runner-ui-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/redshift-query-runner-ui-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/redshift-query-runner-ui-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-query-runner-ui bash scripts/redshift-query-runner-ui-autoloop/run-loop.sh
VERIFY_STAGE=full-query-runner-ui bash scripts/redshift-query-runner-ui-autoloop/verify.sh
```

## Verification Stages

- `foundation`: loop contract, existing Redshift dashboard/query backend tests, and current full-remaining gate.
- `frontend`: API wrapper/types, React UI query runner strings/control markers, and dashboard typecheck/build.
- `dashboard-api`: focused Go dashboard query runner contract tests.
- `e2e`: Redshift E2E dashboard query runner API journey.
- `full-query-runner-ui`: all query runner UI gates plus `go test ./...`.

Runtime files are ignored by git: `progress.md`, `state.env`, `state.env.sha256`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, prompts, lock, and circuit state.
