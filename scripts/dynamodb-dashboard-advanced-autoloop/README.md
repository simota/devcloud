# DynamoDB Dashboard Advanced Autoloop

Codex-driven loop for the next DynamoDB dashboard UI expansion after guarded management flows are complete.

## Usage

```bash
bash scripts/dynamodb-dashboard-advanced-autoloop/bootstrap.sh
bash scripts/dynamodb-dashboard-advanced-autoloop/run-loop.sh
```

Common options:

```bash
MAX_ITERATIONS=40 bash scripts/dynamodb-dashboard-advanced-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/dynamodb-dashboard-advanced-autoloop/run-loop.sh
VERIFY_STAGE=pagination bash scripts/dynamodb-dashboard-advanced-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-advanced-ui bash scripts/dynamodb-dashboard-advanced-autoloop/run-loop.sh
VERIFY_STAGE=full-advanced-ui bash scripts/dynamodb-dashboard-advanced-autoloop/verify.sh
```

## Stages

- `foundation`: current management UI and existing full-management-ui gate.
- `pagination`: Query/Scan pagination.
- `saved-recent`: saved query or recent operation history.
- `wizard-validation`: table creation wizard and JSON validation.
- `delete-confirmation`: improved destructive confirmation.
- `e2e-docs`: E2E and docs.
- `full-advanced-ui`: all stages plus repository tests.

Runtime files such as `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `done.md`, and `iteration-*.out` are ignored by git.
