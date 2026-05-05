# DynamoDB Dashboard Management Autoloop

Codex-driven loop for upgrading the DynamoDB dashboard management UI after the metadata inspector and key lookup helper.

## Usage

```bash
bash scripts/dynamodb-dashboard-management-autoloop/bootstrap.sh
bash scripts/dynamodb-dashboard-management-autoloop/run-loop.sh
```

Common options:

```bash
MAX_ITERATIONS=40 bash scripts/dynamodb-dashboard-management-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/dynamodb-dashboard-management-autoloop/run-loop.sh
VERIFY_STAGE=operations-ui bash scripts/dynamodb-dashboard-management-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-management-ui bash scripts/dynamodb-dashboard-management-autoloop/run-loop.sh
VERIFY_STAGE=full-management-ui bash scripts/dynamodb-dashboard-management-autoloop/verify.sh
```

## Stages

- `foundation`: current dashboard metadata/key lookup and existing DynamoDB gates.
- `operations-ui`: guarded management operation UI/API.
- `query-scan-ui`: Query/Scan UI/API.
- `e2e-docs`: E2E and docs.
- `full-management-ui`: all stages plus repository tests.

Runtime files such as `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `done.md`, and `iteration-*.out` are ignored by git.
