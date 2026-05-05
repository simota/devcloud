# SQS Dashboard Management Autoloop

Codex loop for upgrading the SQS React dashboard from inspection plus purge into a local management console.

## Usage

```bash
bash scripts/sqs-dashboard-management-autoloop/bootstrap.sh
bash scripts/sqs-dashboard-management-autoloop/run-loop.sh
```

Useful variants:

```bash
MAX_ITERATIONS=40 bash scripts/sqs-dashboard-management-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/sqs-dashboard-management-autoloop/run-loop.sh
VERIFY_STAGE=queue-send bash scripts/sqs-dashboard-management-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-management-ui bash scripts/sqs-dashboard-management-autoloop/run-loop.sh
VERIFY_STAGE=full-management-ui bash scripts/sqs-dashboard-management-autoloop/verify.sh
```

## Stages

- `foundation`: loop scripts and existing SQS full compatibility gate.
- `queue-send`: CreateQueue and SendMessage flows.
- `receive-delete`: ReceiveMessage and DeleteMessage workflows.
- `visibility-purge-dlq`: visibility timeout, safer purge, and DLQ detail.
- `e2e-docs`: tests/E2E and docs.
- `full-management-ui`: all gates plus `go test ./...`.

Runtime files such as `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `done.md`, and `iteration-*.out` are ignored by git.
