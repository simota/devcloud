# SQS Server Autoloop

Codex-driven autonomous loop for completing the `devcloud` Amazon SQS compatible local server.

## Usage

```bash
bash scripts/sqs-autoloop/bootstrap.sh
bash scripts/sqs-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=40 bash scripts/sqs-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/sqs-autoloop/run-loop.sh
CODEX_ARGS="--full-auto" bash scripts/sqs-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/sqs-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full bash scripts/sqs-autoloop/run-loop.sh
VERIFY_STAGE=full bash scripts/sqs-autoloop/verify.sh
scripts/sqs-e2e.sh
```

## Verification Stages

- `foundation`: checks the SQS design contract, runner contract, repository build, CLI help, and current Rust test suite.
- `config`: checks SQS config, daemon wiring, and workspace initialization once implemented.
- `protocol`: checks endpoint startup plus AWS JSON and Query protocol `ListQueues`.
- `queue`: checks queue CRUD, attributes, and Query protocol `CreateQueue`.
- `message`: checks send, receive, visibility timeout, delete, and receipt behavior.
- `fifo`: checks FIFO queue validation and creation behavior.
- `dashboard`: checks dashboard service registry, `/dashboard/sqs`, and `/api/sqs/*` visibility.
- `full`: final SQS compatibility acceptance gate, including `scripts/sqs-e2e.sh`.

Runtime files are ignored by git: `progress.md`, `state.env`, `state.env.sha256`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, and lock/circuit files.
