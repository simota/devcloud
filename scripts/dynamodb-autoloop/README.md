# DynamoDB Server Autoloop

Codex-driven autonomous loop for completing the `devcloud` Amazon DynamoDB compatible local server.

## Usage

```bash
bash scripts/dynamodb-autoloop/bootstrap.sh
bash scripts/dynamodb-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=40 bash scripts/dynamodb-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/dynamodb-autoloop/run-loop.sh
CODEX_ARGS="--full-auto" bash scripts/dynamodb-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/dynamodb-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full bash scripts/dynamodb-autoloop/run-loop.sh
VERIFY_STAGE=full bash scripts/dynamodb-autoloop/verify.sh
```

## Verification Stages

- `foundation`: checks DynamoDB design contract, repository build, CLI help, and current Go test suite.
- `config`: checks DynamoDB config, daemon wiring, and workspace initialization once implemented.
- `protocol`: checks JSON 1.0 endpoint dispatch, `CreateTable`, `DescribeTable`, and `ListTables`.
- `item-core`: checks `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem`, and conditional write behavior.
- `query-index`: checks `Query`, `Scan`, pagination, and GSI/LSI-oriented behavior.
- `dashboard`: checks dashboard service registry and `/api/dynamodb/*` visibility.
- `full`: final DynamoDB compatibility acceptance gate, including AWS CLI smoke when `aws` is installed.

Runtime files are ignored by git: `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, and lock/circuit files.
