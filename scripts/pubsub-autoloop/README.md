# Google Cloud Pub/Sub Server Autoloop

Codex-driven autonomous loop for completing the `devcloud` Google Cloud Pub/Sub compatible local server.

## Usage

```bash
bash scripts/pubsub-autoloop/bootstrap.sh
bash scripts/pubsub-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=50 bash scripts/pubsub-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/pubsub-autoloop/run-loop.sh
CODEX_ARGS="--full-auto" bash scripts/pubsub-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/pubsub-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full bash scripts/pubsub-autoloop/run-loop.sh
VERIFY_STAGE=full bash scripts/pubsub-autoloop/verify.sh
scripts/pubsub-e2e.sh
```

## Verification Stages

- `foundation`: checks the Pub/Sub design contract, runner contract, repository build, CLI help, and current Rust test suite.
- `config`: checks Pub/Sub config shape, daemon wiring, and workspace initialization once implemented.
- `resource`: checks topic and subscription CRUD over REST.
- `message`: checks publish, pull, acknowledge, and ack deadline behavior.
- `scheduler`: checks redelivery after ack deadline expiration.
- `dashboard`: checks dashboard service registry, `/dashboard/pubsub`, and `/api/pubsub/*` visibility.
- `e2e`: runs `scripts/pubsub-e2e.sh`.
- `full`: final Pub/Sub compatibility acceptance gate.

Runtime files are ignored by git: `progress.md`, `state.env`, `state.env.sha256`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, prompts, lock, and circuit state.
