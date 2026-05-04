# Pub/Sub Full Compatibility Autoloop

Codex-driven autonomous loop for the remaining Google Cloud Pub/Sub compatibility work after the local Pub/Sub MVP.

## Usage

```bash
bash scripts/pubsub-full-compat-autoloop/bootstrap.sh
bash scripts/pubsub-full-compat-autoloop/run-loop.sh
```

Useful options:

```bash
MAX_ITERATIONS=40 bash scripts/pubsub-full-compat-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/pubsub-full-compat-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/pubsub-full-compat-autoloop/run-loop.sh
VERIFY_STAGE=full-compat bash scripts/pubsub-full-compat-autoloop/verify.sh
DONE_VERIFY_STAGE=full-compat bash scripts/pubsub-full-compat-autoloop/run-loop.sh
```

## Verification Stages

- `foundation`: verifies the MVP gate still passes and the remaining-task contract is parseable.
- `streaming`: adds gRPC `StreamingPull` checks.
- `snapshots`: adds gRPC snapshot and seek checks.
- `schemas`: adds gRPC SchemaService checks.
- `push`: adds local push delivery and retry checks.
- `ordering`: adds stricter ordering-key compatibility checks.
- `sdk`: adds Google Pub/Sub client compatibility smoke.
- `full-compat`: runs all remaining compatibility gates plus existing MVP verification.

Runtime files are ignored by git: `progress.md`, `state.env`, `state.env.sha256`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`, prompts, lock, and circuit state.
