# Pub/Sub Server Refactor Autoloop

This loop refactors `services/pubsub/server.rs` into smaller files without changing Pub/Sub behavior.

## Usage

Initialize state:

```bash
bash scripts/pubsub-server-refactor-autoloop/bootstrap.sh
```

Run bounded iterations:

```bash
MAX_ITERATIONS=10 VERIFY_STAGE=foundation bash scripts/pubsub-server-refactor-autoloop/run-loop.sh
```

Before claiming completion:

```bash
VERIFY_STAGE=full bash scripts/pubsub-server-refactor-autoloop/verify.sh
```

## Stages

- `foundation`: validates loop contract, syntax, package tests, and formatting.
- `shape`: validates the refactored file layout and `server.rs` line budget.
- `pubsub-full`: runs the existing Pub/Sub compatibility gate.
- `full`: runs all checks.

## Constraints

- Behavior-preserving movement only.
- No feature work.
- No new dependencies.
- Keep package name `pubsub`.
- Do not stage loop runtime state files.
