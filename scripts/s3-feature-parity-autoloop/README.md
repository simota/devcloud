# S3 Feature Parity Autoloop

This loop implements the missing S3-compatible feature set in bounded Codex iterations.

## Target Features

1. Versioning
2. Bucket policy and ACL metadata
3. Lifecycle expiration
4. Event notifications
5. SSE/KMS metadata
6. Virtual-host style routes
7. Object Lock
8. S3 Select
9. Inventory and analytics
10. Replication

## Usage

```bash
bash scripts/s3-feature-parity-autoloop/bootstrap.sh
MAX_ITERATIONS=20 VERIFY_STAGE=foundation bash scripts/s3-feature-parity-autoloop/run-loop.sh
```

Run a final gate after the loop claims completion:

```bash
VERIFY_STAGE=full bash scripts/s3-feature-parity-autoloop/verify.sh
```

Useful stages:

- `foundation`: script contract, goal contract, and repository tests.
- `s3-core`: existing S3 full compatibility gate.
- `feature-contract`: checks that feature targets are represented in code/tests/docs.
- `full`: all of the above.

## Runtime Files

The runner owns these files and they should not be committed:

- `.run-loop.lock`
- `.circuit-state`
- `state.env`
- `state.env.sha256`
- `runner.log`
- `runner.jsonl`
- `progress.md`
- `done.md`
- `iteration-*.out`
- `prompt-*.*`
- `verify-*.out`

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: S3 feature parity autoloop scripts are documented.
