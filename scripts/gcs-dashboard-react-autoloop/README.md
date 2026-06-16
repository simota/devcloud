# GCS React Dashboard Autoloop

Codex loop for moving the GCS dashboard into the shared React dashboard shell.

## Usage

```bash
bash scripts/gcs-dashboard-react-autoloop/bootstrap.sh
bash scripts/gcs-dashboard-react-autoloop/run-loop.sh
```

Useful variants:

```bash
MAX_ITERATIONS=40 bash scripts/gcs-dashboard-react-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/gcs-dashboard-react-autoloop/run-loop.sh
VERIFY_STAGE=react-route bash scripts/gcs-dashboard-react-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full-react-gcs bash scripts/gcs-dashboard-react-autoloop/run-loop.sh
VERIFY_STAGE=full-react-gcs bash scripts/gcs-dashboard-react-autoloop/verify.sh
```

## Stages

- `foundation`: loop scripts and existing GCS full compatibility gate.
- `react-route`: React route/service module and shared shell integration.
- `inspect`: GCS status, bucket/object metadata, download links, and upload sessions.
- `management`: guarded create/delete flows through `/api/gcs/*`.
- `e2e-docs`: dashboard tests or E2E plus docs.
- `full-react-gcs`: all gates plus `cargo test --workspace`.

Runtime files such as `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `done.md`, and `iteration-*.out` are ignored by git.
