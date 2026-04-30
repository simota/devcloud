# Mail Server Autoloop

Codex-driven autonomous loop for completing the `devcloud` Mail MVP.

## Start

```bash
bash scripts/mail-autoloop/run-loop.sh
```

The default loop gate is `foundation`, so the runner can start from the current scaffold and advance one implementation phase at a time. A `DONE` footer from Codex is accepted only after the full gate passes.

## Useful Options

```bash
MAX_ITERATIONS=12 bash scripts/mail-autoloop/run-loop.sh
AUTOCOMMIT=true bash scripts/mail-autoloop/run-loop.sh
CODEX_ARGS="--full-auto" bash scripts/mail-autoloop/run-loop.sh
VERIFY_STAGE=foundation bash scripts/mail-autoloop/run-loop.sh
DONE_VERIFY_STAGE=full bash scripts/mail-autoloop/run-loop.sh
VERIFY_STAGE=full bash scripts/mail-autoloop/verify.sh
```

## Verification Stages

- `foundation`: Go tests, CLI help, daemon help, and `cmd/devcloud` build.
- `smtp-protocol` / `smtp-persist`: starts `devcloud up` and checks dashboard HTTP plus SMTP send.
- `api-smoke`: checks SMTP send, message list API, and raw source API.
- `dashboard-static` / `hardening` / `full`: final acceptance gate for the MVP.

## Files

- `goal.md`: measurable implementation contract.
- `run-loop.sh`: Codex runner with bounded iterations, lock, status parsing, and checkpoint writes.
- `verify.sh`: acceptance verification gate.
- `recover.sh`: rebuilds `state.env` from `progress.md` and clears stale lock/circuit state.
- `progress.md`: append-only loop timeline.
- `state.env`: resumable execution state.

The runner expects Codex CLI to be available as `codex`.
