# Redis Autoloop

Codex-driven nexus-autoloop runner for adding the Redis service to `devcloud`.

## Prerequisites

- Codex CLI installed and authenticated: `command -v codex` must succeed.
- `redis-server` and `redis-cli` in `$PATH` for the `managed-lifecycle` and `e2e` verify stages. The loop runs without them, but those stages are reported as `[SKIP]`.
- Repo must be clean enough that the loop can land focused commits.

## Files

| File | Purpose |
|------|---------|
| `goal.md` | Loop contract: objective, why, acceptance criteria, out-of-scope, footer rules |
| `bootstrap.sh` | Initialize `state.env`, `progress.md`, validate sibling scripts with `bash -n` |
| `run-loop.sh` | Main runner: preflight → build prompt → `codex exec` → verify → footer parse → commit (opt) |
| `verify.sh` | Staged verifier: `foundation` / `config` / `redis-core` / `dashboard-static` / `hardening` / `e2e` / `full` |
| `recover.sh` | Rebuild `state.env` from `progress.md` evidence; clear circuit/lock |
| `state.env` | Runner-owned resume state (created by bootstrap) |
| `progress.md` | Append-only iteration log (created by bootstrap) |
| `runner.log` / `runner.jsonl` | Iteration output and structured events |
| `iteration-N.out` | Captured Codex stdout/stderr per iteration |
| `done.md` | Written only when DONE is reached with passing `full` verification |

## Usage

```sh
# One-time
bash scripts/redis-autoloop/bootstrap.sh

# Run a single iteration (default MAX_ITERATIONS=30; raise via env)
bash scripts/redis-autoloop/run-loop.sh

# Run with autocommit during the loop
AUTOCOMMIT=true bash scripts/redis-autoloop/run-loop.sh

# Pin a specific verify stage during the loop
VERIFY_STAGE=redis-core bash scripts/redis-autoloop/run-loop.sh

# Final acceptance gate (must pass before DONE is honored)
VERIFY_STAGE=full bash scripts/redis-autoloop/verify.sh

# Recover from drift or stale lock
bash scripts/redis-autoloop/recover.sh
```

## Tunables

| Env | Default | Notes |
|-----|---------|-------|
| `CODEX_BIN` | `codex` | Path to Codex CLI |
| `CODEX_ARGS` | `--full-auto` | Forwarded to `codex exec` |
| `MAX_ITERATIONS` | `30` | Per-run iteration cap |
| `ITER_TIMEOUT` | `1200` | Per-iteration timeout (s) |
| `LOOP_TIMEOUT` | `0` | 0 = unlimited |
| `CIRCUIT_THRESHOLD` | `3` | Consecutive failures to open circuit |
| `AUTOCOMMIT` | `false` | Commit per iteration when true |
| `VERIFY_STAGE` | `foundation` | Stage used during the loop |
| `DONE_VERIFY_STAGE` | `full` | Stage required to honor DONE |

## Verify Stages

- `foundation`: design doc + script contract + `cargo build` + `cargo test --workspace`
- `config`: foundation + `orchestrator` tests + Redis config shape
- `redis-core`: previous + `services/redis` tests + managed-redis shape
- `dashboard-static`: previous + `services/dashboard` tests + dashboard handler/UI shape
- `hardening`: previous + managed-lifecycle tests (requires `redis-server`)
- `e2e`: foundation + `scripts/redis-e2e.sh`
- `full`: all of the above

Stages requiring `redis-server` or loopback bind permission emit `[SKIP]` rather than `[FAIL]` when unavailable, so the runner remains usable in restricted environments.

## DONE Evidence Gate

`done.md` is written only when:
- `NEXUS_LOOP_STATUS: DONE` appears in the iteration output
- `VERIFY_STAGE=full bash scripts/redis-autoloop/verify.sh` exits 0

Otherwise the runner downgrades the iteration to `CONTINUE` regardless of Codex's claim.

## Files the runner owns (do not edit by hand)

- `state.env`, `state.env.sha256`, `.run-loop.lock`, `.circuit-state`
- `progress.md`, `runner.log`, `runner.jsonl`, `iteration-*.out`, `done.md`

Recovery is the only safe way to reset these.
