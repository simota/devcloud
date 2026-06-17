# Nexus Kaizen 5x Runner

Runs `$nexus kaizen` through Codex CLI five times by default.

```bash
./scripts/nexus-kaizen-5x/verify.sh
./scripts/nexus-kaizen-5x/run-loop.sh
```

Useful overrides:

```bash
MAX_ITERATIONS=5 ITER_TIMEOUT=1200 LOOP_TIMEOUT=7200 ./scripts/nexus-kaizen-5x/run-loop.sh
ALLOW_DIRTY=true ./scripts/nexus-kaizen-5x/run-loop.sh
CODEX_BIN=codex CODEX_MODEL=gpt-5.5 REASONING_EFFORT=high ./scripts/nexus-kaizen-5x/run-loop.sh
```

Artifacts:

- `runner.log`: human-readable runner events and Codex stderr
- `runner.jsonl`: Codex JSONL event stream plus runner JSON events
- `iteration-N.prompt`: prompt sent to Codex
- `iteration-N.out`: final Codex message for iteration N
- `progress.md`: append-only iteration summary
- `state.env`: resumable state

Recovery:

```bash
./scripts/nexus-kaizen-5x/recover.sh --clear-lock
./scripts/nexus-kaizen-5x/recover.sh --rebuild-state
./scripts/nexus-kaizen-5x/recover.sh --set-next 3
```
