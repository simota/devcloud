# Nexus Kaizen 5x Loop Goal

## Objective

Run `$nexus kaizen` through Codex CLI exactly five times by default, with the loop bounded and auditable from filesystem artifacts.

## Why

Repeated Kaizen runs are useful only when the runner, not the agent, controls termination and leaves enough evidence to inspect each iteration.

## Acceptance Criteria

1. `run-loop.sh` enforces `MAX_ITERATIONS=5` by default and an external `LOOP_TIMEOUT`.
   - Verify: `VERIFY_ONLY=true ./scripts/nexus-kaizen-5x/verify.sh`
2. Each iteration invokes `codex exec` with model `gpt-5.5`, workspace-write sandboxing, and prompt text containing `$nexus kaizen`.
   - Verify: `VERIFY_ONLY=true ./scripts/nexus-kaizen-5x/verify.sh`
3. Each iteration writes an `iteration-N.out` final-response artifact and appends runner events to `runner.log`, `runner.jsonl`, and `progress.md`.
   - Verify: run `./scripts/nexus-kaizen-5x/run-loop.sh` and inspect generated artifacts.
4. `state.env` is written atomically and records `NEXT_ITERATION`, `LAST_STATUS`, and contract version for resume.
   - Verify: `VERIFY_ONLY=true ./scripts/nexus-kaizen-5x/verify.sh`
5. The script set has deterministic syntax validation and recovery entry points.
   - Verify: `bash -n scripts/nexus-kaizen-5x/run-loop.sh scripts/nexus-kaizen-5x/verify.sh scripts/nexus-kaizen-5x/recover.sh`

## Out Of Scope

- Running the five Codex iterations during generation.
- Choosing the Kaizen targets; Nexus handles that inside each Codex invocation.
- Editing product code from Orbit.

## External Terminators

- `MAX_ITERATIONS=5`
- `LOOP_TIMEOUT=7200` seconds by default
- Optional hard cap: `USD_PER_RUN_CAP` if `.cost-usd` is populated by a caller

## Footer Contract

The runner emits:

```text
NEXUS_LOOP_STATUS: READY | CONTINUE | DONE
NEXUS_LOOP_SUMMARY: <single-line summary>
```
