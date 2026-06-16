# scripts

The executable verification surface is:

- `scripts/*-e2e.sh`
- `scripts/*-autoloop/verify.sh`
- `scripts/lib/devcloud-engine.sh`

These scripts build and run the Rust orchestrator from `Cargo.toml`.
Historical autoloop files such as `goal.md`, `run-loop.sh`, `recover.sh`, and
`bootstrap.sh` document how earlier loops were driven. Some of those historical
instructions may describe earlier task contracts; they are not the current
source of truth for the Rust workspace.

Generated loop state and transcripts are ignored by git and by ripgrep through
the repository `.gitignore` and `.ignore` files:

- `runner.log`, `runner.jsonl`, `state.env`
- `progress.md`, `done.md`
- `iteration-*.out`, `verify-*.out`, `prompt-*.*`

Use `README.md`, `AGENTS.md`, `CLAUDE.md`, and
`docs/ROADMAP.md` for current Rust-only build, test, and runtime
guidance.
