# Redshift PG Wire Refactor Autoloop

This loop refactors `services/redshift/pgwire.rs` into smaller files without changing PostgreSQL wire protocol or Redshift behavior.

## Usage

Initialize state:

```bash
bash scripts/redshift-pgwire-refactor-autoloop/bootstrap.sh
```

Run bounded iterations:

```bash
MAX_ITERATIONS=10 VERIFY_STAGE=foundation bash scripts/redshift-pgwire-refactor-autoloop/run-loop.sh
```

Before claiming completion:

```bash
VERIFY_STAGE=full bash scripts/redshift-pgwire-refactor-autoloop/verify.sh
```

## Stages

- `foundation`: validates loop contract, syntax, package tests, and formatting.
- `shape`: validates the refactored file layout and `pgwire.rs` line budget.
- `redshift-full`: runs the existing Redshift compatibility gate.
- `full`: runs all checks.

## Constraints

- Behavior-preserving movement only.
- No feature work.
- No new dependencies.
- Keep package name `redshift`.
- Do not stage loop runtime state files.
