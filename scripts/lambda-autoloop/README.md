# Lambda Autoloop

Acceptance gate for the `devcloud` AWS Lambda compatible service
(`services/lambda`, default port 19010, Lambda REST API under
`/2015-03-31/functions`). Handlers run as local `python3` / `node` processes, so
both interpreters should be on `PATH` (the node check is skipped when `node` is
missing).

## Usage

```bash
VERIFY_STAGE=full bash scripts/lambda-autoloop/verify.sh
```

Ports default to free ephemeral ports so runs never collide with a real
`devcloud up`. Override when fixed ports are needed:

```bash
LAMBDA_VERIFY_PORT=19910 \
DASHBOARD_VERIFY_PORT=18925 \
VERIFY_STAGE=full bash scripts/lambda-autoloop/verify.sh
```

## Verification Stages

- `foundation`: autoloop script contract (`bash -n`) and the `devcloud-lambda`
  crate test suite.
- `config`: confirms the orchestrator config, supervisor, and dashboard
  registry reference Lambda.
- `lambda` (alias `lambda-core`): boots the orchestrator and exercises the
  provider protocol — CreateFunction (python + node), ListFunctions,
  GetFunction by ARN, Invoke (success, function error, node),
  UpdateFunctionConfiguration, and the tag lifecycle.
- `dashboard` (alias `dashboard-static`): `/api/lambda/*` forwarding (status,
  functions, test invoke through the Invoke API, invocations) and the
  `/dashboard/lambda` SPA route.
- `hardening` / `full`: duplicate function, invalid package, invalid payload,
  missing function, handler timeout, dashboard method rejection, DeleteFunction,
  and the standalone `scripts/lambda-e2e.sh` run.

This folder only contains the acceptance gate. Runner state files
(`progress.md`, `state.env`, `runner.log`, `iteration-*.out`) are generated and
must not be committed.
