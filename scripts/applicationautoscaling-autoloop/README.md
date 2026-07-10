# Application Auto Scaling Autoloop

Acceptance gate for the `devcloud` Application Auto Scaling compatible service
(`services/applicationautoscaling`, default port 18030, AWS JSON 1.1 protocol
with `X-Amz-Target: AnyScaleFrontendService.*`).

## Usage

```bash
VERIFY_STAGE=full bash scripts/applicationautoscaling-autoloop/verify.sh
```

Ports default to free ephemeral ports so runs never collide with a real
`devcloud up`. Override when fixed ports are needed:

```bash
AAS_VERIFY_PORT=19030 \
DASHBOARD_VERIFY_PORT=18925 \
VERIFY_STAGE=full bash scripts/applicationautoscaling-autoloop/verify.sh
```

## Verification Stages

- `foundation`: autoloop script contract (`bash -n`) and the
  `devcloud-applicationautoscaling` crate test suite.
- `config`: confirms the orchestrator/services wiring references the
  `appAutoScaling` config keys, plus the crate tests.
- `applicationautoscaling` (alias `aas-core`): boots the orchestrator and
  exercises the provider protocol — RegisterScalableTarget,
  DescribeScalableTargets, PutScalingPolicy, DescribeScalingPolicies,
  DescribeScalingActivities, Tag/Untag/ListTagsForResource, PutScheduledAction,
  DescribeScheduledActions.
- `dashboard` (alias `dashboard-static`): verifies read-only dashboard
  forwarding through `/api/applicationautoscaling/*` and the
  `/dashboard/applicationautoscaling` SPA route (never touches storage directly).
- `hardening` / `full`: validation/error edge cases (unsupported namespace,
  missing required field, unknown operation, dashboard non-GET rejection), the
  delete/deregister lifecycle, and the standalone
  `scripts/applicationautoscaling-e2e.sh` end-to-end run.

This folder only contains the acceptance gate. Runner state files
(`progress.md`, `state.env`, `runner.log`, `iteration-*.out`) are generated and
must not be committed.
