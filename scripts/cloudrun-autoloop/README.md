# Cloud Run Autoloop

Acceptance gate for the `devcloud` Cloud Run compatible service
(`services/cloudrun`, default port 18095, Cloud Run Admin API v2 under
`/v2/projects/{project}/locations/{location}/services`, plus the data plane on
`Host: <svc>.<location>.<project>.run.localhost`). The sample service is a
`python3` HTTP server run as a local process via `containers[0].command`.

## Usage

```bash
VERIFY_STAGE=full bash scripts/cloudrun-autoloop/verify.sh
```

Ports default to free ephemeral ports so runs never collide with a real
`devcloud up`. Override when fixed ports are needed:

```bash
CLOUDRUN_VERIFY_PORT=18995 \
DASHBOARD_VERIFY_PORT=18925 \
VERIFY_STAGE=full bash scripts/cloudrun-autoloop/verify.sh
```

## Verification Stages

- `foundation`: autoloop script contract (`bash -n`) and the
  `devcloud-cloudrun` crate test suite.
- `config`: confirms the orchestrator config, supervisor, and dashboard
  registry reference Cloud Run.
- `cloudrun` (alias `cloudrun-core`): CreateService, GetService, ListServices,
  host- and path-routed requests reaching the local instance, a template update
  rolling a new revision (and the instance), a label-only update keeping the
  revision, ListRevisions, and the IAM policy round trip.
- `dashboard` (alias `dashboard-static`): `/api/cloudrun/*` forwarding (status,
  services, instances, revisions, logs) and the `/dashboard/cloudrun` SPA route.
- `hardening` / `full`: duplicate service, invalid service id, missing image,
  image-only service without Docker (503), unknown service host (404),
  dashboard method rejection, DeleteService stopping instances, and the
  standalone `scripts/cloudrun-e2e.sh` run.

This folder only contains the acceptance gate. Runner state files
(`progress.md`, `state.env`, `runner.log`, `iteration-*.out`) are generated and
must not be committed.
