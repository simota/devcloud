# CHARTER - Dashboard Runtime

> Status: **CURRENT**.
> Revised: 2026-06-16.

## Goal

The devcloud dashboard is the Rust-served operational console for local cloud-compatible
services. It exposes `/dashboard/<svc>` pages, `/api/*` JSON endpoints, event streaming,
and compatibility redirects for the short service paths.

## Architecture

- Runtime owner: `services/dashboard`.
- Frontend source: `web/dashboard`.
- Embedded assets: `services/dashboard/assets/react`.
- Process owner: `devcloud-orchestrator`.
- Service state access: dashboard handlers call Rust service APIs or local network endpoints,
  depending on the service boundary.

All dashboard mutations must go through the service API boundary. The dashboard must not
write service storage files directly.

## Route Convention

- Primary pages live under `/dashboard/<svc>`.
- Short service paths such as `/mail`, `/s3`, `/gcs`, `/dynamodb`, and `/bigquery` are
  compatibility redirects to `/dashboard/<svc>`.
- New functionality belongs under `/dashboard/<svc>` and `/api/*`, not under redirect-only
  routes.

## Implementation Boundaries

- `services/dashboard` owns HTTP routing, API handlers, asset embedding, cache headers,
  index fallback, and service registry responses.
- `web/dashboard` owns React UI source and Vite build output.
- `orchestrator` owns service enablement, port selection, process supervision, and dashboard
  startup.
- Individual service crates own provider-compatible behavior and any service-specific
  introspection/control surface.

## Safety Rules

- Do not log credentials, authorization headers, signatures, message bodies, object payloads,
  or sensitive local paths.
- Do not commit `.devcloud/` runtime state.
- Keep dashboard mutations routed through service methods or service HTTP/gRPC APIs.
- Keep compatibility redirects behavior-preserving.

## Verification

Run the dashboard-relevant gate before claiming dashboard runtime changes complete:

```sh
cargo test --workspace
VERIFY_STAGE=full bash scripts/dashboard-design-renewal-autoloop/verify.sh
```

For service-specific dashboard pages, also run that service's full acceptance gate when the
change touches service API behavior.
