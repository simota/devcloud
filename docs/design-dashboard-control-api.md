# Design - Dashboard Control API

> Status: **CURRENT CONTRACT**.
> Revised: 2026-06-16.

## Overview

Dashboard-only service reads and mutations use explicit HTTP namespaces when a service needs
an operational surface that is separate from its provider-compatible public API.

- `/_introspect/*`: read-only service state for dashboard and diagnostics.
- `/_control/*`: dashboard-initiated service mutations or commands.

Both namespaces are local operational APIs. They are not cloud-provider compatibility APIs.

## Namespace Rules

| Operation kind | Method | Prefix | Example |
|---|---|---|---|
| Snapshot/list/detail read | `GET` | `/_introspect/` | `GET /_introspect/messages` |
| Delete a resource | `DELETE` | `/_control/` | `DELETE /_control/messages/{id}` |
| Bulk delete or flush | `DELETE` | `/_control/` | `DELETE /_control/messages` |
| State change, command, or SQL | `POST` | `/_control/` | `POST /_control/query` |

`/_introspect/*` must stay read-only. `/_control/*` must reject unsupported methods with
405. Both prefixes should be intercepted before provider-protocol dispatch so operational
routes cannot shadow provider actions.

## Implementation Rules

- Reuse the service's existing Rust data model and service methods.
- Do not write storage files directly from dashboard handlers.
- Keep auth behavior aligned with the service's local auth mode.
- Bind operational listeners to loopback addresses.
- Emit the same service events as the equivalent in-process operation.
- Return structured JSON errors without exposing secrets or sensitive payloads.

## Dashboard Integration

`services/dashboard` may call these operational APIs when direct in-process service access is
not the right boundary. The React UI should continue to consume the dashboard's `/api/*`
surface; service operational routes are an internal implementation detail.

## Test Expectations

- Read endpoints: status code, JSON shape, empty-state behavior, and 404/405 handling.
- Mutation endpoints: state transition, emitted event type, idempotency where applicable,
  and 404/405 handling.
- Dashboard API tests: `/api/*` behavior remains stable while the underlying service boundary
  can evolve.
