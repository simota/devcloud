# Goal

## Objective

Complete the `devcloud` common dashboard design renewal with a React/Vite shell while preserving the existing Go-served Mail and S3 dashboard contracts during migration.

## Why

The dashboard is becoming a cross-service operational console for Mail, S3, and future services. The current Go string-based static pages are useful but hard to extend consistently. This loop should move the dashboard toward the shared shell described in `docs/design-dashboard-shell.md`, with safe incremental migration and no Node.js requirement at production runtime.

## Acceptance Criteria

1. Foundation remains healthy: `go test ./...` passes.
2. React dashboard scaffold remains healthy: `cd web/dashboard && npm run build` passes.
3. Common dashboard registry remains available at `/api/dashboard/services` and reports Mail/S3 service status without marking disabled services as running.
4. Existing compatibility routes remain intact while migration is in progress: `/`, `/mail`, `/s3`, `/api/messages/*`, and `/api/s3/*` keep their current contracts.
5. React shell uses the shared tokens and shell structure from `docs/design-dashboard-shell.md`: service index, service switcher, status pattern, activity footer, and service-specific surfaces.
6. Production `devcloud up` can serve dashboard assets without requiring Node/Vite at runtime once asset embedding is introduced.
7. Dynamic dashboard data is rendered through React escaping or safe DOM APIs; no `dangerouslySetInnerHTML` or raw `innerHTML` rendering is introduced.
8. Full verification passes: `VERIFY_STAGE=full bash scripts/dashboard-design-renewal-autoloop/verify.sh`.

## Implementation Phases

Work in this order. Each Codex iteration should complete one small slice and keep earlier stages passing.

1. `foundation`: keep Go tests, existing dashboard APIs, and current static routes healthy.
2. `registry`: harden `/api/dashboard/services` tests and disabled service behavior.
3. `react-shell`: improve `web/dashboard` shell, routing, shared tokens, typed dashboard API client, and loading/error/empty states.
4. `embed-assets`: add Go embedded asset serving for built React dashboard assets without intercepting `/api/*`.
5. `s3-react`: port S3 Object Explorer into the React shell while preserving `/api/s3/*`.
6. `mail-react`: port Mail inbox into the React shell while preserving `/api/messages/*`.
7. `hardening`: add compatibility, accessibility, and E2E smoke coverage for the migrated dashboard.

## Out of Scope

- Authentication, user management, or multi-tenant dashboard controls.
- Replacing Mail/S3 backend APIs.
- Importing the large mock dependency graphs from `mock/mail` or `mock/s3`.
- Desktop app packaging with Electron/Tauri.
- Big-bang replacement of all static dashboards in one iteration.

## Implementation Constraints

- Preserve existing user changes and do not rewrite unrelated files.
- Keep production runtime as a Go binary; Node/Vite is development/build-time only.
- Keep dependency additions minimal and justified.
- Do not log credentials, authorization headers, object payloads, message bodies, or sensitive metadata.
- Keep runtime state under `.devcloud/`.
- Use small, reviewable changes and add tests for behavior changes.
- The runner owns `progress.md`, `state.env`, `runner.log`, `runner.jsonl`, `.run-loop.lock`, and `.circuit-state`.

## Verification Command

```bash
VERIFY_STAGE=foundation bash scripts/dashboard-design-renewal-autoloop/verify.sh
VERIFY_STAGE=react-build bash scripts/dashboard-design-renewal-autoloop/verify.sh
VERIFY_STAGE=api-smoke bash scripts/dashboard-design-renewal-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/dashboard-design-renewal-autoloop/verify.sh
```

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Dashboard design renewal contract is ready for Codex execution.
