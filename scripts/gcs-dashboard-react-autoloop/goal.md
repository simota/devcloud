# GCS React Dashboard Integration Goal

## Goal

Move the GCS dashboard from the legacy Go static page into the shared React/Vite dashboard shell while preserving the existing `/gcs` route and `/api/gcs/*` dashboard API behavior.

## Acceptance Criteria

1. Existing GCS compatibility gate remains green:
   - `VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh`
2. The React dashboard router includes a `/gcs` route and service switcher entry renders the shared shell for GCS.
3. `web/dashboard/src/app/services/gcs/` contains typed API helpers, GCS response types, and a `GCSDashboard` component.
4. The GCS React UI can inspect status, buckets, objects, object metadata, download links, and resumable upload sessions.
5. The UI exposes guarded local management flows for create bucket, delete bucket, delete object, and delete upload session without bypassing `/api/gcs/*`.
6. GCS-specific object metadata is visible: `generation`, `metageneration`, `storageClass`, `crc32c`, `contentType`, `metadata`, and `gs://` URI.
7. Disabled GCS state renders read-only/disabled controls and clear status.
8. The legacy static `/gcs` fallback can be removed only after React route, embedded asset, dashboard API, and tests cover the same behavior.
9. E2E or dashboard tests verify React `/gcs` page loading, bucket/object inspection, guarded destructive flows, and upload session inspection.
10. README or design docs describe the React GCS dashboard capabilities and safety boundaries.
11. `VERIFY_STAGE=full-react-gcs bash scripts/gcs-dashboard-react-autoloop/verify.sh` passes.

## Stages

- `foundation`: loop contract and existing `gcs-autoloop` full gate.
- `react-route`: React route/service module exists and `/gcs` uses the shared shell.
- `inspect`: status, buckets, objects, object metadata, download links, and upload sessions render.
- `management`: guarded create/delete bucket/object/session flows call `/api/gcs/*`.
- `e2e-docs`: dashboard tests/E2E/docs cover GCS React behavior.
- `full-react-gcs`: all stages plus repository tests.

## Out of Scope

- Real Google Cloud calls, IAM policy enforcement, billing, quotas, object retention policy, or production durability.
- Implementing new GCS JSON API protocol behavior beyond dashboard needs.
- Replacing the shared S3-backed object store.
- Persisting object body payloads, credentials, Authorization headers, or bearer tokens in UI state/history.

## Implementation Guidance

- Reuse the existing S3 object explorer patterns where helpful, but expose GCS-specific fields instead of S3 labels.
- Keep the UI compact, operational, and consistent with the shared dashboard shell.
- Destructive flows must require explicit confirmation and must be disabled when GCS is unavailable.
- Keep all mutations routed through the existing local dashboard API, not direct storage calls.
- Update generated dashboard assets with `npm run build` after frontend changes.
- Preserve existing Mail, S3, DynamoDB, BigQuery, SQS, Pub/Sub, and Redshift dashboard routes.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: GCS React dashboard integration loop contract is ready.
