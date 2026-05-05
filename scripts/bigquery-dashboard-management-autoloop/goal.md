# BigQuery Dashboard Management UI Goal

## Goal

Upgrade the BigQuery React dashboard from read-only catalog browsing into a safer local management console for common BigQuery developer workflows.

## Acceptance Criteria

1. Existing BigQuery compatibility gate remains green:
   - `VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh`
2. The existing `/dashboard/bigquery` React route remains available and keeps project, dataset, table, row, schema, and job inspection.
3. The dashboard exposes a SQL query runner using the local BigQuery query API, including query text, max results, `useLegacySql=false`, dry-run toggle, status, result table, selected result JSON, and job reference.
4. The dashboard exposes guarded local management flows for `datasets.insert`, `tables.insert`, and `tabledata.insertAll` through `/api/bigquery/*`, not by mutating storage directly.
5. Create dataset/table forms provide guided fields and raw JSON mode where the API shape is useful.
6. Insert row editor validates JSON before sending and shows partial insert errors without logging row payloads.
7. Jobs panel shows query/job detail, recent query metadata, and selected job JSON.
8. Disabled BigQuery state renders read-only/disabled controls and clear status.
9. E2E or dashboard tests verify query runner, create dataset/table, insert row validation, job detail, and disabled-state behavior.
10. README or design docs describe BigQuery dashboard management capabilities and safety boundaries.
11. `VERIFY_STAGE=full-management-ui bash scripts/bigquery-dashboard-management-autoloop/verify.sh` passes.

## Stages

- `foundation`: loop contract and existing `bigquery-autoloop` full gate.
- `query-runner`: SQL query runner and query result UI.
- `management`: create dataset/table and insert row UI/API helpers.
- `jobs-validation`: job detail, recent query metadata, and client-side validation.
- `e2e-docs`: tests/E2E and docs updates.
- `full-management-ui`: all stages plus repository tests.

## Out of Scope

- Real Google Cloud calls, IAM policy enforcement, billing, reservations, slots, BI Engine, or production durability.
- Implementing new BigQuery protocol behavior beyond dashboard needs.
- Full SQL editor intelligence, autocomplete, or schema-aware query planning.
- Persisting row payloads, credentials, Authorization headers, bearer tokens, or full request bodies in browser history.

## Implementation Guidance

- Keep the UI compact, operational, and consistent with the existing dashboard shell.
- Mutations must call existing local dashboard/API endpoints.
- Prefer guided controls for common cases and raw JSON for advanced BigQuery request shapes.
- Never log credentials, Authorization headers, bearer tokens, full row payloads, or query parameter secrets.
- Update generated dashboard assets with `npm run build` after frontend changes.
- Preserve existing Mail, S3, GCS, DynamoDB, SQS, Pub/Sub, and Redshift dashboard routes.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: BigQuery dashboard management UI loop contract is ready.
