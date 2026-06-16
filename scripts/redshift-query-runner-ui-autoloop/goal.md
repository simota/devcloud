# Redshift Dashboard SQL Query Runner UI Goal

## Goal

Add a SQL query runner to the Redshift dashboard UI using the existing `/api/redshift/query` backend endpoint.

## Acceptance Criteria

1. Redshift dashboard includes a visible SQL editor/control surface for running ad hoc SQL.
2. The query runner calls `/api/redshift/query` through the dashboard API client rather than duplicating fetch logic inline.
3. Query results display columns, rows, row count, command tag, and statement status.
4. Query errors display a safe message and do not expose credentials, DSNs, authorization headers, SQL bind values, object payloads, or statement result payloads beyond the requested result.
5. Successful query execution refreshes catalog/statements so the existing table and statement views stay in sync.
6. Empty SQL is rejected client-side with a clear UI state.
7. Disabled Redshift state does not render an active query runner.
8. Dashboard API/unit tests cover the query runner backend contract, and frontend typecheck/build passes.
9. `scripts/redshift-e2e.sh` verifies the dashboard query runner API path remains compatible.
10. `VERIFY_STAGE=full-query-runner-ui bash scripts/redshift-query-runner-ui-autoloop/verify.sh` passes.

## Out of Scope

- Browser automation framework adoption.
- SQL autocomplete, saved queries, history persistence beyond existing statements.
- CREATE table wizard, COPY/UNLOAD form UI, or schema designer.
- Changing Redshift SQL execution semantics.

## Implementation Guidance

- Keep UI consistent with existing dashboard patterns and compact operational layout.
- Prefer an inline textarea and result table inside the existing Redshift inspector/workspace instead of a marketing-style panel.
- Add TypeScript types for query request/result responses.
- Keep the backend endpoint unchanged unless a test proves a contract gap.
- Preserve existing Redshift full, full-postgres, full-managed, and full-remaining gates.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Redshift dashboard query runner UI loop contract is ready.
