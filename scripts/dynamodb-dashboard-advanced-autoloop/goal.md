# DynamoDB Dashboard Advanced UI Goal

## Goal

Add the next layer of DynamoDB dashboard management features after the guarded management UI is complete.

## Acceptance Criteria

1. Existing DynamoDB dashboard management gate remains green:
   - `VERIFY_STAGE=full-management-ui bash scripts/dynamodb-dashboard-management-autoloop/verify.sh`
2. Query/Scan result pagination is exposed in the dashboard using safe local dashboard API state or request payloads.
3. Query/Scan pagination UI shows count, scanned count, page position, next/previous controls where available, and selected result item JSON.
4. Saved query or recent operation history is available locally in the dashboard UI without leaking item payloads or credentials.
5. CreateTable has a guided wizard mode for table name, partition/sort keys, billing mode, and optional GSI/LSI metadata, while preserving raw JSON mode.
6. Item and expression JSON editors provide client-side validation feedback before sending requests.
7. DeleteItem/DeleteTable confirmation UI is upgraded to a clearer two-step destructive flow.
8. E2E coverage verifies pagination, saved/recent query flow, wizard table creation, validation errors, and destructive confirmation behavior.
9. README or design docs describe the advanced DynamoDB dashboard capabilities and safety boundaries.
10. `VERIFY_STAGE=full-advanced-ui bash scripts/dynamodb-dashboard-advanced-autoloop/verify.sh` passes.

## Stages

- `foundation`: loop contract and existing `full-management-ui` gate.
- `pagination`: Query/Scan result pagination UI/API.
- `saved-recent`: saved query or recent operation history.
- `wizard-validation`: CreateTable wizard and JSON validation improvements.
- `delete-confirmation`: improved destructive confirmation flow.
- `e2e-docs`: E2E and docs updates.
- `full-advanced-ui`: all stages plus repository tests.

## Out of Scope

- Real AWS calls, IAM policy enforcement, billing, quotas, autoscaling, or production durability.
- Bulk import/export UI.
- Replacing the existing DynamoDB compatibility service implementation.
- Persisting sensitive item payloads in saved queries or operation history.

## Implementation Guidance

- Keep the UI compact, operational, and consistent with the existing dashboard.
- Prefer safe previews, redacted summaries, and explicit confirmation for destructive actions.
- Never log credentials, Authorization headers, SigV4 material, full request payloads, or sensitive item payloads.
- Update generated dashboard assets with `npm run build` after frontend changes.
- Preserve the existing metadata inspector, key lookup, operation forms, and Query/Scan forms.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: DynamoDB dashboard advanced UI loop contract is ready.
