# DynamoDB Dashboard Management UI Goal

## Goal

Upgrade the DynamoDB dashboard from read-only inspection into a safer local management console.

## Acceptance Criteria

1. Existing DynamoDB compatibility and dashboard gates remain green:
   - `VERIFY_STAGE=full bash scripts/dynamodb-autoloop/verify.sh`
   - `npm run typecheck && npm run build` in `web/dashboard`
   - `go test ./internal/dashboard ./internal/services/dynamodb`
2. Dashboard exposes guarded operation flows for CreateTable, PutItem, UpdateItem, DeleteItem, DeleteTable, and TTL update.
3. Destructive operations require an explicit confirmation UI and cannot run from a disabled DynamoDB state.
4. Dashboard API endpoints call the existing local DynamoDB service through the same compatibility path used by SDK/API clients, not by mutating storage internals directly.
5. Query and Scan forms support TableName, optional IndexName, Limit, KeyConditionExpression, FilterExpression, and ExpressionAttributeValues JSON where applicable.
6. Query/Scan results render selected items, count/scanned count where available, and safe error messages.
7. E2E coverage verifies dashboard table detail, index/TTL/stream visibility, operation flows, and Query/Scan paths.
8. README or design docs describe the upgraded DynamoDB dashboard capabilities and safety boundaries.
9. `VERIFY_STAGE=full-management-ui bash scripts/dynamodb-dashboard-management-autoloop/verify.sh` passes.

## Stages

- `foundation`: loop contract, current DynamoDB dashboard metadata inspector, key lookup, and existing DynamoDB full gate.
- `operations-ui`: guarded CreateTable, PutItem, UpdateItem, DeleteItem, DeleteTable, and TTL update UI/API.
- `query-scan-ui`: Query and Scan dashboard API plus compact UI forms and result rendering.
- `e2e-docs`: dashboard/E2E coverage and README/docs updates.
- `full-management-ui`: all stages plus repository tests.

## Out of Scope

- Real AWS calls, IAM policy enforcement, billing, quotas, autoscaling, or production durability.
- Full expression editor/autocomplete.
- Bulk import/export UI.
- Replacing the existing DynamoDB compatibility service implementation.

## Implementation Guidance

- Keep the dashboard dense, operational, and consistent with existing service pages.
- Prefer non-destructive default controls; require explicit confirmation text for DeleteItem/DeleteTable.
- Never log credentials, Authorization headers, SigV4 material, full request payloads, or sensitive item payloads.
- Validate JSON inputs client-side and server-side before calling DynamoDB operations.
- Keep API error messages actionable but safe.
- Update generated dashboard assets with `npm run build` after frontend changes.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: DynamoDB dashboard management UI loop contract is ready.
