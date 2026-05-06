# BigQuery SDK Compatibility E2E Goal

## Goal

Add automated E2E coverage proving that Google BigQuery SDK clients can operate against devcloud BigQuery using local endpoint override only.

## Acceptance Criteria

1. Add a runnable SDK E2E script, preferably `scripts/bigquery-sdk-e2e.sh`, that starts devcloud in an isolated temporary workspace with only required local services enabled.
2. The E2E path uses the official Go BigQuery client (`cloud.google.com/go/bigquery`) or an equivalent first-party Google BigQuery SDK client. Prefer Go because this repository is Go-first and already uses `go test`.
3. The SDK workflow verifies dataset create/list/get, table create/get, row insert/read, query execution, job status/result handling where supported, table delete, and dataset delete.
4. The SDK workflow uses endpoint override or emulator-style configuration only. It must not call real Google Cloud, require production credentials, or depend on `gcloud auth`.
5. The script must allocate available local ports by default and clean up devcloud process, temporary workspace, datasets, tables, rows, and jobs.
6. Existing REST BigQuery E2E remains green: `scripts/bigquery-e2e.sh`.
7. Existing BigQuery compatibility gate remains green: `VERIFY_STAGE=full bash scripts/bigquery-autoloop/verify.sh`.
8. Repository tests pass: `go test ./...`.
9. README or docs mention the SDK compatibility E2E command and the client compatibility target.
10. `VERIFY_STAGE=full-sdk-compat bash scripts/bigquery-sdk-compat-autoloop/verify.sh` passes.

## Stages

- `foundation`: loop contract and script syntax only.
- `sdk-go`: Go BigQuery SDK client dependency/import and SDK workflow exist.
- `sdk-e2e`: SDK E2E script runs successfully.
- `compat-docs`: docs mention the SDK E2E command and compatibility target.
- `full-sdk-compat`: all checks plus existing BigQuery gates and repository tests.

## Out of Scope

- Real Google Cloud calls.
- Production credentials, IAM, billing, reservations, BI Engine, Storage API, or full SQL dialect parity.
- Adding Docker/Testcontainers or external emulators.
- Broad BigQuery service rewrites unrelated to SDK compatibility.
- Non-BigQuery service changes except config isolation needed for devcloud startup.

## Implementation Guidance

- Prefer a generated temporary Go module under the E2E temp directory if adding a standalone SDK smoke program keeps repository dependencies cleaner.
- If adding a repository dependency is simpler, keep it justified and minimal.
- Use `option.WithEndpoint`, `option.WithoutAuthentication`, and an HTTP client suitable for local insecure HTTP if required by the SDK.
- Keep row payloads small and non-sensitive.
- Do not log Authorization headers, bearer tokens, credentials, full query payloads containing sensitive data, or production-like datasets.
- Reuse the port-allocation and cleanup style from `scripts/bigquery-e2e.sh`.
- Preserve existing BigQuery REST, GCS load/extract, and dashboard behavior.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: BigQuery SDK compatibility E2E loop contract is ready.
