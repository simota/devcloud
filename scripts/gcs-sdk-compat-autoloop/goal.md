# GCS SDK Compatibility E2E Goal

## Goal

Add automated E2E coverage proving that Google Cloud Storage SDK clients can operate against devcloud GCS using local endpoint override only.

## Acceptance Criteria

1. Add a runnable SDK E2E script, preferably `scripts/gcs-sdk-e2e.sh`, that starts devcloud in an isolated temporary workspace with only the required local services enabled.
2. The E2E path uses the official first-party Storage client (`first-party Storage SDK`) or an equivalent first-party Google Cloud Storage SDK client. Prefer the first-party SDK path because this repository is Rust-first and already uses `cargo test`.
3. The SDK workflow verifies bucket create, bucket list/get, object upload, object metadata, object download, object list, object delete, and bucket delete.
4. The SDK workflow uses endpoint override or emulator-style configuration only. It must not call real Google Cloud, require production credentials, or depend on `gcloud auth`.
5. The script must allocate available local ports by default and clean up devcloud process, temporary workspace, bucket, and objects.
6. Existing REST GCS E2E remains green: `scripts/gcs-e2e.sh`.
7. Existing GCS compatibility gate remains green: `VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh`.
8. Repository tests pass: `cargo test --workspace`.
9. README or docs mention the SDK compatibility E2E command and the client compatibility target.
10. `VERIFY_STAGE=full-sdk-compat bash scripts/gcs-sdk-compat-autoloop/verify.sh` passes.

## Stages

- `foundation`: loop contract and script syntax only.
- `sdk-client`: compatibility Storage SDK client dependency/import and SDK workflow exist.
- `sdk-e2e`: SDK E2E script runs successfully.
- `compat-docs`: docs mention the SDK E2E command and compatibility target.
- `full-sdk-compat`: all checks plus existing GCS gates and repository tests.

## Out of Scope

- Real Google Cloud calls.
- Production credentials, IAM, billing, lifecycle policies, retention policies, or object ACL completeness.
- Adding Docker/Testcontainers or external emulators.
- Broad GCS service rewrites unrelated to SDK compatibility.
- Non-GCS service changes except config isolation needed for devcloud startup.

## Implementation Guidance

- Prefer a generated temporary Rust workspace under the E2E temp directory if adding a standalone SDK smoke program keeps repository dependencies cleaner.
- If adding a repository dependency is simpler, keep it justified and minimal.
- Use `option.WithEndpoint`, `option.WithoutAuthentication`, and an HTTP client suitable for local insecure HTTP if required by the SDK.
- Keep object payloads small and non-sensitive.
- Do not log Authorization headers, bearer tokens, object bodies beyond short test literals, or signed URLs.
- Reuse the port-allocation and cleanup style from `scripts/gcs-e2e.sh`.
- Preserve existing GCS REST and dashboard behavior.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: GCS SDK compatibility E2E loop contract is ready.
