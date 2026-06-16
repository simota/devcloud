# GCS Server Autoloop Goal

## Goal

Implement a Google Cloud Storage compatible local server for `devcloud`, following `docs/design-gcs-compat.md`.

## Why

`devcloud` already has local Mail, S3, and dashboard foundations. GCS compatibility should reuse the object storage direction described in `docs/spec-v0.md` and allow local applications to exercise GCS bucket/object workflows without reaching Google Cloud.

## Acceptance Criteria

1. `devcloud up` starts a GCS JSON API endpoint on the configured local port, defaulting to `127.0.0.1:4443`.
2. JSON API bucket operations work: `buckets.insert`, `buckets.get`, `buckets.list`, and `buckets.delete`.
3. JSON API object operations work: media upload, metadata get, media download, list, delete, and copy.
4. Object metadata includes GCS-compatible `generation`, `metageneration`, `etag`, `size`, `contentType`, `md5Hash` or `crc32c` where practical, and user metadata.
5. Media download supports `Range` requests and returns `206 Partial Content` for satisfiable ranges.
6. Resumable upload supports session creation, chunk/final upload, status query, restart-safe session persistence, and final object commit.
7. GCS preconditions are enforced for `ifGenerationMatch`, `ifGenerationNotMatch`, `ifMetagenerationMatch`, and `ifMetagenerationNotMatch`.
8. Existing Mail, S3, and dashboard behavior remains compatible; `cargo test --workspace` passes.
9. Dashboard service registry exposes GCS, and `/api/gcs/*` can inspect buckets, objects, and upload sessions.
10. `VERIFY_STAGE=full bash scripts/gcs-autoloop/verify.sh` passes.

## Out of Scope for This Loop

- Real Google IAM or OAuth token introspection.
- Cloud KMS, CMEK, CSEK, or real encryption.
- Billing, quota, organization policy, and VPC Service Controls.
- Autoclass, Storage Transfer Service, and external Google service calls.

## Implementation Guidance

- Preserve existing S3 behavior before broad Object Core refactors.
- Prefer small vertical slices with tests.
- Keep runtime data under `.devcloud/`.
- Use Rust standard library unless a dependency is clearly justified.
- Keep dashboard code consistent with the existing shared dashboard shell.

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: GCS compatibility loop contract is ready.
