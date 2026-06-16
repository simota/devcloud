# Goal

## Objective

Complete the `devcloud` S3-compatible object storage server implementation.

## Why

The project needs a local S3-compatible server that AWS CLI, AWS SDKs, and common S3 clients can use with only endpoint override configuration. This loop should turn the current Mail-focused emulator into a multi-service local cloud emulator while preserving the documented Rust-served static dashboard direction.

## Acceptance Criteria

1. Foundation remains healthy: `cargo test --workspace`, `devcloud help`, `cargo run -p devcloud-orchestrator -- help`, and `orchestrator` build all pass.
2. Config and daemon support S3 settings, including endpoint port `14566`, region, auth mode, storage path, and service enablement while preserving existing Mail defaults.
3. S3 REST XML endpoint supports path-style ListBuckets, CreateBucket, HeadBucket, DeleteBucket, ListObjectsV2/ListObjects, PutObject, HeadObject, GetObject, DeleteObject, CopyObject, user metadata, ETag, content headers, and Range GET.
4. SigV4 signed requests and presigned URLs validate against local credentials, with a documented relaxed mode for local development.
5. Multipart upload supports create, upload part, list parts, complete, abort, and list multipart uploads with filesystem-backed persistence.
6. Dashboard/API exposes bucket and object state through a usable static S3 Object Explorer following `docs/design-s3-ui.md` and `mock/s3`, without porting the React runtime dependency graph.
7. CLI commands `devcloud init`, `devcloud up`, `devcloud reset`, and `devcloud dashboard` remain functional for both Mail and S3.
8. Full verification passes: `VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh`.

## Implementation Phases

Work in this order. Each Codex iteration should complete one small slice and keep earlier stages passing.

1. `foundation`: keep the Rust scaffold buildable and tested.
2. `config`: add S3 config, workspace directories, and daemon wiring without serving the full API yet.
3. `s3-core`: implement bucket/object storage models, REST XML routing, and core CRUD behavior.
4. `sigv4`: implement SigV4 header and presigned URL verification with safe local defaults.
5. `multipart`: implement multipart upload state and complete/abort flows.
6. `dashboard-static`: add Rust-served S3 dashboard/API following the Object Explorer design.
7. `hardening`: add compatibility/error tests for XML errors, malformed requests, metadata, range edge cases, reset behavior, and path traversal prevention.

## Out of Scope

- Real AWS IAM integration.
- KMS, SSE-KMS, SSE-C encryption semantics beyond header/metadata compatibility.
- S3 Express One Zone / directory buckets.
- Legal Object Lock compliance guarantees.
- Glacier / Deep Archive storage tiers.
- Lifecycle execution, event notification delivery, website hosting, inventory, SelectObjectContent.
- Porting the React/Vite mock into production.

## Implementation Constraints

- Use Rust standard library unless a new dependency is explicitly justified in code comments and docs.
- Do not port the React dependency graph from `mock/s3` into production.
- Store all runtime data under `.devcloud/`.
- Validate external inputs at HTTP boundaries and prevent path traversal in bucket/object storage.
- Return S3-compatible XML error codes where the S3 API surface expects XML.
- Avoid logging secrets, signatures, authorization headers, object bodies, or sensitive metadata values.
- Keep Mail behavior and existing Mail verification gates working.

## Verification Command

```bash
VERIFY_STAGE=foundation bash scripts/s3-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/s3-autoloop/verify.sh
```

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Phased S3-compatible server implementation contract is ready for Codex execution.
