# Goal

## Objective

Complete the `devcloud` Mail MVP implementation.

## Why

The project needs a working MailHog-like local SMTP server before broader cloud emulator services are added. This loop should turn the current Go scaffold into a usable SMTP + dashboard/API MVP while preserving the documented static-dashboard direction.

## Acceptance Criteria

1. Foundation remains healthy: `go test ./...`, `devcloud help`, `devcloudd -h`, and `cmd/devcloud` build all pass.
2. SMTP server accepts `HELO` / `EHLO` / `MAIL FROM` / `RCPT TO` / `DATA` / `RSET` / `NOOP` / `QUIT` and persists accepted messages.
3. Message size is bounded by `services.mail.maxMessageBytes`; oversize messages return `552`.
4. Stored messages preserve raw RFC 5322 source and expose parsed metadata through `GET /api/messages`, `GET /api/messages/{id}`, and `GET /api/messages/{id}/raw`.
5. Dashboard serves a usable static inbox shell without React runtime dependency and follows `docs/design-mail-ui.md` plus `mock/mail` as visual reference.
6. CLI commands `devcloud init`, `devcloud up`, `devcloud reset`, and `devcloud dashboard` remain functional.
7. Full verification passes: `VERIFY_STAGE=full bash scripts/mail-autoloop/verify.sh`.

## Implementation Phases

Work in this order. Each Codex iteration should complete one small slice and keep earlier stages passing.

1. `foundation`: keep the current Go scaffold buildable and tested.
2. `smtp-protocol`: implement SMTP session state and command responses.
3. `smtp-persist`: connect accepted `DATA` payloads to `mail.Service.Receive`.
4. `api-smoke`: ensure list/detail/raw APIs expose received messages.
5. `dashboard-static`: replace the placeholder page with static HTML/CSS/JS following the mock.
6. `hardening`: add edge-case tests for bad sequence, oversize message, reset/delete, and parse failure.

## Out of Scope

- IMAP / POP3.
- STARTTLS.
- External SMTP relay.
- Full attachment browser.
- S3 / SQS / DynamoDB / GCS / Pub/Sub / BigQuery / Redshift / CloudFront.

## Implementation Constraints

- Use Go standard library unless a new dependency is explicitly justified in code comments and docs.
- Do not port the React dependency graph from `mock/mail` into production.
- Sanitize any rendered HTML mail preview or fall back to text/raw source.
- Keep local generated data under `.devcloud/`.
- Avoid logging secrets or full sensitive payloads.

## Verification Command

```bash
VERIFY_STAGE=foundation bash scripts/mail-autoloop/verify.sh
VERIFY_STAGE=full bash scripts/mail-autoloop/verify.sh
```

NEXUS_LOOP_STATUS: READY
NEXUS_LOOP_SUMMARY: Phased Mail MVP implementation contract is ready for Codex execution.
