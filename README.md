# devcloud

Local cloud service emulator focused first on a MailHog-like SMTP inbox.

## Mail Server

Start the local SMTP server and Web UI:

```bash
go run ./cmd/devcloud up
```

Open the dashboard:

```text
http://127.0.0.1:8025/
```

Send mail to:

```text
127.0.0.1:1025
```

Useful commands:

```bash
go run ./cmd/devcloud init
go run ./cmd/devcloud dashboard
go run ./cmd/devcloud reset
```

## Verification

Run unit and integration tests:

```bash
go test ./...
```

Run the Mail MVP acceptance gate:

```bash
VERIFY_STAGE=full bash scripts/mail-autoloop/verify.sh
```

Run the E2E smoke test:

```bash
scripts/mail-e2e.sh
```

Run the E2E script in browser inspection mode. This keeps the server running and leaves the smoke mail visible in the Web UI until `Ctrl-C`.

```bash
E2E_INTERACTIVE=true scripts/mail-e2e.sh
```

If the default ports are already in use:

```bash
E2E_INTERACTIVE=true E2E_SMTP_PORT=1125 E2E_DASHBOARD_PORT=8125 scripts/mail-e2e.sh
```

## Notes

- Runtime data is stored under `.devcloud/`.
- `scripts/mail-autoloop/` contains the Codex-driven implementation loop and final verification gate.
- `mock/mail/` contains the design mock used as the Web UI reference.
