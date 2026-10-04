# devcloud-mailbox

Standalone SMTP catcher with a dedicated embedded web UI and a MailHog v1.0.1
compatible API. The executable needs no orchestrator or runtime JavaScript tools.

```sh
cargo build -p devcloud-mailbox --offline
DEVCLOUD_MAILBOX_STORAGE=/tmp/mailbox-data target/debug/devcloud-mailbox
# SMTP: 127.0.0.1:1025; UI: http://127.0.0.1:8025/

docker build -f services/mailbox/Dockerfile -t devcloud-mailbox .
docker run --rm \
  -p 127.0.0.1:1025:1025 -p 127.0.0.1:8025:8025 \
  -v devcloud-mailbox-data:/data devcloud-mailbox
docker buildx build --platform linux/amd64,linux/arm64 -f services/mailbox/Dockerfile .
```

The image cross-compiles on the native build platform, runs as uid **10001**, and
listens on `0.0.0.0:1025` and `0.0.0.0:8025`. Publish ports on `127.0.0.1` as shown.
For a bind mount, create the directory and make it writable by uid 10001 before
starting the container (for example `sudo chown -R 10001:10001 ./mailbox-data`).
Choose a dedicated directory: `/data` is **single writer**. Never share it with a
running orchestrator or another mailbox process. Metadata is persisted in
`<storage>/mail/messages.jsonl`, raw DATA in `<storage>/blobs/`. Restarting retains
messages; deleting tombstones metadata and does not reclaim raw blobs.

| Environment variable | Binary default | Meaning |
| --- | --- | --- |
| `DEVCLOUD_MAILBOX_SMTP_ADDR` | `127.0.0.1:1025` | SMTP bind address (IP and port) |
| `DEVCLOUD_MAILBOX_HTTP_ADDR` | `127.0.0.1:8025` | HTTP bind address (IP and port) |
| `DEVCLOUD_MAILBOX_STORAGE` | `/data` | Dedicated persistent storage root |
| `DEVCLOUD_MAILBOX_EPHEMERAL` | unset (persistent) | `1`, `true`, or `yes` (case-insensitive) uses a fresh private temporary storage root; all other values are persistent |
| `DEVCLOUD_MAILBOX_MAX_BYTES` | `10485760` | Maximum SMTP message bytes; `0` is unlimited |
| `DEVCLOUD_MAILBOX_AUTH_MODE` | `relaxed` | `off`, `relaxed`, or `strict` |
| `DEVCLOUD_MAILBOX_USERNAME` | empty | SMTP and HTTP Basic username |
| `DEVCLOUD_MAILBOX_PASSWORD` | empty | SMTP and HTTP Basic password |
| `DEVCLOUD_MAILBOX_HOSTNAME` | `mailhog.example` | Hostname in synthesized Received headers |
| `DEVCLOUD_MAILBOX_ALLOWED_HOSTS` | empty | Additional comma-separated HTTP Host names; `*` disables the allowlist |

Docker overrides both bind defaults to `0.0.0.0`. Strict mode requires both
credentials at startup and gates the entire HTTP port, including static files
and SSE. SMTP AUTH uses the existing devcloud behavior; strict SMTP still accepts
MAIL before AUTH. Configuration logging masks credentials and never logs requests
or mail contents. Startup checks both storage directories for writability and
checks the metadata log; corrupt metadata errors identify only the line number.
SIGINT/SIGTERM cancel listeners and SSE with a drain bounded to three seconds.

For a MailHog-like empty inbox on every start, enable ephemeral mode. It ignores
`DEVCLOUD_MAILBOX_STORAGE`, stores metadata and blobs in a fresh private temporary
directory, and removes that directory on SIGINT/SIGTERM. A SIGKILL, OOM kill, or
crash leaves that directory behind in the temporary directory (inside the
container's writable layer under Docker) until it is removed manually or the
container is deleted; the next start still uses a new, empty directory. The startup
configuration line reports `ephemeral=true`. For Docker Compose:

```yaml
services:
  mailbox:
    build:
      context: .
      dockerfile: services/mailbox/Dockerfile
    environment:
      DEVCLOUD_MAILBOX_EPHEMERAL: "true"
    ports:
      - "127.0.0.1:1025:1025"
      - "127.0.0.1:8025:8025"
```

For `docker run`, add `-e DEVCLOUD_MAILBOX_EPHEMERAL=true`.

HTTP permits IP literals, `localhost`, `*.localhost`, `host.docker.internal`, and
dotless single-label names by default. Set `DEVCLOUD_MAILBOX_ALLOWED_HOSTS` to the
hostname used by your reverse proxy, without a port. Other hosts receive 403 before
authentication. No CORS headers are served; OPTIONS returns 405.

```sh
# Dotted hostname used by a reverse proxy:
DEVCLOUD_MAILBOX_ALLOWED_HOSTS=mailhog.local
# Kubernetes service DNS under *.svc.cluster.local (use the actual service name):
DEVCLOUD_MAILBOX_ALLOWED_HOSTS=mailbox.default.svc.cluster.local
# Multiple exact hostnames:
DEVCLOUD_MAILBOX_ALLOWED_HOSTS=mailhog.local,mailbox.default.svc.cluster.local
# Disable the Host allowlist check:
DEVCLOUD_MAILBOX_ALLOWED_HOSTS='*'
```

Suffix wildcards such as `*.svc.cluster.local` are not supported; list the exact
service DNS name, including any short forms clients use (for example
`mailbox.default` or `mailbox.default.svc`, which contain dots). A rejected Host receives a plain-text 403 naming the host and
these configuration options.

Supported compatibility routes:

- `GET /api/v2/messages?start=0&limit=50` (limit cap 250).
- `GET /api/v2/search?kind=from|to|containing&query=...` (undecoded, case-insensitive).
- `GET|DELETE /api/v1/messages`, `GET|DELETE /api/v1/messages/{id}` (v1 list cap 1000).
- `GET /api/v1/messages/{id}/download` and `.../mime/part/{n}/download`.
- `GET /api/v1/events` (SSE; full Message JSON on receipt).

The UI uses `/api/mailbox/messages` (limit cap 100, optional `q`),
`/api/mailbox/messages/{id}`, and its `/html`, `/raw`, `/attachments/{index}` routes.
UI ids equal the compatibility IDs. The UI projection decodes transfer encodings,
RFC2047 headers, RFC2231 filenames and declared charsets, including Japanese
ISO-2022-JP, Shift_JIS and EUC-JP. Latin-1 uses strict byte-to-code-point mapping.
Unknown or broken encodings retain a usable source and report warnings in detail.
MIME decoding caps depth at 32, nodes at 1000, headers at 1 MiB/10,000 fields;
over-limit data is truncated or retained as an opaque leaf with warnings.
HTML responses inject a blank-target base and use a sandbox CSP permitting new
tabs but no scripts or same-origin access. The parsed HTML is otherwise unchanged.
Downloads always use attachment disposition and nosniff; the content-type
allowlist excludes HTML and SVG.

Differences from MailHog:

- No Release/relay, Jim, STARTTLS, WebSocket, outgoing SMTP settings, MongoDB,
  maildir, bcrypt auth-file or `MH_*` environment compatibility.
- Unknown messages and invalid part/attachment indices return safe 404 responses.
  The `start == total` paging bug is not reproduced; search beyond matches returns
  total 0 as in MailHog.
- Storage persists by default. Old devcloud records lacking `envelopeFrom` use
  their From header as an approximation and an empty HELO. A captured null sender
  is separately stored as `envelopeFrom: ""` and projects `Raw.From: ""`.
- SMTP accepts null senders, unlike MailHog. HELO is stripped of controls and
  capped at 255 characters. SMTP line/idle/recipient limits are 1 MiB/300 s/1000.
- Compatibility Body and Headers remain undecoded, and part numbers include the
  naive MailHog preamble/closing chunks. Part downloads decode base64 only, retain
  QP, sanitize header values and reserialize filenames with safe ASCII fallback
  and RFC5987 `filename*`. Unsafe declared types become `application/octet-stream`.
  Whole-message `.eml` downloads use only the MailHog ASCII `filename` parameter.
  Part responses echo only Content-Transfer-Encoding, Content-ID,
  Content-Description and MIME-Version, plus sanitized Content-Type and
  Content-Disposition; arbitrary mail headers are never HTTP response headers.
  Compatibility nesting is capped at 8 and response buffers at 256 MiB;
  oversized responses return 500 with a generic error (SSE events are skipped).
- SSE uses chunked compact single-line `data:` JSON and a `:` comment every 15 s,
  which is EventSource-equivalent to MailHog's multiline data/keepalive field.
  There are at most 64 concurrent streams; lagged consumers disconnect, writes
  time out after 5 s, and reconnection requires refetching current messages.
- All responses have nosniff; API and UI responses have CSP. HTTP headers/body
  are capped at 64 KiB/1 MiB, header reads at 10 s, connections at 512.
- Japanese charset decoding and forgiving bounded MIME processing are additions
  to the compatibility UI, rather than MailHog wire-format changes.

Validation:

```sh
cargo fmt --all -- --check
cargo clippy -p devcloud-mailbox -p devcloud-mail --offline -- -D warnings
cargo test --workspace --offline
```

UI assets are embedded from `assets/ui`. Build them from `web/mailbox` before a
release; rebuilding must preserve the committed asset contents. Rust tests check
that every asset referenced by the embedded index is present.
