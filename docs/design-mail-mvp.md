# Mail MVP Design

## Summary

`devcloud` の最初の実装は、MailHog のようにローカル開発用 SMTP server と inbox dashboard を提供する。

この MVP は S3 / SQS / DynamoDB より先に作る。理由は、SMTP server が単一バイナリ、TCP server、永続化、dashboard、CLI、reset の基礎を小さい範囲で検証できるため。

## Goals

- `devcloud up` で SMTP server と dashboard/API server を起動する。
- アプリケーションから `localhost:11025` にメールを送信できる。
- 受信したメールを raw RFC 5322 message として保存する。
- dashboard/API から inbox 一覧、本文、raw source を確認できる。
- `devcloud reset` でローカル保存データを削除できる。

## Non-Goals

- IMAP / POP3 は実装しない。
- STARTTLS は v0 では実装しない。設定項目だけ将来拡張用に残す。
- 外部 SMTP relay への転送は実装しない。
- MIME の完全な再構成や高度な検索は実装しない。
- 認証は v0 では `off` を既定にする。`AUTH PLAIN` / `AUTH LOGIN` は受け付けても常に成功する `relaxed` mode を後続で追加する。

## User Experience

```bash
devcloud init
devcloud up
```

SMTP client は次のように接続する。

```txt
host: localhost
port: 11025
tls: false
auth: none
```

dashboard は次で開く。

```bash
devcloud dashboard
```

または直接:

```txt
http://localhost:18025
```

## Scope

### v0.1 Mail MVP

```txt
CLI:
  devcloud init
  devcloud up
  devcloud reset
  devcloud dashboard

Daemon:
  SMTP server      localhost:11025
  HTTP dashboard  localhost:18025

SMTP commands:
  HELO
  EHLO
  MAIL FROM
  RCPT TO
  DATA
  RSET
  NOOP
  QUIT

Mail API:
  GET    /api/messages
  GET    /api/messages/{id}
  GET    /api/messages/{id}/raw
  DELETE /api/messages
  DELETE /api/messages/{id}

Dashboard:
  inbox list
  message detail
  raw source view
```

### Later

```txt
AUTH PLAIN
AUTH LOGIN
STARTTLS
MIME attachment browser
full-text search
download attachment
message tags
seed fixture
snapshot restore
```

## Architecture

```txt
SMTP client
    |
    v
+----------------+
| SMTP Server    | localhost:11025
+----------------+
    |
    v
+----------------+
| Mail Service   |
+----------------+
    |        |
    |        +----------------+
    v                         v
+----------------+     +----------------+
| Mail Store     |     | Blob Store     |
| metadata JSON  |     | raw messages   |
+----------------+     +----------------+
    |
    v
+----------------+
| HTTP API       | localhost:18025/api
+----------------+
    |
    v
+----------------+
| Dashboard      |
+----------------+
```

## Repository Layout

```txt
orchestrator/
  devcloud/
    main.rs
  orchestrator/
    main.rs

services/
  app/
    daemon.rs
    config.rs
  storage/
    blob/
    mailstore/
  services/
    mail/
      service.rs
      smtp.rs
      parser.rs
      model.rs
  dashboard/
    server.rs
    static/
```

## Configuration

Default config:

```yaml
project: dev

server:
  smtpPort: 11025
  dashboardPort: 18025

auth:
  smtp:
    mode: off

storage:
  path: .devcloud/data

services:
  mail:
    enabled: true
    maxMessageBytes: 10485760
```

## Data Layout

```txt
.devcloud/
  config.yaml
  data/
    blobs/
      ab/
        cd/
          abcdef...blob
    mail/
      messages.jsonl
      index.json
  logs/
```

`messages.jsonl` は append-only で保存する。

```json
{"id":"msg_01","from":"a@example.com","to":["b@example.com"],"subject":"Hello","raw":"blob_01","receivedAt":"2026-04-30T10:00:00Z"}
```

削除は v0 では `index.json` の状態を更新する。raw blob の物理削除は後続の compaction で扱う。

## Core Models

```go
type MailMessage struct {
	ID          string
	From        string
	To          []string
	Subject     string
	Headers     map[string][]string
	Raw         BlobID
	TextBody    string
	HTMLBody    string
	Attachments []Attachment
	ReceivedAt  time.Time
	DeletedAt   *time.Time
}

type Attachment struct {
	ID          string
	FileName    string
	ContentType string
	Size        int64
	Blob        BlobID
}
```

```go
type MailStore interface {
	Append(ctx context.Context, message MailMessage, raw io.Reader) (MailMessage, error)
	List(ctx context.Context, input ListMessagesInput) (ListMessagesResult, error)
	Get(ctx context.Context, id string) (MailMessage, bool, error)
	GetRaw(ctx context.Context, id string) (io.ReadCloser, bool, error)
	Delete(ctx context.Context, id string) error
	DeleteAll(ctx context.Context) error
}
```

## SMTP Behavior

### Session State

```txt
new connection
  -> greet 220
  -> HELO/EHLO
  -> MAIL FROM
  -> RCPT TO one or more
  -> DATA
  -> persist message
  -> 250 queued
  -> next message or QUIT
```

### Supported Responses

```txt
220 devcloud ESMTP ready
250 OK
354 End data with <CR><LF>.<CR><LF>
500 syntax error
502 command not implemented
503 bad sequence of commands
552 message size exceeds limit
221 bye
```

### Validation

- `MAIL FROM` がない状態で `RCPT TO` を受けたら `503`。
- `RCPT TO` がない状態で `DATA` を受けたら `503`。
- message size が `maxMessageBytes` を超えたら `552`。
- raw message と metadata 保存のどちらかが失敗したら `451` を返す。

## Parser Policy

v0 では compatibility 標準ライブラリを優先する。

```txt
net/textproto
net/mail
mime
mime/multipart
```

raw RFC 5322 message は必ず保存する。parse に失敗しても受信自体は成功させ、metadata に `ParseError` を残す。

## HTTP API

### `GET /api/messages`

Response:

```json
{
  "messages": [
    {
      "id": "msg_01",
      "from": "a@example.com",
      "to": ["b@example.com"],
      "subject": "Hello",
      "receivedAt": "2026-04-30T10:00:00Z"
    }
  ],
  "nextCursor": ""
}
```

### `GET /api/messages/{id}`

Response:

```json
{
  "id": "msg_01",
  "from": "a@example.com",
  "to": ["b@example.com"],
  "subject": "Hello",
  "headers": {
    "Subject": ["Hello"]
  },
  "textBody": "hello",
  "htmlBody": "",
  "attachments": [],
  "receivedAt": "2026-04-30T10:00:00Z"
}
```

### `GET /api/messages/{id}/raw`

Response:

```txt
Content-Type: message/rfc822
```

Body is the raw RFC 5322 message.

## Dashboard

v0 は静的 HTML + small JavaScript でよい。
詳細な UI / UX 方針は [Mail Web UI Design](./design-mail-ui.md) に従う。
デザインモックは `mock/mail` を参照する。ただし、本実装では React 依存を前提にせず、静的 HTML/CSS/JavaScript に移植する。

```txt
left pane:
  inbox list

right pane:
  subject
  from / to / receivedAt
  text body
  raw source tab
```

## CLI

```txt
devcloud init
  - create .devcloud/config.yaml if missing

devcloud up
  - load config
  - start SMTP server
  - start dashboard/API server
  - block until SIGINT/SIGTERM

devcloud reset
  - stop is caller responsibility in v0
  - remove .devcloud/data/mail and .devcloud/data/blobs

devcloud dashboard
  - open http://localhost:18025 when possible
  - print URL as fallback
```

## Implementation Order

1. Rust workspace and command skeleton: `orchestrator`, `orchestrator`
2. Config loader with defaults
3. Data directory initializer
4. Blob store: file-backed `Put`, `Get`, `Delete`, `Stat`
5. Mail store: append JSONL + in-memory index on startup
6. SMTP server command loop
7. RFC 5322 parser and metadata extraction
8. HTTP API
9. Minimal dashboard
10. CLI `init`, `up`, `reset`, `dashboard`

## Verification

### Manual SMTP

```bash
nc localhost 11025
```

```txt
HELO localhost
MAIL FROM:<sender@example.com>
RCPT TO:<user@example.com>
DATA
Subject: Hello

hello
.
QUIT
```

Expected:

- SMTP returns `250 OK` after DATA.
- `GET http://localhost:18025/api/messages` returns the message.
- dashboard shows the received message.
- `GET /api/messages/{id}/raw` returns the original source.

### Automated Tests

```txt
storage/blob:
  put/get/stat/delete

storage/mailstore:
  append/list/get/raw/delete/reset

services/mail:
  SMTP command sequence
  invalid command sequence
  max message size
  parse failure keeps raw source

dashboard/api:
  list/detail/raw/delete
```

## Risks

| Risk | Mitigation |
| --- | --- |
| SMTP command parsing grows ad hoc | Keep session state machine isolated in `services/mail/smtp.rs` |
| message metadata and raw blob become inconsistent | Append raw blob first, then append metadata; expose parse/store errors clearly |
| large messages exhaust memory | Enforce `maxMessageBytes`; stream DATA to temp file or bounded buffer |
| dashboard scope expands too early | v0 dashboard only lists, shows detail, and raw source |

## Definition of Done

- `devcloud up` starts SMTP and dashboard/API.
- A mail client can send a plain text email to `localhost:11025`.
- Inbox API and dashboard show the message.
- Raw source is retrievable.
- `devcloud reset` clears stored messages.
- Unit tests cover blob store, mail store, SMTP happy path, and bad sequence errors.
