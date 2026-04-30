# Mail Web UI Design

## Summary

`devcloud` mail dashboard は、MailHog 風の開発用 inbox を土台にしつつ、「毎日開きたくなる」静かで美しい作業画面にする。

見た目の主張より、メール確認、raw source 確認、削除、再読み込みが速く、状態が明確で、長時間見ても疲れにくい UI を優先する。

## Product Role

この UI はマーケティングサイトではなく、開発中に繰り返し使う operational tool である。

求める体験:

- 受信メールがすぐ見つかる。
- 本文と raw source の切り替えが速い。
- SMTP server の状態が一目で分かる。
- 空の状態でも「動いている」ことが分かる。
- 見た目は洗練されているが、装飾が作業を邪魔しない。

## Success Metrics

| Metric | Target |
| --- | --- |
| First message discovery | 受信後 2 秒以内に list に表示 |
| Message open task success | 95% 以上 |
| Raw source discovery | 初回利用者が 10 秒以内に見つけられる |
| Delete all confidence | 誤操作率 1% 未満 |
| Accessibility | WCAG 2.2 AA |
| Perceived load | 初期表示 1 秒以内 |

## Design Principles

1. **Quiet confidence**: 強い装飾ではなく、余白、階層、状態表示で信頼感を作る。
2. **Developer-first clarity**: subject よりも delivery state、recipient、timestamp、raw source への導線を重視する。
3. **No mystery state**: SMTP server、message count、last received time、empty state を明示する。
4. **Fast inspection**: list と detail を同時に見せ、画面遷移を最小化する。
5. **Beautiful restraint**: 色数は絞るが単調にしない。アクセントは状態と操作に使う。

## Direction Options

### Option A: Calm Inspector

静かな 2-pane inbox。左に message list、右に detail inspector。Linear や Datadog の operational surface に近い。

Pros:

- 実装が軽い。
- 開発者が迷いにくい。
- v0 の機能範囲に合う。

Cons:

- 個性は控えめ。
- attachment や search が増えた時に再整理が必要。

### Option B: Mail Studio

メール本文を中心に置く、リッチな preview-first UI。右側に metadata、headers、raw source を置く。

Pros:

- 見た目の満足度が高い。
- HTML mail の preview が映える。

Cons:

- v0 には重い。
- raw/debug 用途では視線移動が増える。

### Option C: Protocol Console

SMTP session log と message inbox を並べる、debug console 寄り UI。

Pros:

- SMTP server としての透明性が高い。
- 将来の logs / traces に拡張しやすい。

Cons:

- 初見で硬い印象になる。
- 日常的なメール確認には情報量が多い。

## Selected Direction

採用案は **Option A: Calm Inspector**。

理由:

- v0 の `inbox list`, `message detail`, `raw source view` に最も合う。
- 静的 HTML + small JavaScript でも十分に美しく実装できる。
- S3/SQS/DynamoDB dashboard へ拡張する時も、同じ shell と inspector pattern を再利用できる。

Option C の良い点である server state と logs は、上部 status bar と下部 compact activity に限定して取り込む。

## Information Architecture

```txt
Mail Dashboard
  Header
    Brand
    SMTP endpoint
    server status
    actions

  Main
    Message List
      filter / count / refresh
      rows
      empty state

    Message Detail
      summary
      tabs
        Preview
        Headers
        Raw

  Footer / Activity
    last received
    storage path
    API status
```

## Primary Layout

Desktop layout:

```txt
+--------------------------------------------------------------------------------+
| devcloud Mail      SMTP localhost:1025  Running    Refresh  Clear all          |
+-------------------------------+------------------------------------------------+
| Inbox                         | Subject                                        |
| Search / filter               | From / To / Received                           |
|                               |------------------------------------------------|
| message row                   | [Preview] [Headers] [Raw]                      |
| message row selected          |                                                |
| message row                   | Body / header table / raw source               |
|                               |                                                |
+-------------------------------+------------------------------------------------+
| Last received 10:00:00        Storage .devcloud/data        API OK             |
+--------------------------------------------------------------------------------+
```

Mobile layout:

```txt
+--------------------------------+
| devcloud Mail      Running     |
+--------------------------------+
| Inbox                          |
| message row                    |
| message row                    |
+--------------------------------+
| tap row -> detail view         |
| Back  Subject  Delete          |
| Preview / Headers / Raw        |
+--------------------------------+
```

Breakpoints:

| Width | Behavior |
| --- | --- |
| `< 720px` | single column; list and detail are separate views |
| `720px - 1199px` | 40/60 split |
| `>= 1200px` | fixed list width around 360px; detail fills remaining space |

## Visual System

### Color Direction

Avoid one-note dark blue/slate and purple-heavy palettes. Use a neutral work surface with a fresh green signal and amber warning accent.

```txt
surface/base        #F7F8F5
surface/panel       #FFFFFF
surface/subtle      #EEF1EC
text/primary        #1D211C
text/secondary      #5F675D
border/default      #D9DED5
accent/primary      #176B4D
accent/primary-soft #DDEFE7
accent/warning      #9A5B13
accent/danger       #B42318
code/background     #101511
code/text           #E8EFE7
```

Rationale:

- Green accent maps naturally to "server running" and successful delivery.
- Warm neutral background makes the UI calmer than pure white dashboards.
- Dark code surface makes raw source feel inspectable without dominating the whole app.

### Typography

```txt
UI font:
  system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif

Mono font:
  ui-monospace, SFMono-Regular, Menlo, Consolas, monospace
```

Scale:

```txt
title        18px / 24px / 600
section      14px / 20px / 600
body         14px / 20px / 400
meta         12px / 16px / 500
code         12px / 18px / 400
```

Do not scale font size with viewport width. Use fixed type sizes and responsive layout instead.

### Spacing

```txt
space-1  4px
space-2  8px
space-3  12px
space-4  16px
space-5  24px
space-6  32px
```

### Radius and Borders

```txt
radius-sm  4px
radius-md  8px
border     1px solid border/default
```

Cards are only used for repeated message rows and empty-state panels. Do not nest cards.

## Components

### App Header

Purpose:

- Identify product.
- Show SMTP endpoint.
- Show server state.
- Expose global actions.

Elements:

```txt
logo text: devcloud Mail
endpoint chip: localhost:1025
status: Running / Stopped
actions:
  Refresh
  Clear all
```

Rules:

- `Clear all` is visually secondary but uses danger confirmation.
- Endpoint is copyable.
- Status uses icon + text, not color alone.

### Message List

Row content:

```txt
subject or "(No subject)"
from
recipient count
received time
body snippet
parse warning indicator when applicable
```

States:

```txt
default
hover
selected
unread/new within last 10 seconds
parse warning
deleted pending removal
```

Behavior:

- New messages appear at the top.
- Selected row remains visible after refresh.
- Empty subject must not collapse row height.
- Long addresses use middle truncation.

### Detail Header

Content:

```txt
subject
from
to
receivedAt
message ID
actions:
  copy ID
  delete
```

Rules:

- Message ID is visible but low emphasis.
- Delete requires confirmation only for single message if it is the current selection; `Clear all` always requires confirmation.

### Tabs

Tabs:

```txt
Preview
Headers
Raw
```

Behavior:

- `Preview` is default.
- `Raw` uses monospace and preserves line breaks.
- `Headers` uses a two-column table.
- Tab state should be reflected in URL hash when possible.

### Empty State

Empty inbox copy:

```txt
No messages yet
Send mail to localhost:1025 and it will appear here.
```

Include a compact SMTP snippet:

```txt
host localhost
port 1025
TLS off
auth none
```

Do not use large illustrations. This is an operational surface.

### Error State

Examples:

```txt
API unavailable
Unable to load messages
Raw source not found
Message was deleted
```

Rules:

- Error messages must include the next action.
- Do not expose local filesystem internals except configured storage path in diagnostics.

## Interaction Details

### Refresh

- Manual refresh button in header.
- Auto-refresh every 2 seconds while the tab is visible.
- Pause auto-refresh when document is hidden.
- Show subtle `Updated just now` text instead of a loading spinner for successful refresh.

### Selection

- First message auto-selected on desktop.
- No auto-open on mobile; tap opens detail.
- After deletion, select next newest message.

### Keyboard

```txt
j / ArrowDown  next message
k / ArrowUp    previous message
r              refresh
Delete         delete selected message
Esc            close modal or return to list on mobile
```

Keyboard shortcuts are optional in v0 implementation, but the layout must not conflict with adding them later.

### Confirmation

`Clear all` confirmation:

```txt
Clear all messages?
This removes messages from the local devcloud inbox.

[Cancel] [Clear all]
```

Use destructive color only on the final action.

## Accessibility

- Text contrast must meet WCAG 2.2 AA: 4.5:1 minimum.
- UI component contrast must meet 3:1 minimum.
- Focus ring must be visible on all interactive controls.
- Tabs require proper `role="tablist"`, `role="tab"`, and `aria-selected` when implemented.
- Message list selection must not rely on color alone.
- Buttons need accessible names.
- Raw source view must be keyboard scrollable.
- Touch targets should be at least 44px high on mobile.

## Responsive Rules

- Desktop uses persistent split view.
- Mobile uses route-like state: list view and detail view.
- Header actions collapse into an overflow menu below 420px.
- Message rows keep fixed vertical rhythm; dynamic content must not shift controls.
- Raw source uses horizontal scroll instead of wrapping protocol lines by default.

## Motion

Motion should be functional and quiet.

```txt
row hover        120ms background
new message      600ms soft highlight fade
tab switch       instant or 80ms fade
modal open       120ms opacity + scale 0.98 -> 1
```

No decorative or looping animation.

## Implementation Handoff

`mock/mail` is the visual reference for this UI. Treat it as a design mock, not the production dependency baseline.

Recommended v0 stack:

```txt
static HTML
CSS custom properties
vanilla JavaScript
server-rendered index shell from Go
JSON fetch API
```

Reason:

- No frontend build tool is required for v0.
- Single binary distribution stays simple.
- Later migration to React/Vue/Svelte remains possible if dashboard complexity grows.

Mock handling rules:

- Keep `mock/mail` useful as a visual and interaction reference.
- Do not copy the mock's React dependency graph into the Go dashboard implementation.
- Keep v0-only UI actions active; future areas such as Search, Activity, SMTP log, Help, and Settings should be hidden or disabled until implemented.
- HTML email preview must be sanitized before rendering. If sanitization is not implemented, show the plain text body and raw source instead.
- Do not commit generated OS files such as `.DS_Store`.

Suggested files:

```txt
internal/dashboard/
  server.go
  static/
    index.html
    styles.css
    app.js
```

API dependencies:

```txt
GET    /api/messages
GET    /api/messages/{id}
GET    /api/messages/{id}/raw
DELETE /api/messages
DELETE /api/messages/{id}
```

## Validation Checklist

- Inbox with 0 messages shows useful SMTP setup.
- Inbox with 1 message shows list and detail without empty gaps.
- Inbox with 100 messages remains scannable.
- Long subject, long email address, and no subject are handled.
- Plain text message is readable.
- HTML message fallback is readable.
- Raw source preserves original line breaks.
- API error is visible and recoverable.
- Keyboard focus is visible.
- Mobile list/detail flow is usable.

## Open Decisions

- Whether logs/activity panel appears in v0 or waits for service-wide dashboard.
- Whether keyboard shortcuts ship in v0 or only reserve interaction design.
