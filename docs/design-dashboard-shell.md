# Common Dashboard Shell Design

## Summary

`devcloud` の dashboard は、Mail、S3、今後追加される SQS、DynamoDB、GCS などを横断して使う operational console とする。

現在は Rust-served static HTML で Mail/S3 を提供しているが、今後の service 数、state、dialogs、routing、shared components の増加を考えると、dashboard は React/Vite へ段階移行する。最終形は「Rust binary が built assets を配信し、開発時だけ Vite を使う」構成にする。

## Goals

- `/` で利用可能な service を一覧できる。
- `/mail`、`/s3` など各 service dashboard が同じ shell、navigation、status pattern を共有する。
- service 固有 UI は独立しつつ、tokens、layout、loading/error/empty states は共通化する。
- production 実行時に Node/Vite を要求しない。
- Mail/S3 の既存 API と E2E を壊さずに移行できる。

## Non-Goals

- AWS Console の完全再現。
- 認証、ユーザー管理、multi-tenant 管理画面。
- Electron/Tauri など desktop app 化。
- 初期段階で全 dashboard を一括 React 移行すること。

## Product Role

Common Dashboard Shell は、local cloud emulator の状態を一箇所で確認する cockpit である。マーケティング画面ではなく、開発中に繰り返し開く作業画面として、静かで速く、失敗状態が分かりやすいことを優先する。

## Information Architecture

```txt
Dashboard Shell
  Service Index (/)
    service cards
    endpoint summary
    health summary

  App Frame
    top bar
      devcloud identity
      service switcher
      global status
      refresh / reset entry points

    service surface
      Mail (/mail)
      S3 (/s3)
      future services (/sqs, /dynamodb, /gcs)

    activity footer
      last request
      storage path
      API status
```

## Route Model

| Route | Purpose |
| --- | --- |
| `/` | Service index |
| `/mail` | Mail inbox dashboard |
| `/s3` | S3 Object Explorer |
| `/api/dashboard/services` | Common service registry/status |
| `/api/messages/*` | Mail API |
| `/api/s3/*` | S3 dashboard API |

Future services should follow `/api/{service}/...` unless a service has an existing protocol endpoint.

## React Architecture

```txt
web/dashboard/
  package.json
  vite.config.ts
  src/
    main.tsx
    app/
      App.tsx
      routes.tsx
      shell/
        AppShell.tsx
        ServiceIndex.tsx
        ServiceSwitcher.tsx
        StatusBar.tsx
        ActivityFooter.tsx
      services/
        mail/
          MailDashboard.tsx
          api.ts
          types.ts
        s3/
          S3Dashboard.tsx
          BucketSidebar.tsx
          ObjectBrowser.tsx
          ObjectInspector.tsx
          api.ts
          types.ts
      ui/
        Button.tsx
        EmptyState.tsx
        Panel.tsx
        Tabs.tsx
        Dialog.tsx
      styles/
        tokens.css
        globals.css

services/dashboard/
  server.rs
  assets.rs        embed built assets
```

Production build:

```bash
cd web/dashboard
npm run build
cargo test --workspace
```

Runtime:

```txt
devcloud binary -> serves embedded React assets
```

The daemon must not require Node.js at runtime.

## Shared UI Model

### App Shell

The shell owns:

- service navigation
- shared responsive grid
- refresh orchestration
- high-level service health
- common loading/error/empty patterns

Service pages own:

- service-specific data fetching
- domain tables and inspectors
- dialogs and destructive action confirmation
- protocol-specific labels and metadata

### Layout

Desktop:

```txt
+--------------------------------------------------------------------------------+
| devcloud  [Mail] [S3] [future]          status       Refresh                   |
+--------------------------------------------------------------------------------+
| service-specific dashboard surface                                             |
|                                                                                |
+--------------------------------------------------------------------------------+
| activity / storage / last request                                               |
+--------------------------------------------------------------------------------+
```

S3 may keep its 3-pane Object Explorer inside the shared shell. Mail may keep its 2-pane Calm Inspector inside the same frame.

Mobile:

- service switcher becomes a compact horizontal nav.
- service-specific panes collapse into route-like stacked views.
- destructive actions move into explicit dialogs or bottom sheets.

## Design Tokens

Use one token source shared by Mail/S3.

```txt
surface/base        #F7F8F5
surface/panel       #FFFFFF
surface/subtle      #EEF1EC
text/primary        #1D211C
text/secondary      #5F675D
border/default      #D9DED5
accent/primary      #176B4D
accent/object       #245B8F
accent/warning      #9A5B13
accent/danger       #B42318
code/background     #101511
code/text           #E8EFE7
radius/md           8px
```

Service-specific accents are allowed only as secondary signals. The whole dashboard must not become one-note blue/slate, purple, beige, or espresso.

## Service Registry

Add a small registry so `/` and the shell do not hardcode every service.

```go
type DashboardService struct {
    ID          string `json:"id"`
    Name        string `json:"name"`
    Path        string `json:"path"`
    Status      string `json:"status"`
    Endpoint    string `json:"endpoint,omitempty"`
    Description string `json:"description"`
}
```

Initial response:

```json
{
  "services": [
    {"id":"mail","name":"Mail","path":"/mail","status":"running","endpoint":"smtp://127.0.0.1:1025"},
    {"id":"s3","name":"S3","path":"/s3","status":"running","endpoint":"http://127.0.0.1:4566"}
  ]
}
```

Disabled services should appear as `disabled` only if configured but not running. Unconfigured future services should not be shown by default.

## API Client Strategy

Each service owns a typed API client:

```txt
services/mail/api.ts
services/s3/api.ts
```

Shared fetch wrapper:

- adds timeout
- normalizes JSON/text errors
- never logs payload bodies or credentials
- returns typed errors for disabled services and network failures

## Migration Plan

### Phase 1: Shell Contract

- Add `docs/design-dashboard-shell.md`.
- Add `/api/dashboard/services`.
- Keep current static Mail/S3 pages.

### Phase 2: React Scaffold

- Add `web/dashboard` with Vite + React + TypeScript.
- Build a shell-only React app that renders `/`, `/mail`, `/s3` placeholders.
- Embed built React assets from `services/dashboard/assets/react`.
- Keep existing static pages available behind compatibility routes during transition if needed.

### Phase 3: S3 React Page

- Port S3 Object Explorer first because it has the most state.
- Preserve existing `/api/s3/*` contract.
- Add Playwright or script-level visual smoke once the route is React-driven.

### Phase 4: Mail React Page

- Port Mail inbox after the shell and S3 patterns stabilize.
- Preserve `/api/messages/*`.

### Phase 5: Shared Component Hardening

- Extract shared `Panel`, `Toolbar`, `Inspector`, `EmptyState`, `Dialog`, `ServiceSwitcher`.
- Add accessibility and keyboard navigation tests.

## Acceptance Criteria

1. `/` shows all enabled services and links to `/mail` and `/s3`.
2. `/mail` and `/s3` share shell navigation and visual tokens.
3. Production `devcloud up` serves dashboard assets without Node.js.
4. `cargo test --workspace`, `scripts/mail-e2e.sh`, and `scripts/s3-e2e.sh` pass.
5. Disabled services are not exposed as running through dashboard APIs.
6. Dynamic service data is rendered through React escaping or safe DOM APIs, not raw `innerHTML`.

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| React build adds runtime complexity | local binary becomes harder to run | embed built assets; Node only for development |
| Big-bang rewrite breaks Mail/S3 | dashboard regressions | migrate one route at a time |
| API contracts drift during UI port | E2E failures | keep existing dashboard APIs and scripts |
| UI bundle grows quickly | slower startup and review noise | keep dependency set small; avoid broad component libraries initially |
| XSS via object/message data | security issue | rely on React escaping and avoid `dangerouslySetInnerHTML` |

## Open Questions

1. Should React dashboard assets be committed after build, or generated in CI/release only?
2. Should `/mail` and `/s3` remain direct routes forever, or move to `/services/mail` style later?
3. Should the mock directories remain separate once `web/dashboard` exists?
4. Should React migration start with S3 only, or shell + service index first?
