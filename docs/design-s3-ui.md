# S3 Web UI Design

## Summary

`devcloud` S3 dashboard は、S3 互換サーバーの状態を視覚的に確認し、bucket/object の探索、metadata 確認、download/delete、multipart 状態の把握を素早く行うための operational UI とする。

Mail dashboard と同じくマーケティング画面ではなく、開発中に何度も開く作業画面である。装飾よりも「今どの bucket に何があり、SDK/CLI がどう書き込んだか」がすぐ分かることを優先する。

## Product Role

この UI の役割は S3 API の代替ではなく、S3-compatible server の observability layer である。

求める体験:

- bucket と object がすぐ見つかる。
- prefix 階層をファイルブラウザのように移動できる。
- object の metadata、ETag、Content-Type、size、last modified が一目で分かる。
- raw/download、copy path、delete が迷わずできる。
- SDK/CLI から送った結果が即座に反映される。
- multipart upload や versioning の状態が hidden state にならない。

## Success Metrics

| Metric | Target |
| --- | --- |
| First object discovery | upload 後 2 秒以内に object list に表示 |
| Bucket navigation task success | 95% 以上 |
| Metadata discovery | 初回利用者が 10 秒以内に ETag / Content-Type を見つけられる |
| Delete confidence | 誤削除率 1% 未満 |
| Endpoint setup clarity | 初回利用者が 30 秒以内に AWS CLI 設定を理解できる |
| Accessibility | WCAG 2.2 AA |
| Perceived load | 初期表示 1 秒以内 |

## Design Principles

1. **Operational clarity**: S3 API の結果を、bucket/object/prefix/metadata の形で明確に見せる。
2. **Filesystem familiarity**: object key は階層ではないが、prefix navigation は file explorer と同じ感覚で扱えるようにする。
3. **Protocol transparency**: ETag、version ID、storage class、metadata、request compatibility のような S3 固有情報を隠さない。
4. **Safe destructive actions**: bucket delete、object delete、delete all は対象と影響範囲を明確にする。
5. **Calm multi-service shell**: Mail UI と同じ落ち着いた作業画面のトーンを維持し、将来 SQS/DynamoDB/GCS と並べても破綻しない。

## Direction Options

### Option A: Object Explorer

左に bucket list、中央に prefix/object table、右に object inspector を置く 3-pane explorer。

Pros:

- S3 の主要タスクに最も合う。
- bucket/object/metadata を同時に見られる。
- 静的 HTML + small JavaScript で実装しやすい。

Cons:

- モバイルでは画面分割を切り替える必要がある。
- multipart/versioning が増えると inspector の情報量が多くなる。

### Option B: Bucket Studio

bucket を中心に overview、objects、policy、CORS、lifecycle、versions の tabs を展開する管理画面。

Pros:

- AWS Console に近い。
- policy/versioning/lifecycle の設定画面へ拡張しやすい。

Cons:

- v0.1 には重い。
- object 探索の速度が落ちる。

### Option C: Protocol Debugger

request log、SigV4 canonical request、XML response を前面に出す debug console。

Pros:

- S3互換性の問題調査に強い。
- SDK/CLI 互換の開発中に有用。

Cons:

- 日常的な object 確認には硬すぎる。
- 初見で使いにくい。

## Selected Direction

採用案は **Option A: Object Explorer**。

理由:

- v0.1 の bucket/object CRUD、ListObjectsV2、metadata 確認に最も合う。
- Mail dashboard の inspector pattern と統一できる。
- 後続で Option B の bucket tabs と Option C の request diagnostics を追加できる。

## Information Architecture

```txt
S3 Dashboard
  App Shell
    service switcher
    endpoint / region / auth mode
    global actions

  Main
    Bucket Sidebar
      bucket count
      create bucket
      bucket rows

    Object Browser
      breadcrumb / prefix
      search / prefix filter
      object table
      empty state

    Inspector
      selected bucket summary
      selected object summary
      tabs
        Overview
        Metadata
        Versions
        Multipart
        Raw / Preview

  Footer / Activity
    last request
    storage path
    API state
```

## Primary Layout

Desktop:

```txt
+--------------------------------------------------------------------------------+
| devcloud S3   :4566  us-east-1  relaxed auth     Refresh  Create bucket        |
+----------------------+----------------------------------+----------------------+
| Buckets              | demo / assets /                  | Object inspector     |
| + Create             | Search prefix                    | README.md            |
|                      |----------------------------------|----------------------|
| demo                 | Name              Size  Modified | Overview             |
| website-assets       | folder/                         | ETag                 |
| logs                 | README.md         8 KB  10:00    | Content-Type         |
|                      | app.js            42 KB 10:01    | Metadata             |
|                      |                                  | Download Delete      |
+----------------------+----------------------------------+----------------------+
| Last request PUT /demo/README.md   Storage .devcloud/data   S3 API OK          |
+--------------------------------------------------------------------------------+
```

Tablet:

```txt
+----------------------------------------------------------+
| Header                                                   |
+----------------------+-----------------------------------+
| Buckets              | Object Browser + Inspector drawer |
+----------------------+-----------------------------------+
```

Mobile:

```txt
Buckets -> Objects -> Object detail
```

Breakpoints:

| Width | Behavior |
| --- | --- |
| `< 720px` | route-like stacked views: buckets, objects, detail |
| `720px - 1199px` | bucket sidebar + object table; inspector as drawer |
| `>= 1200px` | 3-pane layout; bucket 260px, inspector 360px |

## Visual System

Use the same base system as Mail UI, with S3-specific object signals.

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
```

S3では object/prefix の視認性が重要なため、Mail よりも blue accent を補助的に使う。ただし画面全体が blue/slate に寄らないよう、背景とpanelはMailと共通にする。

Typography:

```txt
title        18px / 24px / 600
section      14px / 20px / 600
body         14px / 20px / 400
meta         12px / 16px / 500
code         12px / 18px / 400
table        13px / 18px / 400
```

## Components

### App Header

Purpose:

- service identity
- endpoint visibility
- auth/region state
- global actions

Elements:

```txt
title: devcloud S3
endpoint chip: http://127.0.0.1:4566
region chip: us-east-1
auth chip: relaxed / strict / off
status: Running / Stopped
actions:
  Refresh
  Create bucket
```

### Service Switcher

Future multi-service shell:

```txt
Mail
S3
```

v0.1 may render this as simple segmented navigation in the header. Do not build a full sidebar until 3+ services exist.

### Bucket Sidebar

Purpose:

- bucket discovery
- bucket-level actions
- quick count/status

Bucket row:

```txt
bucket name
object count
region / versioning status
last modified or created at
```

States:

- selected
- empty
- loading
- delete pending
- error

Actions:

- create bucket
- refresh buckets
- delete empty bucket
- copy bucket ARN/path equivalent

### Create Bucket Dialog

Fields:

```txt
Bucket name
Region
Object ownership / ACL mode (later)
Versioning toggle (later)
```

Validation:

- 3-63 chars
- lowercase letters, numbers, dots, hyphens
- no uppercase
- no adjacent dots
- no IP-address-like name

### Object Browser

Purpose:

- prefix navigation
- object search/filter
- object selection
- object operations

Header:

```txt
breadcrumb: demo / assets / images /
prefix filter
upload button (later)
refresh
```

Table columns:

```txt
Name
Size
Content-Type
Last Modified
ETag
Storage Class
```

Rows:

- prefix row: folder-like icon, trailing slash
- object row: file icon, selectable
- delete marker row after versioning support
- multipart pending row in multipart view

Sorting:

- default: lexicographic by key
- size
- last modified

Filtering:

- client-side filter for currently loaded page
- server-side prefix query for navigation

Pagination:

- ListObjectsV2 continuation token
- "Load more" button rather than infinite scroll

### Object Inspector

Tabs:

```txt
Overview
Metadata
Versions
Multipart
Preview / Raw
```

Overview:

```txt
Key
Bucket
Size
ETag
Last Modified
Content-Type
Storage Class
Version ID
Endpoint URL
S3 URI
```

Metadata:

```txt
System metadata
User metadata x-amz-meta-*
Response headers
```

Preview / Raw:

- text object preview for small UTF-8 objects
- JSON pretty preview when content-type is JSON
- image preview for image types
- binary fallback with download action

Limits:

- preview up to `256 KiB`
- larger objects show metadata and download only

Actions:

- download
- copy S3 URI
- copy endpoint URL
- copy AWS CLI command
- delete object

### Versions Tab

Initially hidden or disabled until versioning support exists.

Fields:

```txt
Version ID
Is latest
Delete marker
Size
ETag
Last Modified
Actions
```

### Multipart Tab

Purpose:

- make incomplete multipart uploads visible
- support cleanup during local development

Fields:

```txt
Upload ID
Key
Initiated
Parts
Uploaded size
Actions: abort
```

### Empty States

No buckets:

```txt
No buckets yet
Create a bucket or run:
aws --endpoint-url http://127.0.0.1:4566 s3api create-bucket --bucket demo
```

Empty bucket:

```txt
This bucket is empty
Upload with:
aws --endpoint-url http://127.0.0.1:4566 s3 cp README.md s3://demo/README.md
```

No object selected:

```txt
Select an object to inspect metadata, versions, and download options.
```

### Error States

API unavailable:

```txt
S3 API unavailable
Check devcloud up and port 4566.
```

Auth mismatch:

```txt
Signature verification failed
Check AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, region, and endpoint URL.
```

Bucket not empty delete:

```txt
Bucket is not empty
Delete objects first or use reset for local data cleanup.
```

## Dashboard API

The UI should not call S3 XML endpoints directly for dashboard state. Use dashboard JSON APIs that wrap the object core.

```txt
GET    /api/s3/status
GET    /api/s3/buckets
POST   /api/s3/buckets
GET    /api/s3/buckets/{bucket}
DELETE /api/s3/buckets/{bucket}

GET    /api/s3/buckets/{bucket}/objects?prefix=&delimiter=/&continuationToken=
GET    /api/s3/buckets/{bucket}/objects/{key}
GET    /api/s3/buckets/{bucket}/objects/{key}/preview
GET    /api/s3/buckets/{bucket}/objects/{key}/download
DELETE /api/s3/buckets/{bucket}/objects/{key}

GET    /api/s3/buckets/{bucket}/multipart
DELETE /api/s3/buckets/{bucket}/multipart/{uploadId}
```

Object key path handling must avoid ambiguous slash routing. Use one of:

```txt
GET /api/s3/buckets/{bucket}/objects?key=a/b/c.txt
GET /api/s3/buckets/{bucket}/objects/{base64url-key}
```

Recommended: query parameter for v0.1 because it is easiest to debug.

## Data Contracts

Bucket list:

```json
{
  "buckets": [
    {
      "name": "demo",
      "region": "us-east-1",
      "createdAt": "2026-04-30T10:00:00Z",
      "objectCount": 12,
      "totalBytes": 1048576,
      "versioning": "Off"
    }
  ]
}
```

Object list:

```json
{
  "bucket": "demo",
  "prefix": "assets/",
  "objects": [
    {
      "key": "assets/app.js",
      "size": 42120,
      "etag": "\"...\"",
      "contentType": "application/javascript",
      "lastModified": "2026-04-30T10:00:00Z",
      "storageClass": "STANDARD"
    }
  ],
  "commonPrefixes": ["assets/images/"],
  "isTruncated": false,
  "nextContinuationToken": ""
}
```

Object detail:

```json
{
  "bucket": "demo",
  "key": "README.md",
  "size": 1234,
  "etag": "\"...\"",
  "contentType": "text/markdown",
  "metadata": {
    "x-amz-meta-source": "local"
  },
  "lastModified": "2026-04-30T10:00:00Z",
  "versionId": "null",
  "s3Uri": "s3://demo/README.md",
  "endpointUrl": "http://127.0.0.1:4566/demo/README.md"
}
```

## Interaction Details

### Upload Visibility

When an external SDK uploads an object:

1. UI polls `/api/s3/status` or `/api/s3/buckets/{bucket}/objects`.
2. New object row appears with subtle highlight for 3 seconds.
3. Footer updates last request if request log is available.

### Delete Flow

Object delete:

1. User clicks delete.
2. Confirm dialog shows exact bucket/key.
3. Delete call runs.
4. Object row disappears.
5. If versioning is enabled, inspector should explain delete marker behavior.

Bucket delete:

1. Allowed directly only when bucket is empty.
2. Non-empty bucket shows `BucketNotEmpty` explanation.
3. Destructive bulk delete is not v0.1 UI scope.

### Copy Commands

Inspector should provide copy buttons:

```bash
aws --endpoint-url http://127.0.0.1:4566 s3 cp s3://demo/README.md ./README.md
aws --endpoint-url http://127.0.0.1:4566 s3api head-object --bucket demo --key README.md
```

## Accessibility

- Bucket and object lists are keyboard navigable.
- Table rows expose accessible names including key, size, and content type.
- Destructive actions require keyboard-accessible confirm dialogs.
- Color is not the only status signal.
- Code/command snippets have visible labels and copy button accessible names.
- Focus moves predictably when switching from bucket list to object list to inspector.

## Performance

- Initial load fetches status and bucket list only.
- Object list fetches current bucket/prefix only.
- Preview fetch is lazy and size-limited.
- Use stable dimensions for table rows and inspector tabs to avoid layout shift.
- Avoid rendering all objects; use pagination and filtering within current page.

## Implementation Strategy

### v0.1 Static Dashboard

Use the same production direction as Mail:

- Go-served static HTML/CSS/JS
- no React runtime dependency
- direct `fetch` calls to dashboard JSON API
- no build step

Files:

```txt
internal/dashboard/
  server.go
  static.go
  s3_static.go
```

Routes:

```txt
/             existing service landing or Mail default
/mail         Mail dashboard
/s3           S3 dashboard
```

If routing simplicity is preferred for the next implementation slice, `/` may remain Mail and `/s3` can be added independently.

### Later

When 3+ services exist, introduce a shared dashboard shell:

```txt
internal/dashboard/static/
  shell.go
  mail.go
  s3.go
  components.go
```

## E2E Test Plan

Script:

```bash
scripts/s3-e2e.sh
```

Flow:

1. Build `devcloud`.
2. Start temporary workspace.
3. Create bucket through AWS CLI or signed HTTP request.
4. Put object.
5. Open `/s3` and verify bucket/object shell.
6. Call dashboard API and verify object appears.
7. Fetch preview/download.
8. Delete object and bucket.

Browser-oriented checks:

- page contains `devcloud S3`
- bucket row appears
- object row appears
- inspector contains ETag and Content-Type
- copy command text contains endpoint URL

## Acceptance Criteria

1. `/s3` renders a usable S3 dashboard shell.
2. Dashboard shows S3 endpoint, region, auth mode, and running status.
3. Bucket list reflects buckets created through S3 API.
4. Object browser supports prefix navigation and ListObjectsV2 pagination.
5. Object inspector shows metadata, ETag, size, content type, and last modified.
6. Text/JSON/image preview works within size limits.
7. Download and delete actions work through dashboard API.
8. Empty, loading, API error, auth error, and bucket-not-empty states are represented.
9. UI remains usable on mobile and desktop.
10. E2E script verifies S3 API -> dashboard visibility.

## Open Questions

1. Should `/` become a service switcher once S3 is added, or should Mail remain default?
2. Should upload be supported in the dashboard v0.1, or should v0.1 only inspect SDK/CLI uploads?
3. Should object preview sanitize HTML objects or always show HTML as raw text?
4. Should dashboard delete support recursive prefix deletion, or avoid bulk delete until versioning semantics are implemented?
5. Should we create `mock/s3` before production UI implementation?
