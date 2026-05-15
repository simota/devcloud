# Goal — Redshift → PostgreSQL Translator Compatibility

## Objective

`internal/services/redshift/translator/translator.go` の `RedshiftToPostgres` translator が対応する Redshift→PostgreSQL 書き換えルールを、`internal/services/redshift/translator/COMPATIBILITY.md` の **R-only** / **R≠P** チェックリスト項目に従って 1 反復 1 件ずつ追加していく。

## Why

devcloud の Redshift サービスは managed PostgreSQL backend で動作する。translator が書き換えない Redshift 固有構文は PostgreSQL の syntax / undefined function エラーとしてクライアントに返るため、開発・テストの障害となる。チェックリストを消化することで、AWS SDK / Terraform / 各種 BI ツールからのローカル接続互換性を段階的に向上させる。

## Acceptance Criteria

1. **AC-1 (translator test)**: 反復終了時に `go test ./internal/services/redshift/translator/...` が PASS。
2. **AC-2 (redshift regression)**: `go test ./internal/services/redshift/...` が PASS（既存パッケージへの回帰なし）。
3. **AC-3 (build)**: `go build -o /tmp/devcloud-verify ./cmd/devcloud` が成功。
4. **AC-4 (checklist flip)**: 1 反復で `COMPATIBILITY.md` の `- [ ] **R-only** ...` または `- [ ] **R≠P** ...` 1 行が `- [x]` に変わる。P-only 項目は対象外。
5. **AC-5 (test coverage)**: 新ルールが `translator_test.go` のテストケースで covered（テーブル駆動形式、既存スタイルに準拠）。
6. **AC-6 (commit)**: `AUTOCOMMIT=true` のとき 1 反復 1 commit、メッセージは `feat(redshift/translator): <Redshift 構文名>` 形式。ステージは `internal/services/redshift/translator/` 配下のみ。

## Out of Scope

- `MERGE INTO` 等で新規パッケージ作成が必要な大規模リファクタ → 別 loop に切り出す。
- `STV_*` / `STL_*` / `SVV_*` 系システムビューのスタブ実装（read-only データ提供）→ 別 loop。
- ダッシュボードや Data API への影響変更。

## Verification Command

```bash
bash scripts/redshift-translator-autoloop/verify.sh
```

## Done Criteria

以下のいずれか:

- **完全消化**: `COMPATIBILITY.md` の `- [ ] **R-only**` / `- [ ] **R≠P**` 行が 0 件。
- **優先帯消化**: `推奨追加優先度` セクションの優先度 1〜3 に該当する項目 (`LEN`, `CHARINDEX`, `NVL2`, `CONVERT_TIMEZONE`, `MEDIAN`, `LAST_DAY`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `RAND`, `SUPER`→`jsonb`, `VARBYTE`→`bytea`, PartiQL ドット記法, `QUALIFY`, `SELECT TOP n`, `APPROXIMATE COUNT(DISTINCT)`, `MERGE INTO`) がすべて `- [x]`。
- **MAX_ITERATIONS** 到達。

## Footer Contract

`run-loop.sh` は最終行に下記を出力:

```
NEXUS_LOOP_STATUS: READY | CONTINUE | DONE | BLOCKED
NEXUS_LOOP_SUMMARY: iter=<N> status=<S> last_item="<text>"
```
