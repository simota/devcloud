package translator

import (
	"context"
	"strings"
	"testing"
)

func TestRedshiftToPostgresRewritesCurrentTimestampFunctions(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select getdate() as created_at, sysdate as snapshot_at")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if translated.BackendSQL != "select CURRENT_TIMESTAMP as created_at, CURRENT_TIMESTAMP as snapshot_at" {
		t.Fatalf("BackendSQL = %q", translated.BackendSQL)
	}
}

func TestRedshiftToPostgresRewritesNVLToCoalesce(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select nvl(payload, 'unknown') as payload from events")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if translated.BackendSQL != "select COALESCE(payload, 'unknown') as payload from events" {
		t.Fatalf("BackendSQL = %q", translated.BackendSQL)
	}
}

func TestRedshiftToPostgresDoesNotRewriteFunctionsInsideStringLiterals(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, `select 'getdate() sysdate nvl(a,b)' as literal, "nvl" as quoted_name, getdate() as now`)
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if !strings.Contains(translated.BackendSQL, "'getdate() sysdate nvl(a,b)'") {
		t.Fatalf("BackendSQL rewrote literal: %q", translated.BackendSQL)
	}
	if !strings.Contains(translated.BackendSQL, "CURRENT_TIMESTAMP as now") {
		t.Fatalf("BackendSQL did not rewrite GETDATE(): %q", translated.BackendSQL)
	}
	if !strings.Contains(translated.BackendSQL, `"nvl" as quoted_name`) {
		t.Fatalf("BackendSQL rewrote quoted identifier: %q", translated.BackendSQL)
	}
}

func TestRedshiftToPostgresRewritesDefaultFunctionInCreateTableBackendSQL(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "create table events(id int, created_at timestamp default getdate())")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if strings.Contains(strings.ToLower(translated.BackendSQL), "getdate") {
		t.Fatalf("BackendSQL still contains GETDATE(): %q", translated.BackendSQL)
	}
	if !strings.Contains(translated.BackendSQL, "default CURRENT_TIMESTAMP") {
		t.Fatalf("BackendSQL did not rewrite default GETDATE(): %q", translated.BackendSQL)
	}
}

func TestRedshiftToPostgresRewritesDecodeToCase(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select decode(status, 'ok', 1, 'failed', 0, -1) as score from events")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	want := "select CASE status WHEN 'ok' THEN 1 WHEN 'failed' THEN 0 ELSE -1 END as score from events"
	if translated.BackendSQL != want {
		t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, want)
	}
}

func TestRedshiftToPostgresRewritesDateAddAndDateDiff(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select dateadd(day, 7, created_at) as expires_at, datediff(hour, started_at, ended_at) as hours from events")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if !strings.Contains(translated.BackendSQL, "created_at + (7 * interval '1 day') as expires_at") {
		t.Fatalf("BackendSQL did not rewrite DATEADD(): %q", translated.BackendSQL)
	}
	if !strings.Contains(translated.BackendSQL, "floor(extract(epoch from (ended_at - started_at)) / 3600)::int as hours") {
		t.Fatalf("BackendSQL did not rewrite DATEDIFF(): %q", translated.BackendSQL)
	}
}

func TestRedshiftToPostgresRewritesConvertTimezone(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "utc to jst",
			sql:  "select convert_timezone('UTC', 'JST', created_at) as created_at_jst from events",
			want: "select created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' as created_at_jst from events",
		},
		{
			name: "convert_timezone inside string literal is ignored",
			sql:  "select 'convert_timezone(''UTC'', ''JST'', created_at)' as label, CONVERT_TIMEZONE('utc', 'jst', created_at) as created_at_jst from events",
			want: "select 'convert_timezone(''UTC'', ''JST'', created_at)' as label, created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo' as created_at_jst from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesDatePartAndDateTruncPartAliases(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "date_part weekday alias",
			sql:  "select date_part(weekday, created_at) as weekday from events",
			want: "select date_part('dow', created_at) as weekday from events",
		},
		{
			name: "date_trunc quarter alias",
			sql:  "select date_trunc(qtr, created_at) as quarter_start from events",
			want: "select date_trunc('quarter', created_at) as quarter_start from events",
		},
		{
			name: "aliases inside string literal are ignored",
			sql:  "select 'date_part(weekday, created_at)' as label, DATE_PART(dw, created_at) as weekday from events",
			want: "select 'date_part(weekday, created_at)' as label, date_part('dow', created_at) as weekday from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesListAggWithinGroup(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select listagg(name, ',') within group (order by created_at desc) as names from events")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	want := "select string_agg(name, ',' ORDER BY created_at desc) as names from events"
	if translated.BackendSQL != want {
		t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, want)
	}
}

func TestRedshiftToPostgresRewritesListAggWindowWithinGroup(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "partitioned window",
			sql:  "select listagg(name, ',') within group (order by created_at desc) over (partition by account_id) as names from events",
			want: "select array_to_string(array_agg(name) OVER (partition by account_id ORDER BY created_at desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), ',') as names from events",
		},
		{
			name: "empty over window",
			sql:  "select LISTAGG(name, ',') WITHIN GROUP (ORDER BY created_at) OVER () as names from events",
			want: "select array_to_string(array_agg(name) OVER (ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), ',') as names from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesMedianToPercentileCont(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "select median(duration_ms) as p50_duration from events")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	want := "select percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) as p50_duration from events"
	if translated.BackendSQL != want {
		t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, want)
	}
}

func TestRedshiftToPostgresRewritesBooleanBitAggregates(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "boolean bit aggregates",
			sql:  "select bit_and(is_ready) as all_ready, bit_or(is_ready) as any_ready from events",
			want: "select bool_and(is_ready) as all_ready, bool_or(is_ready) as any_ready from events",
		},
		{
			name: "boolean bit aggregates inside string literal are ignored",
			sql:  "select 'bit_and(is_ready), bit_or(is_ready)' as label, BIT_AND(is_ready) as all_ready from events",
			want: "select 'bit_and(is_ready), bit_or(is_ready)' as label, bool_and(is_ready) as all_ready from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesRatioToReportOver(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "partitioned window",
			sql:  "select ratio_to_report(revenue) over (partition by category) as revenue_share from sales",
			want: "select revenue / SUM(revenue) OVER (partition by category) as revenue_share from sales",
		},
		{
			name: "ratio_to_report inside string literal is ignored",
			sql:  "select 'ratio_to_report(revenue) over ()' as label, RATIO_TO_REPORT(revenue) OVER () as revenue_share from sales",
			want: "select 'ratio_to_report(revenue) over ()' as label, revenue / SUM(revenue) OVER () as revenue_share from sales",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesLateralColumnAliasReferences(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "select list alias reference",
			sql:  "select clicks / impressions as probability, round(100 * probability, 1) as percentage from raw_data",
			want: "select clicks / impressions as probability, round(100 * (clicks / impressions), 1) as percentage from raw_data",
		},
		{
			name: "alias reference chains through later select items",
			sql:  "select amount as subtotal, subtotal * tax_rate as tax, subtotal + tax as total from invoices",
			want: "select amount as subtotal, (amount) * tax_rate as tax, (amount) + ((amount) * tax_rate) as total from invoices",
		},
		{
			name: "implicit alias reference",
			sql:  "select clicks / impressions probability, probability * 100 percentage from raw_data",
			want: "select clicks / impressions as probability, (clicks / impressions) * 100 as percentage from raw_data",
		},
		{
			name: "string literal and qualified column are unchanged",
			sql:  "select clicks / impressions as probability, 'probability' as label, raw_data.probability as source_probability from raw_data",
			want: "select clicks / impressions as probability, 'probability' as label, raw_data.probability as source_probability from raw_data",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesApproximateCountDistinct(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "approximate count distinct",
			sql:  "select approximate count(distinct user_id) as active_users from events",
			want: "select count(distinct user_id) as active_users from events",
		},
		{
			name: "approximate count inside string literal is ignored",
			sql:  "select 'approximate count(distinct user_id)' as label, approximate count(distinct user_id) as active_users from events",
			want: "select 'approximate count(distinct user_id)' as label, count(distinct user_id) as active_users from events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesLikeDefaultEscape(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "implicit backslash escape",
			sql:  `select id from events where payload like 'promo\_%'`,
			want: `select id from events where payload like 'promo\_%' ESCAPE '\'`,
		},
		{
			name: "explicit escape unchanged",
			sql:  `select id from events where payload like 'promo\_%' escape '\'`,
			want: `select id from events where payload like 'promo\_%' escape '\'`,
		},
		{
			name: "like inside string literal is ignored",
			sql:  `select 'payload like ''promo\_%''' as predicate from events`,
			want: `select 'payload like ''promo\_%''' as predicate from events`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesNullOrderingDefaults(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "ascending and descending defaults",
			sql:  "select id from events order by priority desc, created_at asc, id limit 10",
			want: "select id from events order by priority desc NULLS FIRST, created_at asc NULLS LAST, id NULLS LAST limit 10",
		},
		{
			name: "explicit null ordering unchanged",
			sql:  "select id from events order by priority desc nulls last, created_at nulls first",
			want: "select id from events order by priority desc nulls last, created_at nulls first",
		},
		{
			name: "order by inside string literal is ignored",
			sql:  "select 'order by created_at desc' as label from events order by id",
			want: "select 'order by created_at desc' as label from events order by id NULLS LAST",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesBooleanLiterals(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "typed boolean yes literal",
			sql:  "select boolean 'yes' as enabled",
			want: "select TRUE as enabled",
		},
		{
			name: "typed boolean numeric literal",
			sql:  "select boolean 0 as enabled",
			want: "select FALSE as enabled",
		},
		{
			name: "boolean column default literal",
			sql:  "create table events(id integer, active boolean default y, archived bool default 0)",
			want: "create table events(id integer, active boolean default TRUE, archived bool default FALSE)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresLeavesUnsupportedFunctionFormsUnchanged(t *testing.T) {
	input := "select dateadd(quarter, 1, created_at), datediff(quarter, started_at, ended_at), listagg(name) from events"
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, input)
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	if translated.BackendSQL != input {
		t.Fatalf("BackendSQL = %q, want unchanged %q", translated.BackendSQL, input)
	}
}

func TestRedshiftToPostgresExtractsMixedTableAttributes(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, `create table if not exists analytics.events(
		id integer encode az64,
		created_at timestamp default getdate(),
		payload varchar(64)
	) diststyle even distkey (id) sortkey(created_at,id) backup yes`)
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	lowerBackend := strings.ToLower(translated.BackendSQL)
	for _, forbidden := range []string{"diststyle", "distkey", "sortkey", "encode", "backup", "getdate"} {
		if strings.Contains(lowerBackend, forbidden) {
			t.Fatalf("BackendSQL contains Redshift-only token %q: %s", forbidden, translated.BackendSQL)
		}
	}
	if !strings.Contains(translated.BackendSQL, "default CURRENT_TIMESTAMP") {
		t.Fatalf("BackendSQL did not rewrite default GETDATE(): %s", translated.BackendSQL)
	}
	if len(translated.MetadataEffects) != 1 {
		t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
	}
	effect := translated.MetadataEffects[0]
	if effect.Schema != "analytics" || effect.Table != "events" || effect.Value != "even" || effect.Name != "id" || effect.Backup != "yes" {
		t.Fatalf("metadata effect = %#v", effect)
	}
	if len(effect.SortKeys) != 2 || effect.SortKeys[0] != "created_at" || effect.SortKeys[1] != "id" {
		t.Fatalf("sort keys = %#v", effect.SortKeys)
	}
	if len(effect.Columns) != 3 || effect.Columns[0].Encoding != "az64" || effect.Columns[1].DefaultValue != "getdate()" {
		t.Fatalf("columns = %#v", effect.Columns)
	}
}

func TestRedshiftToPostgresRewritesCreateTableLikeOptions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "including defaults",
			sql:  "create table analytics.events_copy (like analytics.events including defaults) diststyle even",
			want: "create table analytics.events_copy (LIKE analytics.events INCLUDING DEFAULTS)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 1 {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			effect := translated.MetadataEffects[0]
			if effect.Schema != "analytics" || effect.Table != "events_copy" || effect.Value != "even" {
				t.Fatalf("metadata effect = %#v", effect)
			}
			if len(effect.Columns) != 0 {
				t.Fatalf("columns = %#v, want none for LIKE table", effect.Columns)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesTemporaryTableScope(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "temp table",
			sql:  "create temp table session_events(id integer distkey, payload varchar(16)) diststyle key sortkey(id)",
			want: "create temp table session_events(id integer, payload text check (octet_length(payload) <= 16)) on commit preserve rows",
		},
		{
			name: "temporary table if not exists",
			sql:  "create temporary table if not exists session_events(id integer)",
			want: "create temporary table if not exists session_events(id integer) on commit preserve rows",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 0 {
				t.Fatalf("MetadataEffects = %#v, want none for session-scoped temporary table", translated.MetadataEffects)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesCreateExternalTableLocation(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "spectrum location",
			sql:  "create external table spectrum.events(id integer, payload varchar(16)) stored as parquet location 's3://devcloud/events/'",
			want: "create table spectrum.events(id integer, payload text check (octet_length(payload) <= 16))",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if strings.Contains(strings.ToLower(translated.BackendSQL), "external") || strings.Contains(strings.ToLower(translated.BackendSQL), "location") {
				t.Fatalf("BackendSQL contains external table syntax: %q", translated.BackendSQL)
			}
			if len(translated.MetadataEffects) != 1 || translated.MetadataEffects[0].Schema != "spectrum" || translated.MetadataEffects[0].Table != "events" {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			if len(translated.MetadataEffects[0].Columns) != 2 || translated.MetadataEffects[0].Columns[1].DataType != "varchar(16)" {
				t.Fatalf("columns = %#v", translated.MetadataEffects[0].Columns)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesCreateExternalSchemaFromDataCatalog(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "data catalog external schema",
			sql:  "create external schema if not exists spectrum from data catalog database 'analytics' iam_role default",
			want: "create schema if not exists spectrum",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if strings.Contains(strings.ToLower(translated.BackendSQL), "external") || strings.Contains(strings.ToLower(translated.BackendSQL), "data catalog") {
				t.Fatalf("BackendSQL contains external schema syntax: %q", translated.BackendSQL)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesCreateMaterializedViewAutoRefreshYes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "auto refresh yes",
			sql:  "create materialized view analytics.daily_events auto refresh yes as select event_date, count(*) as count from events group by event_date",
			want: "create materialized view analytics.daily_events as select event_date, count(*) as count from events group by event_date",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if strings.Contains(strings.ToLower(translated.BackendSQL), "auto refresh") {
				t.Fatalf("BackendSQL contains Redshift materialized view option: %q", translated.BackendSQL)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesMergeIntoUpdateInsert(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "matched update and not matched insert",
			sql:  "merge into analytics.events as target using staging.events as source on target.id = source.id when matched then update set payload = source.payload, updated_at = getdate() when not matched then insert (id, payload, updated_at) values (source.id, source.payload, getdate())",
			want: "with updated as (update analytics.events as target set payload = source.payload, updated_at = CURRENT_TIMESTAMP from staging.events as source where target.id = source.id returning 1) insert into analytics.events (id, payload, updated_at) select source.id, source.payload, CURRENT_TIMESTAMP from staging.events as source where not exists (select 1 from analytics.events as target where target.id = source.id)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesInsertValuesDefaultIdentity(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "default identity column value",
			sql:  "insert into analytics.events(event_id) values(default)",
			want: "insert into analytics.events(event_id) default values",
		},
		{
			name: "uppercase with semicolon",
			sql:  "INSERT INTO events VALUES ( DEFAULT );",
			want: "INSERT INTO events default values",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRemovesInsertSelectReturning(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "insert select returning all columns",
			sql:  "insert into analytics.events(id, created_at) select id, getdate() from staging.events returning *",
			want: "insert into analytics.events(id, created_at) select id, CURRENT_TIMESTAMP from staging.events",
		},
		{
			name: "returning inside string literal is ignored",
			sql:  "insert into audit.messages(message) select 'returning *' from staging.messages returning message;",
			want: "insert into audit.messages(message) select 'returning *' from staging.messages",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesCreateViewNoSchemaBinding(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "late binding view",
			sql:  "create view analytics.recent_events as select id, created_at from events with no schema binding",
			want: "create view analytics.recent_events as select id, created_at from events",
		},
		{
			name: "quoted literal is unchanged",
			sql:  "create view analytics.labels as select 'with no schema binding' as label from events with no schema binding",
			want: "create view analytics.labels as select 'with no schema binding' as label from events",
		},
		{
			name: "uppercase with semicolon",
			sql:  "create view analytics.recent_events as select id from events WITH NO SCHEMA BINDING;",
			want: "create view analytics.recent_events as select id from events;",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesAlterColumnEncode(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "alter column encode",
			sql:  "alter table analytics.events alter column payload encode zstd",
			want: "alter table analytics.events alter column payload set statistics -1",
		},
		{
			name: "if exists without column keyword",
			sql:  "ALTER TABLE IF EXISTS events ALTER payload ENCODE az64;",
			want: "alter table if exists events alter column payload set statistics -1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if strings.Contains(strings.ToLower(translated.BackendSQL), "encode") {
				t.Fatalf("BackendSQL contains Redshift ENCODE syntax: %q", translated.BackendSQL)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesAlterTableAddColumnDefaultIdentity(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "add column default identity",
			sql:  "alter table analytics.events add column event_id bigint default identity(1, 1)",
			want: "alter table analytics.events add column event_id bigint generated by default as identity (start with 1 increment by 1)",
		},
		{
			name: "if exists with spaced identity arguments",
			sql:  "ALTER TABLE IF EXISTS events ADD id integer DEFAULT IDENTITY ( 100 , 5 );",
			want: "alter table if exists events add column id integer generated by default as identity (start with 100 increment by 5)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if strings.Contains(strings.ToLower(translated.BackendSQL), "default identity") {
				t.Fatalf("BackendSQL contains Redshift DEFAULT IDENTITY syntax: %q", translated.BackendSQL)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesTruncateImmediateCommit(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "truncate table",
			sql:  "truncate table analytics.events",
			want: "commit; truncate table analytics.events",
		},
		{
			name: "uppercase with semicolon",
			sql:  "TRUNCATE events;",
			want: "commit; TRUNCATE events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesQualifyToSubquery(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "window alias predicate",
			sql:  "select user_id, row_number() over (partition by user_id order by created_at desc) as rn from events qualify rn = 1",
			want: "select * from (select user_id, row_number() over (partition by user_id order by created_at desc) as rn from events) as devcloud_qualify where rn = 1",
		},
		{
			name: "preserves outer order by",
			sql:  "select user_id, rank() over (order by score desc) as rank from scores qualify rank <= 10 order by rank",
			want: "select * from (select user_id, rank() over (order by score desc) as rank from scores) as devcloud_qualify where rank <= 10 order by rank NULLS LAST",
		},
		{
			name: "direct window predicate",
			sql:  "select user_id, event_id from events qualify row_number() over (partition by user_id order by created_at desc) = 1",
			want: "select user_id, event_id from (select user_id, event_id, row_number() over (partition by user_id order by created_at desc) as __devcloud_qualify_1 from events) as devcloud_qualify where __devcloud_qualify_1 = 1",
		},
		{
			name: "qualify inside string literal is ignored",
			sql:  "select 'qualify rn = 1' as label, row_number() over (order by id) as rn from events qualify rn = 1",
			want: "select * from (select 'qualify rn = 1' as label, row_number() over (order by id) as rn from events) as devcloud_qualify where rn = 1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesSelectTopToLimit(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "top rows",
			sql:  "select top 10 id, created_at from events order by created_at desc",
			want: "select id, created_at from events order by created_at desc NULLS FIRST limit 10",
		},
		{
			name: "parenthesized top rows with semicolon",
			sql:  "SELECT TOP (5) id, getdate() as observed_at FROM events;",
			want: "SELECT id, CURRENT_TIMESTAMP as observed_at FROM events limit 5",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesSuperColumnTypeToJSONB(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		want     string
		dataType string
	}{
		{
			name:     "create table super column",
			sql:      "create table events(id integer, payload SUPER)",
			want:     "create table events(id integer, payload jsonb)",
			dataType: "super",
		},
		{
			name:     "create table hllsketch column",
			sql:      "create table metrics(id integer, estimate HLLSKETCH)",
			want:     "create table metrics(id integer, estimate bytea)",
			dataType: "hllsketch",
		},
		{
			name:     "create table varbyte column",
			sql:      "create table events(id integer, digest VARBYTE)",
			want:     "create table events(id integer, digest bytea)",
			dataType: "varbyte",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			if translated.MetadataEffects[0].Columns[1].DataType != tc.dataType {
				t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], tc.dataType)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesSpatialColumnTypesToText(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		want     string
		dataType string
	}{
		{
			name:     "create table geometry column",
			sql:      "create table places(id integer, shape GEOMETRY)",
			want:     "create table places(id integer, shape text)",
			dataType: "geometry",
		},
		{
			name:     "create table geography column",
			sql:      "create table places(id integer, footprint GEOGRAPHY)",
			want:     "create table places(id integer, footprint text)",
			dataType: "geography",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			if translated.MetadataEffects[0].Columns[1].DataType != tc.dataType {
				t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], tc.dataType)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesTimestampColumnType(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "create table events(id integer, created_at TIMESTAMP)")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	want := "create table events(id integer, created_at timestamp(6) without time zone)"
	if translated.BackendSQL != want {
		t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, want)
	}
	if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
		t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
	}
	if translated.MetadataEffects[0].Columns[1].DataType != "timestamp" {
		t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], "timestamp")
	}
}

func TestRedshiftToPostgresRewritesTimestampTZColumnType(t *testing.T) {
	translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, "create table events(id integer, observed_at TIMESTAMPTZ)")
	if err != nil {
		t.Fatalf("Translate() error = %v", err)
	}

	want := "create table events(id integer, observed_at timestamp(6) without time zone)"
	if translated.BackendSQL != want {
		t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, want)
	}
	if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
		t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
	}
	if translated.MetadataEffects[0].Columns[1].DataType != "timestamptz" {
		t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], "timestamptz")
	}
}

func TestRedshiftToPostgresRewritesTimeColumnTypes(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		want     string
		dataType string
	}{
		{
			name:     "create table time column",
			sql:      "create table events(id integer, started_at TIME)",
			want:     "create table events(id integer, started_at time(6) without time zone)",
			dataType: "time",
		},
		{
			name:     "create table timetz column",
			sql:      "create table events(id integer, started_at TIMETZ)",
			want:     "create table events(id integer, started_at time(6) with time zone)",
			dataType: "timetz",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			if translated.MetadataEffects[0].Columns[1].DataType != tc.dataType {
				t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], tc.dataType)
			}
		})
	}
}

func TestRedshiftToPostgresRewritesByteLimitedStringColumnTypes(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		want     string
		dataType string
	}{
		{
			name:     "create table varchar column",
			sql:      "create table events(id integer, payload VARCHAR(8))",
			want:     "create table events(id integer, payload text check (octet_length(payload) <= 8))",
			dataType: "varchar(8)",
		},
		{
			name:     "create table char column",
			sql:      "create table events(id integer, code CHAR(4))",
			want:     "create table events(id integer, code text check (octet_length(code) <= 4))",
			dataType: "char(4)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			translated, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql)
			if err != nil {
				t.Fatalf("Translate() error = %v", err)
			}

			if translated.BackendSQL != tc.want {
				t.Fatalf("BackendSQL = %q, want %q", translated.BackendSQL, tc.want)
			}
			if len(translated.MetadataEffects) != 1 || len(translated.MetadataEffects[0].Columns) != 2 {
				t.Fatalf("MetadataEffects = %#v", translated.MetadataEffects)
			}
			if translated.MetadataEffects[0].Columns[1].DataType != tc.dataType {
				t.Fatalf("column metadata = %#v, want data type %q", translated.MetadataEffects[0].Columns[1], tc.dataType)
			}
		})
	}
}

func TestRedshiftToPostgresRejectsMalformedCreateTable(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "unterminated column list", sql: "create table events(id integer"},
		{name: "missing column type", sql: "create table events(id)"},
		{name: "empty column name", sql: `create table events("" integer)`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRedshiftToPostgres().Translate(context.Background(), Session{}, tc.sql); err == nil {
				t.Fatal("Translate() error = nil")
			}
		})
	}
}
