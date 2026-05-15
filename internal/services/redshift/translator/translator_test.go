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
