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
