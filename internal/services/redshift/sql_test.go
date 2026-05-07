package redshift

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestSQLCoreCreateInsertSelectWorkflow(t *testing.T) {
	server := NewServer(Config{})

	statements := []string{
		"create schema if not exists loop",
		"drop table if exists loop.events",
		`create table loop.events(
			id integer encode raw,
			payload varchar(64)
		)
		diststyle key
		distkey(id)
		sortkey(id)`,
		"insert into loop.events values (1, 'created')",
	}
	for _, statement := range statements {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select id, payload from loop.events where id = 1 limit 1")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if result.tag != "SELECT 1" {
		t.Fatalf("tag = %q", result.tag)
	}
	if len(result.fields) != 2 || result.fields[0].Name != "id" || result.fields[1].Name != "payload" {
		t.Fatalf("fields = %#v", result.fields)
	}
	if len(result.rows) != 1 || len(result.rows[0]) != 2 || result.rows[0][0] != "1" || result.rows[0][1] != "created" {
		t.Fatalf("rows = %#v", result.rows)
	}
}

func TestSQLCoreSelectLiteralProjection(t *testing.T) {
	server := NewServer(Config{})

	result, err := server.executeSQL("select 1 as id, 'created' payload")
	if err != nil {
		t.Fatalf("select literals: %v", err)
	}
	if result.tag != "SELECT 1" {
		t.Fatalf("tag = %q", result.tag)
	}
	if len(result.fields) != 2 {
		t.Fatalf("fields = %#v", result.fields)
	}
	if result.fields[0].Name != "id" || result.fields[0].TypeOID != pgTypeInt4OID {
		t.Fatalf("first field = %#v", result.fields[0])
	}
	if result.fields[1].Name != "payload" || result.fields[1].TypeOID != pgTypeVarcharOID {
		t.Fatalf("second field = %#v", result.fields[1])
	}
	if len(result.rows) != 1 || len(result.rows[0]) != 2 || result.rows[0][0] != "1" || result.rows[0][1] != "created" {
		t.Fatalf("rows = %#v", result.rows)
	}
}

func TestSQLClientIntrospectionFunctionsAndShow(t *testing.T) {
	server := NewServer(Config{
		User:     "analyst",
		Password: "local-password",
	})

	for _, tc := range []struct {
		statement string
		field     string
		value     string
	}{
		{statement: "select current_user", field: "current_user", value: "analyst"},
		{statement: "select session_user()", field: "session_user", value: "analyst"},
		{statement: "select pg_backend_pid()", field: "pg_backend_pid", value: "1"},
		{statement: "show search_path", field: "search_path", value: "public"},
		{statement: "show transaction isolation level", field: "transaction isolation level", value: "read committed"},
		{statement: "show standard_conforming_strings", field: "standard_conforming_strings", value: "on"},
	} {
		result, err := server.executeSQL(tc.statement)
		if err != nil {
			t.Fatalf("execute %q: %v", tc.statement, err)
		}
		if len(result.fields) != 1 || result.fields[0].Name != tc.field {
			t.Fatalf("%q fields = %#v", tc.statement, result.fields)
		}
		if len(result.rows) != 1 || len(result.rows[0]) != 1 || result.rows[0][0] != tc.value {
			t.Fatalf("%q rows = %#v", tc.statement, result.rows)
		}
		if strings.Contains(result.rows[0][0], "local-password") {
			t.Fatalf("%q leaked password in result: %#v", tc.statement, result.rows)
		}
	}
}

func TestSQLCoreInsertColumnListDefaultsAndIdentity(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id integer identity, payload varchar(64) default 'new', status varchar(16))",
		"insert into public.events(payload, status) values ('created', 'open')",
		"insert into public.events(status, payload, id) values ('closed', default, default)",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select id, payload, status from public.events order by id")
	if err != nil {
		t.Fatalf("select inserted rows: %v", err)
	}
	if len(result.rows) != 2 {
		t.Fatalf("rows = %#v", result.rows)
	}
	if result.rows[0][0] != "1" || result.rows[0][1] != "created" || result.rows[0][2] != "open" {
		t.Fatalf("first row = %#v", result.rows[0])
	}
	if result.rows[1][0] != "2" || result.rows[1][1] != "new" || result.rows[1][2] != "closed" {
		t.Fatalf("second row = %#v", result.rows[1])
	}
}

func TestSQLCoreInsertMultipleValuesRows(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id integer identity, payload varchar(64) default 'new', status varchar(16))",
		"insert into public.events(payload, status) values ('created', 'open'), ('queued', 'open'), (default, 'closed')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select id, payload, status from public.events order by id")
	if err != nil {
		t.Fatalf("select inserted rows: %v", err)
	}
	want := [][]string{
		{"1", "created", "open"},
		{"2", "queued", "open"},
		{"3", "new", "closed"},
	}
	if !reflect.DeepEqual(result.rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.rows, want)
	}
}

func TestSQLCoreUpdateAndDeleteWorkflow(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id integer, payload varchar(64), status varchar(16))",
		"insert into public.events values (1, 'created', 'open')",
		"insert into public.events values (2, 'queued', 'open')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	updateResult, err := server.executeSQL("update public.events set payload = 'processed', status = 'closed' where id = 2")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updateResult.tag != "UPDATE 1" {
		t.Fatalf("update tag = %q", updateResult.tag)
	}
	updated, err := server.executeSQL("select id, payload, status from public.events where id = 2")
	if err != nil {
		t.Fatalf("select updated row: %v", err)
	}
	if len(updated.rows) != 1 || updated.rows[0][1] != "processed" || updated.rows[0][2] != "closed" {
		t.Fatalf("updated rows = %#v", updated.rows)
	}

	deleteResult, err := server.executeSQL("delete from public.events where status = 'open'")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if deleteResult.tag != "DELETE 1" {
		t.Fatalf("delete tag = %q", deleteResult.tag)
	}
	remaining, err := server.executeSQL("select id, payload from public.events order by id")
	if err != nil {
		t.Fatalf("select remaining rows: %v", err)
	}
	if len(remaining.rows) != 1 || remaining.rows[0][0] != "2" || remaining.rows[0][1] != "processed" {
		t.Fatalf("remaining rows = %#v", remaining.rows)
	}
}

func TestSQLCoreWhereComparisonOperators(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id integer, payload varchar(64), status varchar(16))",
		"insert into public.events values (1, 'alpha', 'open')",
		"insert into public.events values (2, 'bravo', 'open')",
		"insert into public.events values (10, 'charlie', 'closed')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	selected, err := server.executeSQL("select id, payload from public.events where id >= 2 order by id")
	if err != nil {
		t.Fatalf("select comparison: %v", err)
	}
	if !reflect.DeepEqual(selected.rows, [][]string{{"2", "bravo"}, {"10", "charlie"}}) {
		t.Fatalf("selected rows = %#v", selected.rows)
	}

	updated, err := server.executeSQL("update public.events set status = 'archived' where payload <> 'alpha'")
	if err != nil {
		t.Fatalf("update comparison: %v", err)
	}
	if updated.tag != "UPDATE 2" {
		t.Fatalf("update tag = %q", updated.tag)
	}

	deleted, err := server.executeSQL("delete from public.events where id < 10")
	if err != nil {
		t.Fatalf("delete comparison: %v", err)
	}
	if deleted.tag != "DELETE 2" {
		t.Fatalf("delete tag = %q", deleted.tag)
	}

	remaining, err := server.executeSQL("select id, status from public.events")
	if err != nil {
		t.Fatalf("select remaining: %v", err)
	}
	if !reflect.DeepEqual(remaining.rows, [][]string{{"10", "archived"}}) {
		t.Fatalf("remaining rows = %#v", remaining.rows)
	}
}

func TestSQLCoreSelectCountFromTable(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.events(id integer, payload varchar(64), status varchar(16))",
		"insert into public.events values (1, 'alpha', 'open')",
		"insert into public.events values (2, 'bravo', 'open')",
		"insert into public.events values (3, 'charlie', 'closed')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select count(*) as total from public.events where status = 'open'")
	if err != nil {
		t.Fatalf("select count: %v", err)
	}
	if result.tag != "SELECT 1" {
		t.Fatalf("tag = %q", result.tag)
	}
	if len(result.fields) != 1 || result.fields[0].Name != "total" || result.fields[0].TypeOID != pgTypeInt4OID {
		t.Fatalf("fields = %#v", result.fields)
	}
	if !reflect.DeepEqual(result.rows, [][]string{{"2"}}) {
		t.Fatalf("rows = %#v", result.rows)
	}

	columnCount, err := server.executeSQL("select count(id) row_count from public.events")
	if err != nil {
		t.Fatalf("select count column: %v", err)
	}
	if len(columnCount.rows) != 1 || columnCount.rows[0][0] != "3" || columnCount.fields[0].Name != "row_count" {
		t.Fatalf("column count result = %#v", columnCount)
	}
}

func TestSQLCoreCreateSelectDropView(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		"create table analytics.events(id integer, payload varchar(64), status varchar(16))",
		"insert into analytics.events values (1, 'alpha', 'open')",
		"insert into analytics.events values (2, 'bravo', 'closed')",
		"create view analytics.open_events as select id, payload from analytics.events where status = 'open'",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	selected, err := server.executeSQL("select payload from analytics.open_events where id = 1")
	if err != nil {
		t.Fatalf("select view: %v", err)
	}
	if len(selected.fields) != 1 || selected.fields[0].Name != "payload" {
		t.Fatalf("view fields = %#v", selected.fields)
	}
	if !reflect.DeepEqual(selected.rows, [][]string{{"alpha"}}) {
		t.Fatalf("view rows = %#v", selected.rows)
	}

	tables, err := server.executeSQL("select table_schema, table_name, table_type from information_schema.tables where table_name = 'open_events'")
	if err != nil {
		t.Fatalf("information_schema view row: %v", err)
	}
	if !reflect.DeepEqual(tables.rows, [][]string{{"analytics", "open_events", "VIEW"}}) {
		t.Fatalf("view catalog rows = %#v", tables.rows)
	}

	pgClass, err := server.executeSQL("select relname, relkind from pg_catalog.pg_class where relname = 'open_events'")
	if err != nil {
		t.Fatalf("pg_class view row: %v", err)
	}
	if !reflect.DeepEqual(pgClass.rows, [][]string{{"open_events", "v"}}) {
		t.Fatalf("view pg_class rows = %#v", pgClass.rows)
	}

	if _, err := server.executeSQL("drop view if exists analytics.open_events"); err != nil {
		t.Fatalf("drop view: %v", err)
	}
	if _, err := server.executeSQL("select * from analytics.open_events"); err == nil {
		t.Fatal("select from dropped view succeeded")
	}
}

func TestSQLCoreCreateTableAsSelectWorkflow(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		"create table analytics.events(id integer, payload varchar(64), status varchar(16))",
		"insert into analytics.events values (1, 'alpha', 'open')",
		"insert into analytics.events values (2, 'bravo', 'closed')",
		"create table analytics.open_events diststyle key distkey(id) sortkey(id) as select id, payload from analytics.events where status = 'open'",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	selected, err := server.executeSQL("select id, payload from analytics.open_events")
	if err != nil {
		t.Fatalf("select CTAS table: %v", err)
	}
	if !reflect.DeepEqual(selected.rows, [][]string{{"1", "alpha"}}) {
		t.Fatalf("CTAS rows = %#v", selected.rows)
	}

	tableInfo, err := server.executeSQL("select * from svv_table_info")
	if err != nil {
		t.Fatalf("svv_table_info: %v", err)
	}
	if !resultContainsRow(tableInfo, "analytics", "open_events", "key", "id", "id", "1") {
		t.Fatalf("svv_table_info rows = %#v", tableInfo.rows)
	}
}

func TestSQLCoreCreateSelectDropMaterializedView(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		"create table analytics.events(id integer, payload varchar(64), status varchar(16))",
		"insert into analytics.events values (1, 'alpha', 'open')",
		"insert into analytics.events values (2, 'bravo', 'closed')",
		"create materialized view analytics.open_event_mv diststyle key distkey(id) sortkey(id) as select id, payload from analytics.events where status = 'open'",
		"insert into analytics.events values (3, 'charlie', 'open')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	selected, err := server.executeSQL("select id, payload from analytics.open_event_mv order by id")
	if err != nil {
		t.Fatalf("select materialized view: %v", err)
	}
	if !reflect.DeepEqual(selected.rows, [][]string{{"1", "alpha"}}) {
		t.Fatalf("materialized view rows = %#v", selected.rows)
	}

	tables, err := server.executeSQL("select table_schema, table_name, table_type from information_schema.tables where table_name = 'open_event_mv'")
	if err != nil {
		t.Fatalf("information_schema materialized view row: %v", err)
	}
	if !reflect.DeepEqual(tables.rows, [][]string{{"analytics", "open_event_mv", "MATERIALIZED VIEW"}}) {
		t.Fatalf("materialized view catalog rows = %#v", tables.rows)
	}

	pgClass, err := server.executeSQL("select relname, relkind from pg_catalog.pg_class where relname = 'open_event_mv'")
	if err != nil {
		t.Fatalf("pg_class materialized view row: %v", err)
	}
	if !reflect.DeepEqual(pgClass.rows, [][]string{{"open_event_mv", "m"}}) {
		t.Fatalf("materialized view pg_class rows = %#v", pgClass.rows)
	}

	mvInfo, err := server.executeSQL("select schema, name, state, is_stale from svv_mv_info where name = 'open_event_mv'")
	if err != nil {
		t.Fatalf("svv_mv_info: %v", err)
	}
	if !reflect.DeepEqual(mvInfo.rows, [][]string{{"analytics", "open_event_mv", "1", "false"}}) {
		t.Fatalf("svv_mv_info rows = %#v", mvInfo.rows)
	}

	catalog := server.CatalogSnapshot()
	if len(catalog.Tables) != 2 {
		t.Fatalf("catalog tables = %#v", catalog.Tables)
	}
	var materializedView TableSnapshot
	for _, table := range catalog.Tables {
		if table.Name == "open_event_mv" {
			materializedView = table
			break
		}
	}
	if materializedView.Type != "MATERIALIZED_VIEW" || materializedView.RowCount != 1 || materializedView.DistKey != "id" {
		t.Fatalf("materialized view snapshot = %#v", materializedView)
	}

	if _, err := server.executeSQL("drop materialized view if exists analytics.open_event_mv"); err != nil {
		t.Fatalf("drop materialized view: %v", err)
	}
	if _, err := server.executeSQL("select * from analytics.open_event_mv"); err == nil {
		t.Fatal("select from dropped materialized view succeeded")
	}
}

func TestSQLCoreDropSchemaRemovesTablesAndPreservesPublic(t *testing.T) {
	server := NewServer(Config{})
	for _, statement := range []string{
		"create schema if not exists scratch",
		"create table scratch.events(id integer, payload varchar(64))",
		"drop schema if exists scratch cascade",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	schemas, err := server.executeSQL("select * from information_schema.schemata")
	if err != nil {
		t.Fatalf("information_schema.schemata: %v", err)
	}
	if resultContainsRow(schemas, "scratch") {
		t.Fatalf("scratch schema should be removed: %#v", schemas.rows)
	}
	if !resultContainsRow(schemas, "public") {
		t.Fatalf("public schema should be preserved: %#v", schemas.rows)
	}

	if _, err := server.executeSQL("select * from scratch.events"); err == nil {
		t.Fatal("select from dropped schema table succeeded")
	}
}

func TestSimpleQueryRecordsRedactedQueryHistory(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	var wire bytes.Buffer

	server.handleSimpleQuery(&wire, "copy public.missing from 's3://bucket/events.csv' iam_role 'secret-role' csv;")

	statements := server.StatementSnapshots()
	if len(statements) != 1 {
		t.Fatalf("statements = %#v", statements)
	}
	if statements[0].Status != "FAILED" || !statements[0].QueryRedacted || statements[0].QueryPreview != "[redacted]" {
		t.Fatalf("statement history = %#v", statements[0])
	}

	stlQuery, err := server.executeSQL("select * from stl_query")
	if err != nil {
		t.Fatalf("stl_query: %v", err)
	}
	if !resultContainsRow(stlQuery, "[redacted]", "FAILED") {
		t.Fatalf("stl_query should expose redacted preview only: %#v", stlQuery.rows)
	}
	for _, row := range stlQuery.rows {
		for _, value := range row {
			if strings.Contains(value, "secret-role") || strings.Contains(value, "s3://bucket/events.csv") {
				t.Fatalf("stl_query leaked sensitive SQL text: %#v", stlQuery.rows)
			}
		}
	}
}
