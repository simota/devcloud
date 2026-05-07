package redshift

import (
	"strings"
	"testing"
)

func TestCatalogViewsExposeSchemasTablesColumnsAndRedshiftMetadata(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		`create table analytics.events(
			id integer encode raw,
			payload varchar(64) default 'unknown'
		)
		diststyle key
		distkey(id)
		sortkey(id)`,
		"insert into analytics.events values (1, 'created')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	tables, err := server.executeSQL("select * from information_schema.tables")
	if err != nil {
		t.Fatalf("information_schema.tables: %v", err)
	}
	if !resultContainsRow(tables, "analytics", "events", "BASE TABLE") {
		t.Fatalf("tables rows = %#v", tables.rows)
	}

	columns, err := server.executeSQL("select * from information_schema.columns")
	if err != nil {
		t.Fatalf("information_schema.columns: %v", err)
	}
	if !resultContainsRow(columns, "events", "id", "1", "", "integer", "raw") {
		t.Fatalf("columns rows = %#v", columns.rows)
	}

	pgTables, err := server.executeSQL("select * from pg_catalog.pg_tables")
	if err != nil {
		t.Fatalf("pg_catalog.pg_tables: %v", err)
	}
	if !resultContainsRow(pgTables, "analytics", "events", "dev") {
		t.Fatalf("pg_tables rows = %#v", pgTables.rows)
	}

	tableInfo, err := server.executeSQL("select * from svv_table_info")
	if err != nil {
		t.Fatalf("svv_table_info: %v", err)
	}
	if !resultContainsRow(tableInfo, "analytics", "events", "key", "id", "id", "1") {
		t.Fatalf("svv_table_info rows = %#v", tableInfo.rows)
	}

	svvColumns, err := server.executeSQL("select * from svv_columns")
	if err != nil {
		t.Fatalf("svv_columns: %v", err)
	}
	if !resultContainsRow(svvColumns, "dev", "analytics", "events", "id", "1", "", "integer", "raw") {
		t.Fatalf("svv_columns rows = %#v", svvColumns.rows)
	}

	tableDef, err := server.executeSQL("select * from pg_table_def")
	if err != nil {
		t.Fatalf("pg_table_def: %v", err)
	}
	if !resultContainsRow(tableDef, "analytics", "events", "id", "integer", "raw", "true", "1", "false") {
		t.Fatalf("pg_table_def rows = %#v", tableDef.rows)
	}
}

func TestCatalogSelectSupportsProjectionFilterOrderLimitAndCount(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		"create table analytics.events(id integer, payload varchar(64))",
		"create table analytics.logs(id integer, message varchar(64))",
		"create table public.events(id integer)",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	tables, err := server.executeSQL("select t.table_name from information_schema.tables t where t.table_schema = 'analytics' order by t.table_name limit 1")
	if err != nil {
		t.Fatalf("filtered information_schema.tables: %v", err)
	}
	if len(tables.fields) != 1 || tables.fields[0].Name != "table_name" {
		t.Fatalf("projected table fields = %#v", tables.fields)
	}
	if len(tables.rows) != 1 || tables.rows[0][0] != "events" {
		t.Fatalf("projected table rows = %#v", tables.rows)
	}

	count, err := server.executeSQL("select count(t.table_name) as table_count from information_schema.tables t where t.table_schema = 'analytics'")
	if err != nil {
		t.Fatalf("catalog count: %v", err)
	}
	if len(count.fields) != 1 || count.fields[0].Name != "table_count" || len(count.rows) != 1 || count.rows[0][0] != "2" {
		t.Fatalf("catalog count result = fields %#v rows %#v", count.fields, count.rows)
	}
}

func TestCatalogViewsExposeDriverIntrospectionMetadataWithoutSecrets(t *testing.T) {
	server := NewServer(Config{
		Database: "warehouse",
		User:     "analyst",
		Password: "local-password",
	})

	databases, err := server.executeSQL("select * from pg_catalog.pg_database")
	if err != nil {
		t.Fatalf("pg_catalog.pg_database: %v", err)
	}
	if !resultContainsRow(databases, "warehouse", "10", "6", "false", "true") {
		t.Fatalf("pg_database rows = %#v", databases.rows)
	}

	users, err := server.executeSQL("select * from pg_catalog.pg_user")
	if err != nil {
		t.Fatalf("pg_catalog.pg_user: %v", err)
	}
	if !resultContainsRow(users, "analyst", "10", "true", "true", "********") {
		t.Fatalf("pg_user rows = %#v", users.rows)
	}
	for _, row := range users.rows {
		for _, value := range row {
			if strings.Contains(value, "local-password") {
				t.Fatalf("pg_user leaked password: %#v", users.rows)
			}
		}
	}

	types, err := server.executeSQL("select * from pg_catalog.pg_type")
	if err != nil {
		t.Fatalf("pg_catalog.pg_type: %v", err)
	}
	if !resultContainsRow(types, "23", "int4", "4", "N") || !resultContainsRow(types, "1043", "varchar", "-1", "S") {
		t.Fatalf("pg_type rows = %#v", types.rows)
	}
}

func TestCreateTableAcceptsColumnLevelRedshiftAttributes(t *testing.T) {
	server := NewServer(Config{Database: "dev", User: "dev"})
	for _, statement := range []string{
		"create schema if not exists analytics",
		`create table analytics.column_attrs(
			id integer identity(1,1) distkey sortkey encode raw,
			generated_id integer generated by default as identity,
			payload varchar(64) default 'unknown'
		)`,
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	catalog := server.CatalogSnapshot()
	if len(catalog.Tables) != 1 {
		t.Fatalf("tables = %#v", catalog.Tables)
	}
	if len(catalog.Schemas) != 2 {
		t.Fatalf("schemas = %#v", catalog.Schemas)
	}
	for _, schema := range catalog.Schemas {
		switch schema.Name {
		case "analytics":
			if schema.TableCount != 1 {
				t.Fatalf("analytics tableCount = %d, want 1", schema.TableCount)
			}
		case "public":
			if schema.TableCount != 0 {
				t.Fatalf("public tableCount = %d, want 0", schema.TableCount)
			}
		}
	}
	table := catalog.Tables[0]
	if table.ColumnCount != 3 {
		t.Fatalf("columnCount = %d, want 3", table.ColumnCount)
	}
	if table.DistStyle != "key" || table.DistKey != "id" || len(table.SortKeys) != 1 || table.SortKeys[0] != "id" {
		t.Fatalf("table attributes = %#v", table)
	}
	if !columnSnapshotHas(catalog.Columns, "id", "raw", "", true) {
		t.Fatalf("id column metadata = %#v", catalog.Columns)
	}
	if !columnSnapshotHas(catalog.Columns, "generated_id", "", "", true) {
		t.Fatalf("generated identity metadata = %#v", catalog.Columns)
	}
	if !columnSnapshotHas(catalog.Columns, "payload", "", "'unknown'", false) {
		t.Fatalf("default metadata = %#v", catalog.Columns)
	}
}
