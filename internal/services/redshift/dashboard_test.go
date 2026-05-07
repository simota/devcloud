package redshift

import (
	"strings"
	"testing"
)

func TestCatalogAndStatementSnapshotsExposeDashboardMetadata(t *testing.T) {
	server := NewServer(Config{
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	for _, statement := range []string{
		"create schema if not exists loop",
		"create table loop.events(id integer encode raw, payload varchar(64)) diststyle key distkey(id) sortkey(id)",
		"insert into loop.events values (1, 'created')",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}
	redshiftDataAPIRequest(t, server, "ExecuteStatement", `{
		"ClusterIdentifier":"devcloud",
		"Database":"dev",
		"DbUser":"dev",
		"Sql":"copy loop.events from 's3://bucket/events.csv' iam_role 'secret-role' csv"
	}`)

	catalog := server.CatalogSnapshot()
	if catalog.Database != "dev" || len(catalog.Schemas) < 2 {
		t.Fatalf("catalog = %#v", catalog)
	}
	if len(catalog.Tables) != 1 || catalog.Tables[0].Schema != "loop" || catalog.Tables[0].Name != "events" || catalog.Tables[0].RowCount != 1 {
		t.Fatalf("tables = %#v", catalog.Tables)
	}
	if catalog.Tables[0].DistStyle != "key" || catalog.Tables[0].DistKey != "id" || len(catalog.Tables[0].SortKeys) != 1 || catalog.Tables[0].SortKeys[0] != "id" {
		t.Fatalf("table Redshift metadata = %#v", catalog.Tables[0])
	}
	if len(catalog.Columns) != 2 || catalog.Columns[0].Name != "id" || catalog.Columns[0].Encoding != "raw" {
		t.Fatalf("columns = %#v", catalog.Columns)
	}

	statements := server.StatementSnapshots()
	if len(statements) != 1 {
		t.Fatalf("statements = %#v", statements)
	}
	if !statements[0].QueryRedacted || statements[0].QueryPreview != "[redacted]" {
		t.Fatalf("statement preview should be redacted: %#v", statements[0])
	}
	if statements[0].ResultRows != 0 || statements[0].RedshiftQueryID == 0 {
		t.Fatalf("statement metadata = %#v", statements[0])
	}
}

func TestDashboardSQLRejectsOversizeStatementWithoutStoringSQL(t *testing.T) {
	server := NewServer(Config{MaxStatementBytes: 8})

	result, err := server.ExecuteDashboardSQL("select 123456789", 10)
	if err == nil || !strings.Contains(err.Error(), "maxStatementBytes") {
		t.Fatalf("ExecuteDashboardSQL error = %v", err)
	}
	if result.Statement.Status != "FAILED" || result.Statement.QueryPreview != "[statement exceeds maxStatementBytes]" {
		t.Fatalf("dashboard result statement = %#v", result.Statement)
	}
	statements := server.StatementSnapshots()
	if len(statements) != 1 {
		t.Fatalf("statement history = %#v", statements)
	}
	if strings.Contains(statements[0].QueryPreview, "123456789") {
		t.Fatalf("oversize statement leaked into preview: %#v", statements[0])
	}
}
