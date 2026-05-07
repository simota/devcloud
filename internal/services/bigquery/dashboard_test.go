package bigquery

import (
	"testing"
)

func TestSnapshotsExposeDatasetsTablesRowsAndJobs(t *testing.T) {
	server := NewServer(Config{Project: "local-project", Location: "US", StoragePath: t.TempDir()})
	createDatasetForTest(t, server, "local-project", "analytics")
	createTableForTest(t, server, "local-project", "analytics", "people")
	insertRowsForTest(t, server, "local-project", "analytics", "people")
	insertQueryJobForTest(t, server, "local-project", "snapshot_job")

	snapshot := server.Snapshot()
	if !snapshot.Running || len(snapshot.Datasets) != 1 || len(snapshot.Datasets[0].Tables) != 1 || len(snapshot.Jobs) != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}

	dataset, found := server.DatasetSnapshot("local-project", "analytics")
	if !found || dataset.DatasetID != "analytics" || len(dataset.Tables) != 1 {
		t.Fatalf("dataset snapshot found=%t value=%#v", found, dataset)
	}
	table, found := server.TableSnapshot("local-project", "analytics", "people", 1)
	if !found || table.TableID != "people" || len(table.Rows) != 1 || table.Rows[0].JSON["name"] != "Ada" {
		t.Fatalf("table snapshot found=%t value=%#v", found, table)
	}
	job, found := server.JobSnapshot("local-project", "snapshot_job")
	if !found || job.JobID != "snapshot_job" || job.State != "DONE" {
		t.Fatalf("job snapshot found=%t value=%#v", found, job)
	}
	if _, found := server.DatasetSnapshot("local-project", "missing"); found {
		t.Fatal("missing dataset snapshot found")
	}
}
