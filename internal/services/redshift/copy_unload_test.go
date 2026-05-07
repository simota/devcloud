package redshift

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

func TestCopyAndUnloadLocalCSVWorkflow(t *testing.T) {
	server := NewServer(Config{})
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "events.csv")
	if err := os.WriteFile(sourcePath, []byte("2,unload\n1,copy\n"), 0o600); err != nil {
		t.Fatalf("write source CSV: %v", err)
	}

	for _, statement := range []string{
		"drop table if exists public.copy_events",
		"create table public.copy_events(id integer, payload varchar(64))",
		"copy public.copy_events from '" + strings.ReplaceAll(sourcePath, "'", "''") + "' csv",
		"unload ('select * from public.copy_events order by id') to '" + strings.ReplaceAll(filepath.Join(tempDir, "exports", "events_"), "'", "''") + "' csv allowoverwrite",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	exportPath := filepath.Join(tempDir, "exports", "events_000")
	data, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("read export CSV: %v", err)
	}
	if string(data) != "1,copy\n2,unload\n" {
		t.Fatalf("export data = %q", string(data))
	}
}

func TestCopyLocalCSVOptions(t *testing.T) {
	server := NewServer(Config{})
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "events.psv")
	if err := os.WriteFile(sourcePath, []byte("id|payload|note\n1|created|NULL\n2|updated|kept\n"), 0o600); err != nil {
		t.Fatalf("write source CSV: %v", err)
	}

	for _, statement := range []string{
		"create table public.copy_options(id integer, payload varchar(64), note varchar(64))",
		"copy public.copy_options from '" + strings.ReplaceAll(sourcePath, "'", "''") + "' csv delimiter '|' ignoreheader 1 null as 'NULL'",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select id, payload, note from public.copy_options order by id")
	if err != nil {
		t.Fatalf("select copied rows: %v", err)
	}
	want := [][]string{{"1", "created", ""}, {"2", "updated", "kept"}}
	if !reflect.DeepEqual(result.rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.rows, want)
	}
}

func TestCopyAndUnloadLocalS3CSVWorkflow(t *testing.T) {
	ctx := context.Background()
	store := s3svc.NewFileBucketStore(t.TempDir())
	if _, _, err := store.CreateBucket(ctx, "demo-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := store.PutObject(ctx, s3svc.PutObjectInput{
		Bucket:      "demo-bucket",
		Key:         "inputs/events.csv",
		Body:        strings.NewReader("2,unload\n1,copy\n"),
		ContentType: "text/csv",
	}); err != nil {
		t.Fatalf("put source object: %v", err)
	}
	server := NewServer(Config{ObjectStore: store})

	for _, statement := range []string{
		"create table public.copy_events(id integer, payload varchar(64))",
		"copy public.copy_events from 's3://demo-bucket/inputs/events.csv' iam_role default csv",
		"unload ('select * from public.copy_events order by id') to 's3://demo-bucket/exports/events_' iam_role default csv allowoverwrite",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	_, data, ok, err := store.GetObject(ctx, "demo-bucket", "exports/events_000")
	if err != nil {
		t.Fatalf("get export object: %v", err)
	}
	if !ok {
		t.Fatalf("export object was not written")
	}
	if string(data) != "1,copy\n2,unload\n" {
		t.Fatalf("export data = %q", string(data))
	}
}

func TestCopyFromLocalJSONAutoMapsObjectsByColumnName(t *testing.T) {
	source := filepath.Join(t.TempDir(), "events.json")
	if err := os.WriteFile(source, []byte("{\"payload\":\"created\",\"id\":1,\"active\":true}\n{\"id\":2,\"payload\":\"updated\",\"extra\":\"ignored\"}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	server := NewServer(Config{})
	for _, statement := range []string{
		"create table public.copy_events(id integer, payload varchar(64), active boolean)",
		"copy public.copy_events from '" + strings.ReplaceAll(source, "'", "''") + "' json 'auto'",
	} {
		if _, err := server.executeSQL(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	result, err := server.executeSQL("select id, payload, active from public.copy_events order by id")
	if err != nil {
		t.Fatalf("select copied rows: %v", err)
	}
	want := [][]string{{"1", "created", "true"}, {"2", "updated", ""}}
	if !reflect.DeepEqual(result.rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.rows, want)
	}
}

func TestCopyFromJSONRejectsRowsExceedingConfiguredInputLimit(t *testing.T) {
	source := filepath.Join(t.TempDir(), "events.json")
	if err := os.WriteFile(source, []byte("{\"id\":1,\"payload\":\"this-row-is-too-long\"}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	server := NewServer(Config{MaxCopyInputBytes: 8})
	if _, err := server.executeSQL("create table public.copy_events(id integer, payload varchar(64))"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err := server.executeSQL("copy public.copy_events from '" + strings.ReplaceAll(source, "'", "''") + "' json 'auto'")
	if err == nil || !strings.Contains(err.Error(), "maxCopyInputBytes") {
		t.Fatalf("COPY error = %v", err)
	}
	result, err := server.executeSQL("select count(*) from public.copy_events")
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if !reflect.DeepEqual(result.rows, [][]string{{"0"}}) {
		t.Fatalf("rows after rejected COPY = %#v", result.rows)
	}
}

func TestCopyFromS3RequiresObjectStore(t *testing.T) {
	server := NewServer(Config{})
	if _, err := server.executeSQL("create table public.copy_events(id integer, payload varchar(64))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err := server.executeSQL("copy public.copy_events from 's3://demo-bucket/inputs/events.csv' csv")
	if err == nil || !strings.Contains(err.Error(), "local S3 service") {
		t.Fatalf("COPY error = %v", err)
	}
}

func TestCopyRejectsRowsExceedingConfiguredInputLimit(t *testing.T) {
	source := filepath.Join(t.TempDir(), "events.csv")
	if err := os.WriteFile(source, []byte("1,short\n2,this-row-is-too-long\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	server := NewServer(Config{MaxCopyInputBytes: 8})
	if _, err := server.executeSQL("create table public.copy_events(id integer, payload varchar(64))"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err := server.executeSQL("copy public.copy_events from '" + strings.ReplaceAll(source, "'", "''") + "' csv")
	if err == nil || !strings.Contains(err.Error(), "maxCopyInputBytes") {
		t.Fatalf("COPY error = %v", err)
	}
	result, err := server.executeSQL("select count(*) from public.copy_events")
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if !reflect.DeepEqual(result.rows, [][]string{{"0"}}) {
		t.Fatalf("rows after rejected COPY = %#v", result.rows)
	}
}
