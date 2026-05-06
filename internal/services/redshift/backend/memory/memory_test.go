package memory

import (
	"context"
	"errors"
	"testing"

	"devcloud/internal/services/redshift/backend"
)

func TestBackendExecCatalogAndTransaction(t *testing.T) {
	execCalls := 0
	b := New(func(ctx context.Context, statement string) (backend.Result, error) {
		execCalls++
		if statement != "select 1" {
			t.Fatalf("statement = %q", statement)
		}
		return backend.Result{Tag: "SELECT 1", Rows: [][]string{{"1"}}}, ctx.Err()
	}, func(ctx context.Context) (backend.CatalogSnapshot, error) {
		return backend.CatalogSnapshot{Schemas: []backend.Schema{{Name: "public"}}}, ctx.Err()
	})

	result, err := b.Exec(context.Background(), "select 1")
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if result.Tag != "SELECT 1" || len(result.Rows) != 1 || result.Rows[0][0] != "1" {
		t.Fatalf("result = %#v", result)
	}

	tx, err := b.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec(context.Background(), "select 1"); err != nil {
		t.Fatalf("transaction Exec() error = %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if _, err := tx.Exec(context.Background(), "select 1"); err == nil {
		t.Fatal("transaction Exec() after Commit error = nil")
	}

	tx, err = b.Begin(context.Background())
	if err != nil {
		t.Fatalf("second Begin() error = %v", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("second Rollback() error = %v", err)
	}

	catalog, err := b.Catalog(context.Background())
	if err != nil {
		t.Fatalf("Catalog() error = %v", err)
	}
	if len(catalog.Schemas) != 1 || catalog.Schemas[0].Name != "public" {
		t.Fatalf("catalog = %#v", catalog)
	}
	if execCalls != 2 {
		t.Fatalf("execCalls = %d, want 2", execCalls)
	}
}

func TestBackendErrors(t *testing.T) {
	b := New(nil, nil)
	if _, err := b.Exec(context.Background(), "select 1"); err == nil {
		t.Fatal("Exec() without executor error = nil")
	}
	catalog, err := b.Catalog(context.Background())
	if err != nil {
		t.Fatalf("Catalog() without catalog function error = %v", err)
	}
	if len(catalog.Schemas) != 0 {
		t.Fatalf("catalog = %#v", catalog)
	}

	expected := errors.New("boom")
	b = New(func(ctx context.Context, statement string) (backend.Result, error) {
		return backend.Result{}, expected
	}, nil)
	if _, err := b.Exec(context.Background(), "select 1"); !errors.Is(err, expected) {
		t.Fatalf("Exec() error = %v, want %v", err, expected)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := b.Begin(context.Background()); err == nil {
		t.Fatal("Begin() after Close error = nil")
	}
	if _, err := b.Catalog(context.Background()); err == nil {
		t.Fatal("Catalog() after Close error = nil")
	}
}
