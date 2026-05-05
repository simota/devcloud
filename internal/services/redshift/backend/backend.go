package backend

import "context"

// SQLBackend is the execution boundary between Redshift compatibility code and
// the engine that owns SQL execution.
type SQLBackend interface {
	Exec(ctx context.Context, statement string) (Result, error)
	Begin(ctx context.Context) (Transaction, error)
	Catalog(ctx context.Context) (CatalogSnapshot, error)
	Close() error
}

type Transaction interface {
	Exec(ctx context.Context, statement string) (Result, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type Result struct {
	Fields []Field
	Rows   [][]string
	Tag    string
}

type Field struct {
	Name     string
	TypeOID  int32
	TypeSize int16
}

type CatalogSnapshot struct {
	Schemas []Schema
}

type Schema struct {
	Name   string
	Tables []Table
}

type Table struct {
	Schema  string
	Name    string
	Kind    string
	Columns []Column
}

type Column struct {
	Name     string
	DataType string
}
