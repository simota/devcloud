package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"devcloud/internal/services/redshift/backend"
)

const testDriverName = "devcloud_redshift_backend_postgres_test"

var testStores sync.Map

func init() {
	sql.Register(testDriverName, testDriver{})
}

func TestOpenExecCatalogAndTransaction(t *testing.T) {
	store := newTestStore(t)
	b, err := Open(context.Background(), Config{DriverName: testDriverName, DSN: store.dsn})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer b.Close()

	result, err := b.Exec(context.Background(), "select mixed")
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if result.Tag != "SELECT 1" {
		t.Fatalf("Tag = %q", result.Tag)
	}
	if got, want := result.Rows, [][]string{{"42", "hello", "", "2026-05-06 12:34:56"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows = %#v, want %#v", got, want)
	}
	wantFields := []backend.Field{
		{Name: "id", TypeOID: 23, TypeSize: -1},
		{Name: "name", TypeOID: 1043, TypeSize: 64},
		{Name: "missing", TypeOID: 25, TypeSize: -1},
		{Name: "created_at", TypeOID: 1114, TypeSize: -1},
	}
	if !reflect.DeepEqual(result.Fields, wantFields) {
		t.Fatalf("Fields = %#v, want %#v", result.Fields, wantFields)
	}

	catalog, err := b.Catalog(context.Background())
	if err != nil {
		t.Fatalf("Catalog() error = %v", err)
	}
	if len(catalog.Schemas) != 1 || len(catalog.Schemas[0].Tables) != 1 {
		t.Fatalf("Catalog() = %#v", catalog)
	}
	table := catalog.Schemas[0].Tables[0]
	if table.Schema != "public" || table.Name != "events" || table.Kind != "base table" {
		t.Fatalf("table = %#v", table)
	}
	if got, want := table.Columns, []backend.Column{{Name: "id", DataType: "integer"}, {Name: "name", DataType: "character varying"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}

	tx, err := b.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec(context.Background(), "insert into events values (1)"); err != nil {
		t.Fatalf("transaction Exec() error = %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	tx, err = b.Begin(context.Background())
	if err != nil {
		t.Fatalf("second Begin() error = %v", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	if store.begins != 2 || store.commits != 1 || store.rollbacks != 1 {
		t.Fatalf("transaction counts: begins=%d commits=%d rollbacks=%d", store.begins, store.commits, store.rollbacks)
	}
}

func TestOpenAndBackendErrors(t *testing.T) {
	if _, err := Open(context.Background(), Config{DriverName: testDriverName}); err == nil || !strings.Contains(err.Error(), "external dsn") {
		t.Fatalf("Open() empty DSN error = %v", err)
	}
	if _, err := Open(context.Background(), Config{DriverName: testDriverName, DSN: "missing"}); err == nil || !strings.Contains(err.Error(), "ping") {
		t.Fatalf("Open() missing store error = %v", err)
	}

	var nilBackend *Backend
	if _, err := nilBackend.Exec(context.Background(), "select 1"); err == nil {
		t.Fatal("nil Exec() error = nil")
	}
	if _, err := nilBackend.Begin(context.Background()); err == nil {
		t.Fatal("nil Begin() error = nil")
	}
	if _, err := nilBackend.Catalog(context.Background()); err == nil {
		t.Fatal("nil Catalog() error = nil")
	}
	if err := nilBackend.Close(); err != nil {
		t.Fatalf("nil Close() error = %v", err)
	}

	store := newTestStore(t)
	store.queryErr = errors.New("syntax error")
	b, err := Open(context.Background(), Config{DriverName: testDriverName, DSN: store.dsn})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer b.Close()
	if _, err := b.Exec(context.Background(), "select fail"); err == nil || !strings.Contains(err.Error(), "exec") {
		t.Fatalf("Exec() error = %v", err)
	}
	if strings.Contains(errString(b.Exec(context.Background(), "select fail")), store.dsn) {
		t.Fatal("Exec() error leaked DSN")
	}
}

func TestHelpers(t *testing.T) {
	if got := commandTag("  select 1", 3); got != "SELECT 3" {
		t.Fatalf("commandTag(select) = %q", got)
	}
	if got := commandTag("update events set id = 2", 0); got != "UPDATE" {
		t.Fatalf("commandTag(update) = %q", got)
	}
	if got := commandTag("  ", 0); got != "" {
		t.Fatalf("commandTag(empty) = %q", got)
	}

	cases := map[string]int32{
		"boolean":                  16,
		"smallint":                 21,
		"int":                      23,
		"bigint":                   20,
		"real":                     700,
		"double precision":         701,
		"decimal":                  1700,
		"date":                     1082,
		"timestamp with time zone": 1184,
		"char":                     1042,
		"unknown":                  25,
	}
	for name, want := range cases {
		if got := postgresTypeOID(name); got != want {
			t.Fatalf("postgresTypeOID(%q) = %d, want %d", name, got, want)
		}
	}

	wrapped := wrapError("exec", errors.New("boom"))
	var pgErr *Error
	if !errors.As(wrapped, &pgErr) || pgErr.Operation != "exec" || !strings.Contains(pgErr.Error(), "postgres redshift backend exec") {
		t.Fatalf("wrapError() = %#v", wrapped)
	}
	if wrapError("noop", nil) != nil {
		t.Fatal("wrapError(nil) != nil")
	}
}

func errString(_ backend.Result, err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type testStore struct {
	dsn       string
	queryErr  error
	begins    int
	commits   int
	rollbacks int
}

func newTestStore(t *testing.T) *testStore {
	t.Helper()
	store := &testStore{dsn: "postgres://dev:secret@127.0.0.1/dev#" + t.Name()}
	testStores.Store(store.dsn, store)
	t.Cleanup(func() { testStores.Delete(store.dsn) })
	return store
}

type testDriver struct{}

func (testDriver) Open(name string) (driver.Conn, error) {
	value, ok := testStores.Load(name)
	if !ok {
		return nil, errors.New("unknown test postgres dsn")
	}
	return &testConn{store: value.(*testStore)}, nil
}

type testConn struct {
	store *testStore
}

func (c *testConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("Prepare is not implemented")
}

func (c *testConn) Close() error {
	return nil
}

func (c *testConn) Begin() (driver.Tx, error) {
	c.store.begins++
	return &testTx{store: c.store}, nil
}

func (c *testConn) Ping(ctx context.Context) error {
	if _, ok := testStores.Load(c.store.dsn); !ok {
		return errors.New("unknown test postgres dsn")
	}
	return ctx.Err()
}

func (c *testConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.Begin()
}

func (c *testConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.store.queryErr != nil {
		return nil, c.store.queryErr
	}
	if strings.Contains(query, "information_schema.columns") {
		return &testRows{
			columns: []string{"table_schema", "table_name", "table_type", "column_name", "data_type"},
			types:   []string{"varchar", "varchar", "varchar", "varchar", "varchar"},
			values: [][]driver.Value{
				{"public", "events", "BASE TABLE", "id", "integer"},
				{"public", "events", "BASE TABLE", "name", "character varying"},
			},
		}, nil
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(query)), "insert") {
		return &testRows{}, nil
	}
	return &testRows{
		columns: []string{"id", "name", "missing", "created_at"},
		types:   []string{"int4", "varchar", "unknown", "timestamp"},
		lengths: map[int]int64{1: 64},
		values: [][]driver.Value{{
			int64(42),
			[]byte("hello"),
			nil,
			time.Date(2026, 5, 6, 12, 34, 56, 0, time.UTC),
		}},
	}, nil
}

type testTx struct {
	store *testStore
}

func (tx *testTx) Commit() error {
	tx.store.commits++
	return nil
}

func (tx *testTx) Rollback() error {
	tx.store.rollbacks++
	return nil
}

type testRows struct {
	columns []string
	types   []string
	lengths map[int]int64
	values  [][]driver.Value
	pos     int
}

func (r *testRows) Columns() []string {
	return r.columns
}

func (r *testRows) Close() error {
	return nil
}

func (r *testRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.pos])
	r.pos++
	return nil
}

func (r *testRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < len(r.types) {
		return r.types[index]
	}
	return ""
}

func (r *testRows) ColumnTypeLength(index int) (int64, bool) {
	if r.lengths == nil {
		return 0, false
	}
	length, ok := r.lengths[index]
	return length, ok
}
