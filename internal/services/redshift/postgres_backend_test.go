package redshift

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	pgbackend "devcloud/internal/services/redshift/backend/postgres"
)

const fakePostgresDriverName = "devcloud_redshift_postgres_backend_test"

var fakePostgresStores sync.Map

func init() {
	sql.Register(fakePostgresDriverName, fakePostgresDriver{})
}

func TestPostgresBackendExecMapsRowsAndFields(t *testing.T) {
	store := newFakePostgresStore(t)
	backend, err := pgbackend.Open(context.Background(), pgbackend.Config{
		DriverName: fakePostgresDriverName,
		DSN:        store.dsn,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer backend.Close()

	result, err := backend.Exec(context.Background(), "select 42 as answer")
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if result.Tag != "SELECT 1" || len(result.Rows) != 1 || result.Rows[0][0] != "42" {
		t.Fatalf("result = %#v", result)
	}
	if len(result.Fields) != 1 || result.Fields[0].Name != "answer" || result.Fields[0].TypeOID != 23 {
		t.Fatalf("fields = %#v", result.Fields)
	}
}

func TestPostgresBackendTransactionCommitAndRollback(t *testing.T) {
	store := newFakePostgresStore(t)
	backend, err := pgbackend.Open(context.Background(), pgbackend.Config{
		DriverName: fakePostgresDriverName,
		DSN:        store.dsn,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer backend.Close()

	tx, err := backend.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if _, err := tx.Exec(context.Background(), "select 42 as answer"); err != nil {
		t.Fatalf("transaction Exec() error = %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	tx, err = backend.Begin(context.Background())
	if err != nil {
		t.Fatalf("second Begin() error = %v", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.begins != 2 || store.commits != 1 || store.rollbacks != 1 {
		t.Fatalf("transaction counts: begins=%d commits=%d rollbacks=%d", store.begins, store.commits, store.rollbacks)
	}
}

func TestPostgresBackendCatalogSnapshotMapsInformationSchema(t *testing.T) {
	store := newFakePostgresStore(t)
	backend, err := pgbackend.Open(context.Background(), pgbackend.Config{
		DriverName: fakePostgresDriverName,
		DSN:        store.dsn,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer backend.Close()

	catalog, err := backend.Catalog(context.Background())
	if err != nil {
		t.Fatalf("Catalog() error = %v", err)
	}
	table := findBackendTable(catalog, "public", "events")
	if table == nil {
		t.Fatalf("catalog missing public.events: %#v", catalog)
	}
	if table.Kind != "base table" || len(table.Columns) != 2 || table.Columns[0].Name != "id" || table.Columns[1].DataType != "character varying" {
		t.Fatalf("table = %#v", table)
	}
}

func TestPostgresBackendErrorDoesNotLeakDSN(t *testing.T) {
	store := newFakePostgresStore(t)
	store.queryErr = errors.New("syntax error")
	backend, err := pgbackend.Open(context.Background(), pgbackend.Config{
		DriverName: fakePostgresDriverName,
		DSN:        store.dsn,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer backend.Close()

	_, err = backend.Exec(context.Background(), "select fail")
	if err == nil {
		t.Fatal("Exec() error = nil")
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), store.dsn) {
		t.Fatalf("error leaked DSN: %v", err)
	}
}

func TestPostgresBackendMapsMixedTypesAndNulls(t *testing.T) {
	store := newFakePostgresStore(t)
	backend, err := pgbackend.Open(context.Background(), pgbackend.Config{
		DriverName: fakePostgresDriverName,
		DSN:        store.dsn,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer backend.Close()

	result, err := backend.Exec(context.Background(), "select mixed types")
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if result.Tag != "SELECT 1" {
		t.Fatalf("Tag = %q", result.Tag)
	}
	if len(result.Fields) != 5 {
		t.Fatalf("Fields = %#v", result.Fields)
	}
	wantOIDs := []int32{16, 1700, 1114, 1043, 25}
	for i, want := range wantOIDs {
		if result.Fields[i].TypeOID != want {
			t.Fatalf("field %d OID = %d, want %d (%#v)", i, result.Fields[i].TypeOID, want, result.Fields[i])
		}
	}
	wantRow := []string{"true", "12.34", "2026-05-04 15:28:19", "hello", ""}
	if len(result.Rows) != 1 {
		t.Fatalf("Rows = %#v", result.Rows)
	}
	for i, want := range wantRow {
		if result.Rows[0][i] != want {
			t.Fatalf("row[%d] = %q, want %q (row=%#v)", i, result.Rows[0][i], want, result.Rows[0])
		}
	}
}

func TestPostgresBackendCommandTagForNonSelect(t *testing.T) {
	store := newFakePostgresStore(t)
	backend, err := pgbackend.Open(context.Background(), pgbackend.Config{
		DriverName: fakePostgresDriverName,
		DSN:        store.dsn,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer backend.Close()

	result, err := backend.Exec(context.Background(), "insert into events values (1)")
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if result.Tag != "INSERT" || len(result.Fields) != 0 || len(result.Rows) != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestPostgresBackendNilAndClosedStatesAreSafe(t *testing.T) {
	var nilBackend *pgbackend.Backend
	if err := nilBackend.Close(); err != nil {
		t.Fatalf("nil Close() error = %v", err)
	}
	if _, err := nilBackend.Exec(context.Background(), "select 1"); err == nil {
		t.Fatal("nil Exec() error = nil")
	}
	if _, err := nilBackend.Begin(context.Background()); err == nil {
		t.Fatal("nil Begin() error = nil")
	}
	if _, err := nilBackend.Catalog(context.Background()); err == nil {
		t.Fatal("nil Catalog() error = nil")
	}

	store := newFakePostgresStore(t)
	backend, err := pgbackend.Open(context.Background(), pgbackend.Config{
		DriverName: fakePostgresDriverName,
		DSN:        store.dsn,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestPostgresBackendRequiresExternalDSN(t *testing.T) {
	_, err := pgbackend.Open(context.Background(), pgbackend.Config{DriverName: fakePostgresDriverName})
	if err == nil {
		t.Fatal("Open() error = nil")
	}
	if !strings.Contains(err.Error(), "external dsn") {
		t.Fatalf("Open() error = %v", err)
	}
}

type fakePostgresStore struct {
	dsn       string
	mu        sync.Mutex
	begins    int
	commits   int
	rollbacks int
	queryErr  error
}

func newFakePostgresStore(t *testing.T) *fakePostgresStore {
	t.Helper()
	store := &fakePostgresStore{dsn: "postgres://dev:secret@127.0.0.1:5432/dev?sslmode=disable#" + t.Name()}
	fakePostgresStores.Store(store.dsn, store)
	t.Cleanup(func() { fakePostgresStores.Delete(store.dsn) })
	return store
}

type fakePostgresDriver struct{}

func (fakePostgresDriver) Open(name string) (driver.Conn, error) {
	value, ok := fakePostgresStores.Load(name)
	if !ok {
		return nil, errors.New("unknown fake postgres dsn")
	}
	return &fakePostgresConn{store: value.(*fakePostgresStore)}, nil
}

type fakePostgresConn struct {
	store *fakePostgresStore
}

func (c *fakePostgresConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented in fake postgres driver")
}

func (c *fakePostgresConn) Close() error {
	return nil
}

func (c *fakePostgresConn) Begin() (driver.Tx, error) {
	c.store.mu.Lock()
	c.store.begins++
	c.store.mu.Unlock()
	return &fakePostgresTx{store: c.store}, nil
}

func (c *fakePostgresConn) Ping(ctx context.Context) error {
	return ctx.Err()
}

func (c *fakePostgresConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.Begin()
}

func (c *fakePostgresConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.store.queryErr != nil {
		return nil, c.store.queryErr
	}
	return fakeRowsForQuery(query), nil
}

type fakePostgresTx struct {
	store *fakePostgresStore
}

func (tx *fakePostgresTx) Commit() error {
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()
	tx.store.commits++
	return nil
}

func (tx *fakePostgresTx) Rollback() error {
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()
	tx.store.rollbacks++
	return nil
}

type fakePostgresRows struct {
	columns []string
	types   []string
	values  [][]driver.Value
	index   int
}

func fakeRowsForQuery(query string) *fakePostgresRows {
	normalized := strings.ToLower(query)
	if strings.Contains(normalized, "information_schema.columns") {
		return &fakePostgresRows{
			columns: []string{"table_schema", "table_name", "table_type", "column_name", "data_type"},
			types:   []string{"varchar", "varchar", "varchar", "varchar", "varchar"},
			values: [][]driver.Value{
				{"public", "events", "BASE TABLE", "id", "integer"},
				{"public", "events", "BASE TABLE", "payload", "character varying"},
			},
		}
	}
	if strings.Contains(normalized, "mixed types") {
		return &fakePostgresRows{
			columns: []string{"ok", "amount", "created_at", "payload", "missing"},
			types:   []string{"bool", "numeric", "timestamp", "varchar", "unknown"},
			values: [][]driver.Value{{
				true,
				"12.34",
				time.Date(2026, 5, 4, 15, 28, 19, 0, time.UTC),
				[]byte("hello"),
				nil,
			}},
		}
	}
	if strings.HasPrefix(strings.TrimSpace(normalized), "insert") {
		return &fakePostgresRows{}
	}
	return &fakePostgresRows{
		columns: []string{"answer"},
		types:   []string{"int4"},
		values:  [][]driver.Value{{int64(42)}},
	}
}

func (r *fakePostgresRows) Columns() []string {
	return r.columns
}

func (r *fakePostgresRows) Close() error {
	return nil
}

func (r *fakePostgresRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func (r *fakePostgresRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.types) {
		return "text"
	}
	return r.types[index]
}
