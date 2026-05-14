package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"devcloud/internal/services/redshift/backend"

	_ "github.com/lib/pq"
)

const defaultDriverName = "postgres"

type Config struct {
	DriverName   string
	DSN          string
	QueryTimeout time.Duration
}

type Backend struct {
	db           *sql.DB
	queryTimeout time.Duration
}

type Error struct {
	Operation string
	Err       error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Operation == "" {
		return "postgres redshift backend: " + e.Err.Error()
	}
	return "postgres redshift backend " + e.Operation + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func Open(ctx context.Context, cfg Config) (*Backend, error) {
	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, errors.New("postgres redshift backend requires an external dsn")
	}
	driverName := strings.TrimSpace(cfg.DriverName)
	if driverName == "" {
		driverName = defaultDriverName
	}
	db, err := sql.Open(driverName, cfg.DSN)
	if err != nil {
		return nil, wrapError("open", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, wrapError("ping", err)
	}
	return &Backend{db: db, queryTimeout: cfg.QueryTimeout}, nil
}

func NewWithDB(db *sql.DB, queryTimeout time.Duration) *Backend {
	return &Backend{db: db, queryTimeout: queryTimeout}
}

func (b *Backend) Exec(ctx context.Context, statement string) (backend.Result, error) {
	if b == nil || b.db == nil {
		return backend.Result{}, errors.New("postgres redshift backend is not open")
	}
	ctx, cancel := b.withTimeout(ctx)
	defer cancel()
	result, err := queryRows(ctx, b.db, statement)
	if err != nil {
		return backend.Result{}, wrapError("exec", err)
	}
	return result, nil
}

func (b *Backend) Begin(ctx context.Context) (backend.Transaction, error) {
	if b == nil || b.db == nil {
		return nil, errors.New("postgres redshift backend is not open")
	}
	ctx, cancel := b.withTimeout(ctx)
	defer cancel()
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, wrapError("begin", err)
	}
	return &transaction{tx: tx, queryTimeout: b.queryTimeout}, nil
}

func (b *Backend) Catalog(ctx context.Context) (backend.CatalogSnapshot, error) {
	if b == nil || b.db == nil {
		return backend.CatalogSnapshot{}, errors.New("postgres redshift backend is not open")
	}
	ctx, cancel := b.withTimeout(ctx)
	defer cancel()
	catalog, err := queryCatalog(ctx, b.db)
	if err != nil {
		return backend.CatalogSnapshot{}, wrapError("catalog", err)
	}
	return catalog, nil
}

func (b *Backend) Close() error {
	if b == nil || b.db == nil {
		return nil
	}
	if err := b.db.Close(); err != nil {
		return wrapError("close", err)
	}
	return nil
}

func (b *Backend) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if b.queryTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, b.queryTimeout)
}

type transaction struct {
	tx           *sql.Tx
	queryTimeout time.Duration
}

func (tx *transaction) Exec(ctx context.Context, statement string) (backend.Result, error) {
	ctx, cancel := tx.withTimeout(ctx)
	defer cancel()
	result, err := queryRows(ctx, tx.tx, statement)
	if err != nil {
		return backend.Result{}, wrapError("transaction exec", err)
	}
	return result, nil
}

func (tx *transaction) Commit(ctx context.Context) error {
	ctx, cancel := tx.withTimeout(ctx)
	defer cancel()
	if err := tx.tx.Commit(); err != nil {
		return wrapError("commit", err)
	}
	return nil
}

func (tx *transaction) Rollback(ctx context.Context) error {
	ctx, cancel := tx.withTimeout(ctx)
	defer cancel()
	if err := tx.tx.Rollback(); err != nil {
		return wrapError("rollback", err)
	}
	return nil
}

func (tx *transaction) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if tx.queryTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, tx.queryTimeout)
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func queryRows(ctx context.Context, q queryer, statement string) (backend.Result, error) {
	rows, err := q.QueryContext(ctx, statement)
	if err != nil {
		return backend.Result{}, err
	}
	defer rows.Close()
	fields, err := scanFields(rows)
	if err != nil {
		return backend.Result{}, err
	}
	values := make([][]string, 0)
	for rows.Next() {
		raw := make([]any, len(fields))
		dest := make([]any, len(fields))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return backend.Result{}, err
		}
		values = append(values, stringifyRow(raw))
	}
	if err := rows.Err(); err != nil {
		return backend.Result{}, err
	}
	return backend.Result{Fields: fields, Rows: values, Tag: commandTag(statement, len(values))}, nil
}

func scanFields(rows *sql.Rows) ([]backend.Field, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnTypes, _ := rows.ColumnTypes()
	fields := make([]backend.Field, len(columns))
	for i, name := range columns {
		field := backend.Field{Name: name, TypeOID: 25, TypeSize: -1}
		if i < len(columnTypes) {
			dbType := strings.ToUpper(columnTypes[i].DatabaseTypeName())
			field.TypeOID = postgresTypeOID(dbType)
			if length, ok := columnTypes[i].Length(); ok && length <= math.MaxInt16 {
				field.TypeSize = int16(length)
			}
		}
		fields[i] = field
	}
	return fields, nil
}

func stringifyRow(raw []any) []string {
	row := make([]string, len(raw))
	for i, value := range raw {
		switch typed := value.(type) {
		case nil:
			row[i] = ""
		case []byte:
			row[i] = string(typed)
		case time.Time:
			row[i] = typed.Format("2006-01-02 15:04:05")
		default:
			row[i] = fmt.Sprint(typed)
		}
	}
	return row
}

func queryCatalog(ctx context.Context, q queryer) (backend.CatalogSnapshot, error) {
	rows, err := q.QueryContext(ctx, `
select c.table_schema, c.table_name, t.table_type, c.column_name, c.data_type
from information_schema.columns c
join information_schema.tables t
  on t.table_schema = c.table_schema and t.table_name = c.table_name
where c.table_schema not in ('pg_catalog', 'information_schema')
order by c.table_schema, c.table_name, c.ordinal_position`)
	if err != nil {
		return backend.CatalogSnapshot{}, err
	}
	defer rows.Close()

	schemaIndex := map[string]int{}
	tableIndex := map[string]map[string]int{}
	catalog := backend.CatalogSnapshot{}
	for rows.Next() {
		var schemaName, tableName, tableType, columnName, dataType string
		if err := rows.Scan(&schemaName, &tableName, &tableType, &columnName, &dataType); err != nil {
			return backend.CatalogSnapshot{}, err
		}
		schemaPos, ok := schemaIndex[schemaName]
		if !ok {
			schemaPos = len(catalog.Schemas)
			schemaIndex[schemaName] = schemaPos
			tableIndex[schemaName] = map[string]int{}
			catalog.Schemas = append(catalog.Schemas, backend.Schema{Name: schemaName})
		}
		tablePos, ok := tableIndex[schemaName][tableName]
		if !ok {
			tablePos = len(catalog.Schemas[schemaPos].Tables)
			tableIndex[schemaName][tableName] = tablePos
			catalog.Schemas[schemaPos].Tables = append(catalog.Schemas[schemaPos].Tables, backend.Table{
				Schema: schemaName,
				Name:   tableName,
				Kind:   strings.ToLower(tableType),
			})
		}
		table := &catalog.Schemas[schemaPos].Tables[tablePos]
		table.Columns = append(table.Columns, backend.Column{Name: columnName, DataType: dataType})
	}
	if err := rows.Err(); err != nil {
		return backend.CatalogSnapshot{}, err
	}
	return catalog, nil
}

func postgresTypeOID(databaseType string) int32 {
	switch strings.ToUpper(databaseType) {
	case "BOOL", "BOOLEAN":
		return 16
	case "INT2", "SMALLINT":
		return 21
	case "INT4", "INTEGER", "INT":
		return 23
	case "INT8", "BIGINT":
		return 20
	case "FLOAT4", "REAL":
		return 700
	case "FLOAT8", "DOUBLE PRECISION":
		return 701
	case "NUMERIC", "DECIMAL":
		return 1700
	case "DATE":
		return 1082
	case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE":
		return 1114
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return 1184
	case "VARCHAR", "CHARACTER VARYING":
		return 1043
	case "CHAR", "CHARACTER":
		return 1042
	default:
		return 25
	}
}

func commandTag(statement string, rows int) string {
	fields := strings.Fields(strings.TrimSpace(statement))
	if len(fields) == 0 {
		return ""
	}
	command := strings.ToUpper(fields[0])
	if command == "SELECT" {
		return fmt.Sprintf("SELECT %d", rows)
	}
	return command
}

func wrapError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Operation: operation, Err: err}
}
