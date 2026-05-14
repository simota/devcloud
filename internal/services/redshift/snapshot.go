package redshift

import (
	"errors"
	"sort"
)

type Snapshot struct {
	Status      string            `json:"status"`
	Running     bool              `json:"running"`
	Region      string            `json:"region"`
	StoragePath string            `json:"storagePath,omitempty"`
	BackendKind string            `json:"backendKind"`
	BackendMode string            `json:"backendMode"`
	Clusters    []ClusterSnapshot `json:"clusters"`
}

type CatalogSnapshot struct {
	Database string                `json:"database"`
	Schemas  []SchemaSnapshot      `json:"schemas"`
	Tables   []TableSnapshot       `json:"tables"`
	Columns  []TableColumnSnapshot `json:"columns"`
}

type SchemaSnapshot struct {
	Name       string `json:"name"`
	Owner      string `json:"owner"`
	TableCount int    `json:"tableCount"`
}

type TableSnapshot struct {
	Schema      string   `json:"schema"`
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	ColumnCount int      `json:"columnCount"`
	RowCount    int      `json:"rowCount"`
	DistStyle   string   `json:"distStyle"`
	DistKey     string   `json:"distKey,omitempty"`
	SortKeys    []string `json:"sortKeys,omitempty"`
}

type TableDetailSnapshot struct {
	Table   TableSnapshot         `json:"table"`
	Columns []TableColumnSnapshot `json:"columns"`
	Rows    [][]string            `json:"rows,omitempty"`
}

type TableColumnSnapshot struct {
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	Name         string `json:"name"`
	DataType     string `json:"dataType"`
	Ordinal      int    `json:"ordinal"`
	Encoding     string `json:"encoding,omitempty"`
	DefaultValue string `json:"defaultValue,omitempty"`
	Identity     bool   `json:"identity,omitempty"`
}

type StatementSnapshot struct {
	ID                string `json:"id"`
	ClusterIdentifier string `json:"clusterIdentifier"`
	Database          string `json:"database"`
	DbUser            string `json:"dbUser"`
	SessionID         string `json:"sessionId,omitempty"`
	Status            string `json:"status"`
	QueryPreview      string `json:"queryPreview"`
	QueryRedacted     bool   `json:"queryRedacted"`
	QueryTruncated    bool   `json:"queryTruncated"`
	CreatedAt         int64  `json:"createdAt"`
	UpdatedAt         int64  `json:"updatedAt"`
	HasResultSet      bool   `json:"hasResultSet"`
	ResultRows        int    `json:"resultRows"`
	RedshiftQueryID   int64  `json:"redshiftQueryId"`
}

type QueryFieldSnapshot struct {
	Name     string `json:"name"`
	TypeName string `json:"typeName"`
}

type QueryResultSnapshot struct {
	Statement  StatementSnapshot    `json:"statement"`
	Columns    []QueryFieldSnapshot `json:"columns"`
	Rows       [][]string           `json:"rows"`
	RowCount   int                  `json:"rowCount"`
	CommandTag string               `json:"commandTag"`
}

func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	clusters := s.clusterSnapshotsLocked()
	s.mu.Unlock()
	return Snapshot{
		Status:      "running",
		Running:     true,
		Region:      defaultString(s.config.Region, "us-east-1"),
		StoragePath: s.config.StoragePath,
		BackendKind: defaultString(s.config.BackendKind, "memory"),
		BackendMode: defaultString(s.config.BackendMode, "embedded"),
		Clusters:    clusters,
	}
}

func (s *Server) CatalogSnapshot() CatalogSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := CatalogSnapshot{
		Database: defaultString(s.config.Database, "dev"),
		Schemas:  []SchemaSnapshot{},
		Tables:   []TableSnapshot{},
		Columns:  []TableColumnSnapshot{},
	}
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		schemaState := s.db.schemas[schemaName]
		snapshot.Schemas = append(snapshot.Schemas, SchemaSnapshot{
			Name:       schemaName,
			Owner:      defaultString(s.config.User, "dev"),
			TableCount: len(schemaState.tables),
		})
		for _, tableName := range sortedTableNames(schemaState.tables) {
			tableState := schemaState.tables[tableName]
			snapshot.Tables = append(snapshot.Tables, TableSnapshot{
				Schema:      schemaName,
				Name:        tableName,
				Type:        tableSnapshotType(tableState),
				ColumnCount: len(tableState.columns),
				RowCount:    len(tableState.rows),
				DistStyle:   defaultString(tableState.distStyle, "even"),
				DistKey:     tableState.distKey,
				SortKeys:    append([]string(nil), tableState.sortKeys...),
			})
			for i, columnState := range tableState.columns {
				snapshot.Columns = append(snapshot.Columns, TableColumnSnapshot{
					Schema:       schemaName,
					Table:        tableName,
					Name:         columnState.name,
					DataType:     columnState.dataType,
					Ordinal:      i + 1,
					Encoding:     columnState.encoding,
					DefaultValue: columnState.defaultValue,
					Identity:     columnState.identity,
				})
			}
		}
	}
	return snapshot
}

func (s *Server) TableDetailSnapshot(schemaName string, tableName string, limit int) (TableDetailSnapshot, bool) {
	if limit <= 0 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	name := qualifiedName{schema: defaultString(schemaName, "public"), table: tableName}
	tableState := s.lookupTableLocked(name)
	if tableState == nil {
		return TableDetailSnapshot{}, false
	}
	detail := TableDetailSnapshot{
		Table: TableSnapshot{
			Schema:      tableState.name.schema,
			Name:        tableState.name.table,
			Type:        tableSnapshotType(tableState),
			ColumnCount: len(tableState.columns),
			RowCount:    len(tableState.rows),
			DistStyle:   defaultString(tableState.distStyle, "even"),
			DistKey:     tableState.distKey,
			SortKeys:    append([]string(nil), tableState.sortKeys...),
		},
		Columns: []TableColumnSnapshot{},
	}
	for i, columnState := range tableState.columns {
		detail.Columns = append(detail.Columns, TableColumnSnapshot{
			Schema:       tableState.name.schema,
			Table:        tableState.name.table,
			Name:         columnState.name,
			DataType:     columnState.dataType,
			Ordinal:      i + 1,
			Encoding:     columnState.encoding,
			DefaultValue: columnState.defaultValue,
			Identity:     columnState.identity,
		})
	}
	rowLimit := min(limit, len(tableState.rows))
	detail.Rows = cloneRows(tableState.rows[:rowLimit])
	return detail, true
}

func (s *Server) StatementSnapshots() []StatementSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(s.statements))
	for id := range s.statements {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]StatementSnapshot, 0, len(ids))
	for _, id := range ids {
		stmt := s.statements[id]
		result = append(result, statementSnapshotFromStatement(stmt))
	}
	return result
}

func (s *Server) ExecuteDashboardSQL(sql string, maxRows int) (QueryResultSnapshot, error) {
	if maxRows <= 0 {
		maxRows = 100
	}
	statements := splitSQLStatements(sql)
	if len(statements) == 0 {
		return QueryResultSnapshot{}, errors.New("SQL is required")
	}
	var lastResult queryResult
	var lastSnapshot StatementSnapshot
	for _, statementText := range statements {
		if err := s.validateStatementSize(statementText); err != nil {
			lastSnapshot = s.recordSQLHistory("[statement exceeds maxStatementBytes]", queryResult{}, err)
			return QueryResultSnapshot{Statement: lastSnapshot}, err
		}
		result, err := s.executeSQL(statementText)
		lastSnapshot = s.recordSQLHistory(statementText, result, err)
		lastResult = result
		if err != nil {
			return QueryResultSnapshot{Statement: lastSnapshot}, err
		}
	}
	columns := make([]QueryFieldSnapshot, 0, len(lastResult.fields))
	for _, field := range lastResult.fields {
		columns = append(columns, QueryFieldSnapshot{Name: field.Name, TypeName: pgFieldTypeName(field)})
	}
	rowLimit := min(maxRows, len(lastResult.rows))
	return QueryResultSnapshot{
		Statement:  lastSnapshot,
		Columns:    columns,
		Rows:       cloneRows(lastResult.rows[:rowLimit]),
		RowCount:   len(lastResult.rows),
		CommandTag: lastResult.tag,
	}, nil
}

func statementSnapshotFromStatement(stmt *statement) StatementSnapshot {
	preview, redacted, truncated := safeSQLPreview(stmt.QueryString, 200)
	return StatementSnapshot{
		ID:                stmt.ID,
		ClusterIdentifier: stmt.ClusterIdentifier,
		Database:          stmt.Database,
		DbUser:            stmt.DbUser,
		SessionID:         stmt.SessionID,
		Status:            stmt.Status,
		QueryPreview:      preview,
		QueryRedacted:     redacted,
		QueryTruncated:    truncated,
		CreatedAt:         stmt.CreatedAt.Unix(),
		UpdatedAt:         stmt.UpdatedAt.Unix(),
		HasResultSet:      stmt.HasResultSet,
		ResultRows:        len(stmt.Result.rows),
		RedshiftQueryID:   redshiftQueryID(stmt.ID),
	}
}

func pgFieldTypeName(field pgField) string {
	switch field.TypeOID {
	case pgTypeInt4OID:
		return "int4"
	case pgTypeBoolOID:
		return "bool"
	case pgTypeFloat8OID:
		return "float8"
	}
	return "varchar"
}
