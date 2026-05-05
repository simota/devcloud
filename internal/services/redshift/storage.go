package redshift

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const stateFileName = "state.json"

type storedState struct {
	Database         *storedDatabase            `json:"database,omitempty"`
	Clusters         map[string]ClusterSnapshot `json:"clusters,omitempty"`
	Snapshots        map[string]storedSnapshot  `json:"snapshots,omitempty"`
	Statements       map[string]storedStatement `json:"statements,omitempty"`
	ClientTokenIndex map[string]string          `json:"clientTokenIndex,omitempty"`
	NextStatementID  int64                      `json:"nextStatementId,omitempty"`
}

type storedDatabase struct {
	Schemas map[string]storedSchema `json:"schemas"`
}

type storedSchema struct {
	Tables map[string]storedTable `json:"tables"`
}

type storedTable struct {
	Name      storedQualifiedName `json:"name"`
	Columns   []storedColumn      `json:"columns"`
	Rows      [][]string          `json:"rows,omitempty"`
	Kind      string              `json:"kind,omitempty"`
	ViewSQL   string              `json:"viewSql,omitempty"`
	DistStyle string              `json:"distStyle,omitempty"`
	DistKey   string              `json:"distKey,omitempty"`
	SortKeys  []string            `json:"sortKeys,omitempty"`
}

type storedQualifiedName struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

type storedColumn struct {
	Name         string `json:"name"`
	DataType     string `json:"dataType"`
	Encoding     string `json:"encoding,omitempty"`
	DefaultValue string `json:"defaultValue,omitempty"`
	Identity     bool   `json:"identity,omitempty"`
}

type storedStatement struct {
	ID                string            `json:"id"`
	ClusterIdentifier string            `json:"clusterIdentifier"`
	Database          string            `json:"database"`
	DbUser            string            `json:"dbUser"`
	SessionID         string            `json:"sessionId,omitempty"`
	QueryString       string            `json:"queryString"`
	ResultFormat      string            `json:"resultFormat,omitempty"`
	CreatedAt         string            `json:"createdAt"`
	UpdatedAt         string            `json:"updatedAt"`
	Status            string            `json:"status"`
	Error             string            `json:"error,omitempty"`
	HasResultSet      bool              `json:"hasResultSet"`
	Result            storedQueryResult `json:"result,omitempty"`
}

type storedSnapshot struct {
	SnapshotIdentifier string `json:"snapshotIdentifier"`
	ClusterIdentifier  string `json:"clusterIdentifier"`
	SnapshotCreateTime string `json:"snapshotCreateTime"`
	Status             string `json:"status"`
	Port               int    `json:"port"`
	AvailabilityZone   string `json:"availabilityZone"`
	ClusterCreateTime  string `json:"clusterCreateTime"`
	MasterUsername     string `json:"masterUsername"`
	ClusterVersion     string `json:"clusterVersion"`
	EngineFullVersion  string `json:"engineFullVersion"`
	NodeType           string `json:"nodeType"`
	NumberOfNodes      int    `json:"numberOfNodes"`
	DBName             string `json:"dbName"`
	Encrypted          bool   `json:"encrypted"`
}

type storedQueryResult struct {
	Fields []storedPGField `json:"fields,omitempty"`
	Rows   [][]string      `json:"rows,omitempty"`
	Tag    string          `json:"tag,omitempty"`
}

type storedPGField struct {
	Name     string `json:"name"`
	TypeOID  int32  `json:"typeOid"`
	TypeSize int16  `json:"typeSize"`
}

func loadState(cfg Config) (*database, map[string]ClusterSnapshot, map[string]ClusterSnapshotMetadata, map[string]*statement, map[string]string, int64, error) {
	path := stateFilePath(cfg.StoragePath)
	if path == "" {
		return nil, nil, nil, nil, nil, 0, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil, nil, nil, 0, nil
		}
		return nil, nil, nil, nil, nil, 0, fmt.Errorf("load redshift state: %w", err)
	}
	var state storedState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, nil, nil, nil, nil, 0, fmt.Errorf("decode redshift state: %w", err)
	}
	db := databaseFromStored(state.Database)
	snapshots := snapshotsFromStored(state.Snapshots)
	statements := statementsFromStored(state.Statements)
	nextStatementID := state.NextStatementID
	if nextStatementID == 0 {
		nextStatementID = maxStoredStatementSequence(statements)
	}
	return db, state.Clusters, snapshots, statements, state.ClientTokenIndex, nextStatementID, nil
}

func (s *Server) persistLocked() error {
	path := stateFilePath(s.config.StoragePath)
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create redshift state directory: %w", err)
	}
	data, err := json.MarshalIndent(storedState{
		Database:         databaseToStored(s.db),
		Clusters:         s.clusters,
		Snapshots:        snapshotsToStored(s.snapshots),
		Statements:       statementsToStored(s.statements),
		ClientTokenIndex: cloneStringMap(s.clientTokenIndex),
		NextStatementID:  s.nextStatementID,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode redshift state: %w", err)
	}
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o600); err != nil {
		return fmt.Errorf("write redshift state: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace redshift state: %w", err)
	}
	return nil
}

func stateFilePath(storagePath string) string {
	if storagePath == "" {
		return ""
	}
	return filepath.Join(storagePath, stateFileName)
}

func ensurePublicSchema(db *database) {
	if db.schemas == nil {
		db.schemas = map[string]*schema{}
	}
	if db.schemas["public"] == nil {
		db.schemas["public"] = &schema{tables: map[string]*table{}}
	}
}

func databaseToStored(db *database) *storedDatabase {
	if db == nil {
		return &storedDatabase{Schemas: map[string]storedSchema{}}
	}
	result := &storedDatabase{Schemas: map[string]storedSchema{}}
	for schemaName, schemaState := range db.schemas {
		stored := storedSchema{Tables: map[string]storedTable{}}
		if schemaState != nil {
			for tableName, tableState := range schemaState.tables {
				if tableState == nil {
					continue
				}
				stored.Tables[tableName] = storedTable{
					Name:      storedQualifiedName{Schema: tableState.name.schema, Table: tableState.name.table},
					Columns:   columnsToStored(tableState.columns),
					Rows:      cloneRows(tableState.rows),
					Kind:      tableState.kind,
					ViewSQL:   tableState.viewSQL,
					DistStyle: tableState.distStyle,
					DistKey:   tableState.distKey,
					SortKeys:  append([]string(nil), tableState.sortKeys...),
				}
			}
		}
		result.Schemas[schemaName] = stored
	}
	return result
}

func databaseFromStored(stored *storedDatabase) *database {
	if stored == nil {
		return nil
	}
	db := &database{schemas: map[string]*schema{}}
	for schemaName, storedSchema := range stored.Schemas {
		schemaState := &schema{tables: map[string]*table{}}
		for tableName, storedTable := range storedSchema.Tables {
			tableNameState := qualifiedName{schema: storedTable.Name.Schema, table: storedTable.Name.Table}
			if tableNameState.schema == "" {
				tableNameState.schema = schemaName
			}
			if tableNameState.table == "" {
				tableNameState.table = tableName
			}
			schemaState.tables[tableName] = &table{
				name:      tableNameState,
				columns:   columnsFromStored(storedTable.Columns),
				rows:      cloneRows(storedTable.Rows),
				kind:      storedTable.Kind,
				viewSQL:   storedTable.ViewSQL,
				distStyle: storedTable.DistStyle,
				distKey:   storedTable.DistKey,
				sortKeys:  append([]string(nil), storedTable.SortKeys...),
			}
		}
		db.schemas[schemaName] = schemaState
	}
	ensurePublicSchema(db)
	return db
}

func cloneRows(rows [][]string) [][]string {
	if rows == nil {
		return nil
	}
	cloned := make([][]string, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, append([]string(nil), row...))
	}
	return cloned
}

func columnsToStored(columns []column) []storedColumn {
	result := make([]storedColumn, 0, len(columns))
	for _, columnState := range columns {
		result = append(result, storedColumn{
			Name:         columnState.name,
			DataType:     columnState.dataType,
			Encoding:     columnState.encoding,
			DefaultValue: columnState.defaultValue,
			Identity:     columnState.identity,
		})
	}
	return result
}

func columnsFromStored(columns []storedColumn) []column {
	result := make([]column, 0, len(columns))
	for _, storedColumn := range columns {
		result = append(result, column{
			name:         storedColumn.Name,
			dataType:     storedColumn.DataType,
			encoding:     storedColumn.Encoding,
			defaultValue: storedColumn.DefaultValue,
			identity:     storedColumn.Identity,
		})
	}
	return result
}

func statementsToStored(statements map[string]*statement) map[string]storedStatement {
	if len(statements) == 0 {
		return nil
	}
	result := make(map[string]storedStatement, len(statements))
	for id, stmt := range statements {
		if stmt == nil {
			continue
		}
		result[id] = storedStatement{
			ID:                stmt.ID,
			ClusterIdentifier: stmt.ClusterIdentifier,
			Database:          stmt.Database,
			DbUser:            stmt.DbUser,
			SessionID:         stmt.SessionID,
			QueryString:       stmt.QueryString,
			ResultFormat:      defaultString(stmt.ResultFormat, "JSON"),
			CreatedAt:         stmt.CreatedAt.UTC().Format(time.RFC3339Nano),
			UpdatedAt:         stmt.UpdatedAt.UTC().Format(time.RFC3339Nano),
			Status:            stmt.Status,
			Error:             stmt.Error,
			HasResultSet:      stmt.HasResultSet,
			Result:            queryResultToStored(stmt.Result),
		}
	}
	return result
}

func snapshotsToStored(snapshots map[string]ClusterSnapshotMetadata) map[string]storedSnapshot {
	if len(snapshots) == 0 {
		return nil
	}
	result := make(map[string]storedSnapshot, len(snapshots))
	for id, snapshot := range snapshots {
		result[id] = storedSnapshot{
			SnapshotIdentifier: snapshot.SnapshotIdentifier,
			ClusterIdentifier:  snapshot.ClusterIdentifier,
			SnapshotCreateTime: snapshot.SnapshotCreateTime,
			Status:             snapshot.Status,
			Port:               snapshot.Port,
			AvailabilityZone:   snapshot.AvailabilityZone,
			ClusterCreateTime:  snapshot.ClusterCreateTime,
			MasterUsername:     snapshot.MasterUsername,
			ClusterVersion:     snapshot.ClusterVersion,
			EngineFullVersion:  snapshot.EngineFullVersion,
			NodeType:           snapshot.NodeType,
			NumberOfNodes:      snapshot.NumberOfNodes,
			DBName:             snapshot.DBName,
			Encrypted:          snapshot.Encrypted,
		}
	}
	return result
}

func snapshotsFromStored(stored map[string]storedSnapshot) map[string]ClusterSnapshotMetadata {
	if len(stored) == 0 {
		return nil
	}
	result := make(map[string]ClusterSnapshotMetadata, len(stored))
	for id, snapshot := range stored {
		snapshotIdentifier := defaultString(snapshot.SnapshotIdentifier, id)
		result[id] = ClusterSnapshotMetadata{
			SnapshotIdentifier: snapshotIdentifier,
			ClusterIdentifier:  snapshot.ClusterIdentifier,
			SnapshotCreateTime: snapshot.SnapshotCreateTime,
			Status:             snapshot.Status,
			Port:               snapshot.Port,
			AvailabilityZone:   snapshot.AvailabilityZone,
			ClusterCreateTime:  snapshot.ClusterCreateTime,
			MasterUsername:     snapshot.MasterUsername,
			ClusterVersion:     snapshot.ClusterVersion,
			EngineFullVersion:  snapshot.EngineFullVersion,
			NodeType:           snapshot.NodeType,
			NumberOfNodes:      snapshot.NumberOfNodes,
			DBName:             snapshot.DBName,
			Encrypted:          snapshot.Encrypted,
		}
	}
	return result
}

func statementsFromStored(stored map[string]storedStatement) map[string]*statement {
	if len(stored) == 0 {
		return nil
	}
	result := make(map[string]*statement, len(stored))
	for id, storedStmt := range stored {
		createdAt := parseStoredTime(storedStmt.CreatedAt)
		updatedAt := parseStoredTime(storedStmt.UpdatedAt)
		if updatedAt.IsZero() {
			updatedAt = createdAt
		}
		stmtID := defaultString(storedStmt.ID, id)
		result[id] = &statement{
			ID:                stmtID,
			ClusterIdentifier: storedStmt.ClusterIdentifier,
			Database:          storedStmt.Database,
			DbUser:            storedStmt.DbUser,
			SessionID:         storedStmt.SessionID,
			QueryString:       storedStmt.QueryString,
			ResultFormat:      defaultString(storedStmt.ResultFormat, "JSON"),
			CreatedAt:         createdAt,
			UpdatedAt:         updatedAt,
			Status:            storedStmt.Status,
			Error:             storedStmt.Error,
			HasResultSet:      storedStmt.HasResultSet,
			Result:            queryResultFromStored(storedStmt.Result),
		}
	}
	return result
}

func queryResultToStored(result queryResult) storedQueryResult {
	fields := make([]storedPGField, 0, len(result.fields))
	for _, field := range result.fields {
		fields = append(fields, storedPGField{Name: field.Name, TypeOID: field.TypeOID, TypeSize: field.TypeSize})
	}
	return storedQueryResult{
		Fields: fields,
		Rows:   cloneRows(result.rows),
		Tag:    result.tag,
	}
}

func queryResultFromStored(stored storedQueryResult) queryResult {
	fields := make([]pgField, 0, len(stored.Fields))
	for _, field := range stored.Fields {
		fields = append(fields, pgField{Name: field.Name, TypeOID: field.TypeOID, TypeSize: field.TypeSize})
	}
	return queryResult{
		fields: fields,
		rows:   cloneRows(stored.Rows),
		tag:    stored.Tag,
	}
}

func parseStoredTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func maxStoredStatementSequence(statements map[string]*statement) int64 {
	var maxID int64
	for _, stmt := range statements {
		if stmt == nil {
			continue
		}
		parts := strings.Split(stmt.ID, "-")
		if len(parts) == 0 {
			continue
		}
		id, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
		if err == nil && id > maxID {
			maxID = id
		}
	}
	return maxID
}

func normalizeClusterEndpoints(clusters map[string]ClusterSnapshot, cfg Config) {
	for id, cluster := range clusters {
		cluster.Endpoint = ClusterEndpoint{
			Address: hostFromAddr(cfg.SQLAddr),
			Port:    portFromAddr(cfg.SQLAddr, 5439),
		}
		clusters[id] = cluster
	}
}
