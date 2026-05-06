package redshift

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"devcloud/internal/services/redshift/backend"
	"devcloud/internal/services/redshift/translator"
)

type queryResult struct {
	fields []pgField
	rows   [][]string
	tag    string
}

func queryResultToBackend(result queryResult) backend.Result {
	fields := make([]backend.Field, 0, len(result.fields))
	for _, field := range result.fields {
		fields = append(fields, backend.Field{Name: field.Name, TypeOID: field.TypeOID, TypeSize: field.TypeSize})
	}
	return backend.Result{
		Fields: fields,
		Rows:   cloneRows(result.rows),
		Tag:    result.tag,
	}
}

func queryResultFromBackend(result backend.Result) queryResult {
	fields := make([]pgField, 0, len(result.Fields))
	for _, field := range result.Fields {
		fields = append(fields, pgField{Name: field.Name, TypeOID: field.TypeOID, TypeSize: field.TypeSize})
	}
	return queryResult{
		fields: fields,
		rows:   cloneRows(result.Rows),
		tag:    result.Tag,
	}
}

func (s *Server) memoryCatalogSnapshot(ctx context.Context) (backend.CatalogSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return backend.CatalogSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	schemas := make([]backend.Schema, 0, len(s.db.schemas))
	for schemaName, schemaState := range s.db.schemas {
		if schemaState == nil {
			continue
		}
		tables := make([]backend.Table, 0, len(schemaState.tables))
		for tableName, tableState := range schemaState.tables {
			if tableState == nil {
				continue
			}
			columns := make([]backend.Column, 0, len(tableState.columns))
			for _, columnState := range tableState.columns {
				columns = append(columns, backend.Column{Name: columnState.name, DataType: columnState.dataType})
			}
			tables = append(tables, backend.Table{
				Schema:  tableState.name.schema,
				Name:    tableState.name.table,
				Kind:    tableState.kind,
				Columns: columns,
			})
			if tables[len(tables)-1].Schema == "" {
				tables[len(tables)-1].Schema = schemaName
			}
			if tables[len(tables)-1].Name == "" {
				tables[len(tables)-1].Name = tableName
			}
		}
		schemas = append(schemas, backend.Schema{Name: schemaName, Tables: tables})
	}
	return backend.CatalogSnapshot{Schemas: schemas}, nil
}

func (s *Server) executeSQL(statement string) (queryResult, error) {
	ctx := context.Background()
	translated, err := s.translator.Translate(ctx, translator.Session{
		Database: defaultString(s.config.Database, "dev"),
		User:     defaultString(s.config.User, "dev"),
		Schema:   "public",
	}, statement)
	if err != nil {
		return queryResult{}, err
	}
	if translated.HandledByDevcloud {
		return queryResult{}, errors.New("devcloud-handled Redshift translation results are not wired yet")
	}
	if len(translated.Parameters) > 0 {
		return queryResult{}, errors.New("Redshift SQL translation parameters are not supported yet")
	}
	if len(translated.SideEffects) > 0 {
		return queryResult{}, errors.New("Redshift SQL translation side effects are not supported yet")
	}
	backendSQL := translated.BackendSQL
	if strings.TrimSpace(backendSQL) == "" {
		backendSQL = statement
	}
	result, err := s.backend.Exec(ctx, backendSQL)
	if err != nil {
		return queryResult{}, err
	}
	if err := s.applyTranslationMetadataEffects(translated.MetadataEffects); err != nil {
		return queryResult{}, err
	}
	return queryResultFromBackend(result), nil
}

func (s *Server) applyTranslationMetadataEffects(effects []translator.MetadataEffect) error {
	if len(effects) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, effect := range effects {
		switch effect.Kind {
		case translator.MetadataEffectCreateTable:
			schemaName := defaultString(effect.Schema, "public")
			tableName := effect.Table
			if tableName == "" {
				return errors.New("CREATE TABLE metadata effect requires a table name")
			}
			schemaState := s.db.schemas[schemaName]
			if schemaState == nil {
				schemaState = &schema{tables: map[string]*table{}}
				s.db.schemas[schemaName] = schemaState
			}
			columns := make([]column, 0, len(effect.Columns))
			for _, metadata := range effect.Columns {
				columns = append(columns, column{
					name:         metadata.Name,
					dataType:     metadata.DataType,
					encoding:     metadata.Encoding,
					defaultValue: metadata.DefaultValue,
					identity:     metadata.Identity,
				})
			}
			schemaState.tables[tableName] = &table{
				name:      qualifiedName{schema: schemaName, table: tableName},
				columns:   columns,
				distStyle: effect.Value,
				distKey:   effect.Name,
				sortKeys:  append([]string(nil), effect.SortKeys...),
			}
		default:
			return fmt.Errorf("unsupported Redshift SQL metadata effect: %s", effect.Kind)
		}
	}
	return s.persistLocked()
}

func (s *Server) executeSQLMemoryBackend(ctx context.Context, statement string) (backend.Result, error) {
	if err := ctx.Err(); err != nil {
		return backend.Result{}, err
	}
	result, err := s.executeSQLMemory(statement)
	if err != nil {
		return backend.Result{}, err
	}
	return queryResultToBackend(result), nil
}

func (s *Server) executeSQLBatch(statements []string) (queryResult, error) {
	s.mu.Lock()
	previous := databaseFromStored(databaseToStored(s.db))
	s.mu.Unlock()

	var result queryResult
	for _, statement := range statements {
		var err error
		result, err = s.executeSQL(statement)
		if err != nil {
			s.mu.Lock()
			s.db = previous
			ensurePublicSchema(s.db)
			persistErr := s.persistLocked()
			s.mu.Unlock()
			if persistErr != nil {
				return queryResult{}, persistErr
			}
			return queryResult{}, err
		}
	}
	return result, nil
}

func compactSQLStatements(statements []string) []string {
	result := make([]string, 0, len(statements))
	for _, statement := range statements {
		trimmed := strings.TrimSpace(statement)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func stringFunctionResult(name string, value string) queryResult {
	return queryResult{
		fields: []pgField{{Name: name, TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   [][]string{{value}},
		tag:    "SELECT 1",
	}
}

func (s *Server) showParameter(statement string) (queryResult, error) {
	name := strings.TrimSpace(statement[len("show "):])
	if name == "" {
		return queryResult{}, errors.New("SHOW requires a parameter name")
	}
	normalized := strings.ToLower(strings.Trim(name, `"`))
	values := map[string]string{
		"application_name":            "",
		"client_encoding":             "UTF8",
		"datestyle":                   "ISO, MDY",
		"integer_datetimes":           "on",
		"is_superuser":                "on",
		"search_path":                 "public",
		"server_encoding":             "UTF8",
		"server_version":              "8.0.2",
		"session_authorization":       defaultString(s.config.User, "dev"),
		"standard_conforming_strings": "on",
		"transaction isolation level": "read committed",
	}
	value, ok := values[normalized]
	if !ok {
		return queryResult{}, fmt.Errorf("unsupported SHOW parameter: %s", name)
	}
	return queryResult{
		fields: []pgField{{Name: normalized, TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   [][]string{{value}},
		tag:    "SHOW",
	}, nil
}

func (s *Server) validateStatementSize(statement string) error {
	maxBytes := s.config.MaxStatementBytes
	if maxBytes <= 0 {
		return nil
	}
	if int64(len(statement)) > maxBytes {
		return fmt.Errorf("SQL statement exceeds maxStatementBytes (%d bytes)", maxBytes)
	}
	return nil
}

func (s *Server) lookupTableLocked(name qualifiedName) *table {
	schema := s.db.schemas[name.schema]
	if schema == nil {
		return nil
	}
	return schema.tables[name.table]
}
