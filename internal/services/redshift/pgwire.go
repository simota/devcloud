package redshift

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"devcloud/internal/services/redshift/backend"
	"devcloud/internal/services/redshift/translator"
	s3svc "devcloud/internal/services/s3"
)

const (
	pgProtocolVersion   int32 = 196608
	pgSSLRequestCode    int32 = 80877103
	pgAuthOK            int32 = 0
	pgAuthCleartext     int32 = 3
	pgTypeBoolOID       int32 = 16
	pgTypeInt4OID       int32 = 23
	pgTypeVarcharOID    int32 = 1043
	pgTypeFloat8OID     int32 = 701
	pgTransactionIdle         = 'I'
	pgDefaultBackendPID int32 = 1
	pgDefaultSecretKey  int32 = 0
)

func (s *Server) handleSQLConn(conn net.Conn) {
	defer conn.Close()

	params, err := s.readStartup(conn)
	if err != nil {
		return
	}
	if err := writeAuthCleartextPassword(conn); err != nil {
		return
	}
	password, err := readPasswordMessage(conn)
	if err != nil {
		return
	}
	if !s.passwordAllowed(password) {
		writeErrorResponse(conn, "28P01", "password authentication failed")
		return
	}
	if err := writeAuthenticationOK(conn); err != nil {
		return
	}
	if err := writeParameterStatuses(conn, params); err != nil {
		return
	}
	if err := writeBackendKeyData(conn); err != nil {
		return
	}
	if err := writeReadyForQuery(conn); err != nil {
		return
	}

	extended := newExtendedQuerySession()
	for {
		messageType := []byte{0}
		if _, err := io.ReadFull(conn, messageType); err != nil {
			return
		}
		payload, err := readMessagePayload(conn)
		if err != nil {
			return
		}
		switch messageType[0] {
		case 'Q':
			s.handleSimpleQuery(conn, readCString(payload))
		case 'P':
			if extended.failed {
				continue
			}
			extended.handleParse(conn, payload)
		case 'B':
			if extended.failed {
				continue
			}
			extended.handleBind(conn, payload)
		case 'D':
			if extended.failed {
				continue
			}
			extended.handleDescribe(s, conn, payload)
		case 'E':
			if extended.failed {
				continue
			}
			extended.handleExecute(s, conn, payload)
		case 'C':
			if extended.failed {
				continue
			}
			extended.handleClose(conn, payload)
		case 'S':
			extended.handleSync(conn)
			writeReadyForQuery(conn)
		case 'X':
			return
		default:
			writeErrorResponse(conn, "0A000", "unsupported PostgreSQL wire message")
			writeReadyForQuery(conn)
		}
	}
}

type extendedQuerySession struct {
	statements map[string]extendedPreparedStatement
	portals    map[string]extendedPortal
	failed     bool
}

type extendedPreparedStatement struct {
	statement     string
	parameterOIDs []int32
}

type extendedPortal struct {
	statementName       string
	executableStatement string
	executed            bool
	result              queryResult
	nextRow             int
}

type extendedBindParameter struct {
	value string
	null  bool
}

func newExtendedQuerySession() *extendedQuerySession {
	return &extendedQuerySession{
		statements: map[string]extendedPreparedStatement{},
		portals:    map[string]extendedPortal{},
	}
}

func (e *extendedQuerySession) handleParse(w io.Writer, payload []byte) {
	reader := bytes.NewReader(payload)
	name, ok := readCStringFromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	statement, ok := readCStringFromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	parameterCount, ok := readInt16FromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	if parameterCount < 0 {
		e.protocolError(w)
		return
	}
	parameterOIDs := make([]int32, 0, parameterCount)
	for i := 0; i < int(parameterCount); i++ {
		oid, ok := readInt32FromReader(reader)
		if !ok {
			e.protocolError(w)
			return
		}
		parameterOIDs = append(parameterOIDs, oid)
	}
	e.statements[name] = extendedPreparedStatement{statement: statement, parameterOIDs: parameterOIDs}
	writeMessage(w, '1', nil)
}

func (e *extendedQuerySession) handleBind(w io.Writer, payload []byte) {
	reader := bytes.NewReader(payload)
	portalName, ok := readCStringFromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	statementName, ok := readCStringFromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	prepared, ok := e.statements[statementName]
	if !ok {
		e.failed = true
		writeErrorResponse(w, "26000", "prepared statement does not exist")
		return
	}
	formatCodeCount, ok := readInt16FromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	formatCodes, ok := readInt16Values(reader, int(formatCodeCount))
	if !ok {
		e.protocolError(w)
		return
	}
	parameterCount, ok := readInt16FromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	if parameterCount < 0 {
		e.protocolError(w)
		return
	}
	if len(prepared.parameterOIDs) > 0 && int(parameterCount) != len(prepared.parameterOIDs) {
		e.failed = true
		writeErrorResponse(w, "08P01", "bind parameter count does not match prepared statement")
		return
	}
	parameters := make([]extendedBindParameter, 0, parameterCount)
	for i := 0; i < int(parameterCount); i++ {
		formatCode := int16(0)
		switch len(formatCodes) {
		case 0:
		case 1:
			formatCode = formatCodes[0]
		default:
			if i >= len(formatCodes) {
				e.protocolError(w)
				return
			}
			formatCode = formatCodes[i]
		}
		if formatCode != 0 {
			e.failed = true
			writeErrorResponse(w, "0A000", "binary extended query bind parameters are not supported in the local Redshift compatibility layer")
			return
		}
		valueLength, ok := readInt32FromReader(reader)
		if !ok {
			e.protocolError(w)
			return
		}
		if valueLength < -1 {
			e.protocolError(w)
			return
		}
		if valueLength == -1 {
			parameters = append(parameters, extendedBindParameter{null: true})
			continue
		}
		if int64(valueLength) > int64(reader.Len()) {
			e.protocolError(w)
			return
		}
		value := make([]byte, int(valueLength))
		if _, err := io.ReadFull(reader, value); err != nil {
			e.protocolError(w)
			return
		}
		parameters = append(parameters, extendedBindParameter{value: string(value)})
	}
	executableStatement, err := applyExtendedBindParameters(prepared.statement, parameters)
	if err != nil {
		e.failed = true
		writeErrorResponse(w, "08P01", err.Error())
		return
	}
	resultFormatCount, ok := readInt16FromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	resultFormatCodes, ok := readInt16Values(reader, int(resultFormatCount))
	if !ok {
		e.protocolError(w)
		return
	}
	for _, formatCode := range resultFormatCodes {
		if formatCode != 0 {
			e.failed = true
			writeErrorResponse(w, "0A000", "binary extended query result formats are not supported in the local Redshift compatibility layer")
			return
		}
	}
	e.portals[portalName] = extendedPortal{statementName: statementName, executableStatement: executableStatement}
	writeMessage(w, '2', nil)
}

func (e *extendedQuerySession) handleDescribe(s *Server, w io.Writer, payload []byte) {
	targetType, name, ok := parseDescribeOrClosePayload(payload)
	if !ok {
		e.protocolError(w)
		return
	}
	statement, ok := e.statementForTarget(targetType, name)
	if !ok {
		e.failed = true
		writeErrorResponse(w, "26000", "prepared statement or portal does not exist")
		return
	}
	if targetType == 'S' {
		writeParameterDescription(w, e.statements[name].parameterOIDs)
	}
	result, ok := s.describeExtendedQuery(statement)
	if !ok || len(result.fields) == 0 {
		writeMessage(w, 'n', nil)
		return
	}
	writeRowDescription(w, result.fields)
}

func (e *extendedQuerySession) handleExecute(s *Server, w io.Writer, payload []byte) {
	reader := bytes.NewReader(payload)
	portalName, ok := readCStringFromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	maxRows, ok := readInt32FromReader(reader)
	if !ok {
		e.protocolError(w)
		return
	}
	if maxRows < 0 {
		e.protocolError(w)
		return
	}
	portal, ok := e.portals[portalName]
	if !ok {
		e.failed = true
		writeErrorResponse(w, "26000", "portal does not exist")
		return
	}
	prepared := e.statements[portal.statementName]
	if !portal.executed {
		result, err := s.executeSQL(portal.executableStatement)
		s.recordSQLHistory(prepared.statement, result, err)
		if err != nil {
			e.failed = true
			writeErrorResponse(w, "0A000", err.Error())
			return
		}
		portal.result = result
		portal.executed = true
	}
	rowsToWrite := portal.result.rows[portal.nextRow:]
	if maxRows > 0 && int(maxRows) < len(rowsToWrite) {
		rowsToWrite = rowsToWrite[:maxRows]
	}
	for _, row := range rowsToWrite {
		writeDataRow(w, row)
	}
	portal.nextRow += len(rowsToWrite)
	if portal.nextRow < len(portal.result.rows) {
		e.portals[portalName] = portal
		writeMessage(w, 's', nil)
		return
	}
	e.portals[portalName] = portal
	writeCommandComplete(w, portal.result.tag)
}

func (e *extendedQuerySession) handleClose(w io.Writer, payload []byte) {
	targetType, name, ok := parseDescribeOrClosePayload(payload)
	if !ok {
		e.protocolError(w)
		return
	}
	switch targetType {
	case 'S':
		delete(e.statements, name)
	case 'P':
		delete(e.portals, name)
	}
	writeMessage(w, '3', nil)
}

func (e *extendedQuerySession) handleSync(_ io.Writer) {
	e.failed = false
}

func (e *extendedQuerySession) statementForTarget(targetType byte, name string) (string, bool) {
	switch targetType {
	case 'S':
		statement, ok := e.statements[name]
		return statement.statement, ok
	case 'P':
		portal, ok := e.portals[name]
		if !ok {
			return "", false
		}
		if _, ok := e.statements[portal.statementName]; !ok {
			return "", false
		}
		return portal.executableStatement, true
	default:
		return "", false
	}
}

func (e *extendedQuerySession) protocolError(w io.Writer) {
	e.failed = true
	writeErrorResponse(w, "08P01", "invalid PostgreSQL extended query message")
}

func applyExtendedBindParameters(statement string, parameters []extendedBindParameter) (string, error) {
	if len(parameters) == 0 {
		return statement, nil
	}
	var builder strings.Builder
	inString := false
	for i := 0; i < len(statement); i++ {
		ch := statement[i]
		if ch == '\'' {
			builder.WriteByte(ch)
			if inString && i+1 < len(statement) && statement[i+1] == '\'' {
				i++
				builder.WriteByte(statement[i])
				continue
			}
			inString = !inString
			continue
		}
		if !inString && ch == '$' && i+1 < len(statement) && statement[i+1] >= '0' && statement[i+1] <= '9' {
			j := i + 1
			for j < len(statement) && statement[j] >= '0' && statement[j] <= '9' {
				j++
			}
			index, err := strconv.Atoi(statement[i+1 : j])
			if err != nil || index < 1 || index > len(parameters) {
				return "", fmt.Errorf("bind parameter %s has no value", statement[i:j])
			}
			builder.WriteString(sqlLiteralForExtendedBind(parameters[index-1]))
			i = j - 1
			continue
		}
		builder.WriteByte(ch)
	}
	return builder.String(), nil
}

func sqlLiteralForExtendedBind(parameter extendedBindParameter) string {
	if parameter.null {
		return "NULL"
	}
	return "'" + strings.ReplaceAll(parameter.value, "'", "''") + "'"
}

func (s *Server) describeExtendedQuery(statement string) (queryResult, bool) {
	normalized := strings.TrimSpace(strings.TrimRight(statement, ";"))
	if !strings.HasPrefix(strings.ToLower(normalized), "select ") {
		return queryResult{}, false
	}
	result, err := s.executeSQL(normalized)
	if err != nil {
		return queryResult{}, false
	}
	result.rows = nil
	return result, true
}

func (s *Server) readStartup(rw io.ReadWriter) (map[string]string, error) {
	for {
		payload, err := readMessagePayload(rw)
		if err != nil {
			return nil, err
		}
		if len(payload) < 4 {
			return nil, errors.New("short startup message")
		}
		code := int32(binary.BigEndian.Uint32(payload[:4]))
		switch code {
		case pgSSLRequestCode:
			if _, err := rw.Write([]byte{'N'}); err != nil {
				return nil, err
			}
			continue
		case pgProtocolVersion:
			return parseStartupParameters(payload[4:]), nil
		default:
			writeErrorResponse(rw, "08P01", "unsupported PostgreSQL startup protocol")
			return nil, errors.New("unsupported startup protocol")
		}
	}
}

func (s *Server) handleSimpleQuery(w io.Writer, query string) {
	statements := splitSQLStatements(query)
	if len(statements) == 0 {
		writeMessage(w, 'I', nil)
		writeReadyForQuery(w)
		return
	}
	for _, statement := range statements {
		if err := s.validateStatementSize(statement); err != nil {
			s.recordSQLHistory("[statement exceeds maxStatementBytes]", queryResult{}, err)
			writeErrorResponse(w, "54000", err.Error())
			break
		}
		result, err := s.executeSQL(statement)
		s.recordSQLHistory(statement, result, err)
		if err != nil {
			writeErrorResponse(w, "0A000", err.Error())
			break
		}
		if len(result.fields) > 0 {
			writeRowDescription(w, result.fields)
			for _, row := range result.rows {
				writeDataRow(w, row)
			}
		}
		writeCommandComplete(w, result.tag)
	}
	writeReadyForQuery(w)
}

func (s *Server) recordSQLHistory(statementText string, result queryResult, executionErr error) StatementSnapshot {
	now := time.Now().UTC()
	stmt := &statement{
		ID:                s.nextStatementIDValue(),
		ClusterIdentifier: defaultString(s.config.ClusterIdentifier, "devcloud"),
		Database:          defaultString(s.config.Database, "dev"),
		DbUser:            defaultString(s.config.User, "dev"),
		QueryString:       statementText,
		CreatedAt:         now,
		UpdatedAt:         now,
		Status:            "FINISHED",
		HasResultSet:      len(result.fields) > 0,
		Result:            result,
	}
	if executionErr != nil {
		stmt.Status = "FAILED"
		stmt.Error = executionErr.Error()
		stmt.HasResultSet = false
		stmt.Result = queryResult{}
	}

	s.mu.Lock()
	s.statements[stmt.ID] = stmt
	_ = s.persistLocked()
	s.mu.Unlock()
	return statementSnapshotFromStatement(stmt)
}

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

type copyCSVOptions struct {
	delimiter    rune
	format       string
	ignoreHeader int
	nullAs       string
	hasNullAs    bool
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

func (s *Server) executeSQLMemory(statement string) (queryResult, error) {
	normalized := strings.TrimSpace(strings.TrimRight(statement, ";"))
	if normalized == "" {
		return queryResult{tag: ""}, nil
	}
	if err := s.validateStatementSize(normalized); err != nil {
		return queryResult{}, err
	}
	lower := strings.ToLower(normalized)
	switch {
	case strings.EqualFold(normalized, "select 1"):
		return queryResult{
			fields: []pgField{{Name: "?column?", TypeOID: pgTypeInt4OID, TypeSize: 4}},
			rows:   [][]string{{"1"}},
			tag:    "SELECT 1",
		}, nil
	case strings.EqualFold(normalized, "select current_database()"):
		return stringFunctionResult("current_database", defaultString(s.config.Database, "dev")), nil
	case strings.EqualFold(normalized, "select current_schema()"):
		return stringFunctionResult("current_schema", "public"), nil
	case strings.EqualFold(normalized, "select current_user"):
		return stringFunctionResult("current_user", defaultString(s.config.User, "dev")), nil
	case strings.EqualFold(normalized, "select current_user()"):
		return stringFunctionResult("current_user", defaultString(s.config.User, "dev")), nil
	case strings.EqualFold(normalized, "select session_user"):
		return stringFunctionResult("session_user", defaultString(s.config.User, "dev")), nil
	case strings.EqualFold(normalized, "select session_user()"):
		return stringFunctionResult("session_user", defaultString(s.config.User, "dev")), nil
	case strings.EqualFold(normalized, "select pg_backend_pid()"):
		return queryResult{
			fields: []pgField{{Name: "pg_backend_pid", TypeOID: pgTypeInt4OID, TypeSize: 4}},
			rows:   [][]string{{strconv.Itoa(int(pgDefaultBackendPID))}},
			tag:    "SELECT 1",
		}, nil
	case strings.EqualFold(normalized, "select version()"):
		return stringFunctionResult("version", "PostgreSQL 8.0.2 on devcloud Redshift-compatible local server"), nil
	case strings.HasPrefix(lower, "set "):
		return queryResult{tag: "SET"}, nil
	case strings.HasPrefix(lower, "show "):
		return s.showParameter(normalized)
	case lower == "begin" || lower == "begin transaction" || lower == "start transaction":
		return queryResult{tag: "BEGIN"}, nil
	case lower == "commit" || lower == "end":
		return queryResult{tag: "COMMIT"}, nil
	case lower == "rollback":
		return queryResult{tag: "ROLLBACK"}, nil
	case strings.HasPrefix(lower, "create schema"):
		return s.createSchema(normalized)
	case strings.HasPrefix(lower, "drop schema"):
		return s.dropSchema(normalized)
	case strings.HasPrefix(lower, "drop materialized view"):
		return s.dropMaterializedView(normalized)
	case strings.HasPrefix(lower, "drop view"):
		return s.dropView(normalized)
	case strings.HasPrefix(lower, "drop table"):
		return s.dropTable(normalized)
	case strings.HasPrefix(lower, "create materialized view"):
		return s.createMaterializedView(normalized)
	case strings.HasPrefix(lower, "create view") || strings.HasPrefix(lower, "create or replace view"):
		return s.createView(normalized)
	case strings.HasPrefix(lower, "create table"):
		return s.createTable(normalized)
	case strings.HasPrefix(lower, "insert into"):
		return s.insertInto(normalized)
	case strings.HasPrefix(lower, "update "):
		return s.updateTable(normalized)
	case strings.HasPrefix(lower, "delete from "):
		return s.deleteFrom(normalized)
	case isCatalogSelect(lower):
		return s.selectCatalog(normalized)
	case strings.HasPrefix(lower, "select "):
		if !strings.Contains(lower, " from ") {
			return selectLiterals(normalized)
		}
		return s.selectFromTable(normalized)
	case strings.HasPrefix(lower, "copy "):
		return s.copyFromLocalCSV(normalized)
	case strings.HasPrefix(lower, "unload "):
		return s.unloadToLocalCSV(normalized)
	default:
		return queryResult{}, errors.New("unsupported Redshift SQL in local MVP")
	}
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

func selectLiterals(statement string) (queryResult, error) {
	columnPart := strings.TrimSpace(statement[len("select"):])
	if columnPart == "" {
		return queryResult{}, errors.New("SELECT requires at least one expression")
	}
	expressions := splitCommaSeparated(columnPart)
	fields := make([]pgField, 0, len(expressions))
	row := make([]string, 0, len(expressions))
	for i, expression := range expressions {
		value, alias, err := parseSelectLiteral(expression, i+1)
		if err != nil {
			return queryResult{}, err
		}
		typeOID, typeSize := inferLiteralPGType(value)
		fields = append(fields, pgField{Name: alias, TypeOID: typeOID, TypeSize: typeSize})
		row = append(row, value)
	}
	return queryResult{fields: fields, rows: [][]string{row}, tag: "SELECT 1"}, nil
}

func inferLiteralPGType(value string) (int32, int16) {
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return pgTypeInt4OID, 4
	}
	if strings.EqualFold(value, "true") || strings.EqualFold(value, "false") {
		return pgTypeBoolOID, 1
	}
	if strings.ContainsAny(value, ".eE") {
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			return pgTypeFloat8OID, 8
		}
	}
	return pgTypeVarcharOID, -1
}

func parseSelectLiteral(expression string, ordinal int) (string, string, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return "", "", errors.New("SELECT literal expression cannot be empty")
	}
	valuePart := expression
	alias := fmt.Sprintf("?column%d?", ordinal)
	if ordinal == 1 {
		alias = "?column?"
	}
	if value, rest, err := parseLeadingSQLStringLiteral(expression); err == nil {
		valuePart = "'" + strings.ReplaceAll(value, "'", "''") + "'"
		if parsedAlias := parseSelectAlias(rest); parsedAlias != "" {
			alias = parsedAlias
		} else if strings.TrimSpace(rest) != "" {
			return "", "", fmt.Errorf("unsupported SELECT literal alias syntax: %s", strings.TrimSpace(rest))
		}
		value, err := parseLiteral(valuePart)
		return value, alias, err
	}

	fields := strings.Fields(expression)
	if len(fields) == 0 {
		return "", "", errors.New("SELECT literal expression cannot be empty")
	}
	valuePart = fields[0]
	if len(fields) > 1 {
		rest := strings.TrimSpace(expression[len(fields[0]):])
		if parsedAlias := parseSelectAlias(rest); parsedAlias != "" {
			alias = parsedAlias
		} else {
			return "", "", fmt.Errorf("unsupported SELECT literal alias syntax: %s", rest)
		}
	}
	value, err := parseLiteral(valuePart)
	return value, alias, err
}

func parseSelectAlias(rest string) string {
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return ""
	}
	fields := strings.Fields(rest)
	if len(fields) == 2 && strings.EqualFold(fields[0], "as") {
		return cleanIdentifier(fields[1])
	}
	if len(fields) == 1 && !strings.EqualFold(fields[0], "as") {
		return cleanIdentifier(fields[0])
	}
	return ""
}

func (s *Server) createSchema(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("create schema"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "if not exists ") {
		rest = strings.TrimSpace(rest[len("if not exists "):])
	}
	name := strings.Trim(rest, `"`)
	if name == "" {
		return queryResult{}, errors.New("CREATE SCHEMA requires a schema name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.db.schemas[name]; !ok {
		s.db.schemas[name] = &schema{tables: map[string]*table{}}
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "CREATE SCHEMA"}, nil
}

func (s *Server) dropSchema(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("drop schema"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "if exists ") {
		rest = strings.TrimSpace(rest[len("if exists "):])
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return queryResult{}, errors.New("DROP SCHEMA requires a schema name")
	}
	name := cleanIdentifier(fields[0])
	if name == "" {
		return queryResult{}, errors.New("DROP SCHEMA requires a schema name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.db.schemas, name)
	ensurePublicSchema(s.db)
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "DROP SCHEMA"}, nil
}

func (s *Server) dropTable(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("drop table"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "if exists ") {
		rest = strings.TrimSpace(rest[len("if exists "):])
	}
	name := parseQualifiedName(rest)
	s.mu.Lock()
	defer s.mu.Unlock()
	if schema := s.db.schemas[name.schema]; schema != nil {
		delete(schema.tables, name.table)
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "DROP TABLE"}, nil
}

func (s *Server) createTable(statement string) (queryResult, error) {
	if result, ok, err := s.createTableAs(statement); ok || err != nil {
		return result, err
	}
	open := strings.IndexByte(statement, '(')
	close := matchingParen(statement, open)
	if open < 0 || close < 0 {
		return queryResult{}, errors.New("CREATE TABLE requires a column list")
	}
	namePart := strings.TrimSpace(statement[len("create table"):open])
	if strings.HasPrefix(strings.ToLower(namePart), "if not exists ") {
		namePart = strings.TrimSpace(namePart[len("if not exists "):])
	}
	name := parseQualifiedName(namePart)
	columns, err := parseColumns(statement[open+1 : close])
	if err != nil {
		return queryResult{}, err
	}
	distStyle, distKey, sortKeys := parseTableAttributes(statement[close+1:])
	applyColumnTableAttributes(columns, &distStyle, &distKey, &sortKeys)
	s.mu.Lock()
	defer s.mu.Unlock()
	schemaState := s.db.schemas[name.schema]
	if schemaState == nil {
		schemaState = &schema{tables: map[string]*table{}}
		s.db.schemas[name.schema] = schemaState
	}
	schemaState.tables[name.table] = &table{name: name, columns: columns, distStyle: distStyle, distKey: distKey, sortKeys: sortKeys}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "CREATE TABLE"}, nil
}

func (s *Server) createTableAs(statement string) (queryResult, bool, error) {
	rest := strings.TrimSpace(statement[len("create table"):])
	if strings.HasPrefix(strings.ToLower(rest), "if not exists ") {
		rest = strings.TrimSpace(rest[len("if not exists "):])
	}
	namePart, queryPart := splitTopLevelClause(rest, " as ")
	if queryPart == "" {
		return queryResult{}, false, nil
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(queryPart)), "select ") {
		return queryResult{}, true, errors.New("CREATE TABLE AS requires SELECT")
	}
	nameToken := firstIdentifierToken(namePart)
	if strings.TrimSpace(nameToken) == "" {
		return queryResult{}, true, errors.New("CREATE TABLE AS requires a table name")
	}
	name := parseQualifiedName(nameToken)
	result, err := s.executeSQL(queryPart)
	if err != nil {
		return queryResult{}, true, err
	}
	columns := columnsFromFields(result.fields)
	if len(columns) == 0 {
		return queryResult{}, true, errors.New("CREATE TABLE AS SELECT must return columns")
	}
	attributes := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(namePart), nameToken))
	distStyle, distKey, sortKeys := parseTableAttributes(attributes)

	s.mu.Lock()
	defer s.mu.Unlock()
	schemaState := s.db.schemas[name.schema]
	if schemaState == nil {
		schemaState = &schema{tables: map[string]*table{}}
		s.db.schemas[name.schema] = schemaState
	}
	schemaState.tables[name.table] = &table{
		name:      name,
		columns:   columns,
		rows:      cloneRows(result.rows),
		distStyle: distStyle,
		distKey:   distKey,
		sortKeys:  sortKeys,
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, true, err
	}
	return queryResult{tag: fmt.Sprintf("SELECT %d", len(result.rows))}, true, nil
}

func splitTopLevelClause(value string, separator string) (string, string) {
	lower := strings.ToLower(value)
	depth := 0
	inString := false
	for i := 0; i <= len(value)-len(separator); i++ {
		ch := value[i]
		if ch == '\'' {
			if inString && i+1 < len(value) && value[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && strings.HasPrefix(lower[i:], separator) {
			return strings.TrimSpace(value[:i]), strings.TrimSpace(value[i+len(separator):])
		}
	}
	return strings.TrimSpace(value), ""
}

func (s *Server) createView(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("create "):])
	orReplace := false
	if strings.HasPrefix(strings.ToLower(rest), "or replace ") {
		orReplace = true
		rest = strings.TrimSpace(rest[len("or replace "):])
	}
	if !strings.HasPrefix(strings.ToLower(rest), "view ") {
		return queryResult{}, errors.New("CREATE VIEW requires VIEW")
	}
	rest = strings.TrimSpace(rest[len("view "):])
	namePart, queryPart := splitClause(rest, " as ")
	if namePart == "" || queryPart == "" {
		return queryResult{}, errors.New("CREATE VIEW requires name and AS SELECT")
	}
	name := parseQualifiedName(namePart)
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(queryPart)), "select ") {
		return queryResult{}, errors.New("CREATE VIEW requires SELECT")
	}
	result, err := s.executeSQL(queryPart)
	if err != nil {
		return queryResult{}, err
	}
	columns := columnsFromFields(result.fields)
	if len(columns) == 0 {
		return queryResult{}, errors.New("CREATE VIEW SELECT must return columns")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	schemaState := s.db.schemas[name.schema]
	if schemaState == nil {
		schemaState = &schema{tables: map[string]*table{}}
		s.db.schemas[name.schema] = schemaState
	}
	if existing := schemaState.tables[name.table]; existing != nil {
		if !isView(existing) {
			return queryResult{}, fmt.Errorf("relation %s.%s already exists", name.schema, name.table)
		}
		if !orReplace {
			return queryResult{}, fmt.Errorf("view %s.%s already exists", name.schema, name.table)
		}
	}
	schemaState.tables[name.table] = &table{name: name, columns: columns, kind: "VIEW", viewSQL: strings.TrimSpace(queryPart)}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "CREATE VIEW"}, nil
}

func (s *Server) createMaterializedView(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("create materialized view"):])
	if strings.HasPrefix(strings.ToLower(rest), "if not exists ") {
		rest = strings.TrimSpace(rest[len("if not exists "):])
	}
	namePart, queryPart := splitTopLevelClause(rest, " as ")
	if namePart == "" || queryPart == "" {
		return queryResult{}, errors.New("CREATE MATERIALIZED VIEW requires name and AS SELECT")
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(queryPart)), "select ") {
		return queryResult{}, errors.New("CREATE MATERIALIZED VIEW requires SELECT")
	}
	nameToken := firstIdentifierToken(namePart)
	if nameToken == "" {
		return queryResult{}, errors.New("CREATE MATERIALIZED VIEW requires a view name")
	}
	name := parseQualifiedName(nameToken)
	attributes := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(namePart), nameToken))
	distStyle, distKey, sortKeys := parseTableAttributes(attributes)
	result, err := s.executeSQL(queryPart)
	if err != nil {
		return queryResult{}, err
	}
	columns := columnsFromFields(result.fields)
	if len(columns) == 0 {
		return queryResult{}, errors.New("CREATE MATERIALIZED VIEW SELECT must return columns")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	schemaState := s.db.schemas[name.schema]
	if schemaState == nil {
		schemaState = &schema{tables: map[string]*table{}}
		s.db.schemas[name.schema] = schemaState
	}
	if existing := schemaState.tables[name.table]; existing != nil {
		if !isMaterializedView(existing) {
			return queryResult{}, fmt.Errorf("relation %s.%s already exists", name.schema, name.table)
		}
	}
	schemaState.tables[name.table] = &table{
		name:      name,
		columns:   columns,
		rows:      cloneRows(result.rows),
		kind:      "MATERIALIZED VIEW",
		viewSQL:   strings.TrimSpace(queryPart),
		distStyle: distStyle,
		distKey:   distKey,
		sortKeys:  sortKeys,
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "CREATE MATERIALIZED VIEW"}, nil
}

func (s *Server) dropView(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("drop view"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "if exists ") {
		rest = strings.TrimSpace(rest[len("if exists "):])
	}
	name := parseQualifiedName(rest)
	s.mu.Lock()
	defer s.mu.Unlock()
	if schema := s.db.schemas[name.schema]; schema != nil {
		if tableState := schema.tables[name.table]; tableState != nil && isView(tableState) {
			delete(schema.tables, name.table)
		}
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "DROP VIEW"}, nil
}

func (s *Server) dropMaterializedView(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("drop materialized view"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "if exists ") {
		rest = strings.TrimSpace(rest[len("if exists "):])
	}
	name := parseQualifiedName(rest)
	s.mu.Lock()
	defer s.mu.Unlock()
	if schema := s.db.schemas[name.schema]; schema != nil {
		if tableState := schema.tables[name.table]; tableState != nil && isMaterializedView(tableState) {
			delete(schema.tables, name.table)
		}
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "DROP MATERIALIZED VIEW"}, nil
}

func (s *Server) insertInto(statement string) (queryResult, error) {
	lower := strings.ToLower(statement)
	valuesIndex := strings.Index(lower, " values ")
	if valuesIndex < 0 {
		return queryResult{}, errors.New("INSERT requires VALUES")
	}
	namePart := strings.TrimSpace(statement[len("insert into"):valuesIndex])
	var insertColumns []string
	if columnListIndex := strings.IndexByte(namePart, '('); columnListIndex >= 0 {
		close := matchingParen(namePart, columnListIndex)
		if close < 0 {
			return queryResult{}, errors.New("INSERT column list is unterminated")
		}
		for _, columnName := range splitCommaSeparated(namePart[columnListIndex+1 : close]) {
			cleaned := cleanIdentifier(columnName)
			if cleaned == "" {
				return queryResult{}, errors.New("INSERT column list contains an empty column")
			}
			insertColumns = append(insertColumns, cleaned)
		}
		namePart = strings.TrimSpace(namePart[:columnListIndex])
	}
	name := parseQualifiedName(namePart)
	valueRows, err := parseValuesTuples(statement[valuesIndex+len(" values "):])
	if err != nil {
		return queryResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	table := s.lookupTableLocked(name)
	if table == nil {
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isReadOnlyRelation(table) {
		return queryResult{}, fmt.Errorf("cannot insert into view %s.%s", name.schema, name.table)
	}
	inserted := 0
	for _, values := range valueRows {
		row, err := buildInsertRow(table, insertColumns, values)
		if err != nil {
			return queryResult{}, err
		}
		table.rows = append(table.rows, row)
		inserted++
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: fmt.Sprintf("INSERT 0 %d", inserted)}, nil
}

func (s *Server) updateTable(statement string) (queryResult, error) {
	lower := strings.ToLower(statement)
	setIndex := strings.Index(lower, " set ")
	if setIndex < 0 {
		return queryResult{}, errors.New("UPDATE requires SET")
	}
	name := parseQualifiedName(statement[len("update "):setIndex])
	assignmentsPart, wherePart := splitClause(statement[setIndex+len(" set "):], " where ")

	s.mu.Lock()
	defer s.mu.Unlock()
	table := s.lookupTableLocked(name)
	if table == nil {
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isReadOnlyRelation(table) {
		return queryResult{}, fmt.Errorf("cannot update view %s.%s", name.schema, name.table)
	}
	assignments, err := parseAssignments(table, assignmentsPart)
	if err != nil {
		return queryResult{}, err
	}
	wherePredicate, err := parseWherePredicate(table, wherePart)
	if err != nil {
		return queryResult{}, err
	}
	updated := 0
	for rowIndex, stored := range table.rows {
		if !wherePredicate.matches(stored) {
			continue
		}
		next := append([]string(nil), stored...)
		for _, assignment := range assignments {
			next[assignment.index] = assignment.value
		}
		table.rows[rowIndex] = next
		updated++
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: fmt.Sprintf("UPDATE %d", updated)}, nil
}

func (s *Server) deleteFrom(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("delete from "):])
	tablePart, wherePart := splitClause(rest, " where ")
	name := parseQualifiedName(tablePart)

	s.mu.Lock()
	defer s.mu.Unlock()
	table := s.lookupTableLocked(name)
	if table == nil {
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isReadOnlyRelation(table) {
		return queryResult{}, fmt.Errorf("cannot delete from view %s.%s", name.schema, name.table)
	}
	wherePredicate, err := parseWherePredicate(table, wherePart)
	if err != nil {
		return queryResult{}, err
	}
	deleted := 0
	remaining := table.rows[:0]
	for _, stored := range table.rows {
		if wherePredicate.matches(stored) {
			deleted++
			continue
		}
		remaining = append(remaining, stored)
	}
	table.rows = remaining
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: fmt.Sprintf("DELETE %d", deleted)}, nil
}

func (s *Server) selectFromTable(statement string) (queryResult, error) {
	lower := strings.ToLower(statement)
	fromIndex := strings.Index(lower, " from ")
	if fromIndex < 0 {
		return queryResult{}, errors.New("SELECT requires FROM")
	}
	columnPart := strings.TrimSpace(statement[len("select"):fromIndex])
	rest := strings.TrimSpace(statement[fromIndex+len(" from "):])
	tablePart, wherePart := splitClause(rest, " where ")
	tablePart, orderPart := splitClause(tablePart, " order by ")
	tablePart, limitPart := splitClause(tablePart, " limit ")
	if wherePart != "" {
		wherePart, orderPart = splitClause(wherePart, " order by ")
		wherePart, limitPart = splitClause(wherePart, " limit ")
	}
	if orderPart != "" {
		orderPart, limitPart = splitClause(orderPart, " limit ")
	}
	name := parseQualifiedName(tablePart)

	s.mu.Lock()
	tableState := s.lookupTableLocked(name)
	if tableState == nil {
		s.mu.Unlock()
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isView(tableState) {
		viewSQL := tableState.viewSQL
		s.mu.Unlock()
		result, err := s.executeSQL(viewSQL)
		if err != nil {
			return queryResult{}, err
		}
		viewTable := tableFromQueryResult(result)
		viewTable.rows = cloneRows(result.rows)
		return selectFromResolvedTable(viewTable, columnPart, wherePart, orderPart, limitPart)
	}
	table := &table{
		name:      tableState.name,
		columns:   append([]column(nil), tableState.columns...),
		rows:      cloneRows(tableState.rows),
		kind:      tableState.kind,
		viewSQL:   tableState.viewSQL,
		distStyle: tableState.distStyle,
		distKey:   tableState.distKey,
		sortKeys:  append([]string(nil), tableState.sortKeys...),
	}
	s.mu.Unlock()
	return selectFromResolvedTable(table, columnPart, wherePart, orderPart, limitPart)
}

func selectFromResolvedTable(table *table, columnPart string, wherePart string, orderPart string, limitPart string) (queryResult, error) {
	wherePredicate, err := parseWherePredicate(table, wherePart)
	if err != nil {
		return queryResult{}, err
	}
	if countAlias, ok, err := parseCountProjection(table, columnPart); err != nil {
		return queryResult{}, err
	} else if ok {
		count := 0
		for _, stored := range table.rows {
			if wherePredicate.matches(stored) {
				count++
			}
		}
		return queryResult{
			fields: []pgField{{Name: countAlias, TypeOID: pgTypeInt4OID, TypeSize: 4}},
			rows:   [][]string{{strconv.Itoa(count)}},
			tag:    "SELECT 1",
		}, nil
	}
	selectedIndexes, fields, err := selectedColumns(table, columnPart)
	if err != nil {
		return queryResult{}, err
	}
	limit, err := parseLimit(limitPart)
	if err != nil {
		return queryResult{}, err
	}
	orderIndex, err := parseOrderBy(table, orderPart)
	if err != nil {
		return queryResult{}, err
	}
	rows := make([][]string, 0)
	for _, stored := range table.rows {
		if !wherePredicate.matches(stored) {
			continue
		}
		row := make([]string, 0, len(selectedIndexes))
		for _, index := range selectedIndexes {
			row = append(row, stored[index])
		}
		rows = append(rows, row)
	}
	if orderIndex >= 0 {
		sortRowsBySourceColumn(rows, selectedIndexes, orderIndex)
	}
	if limit >= 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return queryResult{fields: fields, rows: rows, tag: fmt.Sprintf("SELECT %d", len(rows))}, nil
}

func parseCountProjection(table *table, columnPart string) (string, bool, error) {
	expression := strings.TrimSpace(columnPart)
	if expression == "" {
		return "", false, nil
	}
	fields := strings.Fields(expression)
	if len(fields) == 0 {
		return "", false, nil
	}
	countExpr := fields[0]
	lower := strings.ToLower(countExpr)
	if !strings.HasPrefix(lower, "count(") || !strings.HasSuffix(lower, ")") {
		return "", false, nil
	}
	argument := strings.TrimSpace(countExpr[len("count(") : len(countExpr)-1])
	switch {
	case argument == "*" || argument == "1":
	case columnIndex(table, cleanColumnIdentifier(argument)) >= 0:
	default:
		return "", false, fmt.Errorf("column %s does not exist", argument)
	}
	alias := "count"
	if len(fields) > 1 {
		rest := strings.TrimSpace(expression[len(countExpr):])
		parsedAlias := parseSelectAlias(rest)
		if parsedAlias == "" {
			return "", false, fmt.Errorf("unsupported SELECT count alias syntax: %s", rest)
		}
		alias = parsedAlias
	}
	return alias, true, nil
}

func (s *Server) copyFromLocalCSV(statement string) (queryResult, error) {
	lower := strings.ToLower(statement)
	fromIndex := strings.Index(lower, " from ")
	if fromIndex < 0 {
		return queryResult{}, errors.New("COPY requires FROM")
	}
	name := parseQualifiedName(statement[len("copy "):fromIndex])
	path, rest, err := parseLeadingSQLStringLiteral(strings.TrimSpace(statement[fromIndex+len(" from "):]))
	if err != nil {
		return queryResult{}, fmt.Errorf("COPY requires a local file path or s3 URI: %w", err)
	}
	options, err := parseCopyCSVOptions(rest)
	if err != nil {
		return queryResult{}, err
	}
	s.mu.Lock()
	table := s.lookupTableLocked(name)
	if table == nil {
		s.mu.Unlock()
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isReadOnlyRelation(table) {
		s.mu.Unlock()
		return queryResult{}, fmt.Errorf("cannot copy into view %s.%s", name.schema, name.table)
	}
	columns := append([]column(nil), table.columns...)
	s.mu.Unlock()

	records, err := s.readCopyRecords(path, options, columns)
	if err != nil {
		return queryResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	table = s.lookupTableLocked(name)
	if table == nil {
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isReadOnlyRelation(table) {
		return queryResult{}, fmt.Errorf("cannot copy into view %s.%s", name.schema, name.table)
	}
	for line, record := range records {
		if len(record) != len(table.columns) {
			return queryResult{}, fmt.Errorf("COPY row %d has %d values for %d columns", line+1, len(record), len(table.columns))
		}
		copied := append([]string(nil), record...)
		table.rows = append(table.rows, copied)
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: fmt.Sprintf("COPY %d", len(records))}, nil
}

func (s *Server) unloadToLocalCSV(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("unload "):])
	if !strings.HasPrefix(rest, "(") {
		return queryResult{}, errors.New("UNLOAD requires a parenthesized SELECT")
	}
	close := matchingParen(rest, 0)
	if close < 0 {
		return queryResult{}, errors.New("UNLOAD has an unterminated SELECT")
	}
	selectSQL, _, err := parseLeadingSQLStringLiteral(strings.TrimSpace(rest[1:close]))
	if err != nil {
		return queryResult{}, fmt.Errorf("UNLOAD requires SELECT SQL as a string literal: %w", err)
	}
	afterSelect := strings.TrimSpace(rest[close+1:])
	if !strings.HasPrefix(strings.ToLower(afterSelect), "to ") {
		return queryResult{}, errors.New("UNLOAD requires TO")
	}
	targetPrefix, _, err := parseLeadingSQLStringLiteral(strings.TrimSpace(afterSelect[len("to "):]))
	if err != nil {
		return queryResult{}, fmt.Errorf("UNLOAD requires a local target prefix or s3 URI: %w", err)
	}

	result, err := s.executeSQL(selectSQL)
	if err != nil {
		return queryResult{}, err
	}
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	for _, row := range result.rows {
		if err := writer.Write(row); err != nil {
			return queryResult{}, fmt.Errorf("UNLOAD write CSV: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return queryResult{}, fmt.Errorf("UNLOAD flush CSV: %w", err)
	}
	if strings.HasPrefix(strings.ToLower(targetPrefix), "s3://") {
		if err := s.writeS3Object(targetPrefix+"000", bytes.NewReader(output.Bytes())); err != nil {
			return queryResult{}, err
		}
		return queryResult{tag: "UNLOAD"}, nil
	}

	outputPath := filepath.Clean(targetPrefix + "000")
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return queryResult{}, fmt.Errorf("UNLOAD create target directory: %w", err)
	}
	if err := os.WriteFile(outputPath, output.Bytes(), 0o644); err != nil {
		return queryResult{}, fmt.Errorf("UNLOAD write target file: %w", err)
	}
	return queryResult{tag: "UNLOAD"}, nil
}

func (s *Server) readCopyRecords(source string, options copyCSVOptions, columns []column) ([][]string, error) {
	if options.format == "json" {
		return s.readCopyJSONRecords(source, columns)
	}
	return s.readCopyCSVRecords(source, options)
}

func (s *Server) readCopyCSVRecords(source string, options copyCSVOptions) ([][]string, error) {
	var reader io.Reader
	if strings.HasPrefix(strings.ToLower(source), "s3://") {
		data, err := s.readS3Object(source)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	} else {
		file, err := os.Open(filepath.Clean(source))
		if err != nil {
			return nil, fmt.Errorf("COPY open source file: %w", err)
		}
		defer file.Close()
		reader = file
	}
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	if options.delimiter != 0 {
		csvReader.Comma = options.delimiter
	}
	records, err := s.readCSVRecordsWithLimit(csvReader, options.delimiter)
	if err != nil {
		return nil, err
	}
	if options.ignoreHeader > 0 {
		if options.ignoreHeader >= len(records) {
			records = nil
		} else {
			records = records[options.ignoreHeader:]
		}
	}
	if options.hasNullAs {
		for rowIndex := range records {
			for columnIndex, value := range records[rowIndex] {
				if value == options.nullAs {
					records[rowIndex][columnIndex] = ""
				}
			}
		}
	}
	return records, nil
}

func (s *Server) readCopyJSONRecords(source string, columns []column) ([][]string, error) {
	var reader io.Reader
	if strings.HasPrefix(strings.ToLower(source), "s3://") {
		data, err := s.readS3Object(source)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	} else {
		file, err := os.Open(filepath.Clean(source))
		if err != nil {
			return nil, fmt.Errorf("COPY open source file: %w", err)
		}
		defer file.Close()
		reader = file
	}

	scanner := bufio.NewScanner(reader)
	maxScanBytes := 4 * 1024 * 1024
	if s.config.MaxCopyInputBytes > int64(maxScanBytes) {
		maxScanBytes = int(s.config.MaxCopyInputBytes)
	}
	scanner.Buffer(make([]byte, 0, 64*1024), maxScanBytes)

	var records [][]string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if err := s.validateCopyJSONLineSize(line); err != nil {
			return nil, err
		}
		record, err := jsonLineToRecord(line, columns)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("COPY read JSON: %w", err)
	}
	return records, nil
}

func (s *Server) validateCopyJSONLineSize(line string) error {
	if s.config.MaxCopyInputBytes <= 0 {
		return nil
	}
	if int64(len(line)) > s.config.MaxCopyInputBytes {
		return fmt.Errorf("COPY input row exceeds maxCopyInputBytes")
	}
	return nil
}

func jsonLineToRecord(line string, columns []column) ([]string, error) {
	decoder := json.NewDecoder(strings.NewReader(line))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, fmt.Errorf("COPY read JSON: %w", err)
	}
	lowerObject := make(map[string]any, len(object))
	for key, value := range object {
		lowerObject[strings.ToLower(key)] = value
	}
	record := make([]string, 0, len(columns))
	for _, column := range columns {
		value, ok := lowerObject[strings.ToLower(column.name)]
		if !ok || value == nil {
			record = append(record, "")
			continue
		}
		record = append(record, jsonCopyValueString(value))
	}
	return record, nil
}

func jsonCopyValueString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprint(typed)
		}
		return string(data)
	}
}

func (s *Server) readCSVRecordsWithLimit(csvReader *csv.Reader, delimiter rune) ([][]string, error) {
	var records [][]string
	for {
		record, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, fmt.Errorf("COPY read CSV: %w", err)
		}
		if err := s.validateCopyRecordSize(record, delimiter); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}

func (s *Server) validateCopyRecordSize(record []string, delimiter rune) error {
	if s.config.MaxCopyInputBytes <= 0 {
		return nil
	}
	size := 0
	for index, value := range record {
		if index > 0 {
			size += len(string(delimiter))
		}
		size += len(value)
		if int64(size) > s.config.MaxCopyInputBytes {
			return fmt.Errorf("COPY input row exceeds maxCopyInputBytes")
		}
	}
	return nil
}

func (s *Server) readS3Object(uri string) ([]byte, error) {
	if s.config.ObjectStore == nil {
		return nil, errors.New("COPY from s3 URI requires local S3 service to be enabled")
	}
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return nil, err
	}
	_, data, ok, err := s.config.ObjectStore.GetObject(context.Background(), bucket, key)
	if err != nil {
		return nil, fmt.Errorf("COPY read S3 object: %w", err)
	}
	if !ok {
		return nil, errors.New("COPY source S3 object does not exist")
	}
	return data, nil
}

func (s *Server) writeS3Object(uri string, body io.Reader) error {
	if s.config.ObjectStore == nil {
		return errors.New("UNLOAD to s3 URI requires local S3 service to be enabled")
	}
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return err
	}
	if _, err := s.config.ObjectStore.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:      bucket,
		Key:         key,
		Body:        body,
		ContentType: "text/csv",
	}); err != nil {
		return fmt.Errorf("UNLOAD write S3 object: %w", err)
	}
	return nil
}

func parseS3URI(uri string) (string, string, error) {
	if !strings.HasPrefix(strings.ToLower(uri), "s3://") {
		return "", "", fmt.Errorf("expected s3 URI")
	}
	rest := uri[len("s3://"):]
	bucket, key, ok := strings.Cut(rest, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", fmt.Errorf("s3 URI requires bucket and key")
	}
	return bucket, key, nil
}

func parseCopyCSVOptions(value string) (copyCSVOptions, error) {
	tokens, err := tokenizeSQLOptions(value)
	if err != nil {
		return copyCSVOptions{}, err
	}
	options := copyCSVOptions{delimiter: ',', format: "csv"}
	for i := 0; i < len(tokens); i++ {
		token := strings.ToLower(tokens[i].value)
		switch token {
		case "", "csv":
			options.format = "csv"
			continue
		case "json":
			options.format = "json"
			if i+1 < len(tokens) && (strings.EqualFold(tokens[i+1].value, "auto") || strings.EqualFold(tokens[i+1].value, "noshred")) {
				i++
			}
		case "iam_role", "credentials", "region":
			if i+1 < len(tokens) {
				i++
			}
		case "delimiter":
			next := i + 1
			if next < len(tokens) && strings.EqualFold(tokens[next].value, "as") {
				next++
			}
			if next >= len(tokens) {
				return copyCSVOptions{}, errors.New("COPY DELIMITER requires a value")
			}
			delimiter, err := parseCSVDelimiter(tokens[next].value)
			if err != nil {
				return copyCSVOptions{}, err
			}
			options.delimiter = delimiter
			i = next
		case "ignoreheader":
			if i+1 >= len(tokens) {
				return copyCSVOptions{}, errors.New("COPY IGNOREHEADER requires a row count")
			}
			count, err := strconv.Atoi(tokens[i+1].value)
			if err != nil || count < 0 {
				return copyCSVOptions{}, errors.New("COPY IGNOREHEADER requires a non-negative row count")
			}
			options.ignoreHeader = count
			i++
		case "null":
			next := i + 1
			if next < len(tokens) && strings.EqualFold(tokens[next].value, "as") {
				next++
			}
			if next >= len(tokens) {
				return copyCSVOptions{}, errors.New("COPY NULL AS requires a value")
			}
			options.nullAs = tokens[next].value
			options.hasNullAs = true
			i = next
		}
	}
	return options, nil
}

type sqlOptionToken struct {
	value string
}

func tokenizeSQLOptions(value string) ([]sqlOptionToken, error) {
	var tokens []sqlOptionToken
	for i := 0; i < len(value); {
		if unicode.IsSpace(rune(value[i])) || value[i] == ';' {
			i++
			continue
		}
		if value[i] == '\'' {
			parsed, rest, err := parseLeadingSQLStringLiteral(value[i:])
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, sqlOptionToken{value: parsed})
			i = len(value) - len(rest)
			continue
		}
		start := i
		for i < len(value) && !unicode.IsSpace(rune(value[i])) && value[i] != ';' {
			i++
		}
		tokens = append(tokens, sqlOptionToken{value: value[start:i]})
	}
	return tokens, nil
}

func parseCSVDelimiter(value string) (rune, error) {
	if value == `\t` {
		return '\t', nil
	}
	runes := []rune(value)
	if len(runes) != 1 {
		return 0, errors.New("COPY DELIMITER requires exactly one character")
	}
	if runes[0] == '\r' || runes[0] == '\n' || runes[0] == 0xfffd {
		return 0, errors.New("COPY DELIMITER contains an unsupported character")
	}
	return runes[0], nil
}

func (s *Server) passwordAllowed(password string) bool {
	if strings.EqualFold(s.config.AuthMode, "strict") {
		return password == s.config.Password
	}
	expected := defaultString(s.config.Password, "dev")
	return password == "" || password == expected
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

func splitSQLStatements(query string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		current.WriteByte(ch)
		if ch == '\'' {
			if inString && i+1 < len(query) && query[i+1] == '\'' {
				current.WriteByte(query[i+1])
				i++
				continue
			}
			inString = !inString
		}
		if ch == ';' && !inString {
			statement := strings.TrimSpace(current.String())
			if strings.Trim(statement, "; \t\r\n") != "" {
				statements = append(statements, statement)
			}
			current.Reset()
		}
	}
	if statement := strings.TrimSpace(current.String()); statement != "" {
		statements = append(statements, statement)
	}
	return statements
}

func parseQualifiedName(value string) qualifiedName {
	token := firstIdentifierToken(value)
	parts := strings.Split(token, ".")
	if len(parts) == 1 {
		return qualifiedName{schema: "public", table: cleanIdentifier(parts[0])}
	}
	return qualifiedName{schema: cleanIdentifier(parts[0]), table: cleanIdentifier(parts[1])}
}

func firstIdentifierToken(value string) string {
	value = strings.TrimSpace(value)
	for i, r := range value {
		if unicode.IsSpace(r) || r == '(' || r == ';' {
			return value[:i]
		}
	}
	return value
}

func cleanIdentifier(value string) string {
	return strings.Trim(strings.TrimSpace(value), `"`)
}

func cleanColumnIdentifier(value string) string {
	cleaned := cleanIdentifier(value)
	if dot := strings.LastIndex(cleaned, "."); dot >= 0 {
		cleaned = cleaned[dot+1:]
	}
	return cleanIdentifier(cleaned)
}

func matchingParen(value string, open int) int {
	if open < 0 || open >= len(value) || value[open] != '(' {
		return -1
	}
	depth := 0
	inString := false
	for i := open; i < len(value); i++ {
		ch := value[i]
		if ch == '\'' {
			if inString && i+1 < len(value) && value[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func parseColumns(value string) ([]column, error) {
	definitions := splitCommaSeparated(value)
	columns := make([]column, 0, len(definitions))
	for _, definition := range definitions {
		fields := strings.Fields(strings.TrimSpace(definition))
		if len(fields) < 2 {
			return nil, errors.New("CREATE TABLE column definition requires name and type")
		}
		name := cleanIdentifier(fields[0])
		if name == "" {
			return nil, errors.New("CREATE TABLE column name cannot be empty")
		}
		columns = append(columns, parseColumnDefinition(name, fields[1], fields[2:]))
	}
	if len(columns) == 0 {
		return nil, errors.New("CREATE TABLE requires at least one column")
	}
	return columns, nil
}

func parseColumnDefinition(name string, dataType string, attributes []string) column {
	column := column{name: name, dataType: strings.ToLower(dataType)}
	for i := 0; i < len(attributes); i++ {
		token := strings.ToLower(attributes[i])
		switch {
		case token == "encode":
			if i+1 < len(attributes) {
				column.encoding = cleanIdentifier(attributes[i+1])
				i++
			}
		case token == "default":
			if i+1 < len(attributes) && !strings.EqualFold(attributes[i+1], "as") {
				column.defaultValue = attributes[i+1]
				i++
			}
		case token == "identity" || strings.HasPrefix(token, "identity("):
			column.identity = true
		case token == "generated":
			for i+1 < len(attributes) {
				i++
				if next := strings.ToLower(attributes[i]); next == "identity" || strings.HasPrefix(next, "identity(") {
					column.identity = true
					break
				}
			}
		case token == "distkey":
			column.distKey = true
		case token == "sortkey":
			column.sortKey = true
		}
	}
	return column
}

func applyColumnTableAttributes(columns []column, distStyle *string, distKey *string, sortKeys *[]string) {
	for _, column := range columns {
		if column.distKey && *distKey == "" {
			*distKey = column.name
			if *distStyle == "" {
				*distStyle = "key"
			}
		}
		if column.sortKey && !containsIdentifier(*sortKeys, column.name) {
			*sortKeys = append(*sortKeys, column.name)
		}
	}
}

func containsIdentifier(values []string, value string) bool {
	for _, item := range values {
		if strings.EqualFold(item, value) {
			return true
		}
	}
	return false
}

func parseTableAttributes(value string) (string, string, []string) {
	fields := strings.Fields(value)
	distStyle := ""
	distKey := ""
	var sortKeys []string
	for i := 0; i < len(fields); i++ {
		token := strings.ToLower(fields[i])
		switch {
		case token == "diststyle" && i+1 < len(fields):
			distStyle = strings.ToLower(cleanIdentifier(fields[i+1]))
			i++
		case strings.HasPrefix(token, "diststyle") && strings.Contains(token, " "):
			distStyle = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(token, "diststyle")))
		case strings.HasPrefix(token, "distkey"):
			if key := parseParenthesizedIdentifier(fields[i], "distkey"); key != "" {
				distKey = key
			} else if i+1 < len(fields) {
				distKey = parseParenthesizedIdentifier(fields[i+1], "")
				i++
			}
		case strings.HasPrefix(token, "sortkey"):
			if keys := parseParenthesizedIdentifierList(fields[i], "sortkey"); len(keys) > 0 {
				sortKeys = keys
			} else if i+1 < len(fields) {
				sortKeys = parseParenthesizedIdentifierList(fields[i+1], "")
				i++
			}
		}
	}
	return distStyle, distKey, sortKeys
}

func parseParenthesizedIdentifier(value string, prefix string) string {
	values := parseParenthesizedIdentifierList(value, prefix)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func parseParenthesizedIdentifierList(value string, prefix string) []string {
	value = strings.TrimSpace(value)
	if prefix != "" {
		if !strings.HasPrefix(strings.ToLower(value), prefix) {
			return nil
		}
		value = strings.TrimSpace(value[len(prefix):])
	}
	value = strings.TrimSpace(value)
	if len(value) < 2 || value[0] != '(' || value[len(value)-1] != ')' {
		return nil
	}
	parts := splitCommaSeparated(value[1 : len(value)-1])
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if cleaned := cleanIdentifier(part); cleaned != "" {
			result = append(result, cleaned)
		}
	}
	return result
}

func splitCommaSeparated(value string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inString := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '\'' {
			if inString && i+1 < len(value) && value[i+1] == '\'' {
				current.WriteByte(ch)
				current.WriteByte(value[i+1])
				i++
				continue
			}
			inString = !inString
		}
		if !inString {
			switch ch {
			case '(':
				depth++
			case ')':
				depth--
			case ',':
				if depth == 0 {
					parts = append(parts, strings.TrimSpace(current.String()))
					current.Reset()
					continue
				}
			}
		}
		current.WriteByte(ch)
	}
	if part := strings.TrimSpace(current.String()); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func parseCSVishValues(value string) ([]string, error) {
	parts := splitCommaSeparated(value)
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		parsed, err := parseLiteral(part)
		if err != nil {
			return nil, err
		}
		values = append(values, parsed)
	}
	return values, nil
}

func parseValuesTuples(value string) ([][]string, error) {
	var rows [][]string
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("INSERT requires at least one VALUES row")
	}
	for {
		if value[0] != '(' {
			return nil, errors.New("INSERT requires parenthesized VALUES rows")
		}
		close := matchingParen(value, 0)
		if close < 0 {
			return nil, errors.New("INSERT has an unterminated row value list")
		}
		row, err := parseCSVishValues(value[1:close])
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		value = strings.TrimSpace(value[close+1:])
		if value == "" {
			break
		}
		if value[0] != ',' {
			return nil, errors.New("INSERT VALUES rows must be separated by commas")
		}
		value = strings.TrimSpace(value[1:])
		if value == "" {
			return nil, errors.New("INSERT requires a VALUES row after comma")
		}
	}
	if len(rows) == 0 {
		return nil, errors.New("INSERT requires at least one VALUES row")
	}
	return rows, nil
}

type columnAssignment struct {
	index int
	value string
}

type wherePredicate struct {
	index int
	op    string
	value string
}

func (p wherePredicate) matches(row []string) bool {
	if p.index < 0 {
		return true
	}
	if p.index >= len(row) {
		return false
	}
	left := row[p.index]
	switch p.op {
	case "=":
		return left == p.value
	case "!=", "<>":
		return left != p.value
	case ">", ">=", "<", "<=":
		comparison := compareSQLValues(left, p.value)
		switch p.op {
		case ">":
			return comparison > 0
		case ">=":
			return comparison >= 0
		case "<":
			return comparison < 0
		case "<=":
			return comparison <= 0
		}
	}
	return false
}

func buildInsertRow(table *table, insertColumns []string, values []string) ([]string, error) {
	if len(insertColumns) == 0 {
		if len(values) != len(table.columns) {
			return nil, fmt.Errorf("INSERT has %d values for %d columns", len(values), len(table.columns))
		}
		row := make([]string, 0, len(values))
		for index, value := range values {
			resolved, err := resolveInsertValue(table, index, value)
			if err != nil {
				return nil, err
			}
			row = append(row, resolved)
		}
		return row, nil
	}
	if len(values) != len(insertColumns) {
		return nil, fmt.Errorf("INSERT has %d values for %d target columns", len(values), len(insertColumns))
	}
	row := make([]string, len(table.columns))
	assigned := make([]bool, len(table.columns))
	for valueIndex, columnName := range insertColumns {
		columnIndex := columnIndex(table, columnName)
		if columnIndex < 0 {
			return nil, fmt.Errorf("column %s does not exist", columnName)
		}
		if assigned[columnIndex] {
			return nil, fmt.Errorf("column %s specified more than once", columnName)
		}
		resolved, err := resolveInsertValue(table, columnIndex, values[valueIndex])
		if err != nil {
			return nil, err
		}
		row[columnIndex] = resolved
		assigned[columnIndex] = true
	}
	for index := range table.columns {
		if assigned[index] {
			continue
		}
		row[index] = defaultInsertValue(table, index)
	}
	return row, nil
}

func resolveInsertValue(table *table, columnIndex int, value string) (string, error) {
	if strings.EqualFold(strings.TrimSpace(value), "default") {
		return defaultInsertValue(table, columnIndex), nil
	}
	return value, nil
}

func defaultInsertValue(table *table, columnIndex int) string {
	column := table.columns[columnIndex]
	if column.defaultValue != "" {
		return strings.Trim(column.defaultValue, "'")
	}
	if column.identity {
		return strconv.Itoa(nextIdentityValue(table, columnIndex))
	}
	return ""
}

func nextIdentityValue(table *table, columnIndex int) int {
	next := 1
	for _, row := range table.rows {
		if columnIndex >= len(row) {
			continue
		}
		value, err := strconv.Atoi(row[columnIndex])
		if err == nil && value >= next {
			next = value + 1
		}
	}
	return next
}

func parseAssignments(table *table, assignmentsPart string) ([]columnAssignment, error) {
	parts := splitCommaSeparated(assignmentsPart)
	if len(parts) == 0 {
		return nil, errors.New("UPDATE requires at least one assignment")
	}
	assignments := make([]columnAssignment, 0, len(parts))
	for _, part := range parts {
		pieces := strings.SplitN(part, "=", 2)
		if len(pieces) != 2 {
			return nil, errors.New("UPDATE assignments must use column = literal")
		}
		name := cleanIdentifier(pieces[0])
		index := columnIndex(table, name)
		if index < 0 {
			return nil, fmt.Errorf("column %s does not exist", name)
		}
		value, err := parseLiteral(pieces[1])
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, columnAssignment{index: index, value: value})
	}
	return assignments, nil
}

func parseLiteral(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		return strings.ReplaceAll(value[1:len(value)-1], "''", "'"), nil
	}
	if value == "" {
		return "", errors.New("empty literal")
	}
	return value, nil
}

func splitClause(value string, separator string) (string, string) {
	lower := strings.ToLower(value)
	index := strings.Index(lower, separator)
	if index < 0 {
		return strings.TrimSpace(value), ""
	}
	return strings.TrimSpace(value[:index]), strings.TrimSpace(value[index+len(separator):])
}

func selectedColumns(table *table, columnPart string) ([]int, []pgField, error) {
	if strings.TrimSpace(columnPart) == "*" {
		indexes := make([]int, 0, len(table.columns))
		fields := make([]pgField, 0, len(table.columns))
		for i, column := range table.columns {
			indexes = append(indexes, i)
			fields = append(fields, pgField{Name: column.name, TypeOID: pgTypeOID(column.dataType), TypeSize: pgTypeSize(column.dataType)})
		}
		return indexes, fields, nil
	}
	names := splitCommaSeparated(columnPart)
	indexes := make([]int, 0, len(names))
	fields := make([]pgField, 0, len(names))
	for _, name := range names {
		cleaned := cleanColumnIdentifier(name)
		index := columnIndex(table, cleaned)
		if index < 0 {
			return nil, nil, fmt.Errorf("column %s does not exist", cleaned)
		}
		column := table.columns[index]
		indexes = append(indexes, index)
		fields = append(fields, pgField{Name: column.name, TypeOID: pgTypeOID(column.dataType), TypeSize: pgTypeSize(column.dataType)})
	}
	return indexes, fields, nil
}

func parseWherePredicate(table *table, wherePart string) (wherePredicate, error) {
	if wherePart == "" {
		return wherePredicate{index: -1}, nil
	}
	left, op, right, ok := splitWhereComparison(wherePart)
	if !ok {
		return wherePredicate{}, errors.New("only simple WHERE comparison is supported")
	}
	name := cleanColumnIdentifier(left)
	index := columnIndex(table, name)
	if index < 0 {
		return wherePredicate{}, fmt.Errorf("column %s does not exist", name)
	}
	value, err := parseLiteral(right)
	if err != nil {
		return wherePredicate{}, err
	}
	return wherePredicate{index: index, op: op, value: value}, nil
}

func splitWhereComparison(wherePart string) (string, string, string, bool) {
	operators := []string{">=", "<=", "!=", "<>", "=", ">", "<"}
	inString := false
	for i := 0; i < len(wherePart); i++ {
		ch := wherePart[i]
		if ch == '\'' {
			if inString && i+1 < len(wherePart) && wherePart[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		for _, op := range operators {
			if strings.HasPrefix(wherePart[i:], op) {
				left := strings.TrimSpace(wherePart[:i])
				right := strings.TrimSpace(wherePart[i+len(op):])
				return left, op, right, left != "" && right != ""
			}
		}
	}
	return "", "", "", false
}

func compareSQLValues(left string, right string) int {
	leftInt, leftErr := strconv.ParseInt(left, 10, 64)
	rightInt, rightErr := strconv.ParseInt(right, 10, 64)
	if leftErr == nil && rightErr == nil {
		switch {
		case leftInt < rightInt:
			return -1
		case leftInt > rightInt:
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(left, right)
}

func parseOrderBy(table *table, orderPart string) (int, error) {
	if orderPart == "" {
		return -1, nil
	}
	fields := strings.Fields(orderPart)
	if len(fields) == 0 {
		return -1, nil
	}
	index := columnIndex(table, cleanColumnIdentifier(fields[0]))
	if index < 0 {
		return -1, fmt.Errorf("column %s does not exist", fields[0])
	}
	if len(fields) > 1 && !strings.EqualFold(fields[1], "asc") {
		return -1, errors.New("only ORDER BY column ASC is supported")
	}
	return index, nil
}

func sortRowsBySourceColumn(rows [][]string, selectedIndexes []int, orderIndex int) {
	selectedIndex := -1
	for i, sourceIndex := range selectedIndexes {
		if sourceIndex == orderIndex {
			selectedIndex = i
			break
		}
	}
	if selectedIndex < 0 {
		return
	}
	sort.SliceStable(rows, func(i, j int) bool {
		left := rows[i][selectedIndex]
		right := rows[j][selectedIndex]
		leftInt, leftErr := strconv.ParseInt(left, 10, 64)
		rightInt, rightErr := strconv.ParseInt(right, 10, 64)
		if leftErr == nil && rightErr == nil {
			return leftInt < rightInt
		}
		return left < right
	})
}

func parseLimit(value string) (int, error) {
	if value == "" {
		return -1, nil
	}
	limit, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || limit < 0 {
		return -1, errors.New("LIMIT must be a non-negative integer")
	}
	return limit, nil
}

func columnIndex(table *table, name string) int {
	for i, column := range table.columns {
		if strings.EqualFold(column.name, name) {
			return i
		}
	}
	return -1
}

func parseLeadingSQLStringLiteral(value string) (string, string, error) {
	value = strings.TrimSpace(value)
	if len(value) < 2 || value[0] != '\'' {
		return "", value, errors.New("expected SQL string literal")
	}
	var builder strings.Builder
	for i := 1; i < len(value); i++ {
		ch := value[i]
		if ch != '\'' {
			builder.WriteByte(ch)
			continue
		}
		if i+1 < len(value) && value[i+1] == '\'' {
			builder.WriteByte('\'')
			i++
			continue
		}
		return builder.String(), strings.TrimSpace(value[i+1:]), nil
	}
	return "", value, errors.New("unterminated SQL string literal")
}

func pgTypeOID(dataType string) int32 {
	normalized := strings.ToLower(dataType)
	if strings.Contains(normalized, "int") {
		return pgTypeInt4OID
	}
	if normalized == "bool" || normalized == "boolean" {
		return pgTypeBoolOID
	}
	if strings.Contains(normalized, "double") || strings.Contains(normalized, "float") || normalized == "real" {
		return pgTypeFloat8OID
	}
	return pgTypeVarcharOID
}

func pgTypeSize(dataType string) int16 {
	switch pgTypeOID(dataType) {
	case pgTypeInt4OID:
		return 4
	case pgTypeBoolOID:
		return 1
	case pgTypeFloat8OID:
		return 8
	}
	return -1
}

func isCatalogSelect(lower string) bool {
	return strings.HasPrefix(lower, "select ") &&
		(strings.Contains(lower, " information_schema.") ||
			strings.Contains(lower, " pg_catalog.") ||
			strings.Contains(lower, " svv_") ||
			strings.Contains(lower, " stl_") ||
			strings.Contains(lower, " stv_") ||
			strings.Contains(lower, " pg_table_def"))
}

func (s *Server) selectCatalog(statement string) (queryResult, error) {
	lower := strings.ToLower(statement)
	var result queryResult
	switch {
	case strings.Contains(lower, "information_schema.schemata"):
		result = s.catalogSchemata()
	case strings.Contains(lower, "information_schema.tables"):
		result = s.catalogTables()
	case strings.Contains(lower, "information_schema.columns"):
		result = s.catalogColumns()
	case strings.Contains(lower, "pg_catalog.pg_namespace"):
		result = s.catalogPGNamespace()
	case strings.Contains(lower, "pg_catalog.pg_database"):
		result = s.catalogPGDatabase()
	case strings.Contains(lower, "pg_catalog.pg_class"):
		result = s.catalogPGClass()
	case strings.Contains(lower, "pg_catalog.pg_attribute"):
		result = s.catalogPGAttribute()
	case strings.Contains(lower, "pg_catalog.pg_tables"):
		result = s.catalogPGTables()
	case strings.Contains(lower, "pg_catalog.pg_type"):
		result = s.catalogPGType()
	case strings.Contains(lower, "pg_catalog.pg_user"):
		result = s.catalogPGUser()
	case strings.Contains(lower, "pg_table_def"):
		result = s.catalogPGTableDef()
	case strings.Contains(lower, "svv_columns"):
		result = s.catalogSVVColumns()
	case strings.Contains(lower, "svv_mv_info"):
		result = s.catalogSVVMVInfo()
	case strings.Contains(lower, "svv_table_info"):
		result = s.catalogSVVTableInfo()
	case strings.Contains(lower, "stl_query"):
		result = s.catalogSTLQuery()
	case strings.Contains(lower, "stv_recents"):
		result = s.catalogSTVRecents()
	default:
		return queryResult{}, errors.New("unsupported Redshift catalog query in local MVP")
	}
	return shapeCatalogResult(statement, result)
}

func shapeCatalogResult(statement string, result queryResult) (queryResult, error) {
	lower := strings.ToLower(statement)
	fromIndex := strings.Index(lower, " from ")
	if fromIndex < 0 {
		return result, nil
	}
	columnPart := strings.TrimSpace(statement[len("select"):fromIndex])
	rest := strings.TrimSpace(statement[fromIndex+len(" from "):])
	_, clausePart := splitCatalogFromClause(rest)
	tableState := tableFromQueryResult(result)

	wherePart, orderPart, limitPart := splitSelectClauses(clausePart)
	wherePredicate, err := parseWherePredicate(tableState, wherePart)
	if err != nil {
		return queryResult{}, err
	}
	if countAlias, ok, err := parseCountProjection(tableState, columnPart); err != nil {
		return queryResult{}, err
	} else if ok {
		count := 0
		for _, row := range result.rows {
			if wherePredicate.matches(row) {
				count++
			}
		}
		return queryResult{
			fields: []pgField{{Name: countAlias, TypeOID: pgTypeInt4OID, TypeSize: 4}},
			rows:   [][]string{{strconv.Itoa(count)}},
			tag:    "SELECT 1",
		}, nil
	}
	selectedIndexes, fields, err := selectedColumns(tableState, columnPart)
	if err != nil {
		return queryResult{}, err
	}
	orderIndex, err := parseOrderBy(tableState, orderPart)
	if err != nil {
		return queryResult{}, err
	}
	limit, err := parseLimit(limitPart)
	if err != nil {
		return queryResult{}, err
	}

	rows := make([][]string, 0, len(result.rows))
	for _, sourceRow := range result.rows {
		if !wherePredicate.matches(sourceRow) {
			continue
		}
		row := make([]string, 0, len(selectedIndexes))
		for _, index := range selectedIndexes {
			row = append(row, sourceRow[index])
		}
		rows = append(rows, row)
	}
	if orderIndex >= 0 {
		sortRowsBySourceColumn(rows, selectedIndexes, orderIndex)
	}
	if limit >= 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return queryResult{fields: fields, rows: rows, tag: fmt.Sprintf("SELECT %d", len(rows))}, nil
}

func splitCatalogFromClause(rest string) (string, string) {
	for _, separator := range []string{" where ", " order by ", " limit "} {
		tablePart, clausePart := splitClause(rest, separator)
		if clausePart != "" {
			return firstCatalogToken(tablePart), separator + clausePart
		}
	}
	return firstCatalogToken(rest), ""
}

func firstCatalogToken(value string) string {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func splitSelectClauses(value string) (string, string, string) {
	clausePart := strings.TrimSpace(value)
	wherePart := ""
	orderPart := ""
	limitPart := ""
	if strings.HasPrefix(strings.ToLower(clausePart), "where ") {
		wherePart = strings.TrimSpace(clausePart[len("where "):])
		wherePart, orderPart = splitClause(wherePart, " order by ")
		wherePart, limitPart = splitClause(wherePart, " limit ")
	}
	if strings.HasPrefix(strings.ToLower(clausePart), "order by ") {
		orderPart = strings.TrimSpace(clausePart[len("order by "):])
	}
	if strings.HasPrefix(strings.ToLower(clausePart), "limit ") {
		limitPart = strings.TrimSpace(clausePart[len("limit "):])
	}
	if orderPart != "" {
		orderPart, limitPart = splitClause(orderPart, " limit ")
	}
	return wherePart, orderPart, limitPart
}

func tableFromQueryResult(result queryResult) *table {
	return &table{columns: columnsFromFields(result.fields)}
}

func (s *Server) catalogSchemata() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0, len(s.db.schemas))
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		rows = append(rows, []string{defaultString(s.config.Database, "dev"), schemaName, defaultString(s.config.User, "dev")})
	}
	return queryResult{
		fields: []pgField{
			{Name: "catalog_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "schema_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "schema_owner", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogTables() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.catalogTableRowsLocked()
	return queryResult{
		fields: []pgField{
			{Name: "table_catalog", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_schema", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_type", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogColumns() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.catalogColumnRowsLocked()
	return queryResult{
		fields: []pgField{
			{Name: "table_catalog", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_schema", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "column_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "ordinal_position", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "column_default", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "data_type", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "encoding", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGNamespace() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0, len(s.db.schemas))
	for i, schemaName := range sortedSchemaNames(s.db.schemas) {
		rows = append(rows, []string{strconv.Itoa(2200 + i), schemaName})
	}
	return queryResult{
		fields: []pgField{{Name: "oid", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "nspname", TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   rows,
		tag:    fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGDatabase() queryResult {
	return queryResult{
		fields: []pgField{
			{Name: "oid", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "datname", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "datdba", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "encoding", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "datistemplate", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "datallowconn", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: [][]string{{
			"1",
			defaultString(s.config.Database, "dev"),
			"10",
			"6",
			"false",
			"true",
		}},
		tag: "SELECT 1",
	}
}

func (s *Server) catalogPGClass() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			rows = append(rows, []string{catalogTableOID(schemaName, tableName), tableName, pgClassRelKind(tableState)})
		}
	}
	return queryResult{
		fields: []pgField{{Name: "oid", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "relname", TypeOID: pgTypeVarcharOID, TypeSize: -1}, {Name: "relkind", TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   rows,
		tag:    fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGAttribute() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			for i, column := range tableState.columns {
				rows = append(rows, []string{catalogTableOID(schemaName, tableName), strconv.Itoa(i + 1), column.name, strconv.Itoa(int(pgTypeOID(column.dataType)))})
			}
		}
	}
	return queryResult{
		fields: []pgField{{Name: "attrelid", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "attnum", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "attname", TypeOID: pgTypeVarcharOID, TypeSize: -1}, {Name: "atttypid", TypeOID: pgTypeInt4OID, TypeSize: 4}},
		rows:   rows,
		tag:    fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGTables() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, row := range s.catalogTableRowsLocked() {
		if row[3] != "BASE TABLE" {
			continue
		}
		rows = append(rows, []string{row[1], row[2], defaultString(s.config.User, "dev")})
	}
	return queryResult{
		fields: []pgField{{Name: "schemaname", TypeOID: pgTypeVarcharOID, TypeSize: -1}, {Name: "tablename", TypeOID: pgTypeVarcharOID, TypeSize: -1}, {Name: "tableowner", TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   rows,
		tag:    fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGType() queryResult {
	rows := [][]string{
		{strconv.Itoa(int(pgTypeInt4OID)), "int4", "4", "N"},
		{strconv.Itoa(int(pgTypeVarcharOID)), "varchar", "-1", "S"},
		{"25", "text", "-1", "S"},
		{"16", "bool", "1", "B"},
	}
	return queryResult{
		fields: []pgField{
			{Name: "oid", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "typname", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "typlen", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "typcategory", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGUser() queryResult {
	return queryResult{
		fields: []pgField{
			{Name: "usename", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "usesysid", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "usecreatedb", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "usesuper", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "passwd", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: [][]string{{
			defaultString(s.config.User, "dev"),
			"10",
			"true",
			"true",
			"********",
		}},
		tag: "SELECT 1",
	}
}

func (s *Server) catalogSVVTableInfo() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			if isView(tableState) {
				continue
			}
			rows = append(rows, []string{
				schemaName,
				tableName,
				defaultString(tableState.distStyle, "even"),
				tableState.distKey,
				strings.Join(tableState.sortKeys, ","),
				strconv.Itoa(len(tableState.rows)),
			})
		}
	}
	return queryResult{
		fields: []pgField{
			{Name: "schema", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "diststyle", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "distkey", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "sortkey1", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "tbl_rows", TypeOID: pgTypeInt4OID, TypeSize: 4},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogSVVColumns() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.catalogColumnRowsLocked()
	return queryResult{
		fields: []pgField{
			{Name: "table_catalog", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_schema", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "column_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "ordinal_position", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "column_default", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "data_type", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "encoding", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogSVVMVInfo() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			if !isMaterializedView(tableState) {
				continue
			}
			rows = append(rows, []string{
				schemaName,
				tableName,
				defaultString(s.config.User, "dev"),
				"1",
				"false",
				"false",
			})
		}
	}
	return queryResult{
		fields: []pgField{
			{Name: "schema", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "owner_user_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "state", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "autorefresh", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "is_stale", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGTableDef() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			if isView(tableState) {
				continue
			}
			for _, column := range tableState.columns {
				distKey := strconv.FormatBool(column.name == tableState.distKey)
				sortKey := "0"
				for sortIndex, sortColumn := range tableState.sortKeys {
					if column.name == sortColumn {
						sortKey = strconv.Itoa(sortIndex + 1)
						break
					}
				}
				rows = append(rows, []string{
					schemaName,
					tableName,
					column.name,
					column.dataType,
					column.encoding,
					distKey,
					sortKey,
					"false",
				})
			}
		}
	}
	return queryResult{
		fields: []pgField{
			{Name: "schemaname", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "tablename", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "column", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "type", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "encoding", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "distkey", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "sortkey", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "notnull", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogSTLQuery() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0, len(s.statements))
	for _, stmt := range s.statements {
		preview, _, _ := safeSQLPreview(stmt.QueryString, 200)
		rows = append(rows, []string{strconv.FormatInt(redshiftQueryID(stmt.ID), 10), preview, stmt.Status})
	}
	return queryResult{
		fields: []pgField{{Name: "query", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "querytxt", TypeOID: pgTypeVarcharOID, TypeSize: -1}, {Name: "status", TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   rows,
		tag:    fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogSTVRecents() queryResult {
	return queryResult{
		fields: []pgField{{Name: "pid", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "status", TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   [][]string{{strconv.Itoa(int(pgDefaultBackendPID)), "Idle"}},
		tag:    "SELECT 1",
	}
}

func (s *Server) catalogTableRowsLocked() [][]string {
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			rows = append(rows, []string{defaultString(s.config.Database, "dev"), schemaName, tableName, informationSchemaTableType(tableState)})
		}
	}
	return rows
}

func (s *Server) catalogColumnRowsLocked() [][]string {
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		schemaState := s.db.schemas[schemaName]
		for _, tableName := range sortedTableNames(schemaState.tables) {
			tableState := schemaState.tables[tableName]
			for i, column := range tableState.columns {
				rows = append(rows, []string{
					defaultString(s.config.Database, "dev"),
					schemaName,
					tableName,
					column.name,
					strconv.Itoa(i + 1),
					column.defaultValue,
					column.dataType,
					column.encoding,
				})
			}
		}
	}
	return rows
}

func sortedSchemaNames(schemas map[string]*schema) []string {
	names := make([]string, 0, len(schemas))
	for name := range schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedTableNames(tables map[string]*table) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func catalogTableOID(schemaName string, tableName string) string {
	var value int64 = 10000
	for _, ch := range schemaName + "." + tableName {
		value = value*31 + int64(ch)
		if value < 0 {
			value = -value
		}
	}
	return strconv.FormatInt(value%1000000000, 10)
}

func readMessagePayload(r io.Reader) ([]byte, error) {
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBytes); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(lengthBytes))
	if length < 4 {
		return nil, errors.New("invalid PostgreSQL message length")
	}
	payload := make([]byte, length-4)
	_, err := io.ReadFull(r, payload)
	return payload, err
}

func readPasswordMessage(r io.Reader) (string, error) {
	messageType := []byte{0}
	if _, err := io.ReadFull(r, messageType); err != nil {
		return "", err
	}
	if messageType[0] != 'p' {
		return "", errors.New("expected password message")
	}
	payload, err := readMessagePayload(r)
	if err != nil {
		return "", err
	}
	return readCString(payload), nil
}

func parseStartupParameters(payload []byte) map[string]string {
	params := make(map[string]string)
	parts := bytes.Split(payload, []byte{0})
	for i := 0; i+1 < len(parts); i += 2 {
		key := string(parts[i])
		if key == "" {
			break
		}
		params[key] = string(parts[i+1])
	}
	return params
}

func readCString(payload []byte) string {
	if idx := bytes.IndexByte(payload, 0); idx >= 0 {
		return string(payload[:idx])
	}
	return string(payload)
}

func readCStringFromReader(reader *bytes.Reader) (string, bool) {
	var builder strings.Builder
	for {
		ch, err := reader.ReadByte()
		if err != nil {
			return "", false
		}
		if ch == 0 {
			return builder.String(), true
		}
		builder.WriteByte(ch)
	}
}

func readInt16FromReader(reader *bytes.Reader) (int16, bool) {
	var value int16
	if err := binary.Read(reader, binary.BigEndian, &value); err != nil {
		return 0, false
	}
	return value, true
}

func readInt32FromReader(reader *bytes.Reader) (int32, bool) {
	var value int32
	if err := binary.Read(reader, binary.BigEndian, &value); err != nil {
		return 0, false
	}
	return value, true
}

func discardInt16Values(reader *bytes.Reader, count int) bool {
	_, ok := readInt16Values(reader, count)
	return ok
}

func readInt16Values(reader *bytes.Reader, count int) ([]int16, bool) {
	if count < 0 {
		return nil, false
	}
	values := make([]int16, 0, count)
	for i := 0; i < count; i++ {
		value, ok := readInt16FromReader(reader)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

func parseDescribeOrClosePayload(payload []byte) (byte, string, bool) {
	if len(payload) == 0 {
		return 0, "", false
	}
	reader := bytes.NewReader(payload[1:])
	name, ok := readCStringFromReader(reader)
	if !ok {
		return 0, "", false
	}
	if payload[0] != 'S' && payload[0] != 'P' {
		return 0, "", false
	}
	return payload[0], name, true
}

func writeAuthCleartextPassword(w io.Writer) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, pgAuthCleartext)
	return writeMessage(w, 'R', body.Bytes())
}

func writeAuthenticationOK(w io.Writer) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, pgAuthOK)
	return writeMessage(w, 'R', body.Bytes())
}

func writeParameterStatuses(w io.Writer, startupParams map[string]string) error {
	clientEncoding := defaultString(startupParams["client_encoding"], "UTF8")
	statuses := map[string]string{
		"server_version":              "8.0.2",
		"server_encoding":             "UTF8",
		"client_encoding":             clientEncoding,
		"DateStyle":                   "ISO, MDY",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
		"application_name":            startupParams["application_name"],
		"is_superuser":                "on",
		"session_authorization":       defaultString(startupParams["user"], "dev"),
	}
	for key, value := range statuses {
		if value == "" {
			continue
		}
		var body bytes.Buffer
		writeCString(&body, key)
		writeCString(&body, value)
		if err := writeMessage(w, 'S', body.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func writeBackendKeyData(w io.Writer) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, pgDefaultBackendPID)
	binary.Write(&body, binary.BigEndian, pgDefaultSecretKey)
	return writeMessage(w, 'K', body.Bytes())
}

func writeReadyForQuery(w io.Writer) error {
	return writeMessage(w, 'Z', []byte{pgTransactionIdle})
}

type pgField struct {
	Name     string
	TypeOID  int32
	TypeSize int16
}

func writeRowDescription(w io.Writer, fields []pgField) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(len(fields)))
	for _, field := range fields {
		writeCString(&body, field.Name)
		binary.Write(&body, binary.BigEndian, int32(0))
		binary.Write(&body, binary.BigEndian, int16(0))
		binary.Write(&body, binary.BigEndian, field.TypeOID)
		binary.Write(&body, binary.BigEndian, field.TypeSize)
		binary.Write(&body, binary.BigEndian, int32(-1))
		binary.Write(&body, binary.BigEndian, int16(0))
	}
	return writeMessage(w, 'T', body.Bytes())
}

func writeParameterDescription(w io.Writer, parameterOIDs []int32) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(len(parameterOIDs)))
	for _, oid := range parameterOIDs {
		binary.Write(&body, binary.BigEndian, oid)
	}
	return writeMessage(w, 't', body.Bytes())
}

func writeDataRow(w io.Writer, values []string) error {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(len(values)))
	for _, value := range values {
		binary.Write(&body, binary.BigEndian, int32(len(value)))
		body.WriteString(value)
	}
	return writeMessage(w, 'D', body.Bytes())
}

func writeCommandComplete(w io.Writer, tag string) error {
	var body bytes.Buffer
	writeCString(&body, tag)
	return writeMessage(w, 'C', body.Bytes())
}

func writeErrorResponse(w io.Writer, sqlState string, message string) error {
	var body bytes.Buffer
	body.WriteByte('S')
	writeCString(&body, "ERROR")
	body.WriteByte('C')
	writeCString(&body, sqlState)
	body.WriteByte('M')
	writeCString(&body, message)
	body.WriteByte(0)
	return writeMessage(w, 'E', body.Bytes())
}

func writeMessage(w io.Writer, messageType byte, body []byte) error {
	if messageType != 0 {
		if _, err := w.Write([]byte{messageType}); err != nil {
			return err
		}
	}
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(body)+4))
	if _, err := w.Write(length); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

func writeCString(w io.Writer, value string) {
	io.WriteString(w, value)
	w.Write([]byte{0})
}
