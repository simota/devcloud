package redshift

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"devcloud/internal/events"
)

func (s *Server) handleExecuteStatement(w http.ResponseWriter, r *http.Request) {
	var request executeStatementRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	if strings.TrimSpace(request.SQL) == "" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "Sql is required")
		return
	}
	if err := s.validateStatementSize(request.SQL); err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if request.ClientToken != "" {
		s.mu.Lock()
		if id := s.clientTokenIndex[request.ClientToken]; id != "" {
			stmt := s.statements[id]
			s.mu.Unlock()
			writeDataAPIJSON(w, http.StatusOK, executeStatementResponseFromStatement(stmt))
			return
		}
		s.mu.Unlock()
	}

	createdAt := time.Now().UTC()
	resultFormat, err := normalizeDataAPIResultFormat(request.ResultFormat)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	sessionID, err := s.sessionIDForRequest(request.SessionID, request.SessionKeepAliveSeconds, createdAt)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	result, err := s.executeSQL(request.SQL)
	stmt := &statement{
		ID:                s.nextStatementIDValue(),
		ClusterIdentifier: defaultString(request.ClusterIdentifier, defaultString(s.config.ClusterIdentifier, "devcloud")),
		Database:          defaultString(request.Database, defaultString(s.config.Database, "dev")),
		DbUser:            defaultString(request.DbUser, defaultString(s.config.User, "dev")),
		SessionID:         sessionID,
		QueryString:       request.SQL,
		ResultFormat:      resultFormat,
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
		Status:            "FINISHED",
		Result:            result,
		HasResultSet:      len(result.fields) > 0,
	}
	if err != nil {
		stmt.Status = "FAILED"
		stmt.Error = err.Error()
	}

	s.mu.Lock()
	s.statements[stmt.ID] = stmt
	if request.ClientToken != "" {
		s.clientTokenIndex[request.ClientToken] = stmt.ID
	}
	_ = s.persistLocked()
	s.mu.Unlock()

	events.Emit(s.eventPublisher, events.Event{
		Type:    "redshift.statement.executed",
		Service: "redshift",
		Payload: map[string]any{"statementID": stmt.ID},
	})
	writeDataAPIJSON(w, http.StatusOK, executeStatementResponseFromStatement(stmt))
}

func (s *Server) handleBatchExecuteStatement(w http.ResponseWriter, r *http.Request) {
	var request batchExecuteStatementRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	sqls := compactSQLStatements(request.SQLs)
	if len(sqls) == 0 {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "Sqls is required")
		return
	}
	for _, sql := range sqls {
		if err := s.validateStatementSize(sql); err != nil {
			writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
	}
	if request.ClientToken != "" {
		s.mu.Lock()
		if id := s.clientTokenIndex[request.ClientToken]; id != "" {
			stmt := s.statements[id]
			s.mu.Unlock()
			writeDataAPIJSON(w, http.StatusOK, executeStatementResponseFromStatement(stmt))
			return
		}
		s.mu.Unlock()
	}

	createdAt := time.Now().UTC()
	resultFormat, err := normalizeDataAPIResultFormat(request.ResultFormat)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	sessionID, err := s.sessionIDForRequest(request.SessionID, request.SessionKeepAliveSeconds, createdAt)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	queryString := strings.Join(sqls, ";\n")
	result, err := s.executeSQLBatch(sqls)
	stmt := &statement{
		ID:                s.nextStatementIDValue(),
		ClusterIdentifier: defaultString(request.ClusterIdentifier, defaultString(s.config.ClusterIdentifier, "devcloud")),
		Database:          defaultString(request.Database, defaultString(s.config.Database, "dev")),
		DbUser:            defaultString(request.DbUser, defaultString(s.config.User, "dev")),
		SessionID:         sessionID,
		QueryString:       queryString,
		ResultFormat:      resultFormat,
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
		Status:            "FINISHED",
		Result:            result,
		HasResultSet:      len(result.fields) > 0,
	}
	if err != nil {
		stmt.Status = "FAILED"
		stmt.Error = err.Error()
		stmt.HasResultSet = false
		stmt.Result = queryResult{}
	}

	s.mu.Lock()
	s.statements[stmt.ID] = stmt
	if request.ClientToken != "" {
		s.clientTokenIndex[request.ClientToken] = stmt.ID
	}
	_ = s.persistLocked()
	s.mu.Unlock()

	events.Emit(s.eventPublisher, events.Event{
		Type:    "redshift.statement.batch_executed",
		Service: "redshift",
		Payload: map[string]any{"statementID": stmt.ID},
	})
	writeDataAPIJSON(w, http.StatusOK, executeStatementResponseFromStatement(stmt))
}

func (s *Server) handleDescribeStatement(w http.ResponseWriter, r *http.Request) {
	var request statementIDRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	stmt := s.statementByID(w, request.ID)
	if stmt == nil {
		return
	}
	writeDataAPIJSON(w, http.StatusOK, describeStatementResponseFromStatement(stmt))
}

func (s *Server) handleGetStatementResult(w http.ResponseWriter, r *http.Request) {
	var request getStatementResultRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	stmt := s.statementByID(w, request.ID)
	if stmt == nil {
		return
	}
	if stmt.Status != "FINISHED" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "statement has no finished result")
		return
	}
	rows, nextToken, err := paginateRows(stmt.Result.rows, request.MaxResults, request.NextToken)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response := getStatementResultResponse(stmt.Result, rows)
	if nextToken != "" {
		response["NextToken"] = nextToken
	}
	writeDataAPIJSON(w, http.StatusOK, response)
}

func (s *Server) handleGetStatementResultV2(w http.ResponseWriter, r *http.Request) {
	var request getStatementResultRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	stmt := s.statementByID(w, request.ID)
	if stmt == nil {
		return
	}
	if stmt.Status != "FINISHED" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "statement has no finished result")
		return
	}
	if !strings.EqualFold(defaultString(stmt.ResultFormat, "JSON"), "CSV") {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "GetStatementResultV2 requires a statement executed with ResultFormat CSV")
		return
	}
	rows, nextToken, err := paginateRows(stmt.Result.rows, request.MaxResults, request.NextToken)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response, err := getStatementResultV2Response(stmt.Result, rows)
	if err != nil {
		writeDataAPIError(w, http.StatusInternalServerError, "InternalServerException", "failed to encode CSV result")
		return
	}
	if nextToken != "" {
		response["NextToken"] = nextToken
	}
	writeDataAPIJSON(w, http.StatusOK, response)
}

func (s *Server) handleCancelStatement(w http.ResponseWriter, r *http.Request) {
	var request statementIDRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	if request.ID == "" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "Id is required")
		return
	}
	s.mu.Lock()
	stmt := s.statements[request.ID]
	if stmt == nil {
		s.mu.Unlock()
		writeDataAPIError(w, http.StatusNotFound, "ResourceNotFoundException", "statement does not exist")
		return
	}
	cancelled := stmt.Status == "SUBMITTED" || stmt.Status == "STARTED"
	if cancelled {
		stmt.Status = "ABORTED"
		stmt.UpdatedAt = time.Now().UTC()
		_ = s.persistLocked()
	}
	s.mu.Unlock()
	writeDataAPIJSON(w, http.StatusOK, map[string]any{"Status": cancelled})
}

func (s *Server) handleListStatements(w http.ResponseWriter, r *http.Request) {
	var request listStatementsRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	s.mu.Lock()
	statements := make([]statementListItem, 0, len(s.statements))
	ids := make([]string, 0, len(s.statements))
	for id := range s.statements {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		stmt := s.statements[id]
		if request.Status != "" && !strings.EqualFold(stmt.Status, request.Status) {
			continue
		}
		statements = append(statements, statementListItem{
			ID:           stmt.ID,
			QueryString:  safeStatementQueryString(stmt.QueryString),
			Status:       stmt.Status,
			CreatedAt:    stmt.CreatedAt.Unix(),
			UpdatedAt:    stmt.UpdatedAt.Unix(),
			HasResultSet: stmt.HasResultSet,
		})
	}
	s.mu.Unlock()
	writeDataAPIJSON(w, http.StatusOK, map[string]any{"Statements": statements})
}

func (s *Server) handleListDatabases(w http.ResponseWriter, r *http.Request) {
	var request listMetadataRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	_ = request
	writeDataAPIJSON(w, http.StatusOK, map[string]any{
		"Databases": []string{defaultString(s.config.Database, "dev")},
	})
}

func (s *Server) handleListSchemas(w http.ResponseWriter, r *http.Request) {
	var request listMetadataRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	s.mu.Lock()
	schemas := make([]string, 0, len(s.db.schemas))
	for name := range s.db.schemas {
		if metadataPatternMatches(name, request.SchemaPattern) {
			schemas = append(schemas, name)
		}
	}
	s.mu.Unlock()
	sort.Strings(schemas)
	page, nextToken, err := paginateStrings(schemas, request.MaxResults, request.NextToken)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response := map[string]any{"Schemas": page}
	if nextToken != "" {
		response["NextToken"] = nextToken
	}
	writeDataAPIJSON(w, http.StatusOK, response)
}

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	var request listMetadataRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	s.mu.Lock()
	var tables []tableMember
	schemaNames := make([]string, 0, len(s.db.schemas))
	for schemaName := range s.db.schemas {
		if request.Schema != "" && schemaName != request.Schema {
			continue
		}
		if metadataPatternMatches(schemaName, request.SchemaPattern) {
			schemaNames = append(schemaNames, schemaName)
		}
	}
	sort.Strings(schemaNames)
	for _, schemaName := range schemaNames {
		schemaState := s.db.schemas[schemaName]
		tableNames := make([]string, 0, len(schemaState.tables))
		for name := range schemaState.tables {
			if metadataPatternMatches(name, request.TablePattern) {
				tableNames = append(tableNames, name)
			}
		}
		sort.Strings(tableNames)
		for _, name := range tableNames {
			tables = append(tables, tableMember{Name: name, Schema: schemaName, Type: tableDataAPIType(schemaState.tables[name])})
		}
	}
	s.mu.Unlock()
	page, nextToken, err := paginateTableMembers(tables, request.MaxResults, request.NextToken)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response := map[string]any{"Tables": page}
	if nextToken != "" {
		response["NextToken"] = nextToken
	}
	writeDataAPIJSON(w, http.StatusOK, response)
}

func (s *Server) handleDescribeTable(w http.ResponseWriter, r *http.Request) {
	var request describeTableRequest
	if !decodeDataAPIRequest(w, r, &request) {
		return
	}
	name := qualifiedName{schema: defaultString(request.Schema, "public"), table: request.Table}
	if name.table == "" {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", "Table is required")
		return
	}
	s.mu.Lock()
	tableState := s.lookupTableLocked(name)
	if tableState == nil {
		s.mu.Unlock()
		writeDataAPIError(w, http.StatusNotFound, "ResourceNotFoundException", "table does not exist")
		return
	}
	columns := make([]columnMetadata, 0, len(tableState.columns))
	for i, column := range tableState.columns {
		columns = append(columns, columnMetadataFromColumn(column, i))
	}
	s.mu.Unlock()
	page, nextToken, err := paginateColumnMetadata(columns, request.MaxResults, request.NextToken)
	if err != nil {
		writeDataAPIError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response := map[string]any{"ColumnList": page, "TableName": name.table}
	if nextToken != "" {
		response["NextToken"] = nextToken
	}
	writeDataAPIJSON(w, http.StatusOK, response)
}
