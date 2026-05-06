package dynamodb

import (
	"net/http"
	"strings"
)

func (s *Server) handleBatchGetItem(w http.ResponseWriter, r *http.Request) {
	var request batchGetItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.RequestItems) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "request items are required")
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	responses := map[string][]item{}
	consumedCapacity := []map[string]any{}
	for tableName, tableRequest := range request.RequestItems {
		state, ok := s.tables[tableName]
		if !ok {
			writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
			return
		}
		if len(tableRequest.Keys) == 0 {
			writeError(w, http.StatusBadRequest, "ValidationException", "keys are required")
			return
		}
		for _, keyValue := range tableRequest.Keys {
			key, err := itemKey(state.description, keyValue)
			if err != nil {
				writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
				return
			}
			found, ok := state.items[key]
			if !ok {
				continue
			}
			responses[tableName] = append(responses[tableName], projectItem(found, tableRequest.ProjectionExpression, tableRequest.ExpressionAttributeNames))
		}
		if _, ok := responses[tableName]; !ok {
			responses[tableName] = []item{}
		}
		appendBatchConsumedCapacity(&consumedCapacity, tableName, request.ReturnConsumedCapacity)
	}

	response := map[string]any{
		"Responses":       responses,
		"UnprocessedKeys": map[string]batchGetTableRequest{},
	}
	if len(consumedCapacity) > 0 {
		response["ConsumedCapacity"] = consumedCapacity
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleBatchWriteItem(w http.ResponseWriter, r *http.Request) {
	var request batchWriteItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.RequestItems) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "request items are required")
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	writesToApply := []validatedWrite{}
	backups := map[*tableState]map[string]itemBackup{}
	touched := map[*tableState]bool{}
	consumedCapacity := []map[string]any{}
	for tableName, writes := range request.RequestItems {
		state, ok := s.tables[tableName]
		if !ok {
			writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
			return
		}
		if len(writes) == 0 {
			writeError(w, http.StatusBadRequest, "ValidationException", "write requests are required")
			return
		}
		for _, write := range writes {
			if (write.PutRequest == nil) == (write.DeleteRequest == nil) {
				writeError(w, http.StatusBadRequest, "ValidationException", "each write request must contain exactly one operation")
				return
			}
			if write.PutRequest != nil {
				if len(write.PutRequest.Item) == 0 {
					writeError(w, http.StatusBadRequest, "ValidationException", "put item is required")
					return
				}
				if err := s.validateItemSize(write.PutRequest.Item); err != nil {
					writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
					return
				}
				key, err := itemKey(state.description, write.PutRequest.Item)
				if err != nil {
					writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
					return
				}
				rememberItemBackup(backups, state, key)
				writesToApply = append(writesToApply, validatedWrite{state: state, key: key, put: cloneItem(write.PutRequest.Item)})
			}
			if write.DeleteRequest != nil {
				key, err := itemKey(state.description, write.DeleteRequest.Key)
				if err != nil {
					writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
					return
				}
				rememberItemBackup(backups, state, key)
				writesToApply = append(writesToApply, validatedWrite{state: state, key: key, delete: true})
			}
		}
		appendBatchConsumedCapacity(&consumedCapacity, tableName, request.ReturnConsumedCapacity)
	}

	for _, write := range writesToApply {
		if write.delete {
			delete(write.state.items, write.key)
		} else {
			write.state.items[write.key] = write.put
		}
		touched[write.state] = true
	}
	for state := range touched {
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
	}
	if err := s.persistLocked(); err != nil {
		restoreBackups(backups)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}

	response := map[string]any{
		"UnprocessedItems": map[string][]writeRequest{},
	}
	if len(consumedCapacity) > 0 {
		response["ConsumedCapacity"] = consumedCapacity
	}
	writeJSON(w, http.StatusOK, response)
}
func (s *Server) handleBatchExecuteStatement(w http.ResponseWriter, r *http.Request) {
	var request batchExecuteStatementRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.Statements) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "statements are required")
		return
	}
	if len(request.Statements) > 25 {
		writeError(w, http.StatusBadRequest, "ValidationException", "statements must contain 25 or fewer entries")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	responses := make([]batchStatementResponse, 0, len(request.Statements))
	consumedCapacity := make([]map[string]any, 0, len(request.Statements))
	for _, statementRequest := range request.Statements {
		statement, err := parsePartiQLSelect(statementRequest.Statement, statementRequest.Parameters)
		if err != nil {
			responses = append(responses, batchStatementResponse{
				Error: &batchStatementError{Code: "ValidationError", Message: err.Error()},
			})
			continue
		}
		state, ok := s.tables[statement.tableName]
		if !ok {
			responses = append(responses, batchStatementResponse{
				TableName: statement.tableName,
				Error:     &batchStatementError{Code: "ResourceNotFound", Message: "table not found"},
			})
			appendBatchConsumedCapacity(&consumedCapacity, statement.tableName, request.ReturnConsumedCapacity)
			continue
		}
		if !partiQLConditionsCoverKey(state.description, statement.conditions) {
			responses = append(responses, batchStatementResponse{
				TableName: statement.tableName,
				Error:     &batchStatementError{Code: "ValidationError", Message: "SELECT statement must include equality conditions for all key attributes"},
			})
			appendBatchConsumedCapacity(&consumedCapacity, statement.tableName, request.ReturnConsumedCapacity)
			continue
		}

		response := batchStatementResponse{TableName: statement.tableName}
		for _, candidate := range sortedItemsForQuery(state, "") {
			if partiQLConditionsMatch(candidate.value, statement.conditions) {
				response.Item = projectPartiQLItem(candidate.value, statement.projections)
				break
			}
		}
		responses = append(responses, response)
		appendBatchConsumedCapacity(&consumedCapacity, statement.tableName, request.ReturnConsumedCapacity)
	}

	response := map[string]any{"Responses": responses}
	if len(consumedCapacity) > 0 {
		response["ConsumedCapacity"] = consumedCapacity
	}
	writeJSON(w, http.StatusOK, response)
}
func addConsumedCapacity(response map[string]any, tableName string, mode string) {
	if mode == "" || strings.EqualFold(mode, "NONE") {
		return
	}
	response["ConsumedCapacity"] = map[string]any{
		"TableName":     tableName,
		"CapacityUnits": float64(1),
	}
}

func validReturnConsumedCapacity(value string) bool {
	switch strings.ToUpper(defaultString(value, "NONE")) {
	case "NONE", "TOTAL", "INDEXES":
		return true
	default:
		return false
	}
}

func appendBatchConsumedCapacity(values *[]map[string]any, tableName string, mode string) {
	if mode == "" || strings.EqualFold(mode, "NONE") {
		return
	}
	*values = append(*values, map[string]any{
		"TableName":     tableName,
		"CapacityUnits": float64(1),
	})
}
