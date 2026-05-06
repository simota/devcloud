package dynamodb

import (
	"errors"
	"net/http"
	"strings"
)

func (s *Server) handleExecuteTransaction(w http.ResponseWriter, r *http.Request) {
	var request executeTransactionRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.TransactStatements) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "transaction statements are required")
		return
	}
	if len(request.TransactStatements) > 100 {
		writeError(w, http.StatusBadRequest, "ValidationException", "transaction statements must contain 100 or fewer entries")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	responses := make([]batchStatementResponse, 0, len(request.TransactStatements))
	consumedCapacity := make([]map[string]any, 0, len(request.TransactStatements))
	for _, statementRequest := range request.TransactStatements {
		statement, err := parsePartiQLSelect(statementRequest.Statement, statementRequest.Parameters)
		if err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		state, ok := s.tables[statement.tableName]
		if !ok {
			writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
			return
		}
		if !partiQLConditionsCoverKey(state.description, statement.conditions) {
			writeError(w, http.StatusBadRequest, "ValidationException", "SELECT statement must include equality conditions for all key attributes")
			return
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

func (s *Server) handleTransactGetItems(w http.ResponseWriter, r *http.Request) {
	var request transactGetItemsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.TransactItems) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "transaction items are required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	responses := make([]transactGetItemResponse, 0, len(request.TransactItems))
	for _, transactionItem := range request.TransactItems {
		if transactionItem.Get == nil {
			writeError(w, http.StatusBadRequest, "ValidationException", "each transaction item must contain a Get operation")
			return
		}
		get := transactionItem.Get
		if get.TableName == "" {
			writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
			return
		}
		state, ok := s.tables[get.TableName]
		if !ok {
			writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
			return
		}
		key, err := itemKey(state.description, get.Key)
		if err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		found, ok := state.items[key]
		if !ok {
			responses = append(responses, transactGetItemResponse{})
			continue
		}
		responses = append(responses, transactGetItemResponse{
			Item: projectItem(found, get.ProjectionExpression, get.ExpressionAttributeNames),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"Responses": responses})
}

func (s *Server) handleTransactWriteItems(w http.ResponseWriter, r *http.Request) {
	var request transactWriteItemsRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if len(request.TransactItems) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "transaction items are required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	writesToApply := []validatedWrite{}
	backups := map[*tableState]map[string]itemBackup{}
	touched := map[*tableState]bool{}
	for _, transactionItem := range request.TransactItems {
		operationCount := countTransactWriteOperations(transactionItem)
		if operationCount != 1 {
			writeError(w, http.StatusBadRequest, "ValidationException", "each transaction item must contain exactly one operation")
			return
		}
		if transactionItem.Put != nil {
			write, err := s.validateTransactPut(transactionItem.Put, backups)
			if err != nil {
				writeTransactError(w, err)
				return
			}
			writesToApply = append(writesToApply, write)
			continue
		}
		if transactionItem.Update != nil {
			write, err := s.validateTransactUpdate(transactionItem.Update, backups)
			if err != nil {
				writeTransactError(w, err)
				return
			}
			writesToApply = append(writesToApply, write)
			continue
		}
		if transactionItem.Delete != nil {
			write, err := s.validateTransactDelete(transactionItem.Delete, backups)
			if err != nil {
				writeTransactError(w, err)
				return
			}
			writesToApply = append(writesToApply, write)
			continue
		}
		if transactionItem.ConditionCheck != nil {
			if err := s.validateTransactConditionCheck(transactionItem.ConditionCheck); err != nil {
				writeTransactError(w, err)
				return
			}
		}
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

	writeJSON(w, http.StatusOK, map[string]any{})
}
func rememberItemBackup(backups map[*tableState]map[string]itemBackup, state *tableState, key string) {
	if backups[state] == nil {
		backups[state] = map[string]itemBackup{}
	}
	if _, ok := backups[state][key]; ok {
		return
	}
	existing, exists := state.items[key]
	backups[state][key] = itemBackup{item: cloneItem(existing), exists: exists}
}

func restoreBackups(backups map[*tableState]map[string]itemBackup) {
	for state, tableBackups := range backups {
		for key, backup := range tableBackups {
			if backup.exists {
				state.items[key] = backup.item
			} else {
				delete(state.items, key)
			}
		}
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
	}
}

type transactValidationError struct {
	name    string
	message string
}

func (e transactValidationError) Error() string {
	return e.message
}

func newTransactValidationError(name string, message string) error {
	return transactValidationError{name: name, message: message}
}

func writeTransactError(w http.ResponseWriter, err error) {
	var transactionErr transactValidationError
	if errors.As(err, &transactionErr) {
		writeError(w, http.StatusBadRequest, transactionErr.name, transactionErr.message)
		return
	}
	writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
}

func countTransactWriteOperations(transactionItem transactWriteItem) int {
	count := 0
	if transactionItem.Put != nil {
		count++
	}
	if transactionItem.Update != nil {
		count++
	}
	if transactionItem.Delete != nil {
		count++
	}
	if transactionItem.ConditionCheck != nil {
		count++
	}
	return count
}

func (s *Server) validateTransactPut(request *transactPut, backups map[*tableState]map[string]itemBackup) (validatedWrite, error) {
	if request.TableName == "" {
		return validatedWrite{}, errors.New("table name is required")
	}
	if len(request.Item) == 0 {
		return validatedWrite{}, errors.New("item is required")
	}
	state, ok := s.tables[request.TableName]
	if !ok {
		return validatedWrite{}, newTransactValidationError("ResourceNotFoundException", "table not found")
	}
	key, err := itemKey(state.description, request.Item)
	if err != nil {
		return validatedWrite{}, err
	}
	oldItem, existed := state.items[key]
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		return validatedWrite{}, newTransactValidationError("TransactionCanceledException", "transaction cancelled")
	}
	if err := s.validateItemSize(request.Item); err != nil {
		return validatedWrite{}, err
	}
	rememberItemBackup(backups, state, key)
	return validatedWrite{state: state, key: key, put: cloneItem(request.Item)}, nil
}

func (s *Server) validateTransactUpdate(request *transactUpdate, backups map[*tableState]map[string]itemBackup) (validatedWrite, error) {
	if request.TableName == "" {
		return validatedWrite{}, errors.New("table name is required")
	}
	state, ok := s.tables[request.TableName]
	if !ok {
		return validatedWrite{}, newTransactValidationError("ResourceNotFoundException", "table not found")
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		return validatedWrite{}, err
	}
	updated := cloneItem(request.Key)
	oldItem, existed := state.items[key]
	if existed {
		updated = cloneItem(oldItem)
	}
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		return validatedWrite{}, newTransactValidationError("TransactionCanceledException", "transaction cancelled")
	}
	if err := applyUpdateExpression(updated, request.UpdateExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues); err != nil {
		return validatedWrite{}, err
	}
	if err := s.validateItemSize(updated); err != nil {
		return validatedWrite{}, err
	}
	rememberItemBackup(backups, state, key)
	return validatedWrite{state: state, key: key, put: updated}, nil
}

func (s *Server) validateTransactDelete(request *transactDelete, backups map[*tableState]map[string]itemBackup) (validatedWrite, error) {
	if request.TableName == "" {
		return validatedWrite{}, errors.New("table name is required")
	}
	state, ok := s.tables[request.TableName]
	if !ok {
		return validatedWrite{}, newTransactValidationError("ResourceNotFoundException", "table not found")
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		return validatedWrite{}, err
	}
	oldItem, existed := state.items[key]
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		return validatedWrite{}, newTransactValidationError("TransactionCanceledException", "transaction cancelled")
	}
	rememberItemBackup(backups, state, key)
	return validatedWrite{state: state, key: key, delete: true}, nil
}

func (s *Server) validateTransactConditionCheck(request *transactConditionCheck) error {
	if request.TableName == "" {
		return errors.New("table name is required")
	}
	if strings.TrimSpace(request.ConditionExpression) == "" {
		return errors.New("condition expression is required")
	}
	state, ok := s.tables[request.TableName]
	if !ok {
		return newTransactValidationError("ResourceNotFoundException", "table not found")
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		return err
	}
	oldItem, existed := state.items[key]
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		return newTransactValidationError("TransactionCanceledException", "transaction cancelled")
	}
	return nil
}
