package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strings"
)

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var request queryRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if strings.TrimSpace(request.KeyConditionExpression) == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "key condition expression is required")
		return
	}
	if err := validateSelect(request.Select, request.ProjectionExpression); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	if request.IndexName != "" && !tableHasIndex(state.description, request.IndexName) {
		writeError(w, http.StatusBadRequest, "ValidationException", "index not found")
		return
	}
	items := sortedItemsForQuery(state, request.IndexName)
	if request.ScanIndexForward != nil && !*request.ScanIndexForward {
		reverseItems(items)
	}
	startKey, err := startKeyString(state.description, request.ExclusiveStartKey)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response, err := collectItems(state.description, request.IndexName, items, request.Limit, startKey, request.ProjectionExpression, request.ExpressionAttributeNames, false, func(candidate item) (bool, error) {
		return matchKeyCondition(request.KeyConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, candidate)
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	applySelect(response, request.Select)
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	var request scanRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if err := validateSelect(request.Select, request.ProjectionExpression); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	if request.IndexName != "" && !tableHasIndex(state.description, request.IndexName) {
		writeError(w, http.StatusBadRequest, "ValidationException", "index not found")
		return
	}
	startKey, err := startKeyString(state.description, request.ExclusiveStartKey)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	response, err := collectItems(state.description, request.IndexName, sortedItemsForScan(state, request.IndexName), request.Limit, startKey, request.ProjectionExpression, request.ExpressionAttributeNames, true, func(candidate item) (bool, error) {
		return matchFilter(request.FilterExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, candidate)
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	applySelect(response, request.Select)
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}
func sortedItemsForQuery(state *tableState, indexName string) []keyedItem {
	items := make([]keyedItem, 0, len(state.items))
	for key, value := range state.items {
		items = append(items, keyedItem{key: key, value: cloneItem(value)})
	}
	schema := queryKeySchema(state.description, indexName)
	sort.Slice(items, func(i, j int) bool {
		if comparison := compareItemsBySchema(items[i].value, items[j].value, schema); comparison != 0 {
			return comparison < 0
		}
		return items[i].key < items[j].key
	})
	return items
}

func sortedItemsForScan(state *tableState, indexName string) []keyedItem {
	items := sortedItemsForQuery(state, indexName)
	if indexName == "" {
		return items
	}
	schema := queryKeySchema(state.description, indexName)
	filtered := items[:0]
	for _, candidate := range items {
		if itemHasAllKeys(candidate.value, schema) {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func queryKeySchema(description tableDescription, indexName string) []keySchemaElement {
	if indexName == "" {
		return description.KeySchema
	}
	for _, index := range description.GlobalSecondaryIndexes {
		if index.IndexName == indexName {
			return index.KeySchema
		}
	}
	for _, index := range description.LocalSecondaryIndexes {
		if index.IndexName == indexName {
			return index.KeySchema
		}
	}
	return description.KeySchema
}

func compareItemsBySchema(left item, right item, schema []keySchemaElement) int {
	for _, element := range schema {
		comparison := compareAttributeValues(left[element.AttributeName], right[element.AttributeName])
		if comparison != 0 {
			return comparison
		}
	}
	return 0
}

func compareAttributeValues(left attributeValue, right attributeValue) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	if leftNumber, ok := left["N"].(string); ok {
		rightNumber, ok := right["N"].(string)
		if !ok {
			return strings.Compare(attributeTypeName(left), attributeTypeName(right))
		}
		leftRat, leftOK := new(big.Rat).SetString(leftNumber)
		rightRat, rightOK := new(big.Rat).SetString(rightNumber)
		if leftOK && rightOK {
			return leftRat.Cmp(rightRat)
		}
		return strings.Compare(leftNumber, rightNumber)
	}
	if leftString, ok := left["S"].(string); ok {
		rightString, ok := right["S"].(string)
		if !ok {
			return strings.Compare(attributeTypeName(left), attributeTypeName(right))
		}
		return strings.Compare(leftString, rightString)
	}
	if leftBinary, ok := left["B"].(string); ok {
		rightBinary, ok := right["B"].(string)
		if !ok {
			return strings.Compare(attributeTypeName(left), attributeTypeName(right))
		}
		return strings.Compare(leftBinary, rightBinary)
	}
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return strings.Compare(string(leftJSON), string(rightJSON))
}

func attributeTypeName(value attributeValue) string {
	for _, name := range []string{"S", "N", "B", "BOOL", "NULL", "M", "L", "SS", "NS", "BS"} {
		if _, ok := value[name]; ok {
			return name
		}
	}
	return ""
}

func reverseItems(items []keyedItem) {
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
}

func startKeyString(description tableDescription, start item) (string, error) {
	if len(start) == 0 {
		return "", nil
	}
	return itemKey(description, start)
}

func collectItems(description tableDescription, indexName string, source []keyedItem, limit int, startKey string, projection string, names map[string]string, limitCountsUnmatched bool, match func(item) (bool, error)) (map[string]any, error) {
	responseItems := []item{}
	scanned := 0
	started := startKey == ""
	for _, candidate := range source {
		if !started {
			started = candidate.key == startKey
			continue
		}
		matched, err := match(candidate.value)
		if err != nil {
			return nil, err
		}
		if matched || limitCountsUnmatched {
			scanned++
		}
		if !matched {
			if limitCountsUnmatched && limit > 0 && scanned == limit {
				return limitedItemsResponse(description, indexName, candidate, responseItems, scanned, hasMoreItems(source, candidate.key)), nil
			}
			continue
		}
		responseItems = append(responseItems, projectResultItem(description, indexName, candidate.value, projection, names))
		if limit > 0 && scanned == limit {
			hasMore := hasMoreItems(source, candidate.key)
			if !limitCountsUnmatched {
				hasMore = hasMoreMatches(source, candidate.key, match)
			}
			return limitedItemsResponse(description, indexName, candidate, responseItems, scanned, hasMore), nil
		}
	}
	return map[string]any{
		"Items":        responseItems,
		"Count":        len(responseItems),
		"ScannedCount": scanned,
	}, nil
}

func limitedItemsResponse(description tableDescription, indexName string, candidate keyedItem, responseItems []item, scanned int, hasMore bool) map[string]any {
	response := map[string]any{
		"Items":        responseItems,
		"Count":        len(responseItems),
		"ScannedCount": scanned,
	}
	if lastKey, err := extractKey(description, candidate.value); err == nil && hasMore {
		response["LastEvaluatedKey"] = lastKey
	}
	return response
}

func validateSelect(selectValue string, projectionExpression string) error {
	selectValue = strings.ToUpper(strings.TrimSpace(selectValue))
	switch selectValue {
	case "", "ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES":
		return nil
	case "COUNT":
		if strings.TrimSpace(projectionExpression) != "" {
			return errors.New("select COUNT cannot be used with ProjectionExpression")
		}
		return nil
	default:
		return fmt.Errorf("unsupported select value %s", selectValue)
	}
}

func applySelect(response map[string]any, selectValue string) {
	if strings.EqualFold(strings.TrimSpace(selectValue), "COUNT") {
		delete(response, "Items")
	}
}

func hasMoreItems(source []keyedItem, afterKey string) bool {
	found := false
	for _, candidate := range source {
		if !found {
			found = candidate.key == afterKey
			continue
		}
		return true
	}
	return false
}

func hasMoreMatches(source []keyedItem, afterKey string, match func(item) (bool, error)) bool {
	found := false
	for _, candidate := range source {
		if !found {
			found = candidate.key == afterKey
			continue
		}
		matched, err := match(candidate.value)
		if err == nil && matched {
			return true
		}
	}
	return false
}
