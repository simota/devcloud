package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strings"
)

func (s *Server) handlePutItem(w http.ResponseWriter, r *http.Request) {
	var request putItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if len(request.Item) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "item is required")
		return
	}
	if err := s.validateItemSize(request.Item); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	returnValues := strings.ToUpper(defaultString(request.ReturnValues, "NONE"))
	if !validPutDeleteReturnValues(returnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values must be NONE or ALL_OLD")
		return
	}
	conditionFailureReturnValues := strings.ToUpper(defaultString(request.ReturnValuesOnConditionCheckFailure, "NONE"))
	if !validConditionFailureReturnValues(conditionFailureReturnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values on condition check failure must be NONE or ALL_OLD")
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
	key, err := itemKey(state.description, request.Item)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	oldItem, existed := state.items[key]
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		writeConditionCheckFailed(w, err.Error(), conditionFailureReturnValues, oldItem, existed)
		return
	}

	previousStreamLen := len(state.streamRecords)
	state.items[key] = cloneItem(request.Item)
	state.description.ItemCount = len(state.items)
	updateIndexItemCounts(state)
	s.appendStreamRecordLocked(state, streamEventName(existed, false), oldItem, request.Item, existed)
	if err := s.persistLocked(); err != nil {
		if existed {
			state.items[key] = oldItem
		} else {
			delete(state.items, key)
		}
		state.streamRecords = state.streamRecords[:previousStreamLen]
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	response := map[string]any{}
	if returnValues == "ALL_OLD" && existed {
		response["Attributes"] = oldItem
	}
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}
func (s *Server) handleGetItem(w http.ResponseWriter, r *http.Request) {
	var request getItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
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
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	found, ok := state.items[key]
	response := map[string]any{}
	if !ok {
		addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
		writeJSON(w, http.StatusOK, response)
		return
	}
	response["Item"] = projectItem(found, request.ProjectionExpression, request.ExpressionAttributeNames)
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleDeleteItem(w http.ResponseWriter, r *http.Request) {
	var request deleteItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	returnValues := strings.ToUpper(defaultString(request.ReturnValues, "NONE"))
	if !validPutDeleteReturnValues(returnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values must be NONE or ALL_OLD")
		return
	}
	conditionFailureReturnValues := strings.ToUpper(defaultString(request.ReturnValuesOnConditionCheckFailure, "NONE"))
	if !validConditionFailureReturnValues(conditionFailureReturnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values on condition check failure must be NONE or ALL_OLD")
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}
	oldItem, existed := state.items[key]
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		writeConditionCheckFailed(w, err.Error(), conditionFailureReturnValues, oldItem, existed)
		return
	}
	previousStreamLen := len(state.streamRecords)
	delete(state.items, key)
	state.description.ItemCount = len(state.items)
	updateIndexItemCounts(state)
	s.appendStreamRecordLocked(state, "REMOVE", oldItem, nil, existed)
	if err := s.persistLocked(); err != nil {
		if existed {
			state.items[key] = oldItem
		}
		state.streamRecords = state.streamRecords[:previousStreamLen]
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}

	response := map[string]any{}
	if returnValues == "ALL_OLD" && existed {
		response["Attributes"] = oldItem
	}
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func validPutDeleteReturnValues(value string) bool {
	switch value {
	case "NONE", "ALL_OLD":
		return true
	default:
		return false
	}
}

func validConditionFailureReturnValues(value string) bool {
	switch value {
	case "NONE", "ALL_OLD":
		return true
	default:
		return false
	}
}

func (s *Server) handleUpdateItem(w http.ResponseWriter, r *http.Request) {
	var request updateItemRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.tables[request.TableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	key, err := itemKey(state.description, request.Key)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	returnValues := strings.ToUpper(defaultString(request.ReturnValues, "NONE"))
	if !validUpdateReturnValues(returnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values must be NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, or UPDATED_NEW")
		return
	}
	conditionFailureReturnValues := strings.ToUpper(defaultString(request.ReturnValuesOnConditionCheckFailure, "NONE"))
	if !validConditionFailureReturnValues(conditionFailureReturnValues) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return values on condition check failure must be NONE or ALL_OLD")
		return
	}
	if !validReturnConsumedCapacity(request.ReturnConsumedCapacity) {
		writeError(w, http.StatusBadRequest, "ValidationException", "return consumed capacity must be NONE, TOTAL, or INDEXES")
		return
	}
	updated := cloneItem(request.Key)
	oldItem, existed := state.items[key]
	if existing, ok := state.items[key]; ok {
		updated = cloneItem(existing)
	}
	if err := checkCondition(request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, oldItem, existed); err != nil {
		writeConditionCheckFailed(w, err.Error(), conditionFailureReturnValues, oldItem, existed)
		return
	}
	if err := applyUpdateExpression(updated, request.UpdateExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if err := s.validateItemSize(updated); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	previousStreamLen := len(state.streamRecords)
	state.items[key] = updated
	state.description.ItemCount = len(state.items)
	updateIndexItemCounts(state)
	s.appendStreamRecordLocked(state, streamEventName(existed, false), oldItem, updated, existed)
	if err := s.persistLocked(); err != nil {
		if existed {
			state.items[key] = oldItem
		} else {
			delete(state.items, key)
		}
		state.streamRecords = state.streamRecords[:previousStreamLen]
		state.description.ItemCount = len(state.items)
		updateIndexItemCounts(state)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}

	response := map[string]any{}
	switch returnValues {
	case "NONE":
	case "ALL_NEW":
		response["Attributes"] = cloneItem(updated)
	case "ALL_OLD":
		if existed {
			response["Attributes"] = cloneItem(oldItem)
		}
	case "UPDATED_NEW":
		if attributes := updatedAttributes(oldItem, updated); len(attributes) > 0 {
			response["Attributes"] = attributes
		}
	case "UPDATED_OLD":
		if attributes := updatedOldAttributes(oldItem, updated); len(attributes) > 0 {
			response["Attributes"] = attributes
		}
	}
	addConsumedCapacity(response, request.TableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

func validUpdateReturnValues(value string) bool {
	switch value {
	case "NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW":
		return true
	default:
		return false
	}
}

func updatedAttributes(oldValue item, newValue item) item {
	result := item{}
	for name, newAttr := range newValue {
		oldAttr, existed := oldValue[name]
		if !existed || !attributeValuesEqual(oldAttr, newAttr) {
			result[name] = cloneAttributeValue(newAttr)
		}
	}
	return result
}

func updatedOldAttributes(oldValue item, newValue item) item {
	result := item{}
	for name, oldAttr := range oldValue {
		newAttr, existsNow := newValue[name]
		if !existsNow || !attributeValuesEqual(oldAttr, newAttr) {
			result[name] = cloneAttributeValue(oldAttr)
		}
	}
	return result
}
func (s *Server) validateItemSize(value item) error {
	if err := validateItemAttributeValues(value); err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode item: %w", err)
	}
	if int64(len(encoded)) > s.maxItemBytes() {
		return fmt.Errorf("item size exceeds maximum of %d bytes", s.maxItemBytes())
	}
	return nil
}

func validateItemAttributeValues(value item) error {
	for name, attr := range value {
		if name == "" {
			return errors.New("attribute name is required")
		}
		if err := validateAttributeValue(attr, name); err != nil {
			return err
		}
	}
	return nil
}

func validateAttributeValue(value attributeValue, path string) error {
	if len(value) != 1 {
		return fmt.Errorf("attribute %s must contain exactly one AttributeValue type", path)
	}
	for kind, raw := range value {
		switch kind {
		case "S":
			if _, ok := raw.(string); !ok {
				return fmt.Errorf("attribute %s %s value must be a string", path, kind)
			}
		case "B":
			binary, ok := raw.(string)
			if !ok {
				return fmt.Errorf("attribute %s B value must be a string", path)
			}
			if _, err := base64.StdEncoding.DecodeString(binary); err != nil {
				return fmt.Errorf("attribute %s B value must be base64 encoded", path)
			}
		case "N":
			number, ok := raw.(string)
			if !ok {
				return fmt.Errorf("attribute %s N value must be a string", path)
			}
			if _, ok := new(big.Rat).SetString(number); !ok {
				return fmt.Errorf("attribute %s N value must be a valid number", path)
			}
		case "BOOL":
			if _, ok := raw.(bool); !ok {
				return fmt.Errorf("attribute %s BOOL value must be a boolean", path)
			}
		case "NULL":
			isNull, ok := raw.(bool)
			if !ok || !isNull {
				return fmt.Errorf("attribute %s NULL value must be true", path)
			}
		case "M":
			entries, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("attribute %s M value must be a map", path)
			}
			for name, nested := range entries {
				nestedValue, ok := nested.(map[string]any)
				if !ok {
					return fmt.Errorf("attribute %s.%s must be an AttributeValue object", path, name)
				}
				if err := validateAttributeValue(attributeValue(nestedValue), path+"."+name); err != nil {
					return err
				}
			}
		case "L":
			entries, ok := raw.([]any)
			if !ok {
				return fmt.Errorf("attribute %s L value must be a list", path)
			}
			for index, nested := range entries {
				nestedValue, ok := nested.(map[string]any)
				if !ok {
					return fmt.Errorf("attribute %s[%d] must be an AttributeValue object", path, index)
				}
				if err := validateAttributeValue(attributeValue(nestedValue), fmt.Sprintf("%s[%d]", path, index)); err != nil {
					return err
				}
			}
		case "SS", "BS":
			values, ok := stringSliceAttribute(value, kind)
			if !ok {
				return fmt.Errorf("attribute %s %s value must be a string list", path, kind)
			}
			if len(values) == 0 {
				return fmt.Errorf("attribute %s %s value must not be empty", path, kind)
			}
			if hasDuplicateString(values) {
				return fmt.Errorf("attribute %s %s value must not contain duplicates", path, kind)
			}
			if kind == "BS" {
				for _, binary := range values {
					if _, err := base64.StdEncoding.DecodeString(binary); err != nil {
						return fmt.Errorf("attribute %s BS value must contain base64 encoded strings", path)
					}
				}
			}
		case "NS":
			values, ok := stringSliceAttribute(value, kind)
			if !ok {
				return fmt.Errorf("attribute %s NS value must be a string list", path)
			}
			if len(values) == 0 {
				return fmt.Errorf("attribute %s NS value must not be empty", path)
			}
			if hasDuplicateString(values) {
				return fmt.Errorf("attribute %s NS value must not contain duplicates", path)
			}
			for _, number := range values {
				if _, ok := new(big.Rat).SetString(number); !ok {
					return fmt.Errorf("attribute %s NS value must contain valid numbers", path)
				}
			}
		default:
			return fmt.Errorf("attribute %s has unsupported AttributeValue type %s", path, kind)
		}
	}
	return nil
}

func hasDuplicateString(values []string) bool {
	seen := map[string]bool{}
	for _, value := range values {
		if seen[value] {
			return true
		}
		seen[value] = true
	}
	return false
}

func itemKey(description tableDescription, values item) (string, error) {
	keyValues := make([]attributeValue, 0, len(description.KeySchema))
	for _, element := range description.KeySchema {
		value, ok := values[element.AttributeName]
		if !ok {
			return "", fmt.Errorf("missing key attribute %s", element.AttributeName)
		}
		if err := validateAttributeValue(value, element.AttributeName); err != nil {
			return "", err
		}
		keyValues = append(keyValues, value)
	}
	encoded, err := json.Marshal(keyValues)
	if err != nil {
		return "", fmt.Errorf("encode key: %w", err)
	}
	return string(encoded), nil
}

type keyedItem struct {
	key   string
	value item
}

func sortedItems(state *tableState) []keyedItem {
	items := make([]keyedItem, 0, len(state.items))
	for key, value := range state.items {
		items = append(items, keyedItem{key: key, value: cloneItem(value)})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].key < items[j].key
	})
	return items
}
func extractKey(description tableDescription, value item) (item, error) {
	key := item{}
	for _, element := range description.KeySchema {
		attr, ok := value[element.AttributeName]
		if !ok {
			return nil, fmt.Errorf("missing key attribute %s", element.AttributeName)
		}
		key[element.AttributeName] = cloneAttributeValue(attr)
	}
	return key, nil
}

func projectItem(value item, expression string, names map[string]string) item {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return cloneItem(value)
	}
	projected := item{}
	for _, token := range strings.Split(expression, ",") {
		name := resolveAttributeName(strings.TrimSpace(token), names)
		if attr, ok := value[name]; ok {
			projected[name] = cloneAttributeValue(attr)
		}
	}
	return projected
}

func projectResultItem(description tableDescription, indexName string, value item, expression string, names map[string]string) item {
	projected := projectIndexItem(description, indexName, value)
	return projectItem(projected, expression, names)
}

func projectIndexItem(description tableDescription, indexName string, value item) item {
	projection, schema, ok := indexProjectionForName(description, indexName)
	if !ok || projection.ProjectionType == "" || projection.ProjectionType == "ALL" {
		return cloneItem(value)
	}
	allowed := map[string]bool{}
	for _, element := range description.KeySchema {
		allowed[element.AttributeName] = true
	}
	for _, element := range schema {
		allowed[element.AttributeName] = true
	}
	if projection.ProjectionType == "INCLUDE" {
		for _, name := range projection.NonKeyAttributes {
			allowed[name] = true
		}
	}
	projected := item{}
	for name := range allowed {
		if attr, ok := value[name]; ok {
			projected[name] = cloneAttributeValue(attr)
		}
	}
	return projected
}

func indexProjectionForName(description tableDescription, indexName string) (indexProjection, []keySchemaElement, bool) {
	if indexName == "" {
		return indexProjection{}, nil, false
	}
	for _, index := range description.GlobalSecondaryIndexes {
		if index.IndexName == indexName {
			return index.Projection, index.KeySchema, true
		}
	}
	for _, index := range description.LocalSecondaryIndexes {
		if index.IndexName == indexName {
			return index.Projection, index.KeySchema, true
		}
	}
	return indexProjection{}, nil, false
}
func cloneItem(value item) item {
	clone := make(item, len(value))
	for name, attr := range value {
		clone[name] = cloneAttributeValue(attr)
	}
	return clone
}

func cloneItems(values map[string]item) map[string]item {
	clone := make(map[string]item, len(values))
	for key, value := range values {
		clone[key] = cloneItem(value)
	}
	return clone
}
func cloneAttributeValue(value attributeValue) attributeValue {
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var clone attributeValue
	if err := json.Unmarshal(encoded, &clone); err != nil {
		return value
	}
	return clone
}
