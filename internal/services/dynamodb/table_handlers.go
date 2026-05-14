package dynamodb

import (
	"errors"
	"net/http"
	"sort"
	"time"

	"devcloud/internal/events"
)

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	var request listTablesRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.Limit < 0 || request.Limit > 100 {
		writeError(w, http.StatusBadRequest, "ValidationException", "limit must be between 1 and 100")
		return
	}

	s.mu.Lock()
	names := make([]string, 0, len(s.tables))
	for name := range s.tables {
		names = append(names, name)
	}
	s.mu.Unlock()

	sort.Strings(names)
	start := 0
	if request.ExclusiveStartTableName != "" {
		found := false
		for i, name := range names {
			if name == request.ExclusiveStartTableName {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			writeError(w, http.StatusBadRequest, "ValidationException", "exclusive start table name does not exist")
			return
		}
	}
	if start > len(names) {
		start = len(names)
	}
	end := len(names)
	if request.Limit > 0 && start+request.Limit < end {
		end = start + request.Limit
	}
	response := map[string]any{"TableNames": names[start:end]}
	if end < len(names) {
		response["LastEvaluatedTableName"] = names[end-1]
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	var request createTableRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if err := validateCreateTableRequest(request); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}

	createdAt := time.Now().Unix()
	description := tableDescription{
		AttributeDefinitions:   append([]attributeDefinition(nil), request.AttributeDefinitions...),
		BillingModeSummary:     &billingModeSummary{BillingMode: billingMode(request.BillingMode)},
		CreationDateTime:       createdAt,
		GlobalSecondaryIndexes: gsiDescriptions(defaultString(s.config.Region, "us-east-1"), request.TableName, request.GlobalSecondaryIndexes),
		ItemCount:              0,
		KeySchema:              append([]keySchemaElement(nil), request.KeySchema...),
		LocalSecondaryIndexes:  lsiDescriptions(defaultString(s.config.Region, "us-east-1"), request.TableName, request.LocalSecondaryIndexes),
		TableArn:               "arn:aws:dynamodb:" + defaultString(s.config.Region, "us-east-1") + ":000000000000:table/" + request.TableName,
		TableName:              request.TableName,
		TableSizeBytes:         0,
		TableStatus:            "ACTIVE",
	}
	if request.StreamSpecification.StreamEnabled {
		if err := validateStreamSpecification(request.StreamSpecification); err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		enableStreamDescription(&description, defaultString(s.config.Region, "us-east-1"), request.StreamSpecification)
	}

	s.mu.Lock()
	if _, exists := s.tables[request.TableName]; exists {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, "ResourceInUseException", "table already exists")
		return
	}
	if len(s.tables) >= s.maxTables() {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, "LimitExceededException", "table limit exceeded")
		return
	}
	s.tables[request.TableName] = &tableState{
		description: description,
		items:       map[string]item{},
		tags:        map[string]string{},
	}
	if err := s.persistLocked(); err != nil {
		delete(s.tables, request.TableName)
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	s.mu.Unlock()

	events.Emit(s.eventPublisher, events.Event{
		Type:    "dynamodb.table.created",
		Service: "dynamodb",
		Payload: map[string]any{"table": request.TableName},
	})
	writeJSON(w, http.StatusOK, map[string]any{"TableDescription": description})
}

func (s *Server) handleDescribeTable(w http.ResponseWriter, r *http.Request) {
	var request tableNameRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	description, ok := s.table(request.TableName)
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"Table": description})
}

func (s *Server) handleDeleteTable(w http.ResponseWriter, r *http.Request) {
	var request tableNameRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	if ok {
		delete(s.tables, request.TableName)
	}
	if ok {
		if err := s.persistLocked(); err != nil {
			s.tables[request.TableName] = state
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
			return
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	events.Emit(s.eventPublisher, events.Event{
		Type:    "dynamodb.table.deleted",
		Service: "dynamodb",
		Payload: map[string]any{"table": request.TableName},
	})
	writeJSON(w, http.StatusOK, map[string]any{"TableDescription": state.description})
}

func (s *Server) handleUpdateTable(w http.ResponseWriter, r *http.Request) {
	var request updateTableRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.TableName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "table name is required")
		return
	}
	if request.BillingMode != "" && request.BillingMode != "PAY_PER_REQUEST" && request.BillingMode != "PROVISIONED" {
		writeError(w, http.StatusBadRequest, "ValidationException", "billing mode must be PAY_PER_REQUEST or PROVISIONED")
		return
	}
	if request.StreamSpecification != nil {
		if err := validateStreamSpecification(*request.StreamSpecification); err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
	}

	s.mu.Lock()
	state, ok := s.tables[request.TableName]
	var previous tableDescription
	if ok {
		previous = state.description
	}
	if ok && request.BillingMode != "" {
		state.description.BillingModeSummary = &billingModeSummary{BillingMode: request.BillingMode}
	}
	if ok && request.StreamSpecification != nil {
		if request.StreamSpecification.StreamEnabled {
			enableStreamDescription(&state.description, defaultString(s.config.Region, "us-east-1"), *request.StreamSpecification)
		} else {
			state.description.StreamSpecification = &streamSpecification{StreamEnabled: false}
			state.description.LatestStreamArn = ""
			state.description.LatestStreamLabel = ""
		}
	}
	if ok && len(request.GlobalSecondaryIndexUpdates) > 0 {
		if err := applyGlobalSecondaryIndexUpdates(&state.description, defaultString(s.config.Region, "us-east-1"), request.AttributeDefinitions, request.GlobalSecondaryIndexUpdates); err != nil {
			state.description = previous
			s.mu.Unlock()
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
		updateIndexItemCounts(state)
	}
	var description tableDescription
	if ok {
		description = state.description
		if err := s.persistLocked(); err != nil {
			state.description = previous
			s.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
			return
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"TableDescription": description})
}

func (s *Server) handleDescribeLimits(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]int{
		"AccountMaxReadCapacityUnits":  80000,
		"AccountMaxWriteCapacityUnits": 80000,
		"TableMaxReadCapacityUnits":    40000,
		"TableMaxWriteCapacityUnits":   40000,
	})
}

func (s *Server) handleDescribeEndpoints(w http.ResponseWriter) {
	address := defaultString(s.config.Addr, "127.0.0.1:8000")
	writeJSON(w, http.StatusOK, map[string]any{
		"Endpoints": []map[string]any{
			{
				"Address":              address,
				"CachePeriodInMinutes": int64(1440),
			},
		},
	})
}
func validateCreateTableRequest(request createTableRequest) error {
	if request.TableName == "" {
		return errors.New("table name is required")
	}
	if len(request.KeySchema) == 0 {
		return errors.New("key schema is required")
	}
	hashKeys := 0
	rangeKeys := 0
	attributes := map[string]bool{}
	for _, definition := range request.AttributeDefinitions {
		if definition.AttributeName == "" {
			return errors.New("attribute name is required")
		}
		switch definition.AttributeType {
		case "S", "N", "B":
		default:
			return errors.New("attribute type must be S, N, or B")
		}
		attributes[definition.AttributeName] = true
	}
	for _, element := range request.KeySchema {
		if element.AttributeName == "" {
			return errors.New("key attribute name is required")
		}
		if !attributes[element.AttributeName] {
			return errors.New("key schema attributes must be defined")
		}
		switch element.KeyType {
		case "HASH":
			hashKeys++
		case "RANGE":
			rangeKeys++
		default:
			return errors.New("key type must be HASH or RANGE")
		}
	}
	if hashKeys != 1 || rangeKeys > 1 || len(request.KeySchema) > 2 {
		return errors.New("key schema must include one HASH key and at most one RANGE key")
	}
	if request.BillingMode != "" && request.BillingMode != "PAY_PER_REQUEST" && request.BillingMode != "PROVISIONED" {
		return errors.New("billing mode must be PAY_PER_REQUEST or PROVISIONED")
	}
	if request.StreamSpecification.StreamEnabled {
		if err := validateStreamSpecification(request.StreamSpecification); err != nil {
			return err
		}
	}
	indexNames := map[string]bool{}
	for _, index := range request.GlobalSecondaryIndexes {
		if index.IndexName == "" {
			return errors.New("global secondary index name is required")
		}
		if indexNames[index.IndexName] {
			return errors.New("global secondary index names must be unique")
		}
		indexNames[index.IndexName] = true
		if len(index.KeySchema) == 0 {
			return errors.New("global secondary index key schema is required")
		}
		indexHashKeys := 0
		indexRangeKeys := 0
		for _, element := range index.KeySchema {
			if !attributes[element.AttributeName] {
				return errors.New("global secondary index key schema attributes must be defined")
			}
			switch element.KeyType {
			case "HASH":
				indexHashKeys++
			case "RANGE":
				indexRangeKeys++
			default:
				return errors.New("global secondary index key type must be HASH or RANGE")
			}
		}
		if indexHashKeys != 1 || indexRangeKeys > 1 || len(index.KeySchema) > 2 {
			return errors.New("global secondary index key schema must include one HASH key and at most one RANGE key")
		}
		if index.Projection.ProjectionType != "" && index.Projection.ProjectionType != "ALL" && index.Projection.ProjectionType != "KEYS_ONLY" && index.Projection.ProjectionType != "INCLUDE" {
			return errors.New("global secondary index projection type must be ALL, KEYS_ONLY, or INCLUDE")
		}
	}
	for _, index := range request.LocalSecondaryIndexes {
		if index.IndexName == "" {
			return errors.New("local secondary index name is required")
		}
		if indexNames[index.IndexName] {
			return errors.New("secondary index names must be unique")
		}
		indexNames[index.IndexName] = true
		if len(index.KeySchema) != 2 {
			return errors.New("local secondary index key schema must include table HASH key and one RANGE key")
		}
		if index.KeySchema[0].KeyType != "HASH" || index.KeySchema[0].AttributeName != tableHashKey(request.KeySchema) {
			return errors.New("local secondary index HASH key must match table HASH key")
		}
		rangeKeys := 0
		for _, element := range index.KeySchema {
			if !attributes[element.AttributeName] {
				return errors.New("local secondary index key schema attributes must be defined")
			}
			switch element.KeyType {
			case "HASH":
			case "RANGE":
				rangeKeys++
			default:
				return errors.New("local secondary index key type must be HASH or RANGE")
			}
		}
		if rangeKeys != 1 {
			return errors.New("local secondary index key schema must include one RANGE key")
		}
		if index.Projection.ProjectionType != "" && index.Projection.ProjectionType != "ALL" && index.Projection.ProjectionType != "KEYS_ONLY" && index.Projection.ProjectionType != "INCLUDE" {
			return errors.New("local secondary index projection type must be ALL, KEYS_ONLY, or INCLUDE")
		}
	}
	return nil
}

func tableHashKey(schema []keySchemaElement) string {
	for _, element := range schema {
		if element.KeyType == "HASH" {
			return element.AttributeName
		}
	}
	return ""
}

func tableHasIndex(description tableDescription, indexName string) bool {
	for _, index := range description.GlobalSecondaryIndexes {
		if index.IndexName == indexName {
			return true
		}
	}
	for _, index := range description.LocalSecondaryIndexes {
		if index.IndexName == indexName {
			return true
		}
	}
	return false
}

func gsiDescriptions(region string, tableName string, indexes []globalSecondaryIndexRequest) []globalSecondaryIndexDescription {
	if len(indexes) == 0 {
		return nil
	}
	descriptions := make([]globalSecondaryIndexDescription, 0, len(indexes))
	for _, index := range indexes {
		projection := index.Projection
		if projection.ProjectionType == "" {
			projection.ProjectionType = "ALL"
		}
		descriptions = append(descriptions, globalSecondaryIndexDescription{
			IndexArn:       "arn:aws:dynamodb:" + region + ":000000000000:table/" + tableName + "/index/" + index.IndexName,
			IndexName:      index.IndexName,
			IndexSizeBytes: 0,
			IndexStatus:    "ACTIVE",
			ItemCount:      0,
			KeySchema:      append([]keySchemaElement(nil), index.KeySchema...),
			Projection:     projection,
		})
	}
	return descriptions
}

func lsiDescriptions(region string, tableName string, indexes []localSecondaryIndexRequest) []localSecondaryIndexDescription {
	if len(indexes) == 0 {
		return nil
	}
	descriptions := make([]localSecondaryIndexDescription, 0, len(indexes))
	for _, index := range indexes {
		projection := index.Projection
		if projection.ProjectionType == "" {
			projection.ProjectionType = "ALL"
		}
		descriptions = append(descriptions, localSecondaryIndexDescription{
			IndexArn:       "arn:aws:dynamodb:" + region + ":000000000000:table/" + tableName + "/index/" + index.IndexName,
			IndexName:      index.IndexName,
			IndexSizeBytes: 0,
			ItemCount:      0,
			KeySchema:      append([]keySchemaElement(nil), index.KeySchema...),
			Projection:     projection,
		})
	}
	return descriptions
}

func applyGlobalSecondaryIndexUpdates(description *tableDescription, region string, definitions []attributeDefinition, updates []globalSecondaryIndexUpdate) error {
	if err := validateAttributeDefinitionUpdates(description.AttributeDefinitions, definitions); err != nil {
		return err
	}
	attributes := attributeDefinitionSet(description.AttributeDefinitions, definitions)
	for _, update := range updates {
		actions := 0
		if update.Create != nil {
			actions++
		}
		if update.Delete != nil {
			actions++
		}
		if update.Update != nil {
			actions++
		}
		if actions != 1 {
			return errors.New("each global secondary index update must contain exactly one action")
		}
		if update.Update != nil {
			return errors.New("global secondary index throughput updates are not supported")
		}
		if update.Create != nil {
			if err := validateGlobalSecondaryIndexCreate(*update.Create, attributes, *description); err != nil {
				return err
			}
			description.AttributeDefinitions = mergeAttributeDefinitions(description.AttributeDefinitions, definitions)
			description.GlobalSecondaryIndexes = append(description.GlobalSecondaryIndexes, gsiDescriptions(region, description.TableName, []globalSecondaryIndexRequest{*update.Create})...)
			continue
		}
		if update.Delete != nil {
			index := indexOfGlobalSecondaryIndex(description.GlobalSecondaryIndexes, update.Delete.IndexName)
			if index < 0 {
				return errors.New("global secondary index does not exist")
			}
			description.GlobalSecondaryIndexes = append(description.GlobalSecondaryIndexes[:index], description.GlobalSecondaryIndexes[index+1:]...)
		}
	}
	return nil
}

func validateAttributeDefinitionUpdates(existing []attributeDefinition, updates []attributeDefinition) error {
	types := map[string]string{}
	for _, definition := range existing {
		types[definition.AttributeName] = definition.AttributeType
	}
	for _, definition := range updates {
		if definition.AttributeName == "" {
			return errors.New("attribute name is required")
		}
		switch definition.AttributeType {
		case "S", "N", "B":
		default:
			return errors.New("attribute type must be S, N, or B")
		}
		if existingType, ok := types[definition.AttributeName]; ok && existingType != definition.AttributeType {
			return errors.New("attribute definitions cannot change existing attribute type")
		}
		types[definition.AttributeName] = definition.AttributeType
	}
	return nil
}

func attributeDefinitionSet(existing []attributeDefinition, updates []attributeDefinition) map[string]bool {
	attributes := map[string]bool{}
	for _, definition := range existing {
		attributes[definition.AttributeName] = true
	}
	for _, definition := range updates {
		if definition.AttributeName != "" {
			attributes[definition.AttributeName] = true
		}
	}
	return attributes
}

func mergeAttributeDefinitions(existing []attributeDefinition, updates []attributeDefinition) []attributeDefinition {
	merged := append([]attributeDefinition(nil), existing...)
	seen := map[string]bool{}
	for _, definition := range existing {
		seen[definition.AttributeName] = true
	}
	for _, definition := range updates {
		if definition.AttributeName == "" || seen[definition.AttributeName] {
			continue
		}
		merged = append(merged, definition)
		seen[definition.AttributeName] = true
	}
	return merged
}

func validateGlobalSecondaryIndexCreate(index globalSecondaryIndexRequest, attributes map[string]bool, description tableDescription) error {
	if index.IndexName == "" {
		return errors.New("global secondary index name is required")
	}
	if tableHasIndex(description, index.IndexName) {
		return errors.New("secondary index name already exists")
	}
	if len(index.KeySchema) == 0 {
		return errors.New("global secondary index key schema is required")
	}
	hashKeys := 0
	rangeKeys := 0
	for _, element := range index.KeySchema {
		if !attributes[element.AttributeName] {
			return errors.New("global secondary index key schema attributes must be defined")
		}
		switch element.KeyType {
		case "HASH":
			hashKeys++
		case "RANGE":
			rangeKeys++
		default:
			return errors.New("global secondary index key type must be HASH or RANGE")
		}
	}
	if hashKeys != 1 || rangeKeys > 1 || len(index.KeySchema) > 2 {
		return errors.New("global secondary index key schema must include one HASH key and at most one RANGE key")
	}
	if index.Projection.ProjectionType != "" && index.Projection.ProjectionType != "ALL" && index.Projection.ProjectionType != "KEYS_ONLY" && index.Projection.ProjectionType != "INCLUDE" {
		return errors.New("global secondary index projection type must be ALL, KEYS_ONLY, or INCLUDE")
	}
	return nil
}

func indexOfGlobalSecondaryIndex(indexes []globalSecondaryIndexDescription, indexName string) int {
	for i, index := range indexes {
		if index.IndexName == indexName {
			return i
		}
	}
	return -1
}

func updateIndexItemCounts(state *tableState) {
	for i := range state.description.GlobalSecondaryIndexes {
		count := 0
		for _, candidate := range state.items {
			if itemHasAllKeys(candidate, state.description.GlobalSecondaryIndexes[i].KeySchema) {
				count++
			}
		}
		state.description.GlobalSecondaryIndexes[i].ItemCount = count
	}
	for i := range state.description.LocalSecondaryIndexes {
		count := 0
		for _, candidate := range state.items {
			if itemHasAllKeys(candidate, state.description.LocalSecondaryIndexes[i].KeySchema) {
				count++
			}
		}
		state.description.LocalSecondaryIndexes[i].ItemCount = count
	}
}

func itemHasAllKeys(value item, schema []keySchemaElement) bool {
	for _, element := range schema {
		if _, ok := value[element.AttributeName]; !ok {
			return false
		}
	}
	return true
}

func billingMode(value string) string {
	if value == "" {
		return "PAY_PER_REQUEST"
	}
	return value
}
func cloneTableDescription(value tableDescription) tableDescription {
	clone := value
	clone.AttributeDefinitions = append([]attributeDefinition(nil), value.AttributeDefinitions...)
	clone.KeySchema = append([]keySchemaElement(nil), value.KeySchema...)
	clone.GlobalSecondaryIndexes = append([]globalSecondaryIndexDescription(nil), value.GlobalSecondaryIndexes...)
	clone.LocalSecondaryIndexes = append([]localSecondaryIndexDescription(nil), value.LocalSecondaryIndexes...)
	clone.StreamSpecification = cloneStreamSpecification(value.StreamSpecification)
	clone.TimeToLiveDescription = cloneTTLDescription(value.TimeToLiveDescription)
	return clone
}
func cloneTags(value map[string]string) map[string]string {
	clone := make(map[string]string, len(value))
	for key, val := range value {
		clone[key] = val
	}
	return clone
}
func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
