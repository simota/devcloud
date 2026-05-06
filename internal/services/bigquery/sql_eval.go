package bigquery

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func executeParsedQuery(schema tableSchema, rows []storedRow, query simpleSelectQuery) (queryExecutionResult, error) {
	selectedFields, err := fieldsForQuery(schema, query.SelectedFields)
	if err != nil {
		return queryExecutionResult{}, err
	}
	filtered := make([]storedRow, 0, len(rows))
	for _, row := range rows {
		matches, err := rowMatchesQuery(row, query)
		if err != nil {
			return queryExecutionResult{}, err
		}
		if matches {
			filtered = append(filtered, row)
		}
	}
	if query.Aggregate.Function != "" {
		if query.GroupBy != "" {
			return executeGroupedAggregateQuery(filtered, schema, query)
		}
		return executeAggregateQuery(filtered, schema, query)
	}
	if query.OrderBy != "" {
		sort.SliceStable(filtered, func(i, j int) bool {
			left := filtered[i].JSON[query.OrderBy]
			right := filtered[j].JSON[query.OrderBy]
			cmp := compareRawValues(left, right)
			if query.OrderDesc {
				return cmp > 0
			}
			return cmp < 0
		})
	}
	if query.Offset > 0 {
		if query.Offset >= len(filtered) {
			filtered = nil
		} else {
			filtered = filtered[query.Offset:]
		}
	}
	if query.Limit >= 0 && query.Limit < len(filtered) {
		filtered = filtered[:query.Limit]
	}
	responseRows := make([]tableDataRow, 0, len(filtered))
	for _, row := range filtered {
		responseRows = append(responseRows, tableDataRow{F: formatRowValues(row.JSON, selectedFields)})
	}
	return queryExecutionResult{Fields: selectedFields, Rows: responseRows}, nil
}

func queryResultRowsToStoredRows(result queryExecutionResult) []storedRow {
	rows := make([]storedRow, 0, len(result.Rows))
	for _, row := range result.Rows {
		values := make(map[string]json.RawMessage, len(result.Fields))
		for i, field := range result.Fields {
			var value any
			if i < len(row.F) {
				value = row.F[i].V
			}
			raw, err := json.Marshal(value)
			if err != nil {
				raw = []byte("null")
			}
			values[field.Name] = json.RawMessage(raw)
		}
		rows = append(rows, storedRow{JSON: values})
	}
	return rows
}

func executeAggregateQuery(rows []storedRow, schema tableSchema, query simpleSelectQuery) (queryExecutionResult, error) {
	fieldName := query.Aggregate.Alias
	if fieldName == "" {
		fieldName = "f0_"
	}

	field, hasField := aggregateField(schema, query.Aggregate)

	switch query.Aggregate.Field {
	case "*":
		if query.Aggregate.Function != "COUNT" {
			return queryExecutionResult{}, fmt.Errorf("%s requires a field", query.Aggregate.Function)
		}
		data, _ := json.Marshal(strconv.Itoa(len(rows)))
		return queryExecutionResult{
			Fields: []tableFieldSchema{{
				Name: fieldName,
				Type: "INTEGER",
				Mode: "NULLABLE",
			}},
			Rows: []tableDataRow{{
				F: []tableCell{{V: rawValueForResponse(data)}},
			}},
		}, nil
	default:
		if !hasField {
			return queryExecutionResult{}, fmt.Errorf("aggregate field %q does not exist", query.Aggregate.Field)
		}
	}

	switch query.Aggregate.Function {
	case "COUNT":
		count := 0
		for _, row := range rows {
			raw, ok := row.JSON[query.Aggregate.Field]
			if ok && !isJSONNull(raw) {
				count++
			}
		}
		return singleAggregateResult(fieldName, "INTEGER", strconv.Itoa(count)), nil
	case "SUM":
		if !isNumericField(field) {
			return queryExecutionResult{}, fmt.Errorf("SUM requires a numeric field")
		}
		sum, count, err := sumAggregate(rows, query.Aggregate.Field, isIntegerField(field))
		if err != nil {
			return queryExecutionResult{}, err
		}
		if count == 0 {
			return singleAggregateNullResult(fieldName, aggregateNumericType(field)), nil
		}
		return singleAggregateResult(fieldName, aggregateNumericType(field), sum), nil
	case "AVG":
		if !isNumericField(field) {
			return queryExecutionResult{}, fmt.Errorf("AVG requires a numeric field")
		}
		sum, count, err := floatAggregate(rows, query.Aggregate.Field)
		if err != nil {
			return queryExecutionResult{}, err
		}
		if count == 0 {
			return singleAggregateNullResult(fieldName, "FLOAT"), nil
		}
		return singleAggregateResult(fieldName, "FLOAT", strconv.FormatFloat(sum/float64(count), 'f', -1, 64)), nil
	case "MIN", "MAX":
		raw, ok := minMaxAggregate(rows, query.Aggregate.Field, query.Aggregate.Function)
		if !ok {
			return singleAggregateNullResult(fieldName, defaultString(field.Type, "STRING")), nil
		}
		return queryExecutionResult{
			Fields: []tableFieldSchema{{
				Name: fieldName,
				Type: defaultString(field.Type, "STRING"),
				Mode: "NULLABLE",
			}},
			Rows: []tableDataRow{{
				F: []tableCell{{V: rawValueForResponse(raw)}},
			}},
		}, nil
	default:
		return queryExecutionResult{}, fmt.Errorf("unsupported aggregate function")
	}
}

func executeGroupedAggregateQuery(rows []storedRow, schema tableSchema, query simpleSelectQuery) (queryExecutionResult, error) {
	fields, err := groupedAggregateDryRunFields(schema, query)
	if err != nil {
		return queryExecutionResult{}, err
	}
	groups := make(map[string][]storedRow)
	groupValues := make(map[string]json.RawMessage)
	for _, row := range rows {
		raw, ok := row.JSON[query.GroupBy]
		if !ok {
			raw = json.RawMessage("null")
		}
		key := string(raw)
		groups[key] = append(groups[key], row)
		groupValues[key] = raw
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		cmp := compareRawValues(groupValues[keys[i]], groupValues[keys[j]])
		if query.OrderDesc {
			return cmp > 0
		}
		return cmp < 0
	})
	if query.OrderBy != "" && query.OrderBy != query.GroupBy {
		return queryExecutionResult{}, fmt.Errorf("ORDER BY supports grouped field only for GROUP BY queries")
	}
	if query.Offset > 0 {
		if query.Offset >= len(keys) {
			keys = nil
		} else {
			keys = keys[query.Offset:]
		}
	}
	if query.Limit >= 0 && query.Limit < len(keys) {
		keys = keys[:query.Limit]
	}

	responseRows := make([]tableDataRow, 0, len(keys))
	for _, key := range keys {
		aggregate, err := executeAggregateQuery(groups[key], schema, simpleSelectQuery{Aggregate: query.Aggregate})
		if err != nil {
			return queryExecutionResult{}, err
		}
		cell := tableCell{V: nil}
		if raw := groupValues[key]; !isJSONNull(raw) {
			cell.V = rawValueForFieldResponse(raw, fields[0])
		}
		if len(aggregate.Rows) == 0 || len(aggregate.Rows[0].F) == 0 {
			return queryExecutionResult{}, fmt.Errorf("aggregate result is empty")
		}
		responseRows = append(responseRows, tableDataRow{
			F: []tableCell{cell, aggregate.Rows[0].F[0]},
		})
	}
	return queryExecutionResult{Fields: fields, Rows: responseRows}, nil
}

func singleAggregateResult(name string, fieldType string, value string) queryExecutionResult {
	data, _ := json.Marshal(value)
	return queryExecutionResult{
		Fields: []tableFieldSchema{{
			Name: name,
			Type: fieldType,
			Mode: "NULLABLE",
		}},
		Rows: []tableDataRow{{
			F: []tableCell{{V: rawValueForResponse(data)}},
		}},
	}
}

func singleAggregateNullResult(name string, fieldType string) queryExecutionResult {
	return queryExecutionResult{
		Fields: []tableFieldSchema{{
			Name: name,
			Type: fieldType,
			Mode: "NULLABLE",
		}},
		Rows: []tableDataRow{{
			F: []tableCell{{V: nil}},
		}},
	}
}

func sumAggregate(rows []storedRow, fieldName string, integer bool) (string, int, error) {
	if integer {
		var sum int64
		count := 0
		for _, row := range rows {
			raw, ok := row.JSON[fieldName]
			if !ok || isJSONNull(raw) {
				continue
			}
			value, ok := rawInt(raw)
			if !ok {
				return "", 0, fmt.Errorf("SUM field contains a non-integer value")
			}
			sum += value
			count++
		}
		return strconv.FormatInt(sum, 10), count, nil
	}
	sum, count, err := floatAggregate(rows, fieldName)
	if err != nil {
		return "", 0, err
	}
	return strconv.FormatFloat(sum, 'f', -1, 64), count, nil
}

func floatAggregate(rows []storedRow, fieldName string) (float64, int, error) {
	var sum float64
	count := 0
	for _, row := range rows {
		raw, ok := row.JSON[fieldName]
		if !ok || isJSONNull(raw) {
			continue
		}
		value, ok := rawFloat(raw)
		if !ok {
			return 0, 0, fmt.Errorf("aggregate field contains a non-numeric value")
		}
		sum += value
		count++
	}
	return sum, count, nil
}

func minMaxAggregate(rows []storedRow, fieldName string, function string) (json.RawMessage, bool) {
	var selected json.RawMessage
	found := false
	for _, row := range rows {
		raw, ok := row.JSON[fieldName]
		if !ok || isJSONNull(raw) {
			continue
		}
		if !found {
			selected = raw
			found = true
			continue
		}
		cmp := compareRawValues(raw, selected)
		if function == "MIN" && cmp < 0 || function == "MAX" && cmp > 0 {
			selected = raw
		}
	}
	return selected, found
}

func isNumericField(field tableFieldSchema) bool {
	fieldType := strings.ToUpper(defaultString(field.Type, "STRING"))
	switch fieldType {
	case "INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC":
		return true
	default:
		return false
	}
}

func isIntegerField(field tableFieldSchema) bool {
	fieldType := strings.ToUpper(defaultString(field.Type, "STRING"))
	return fieldType == "INTEGER" || fieldType == "INT64"
}

func aggregateNumericType(field tableFieldSchema) string {
	if isIntegerField(field) {
		return "INTEGER"
	}
	fieldType := strings.ToUpper(defaultString(field.Type, "FLOAT"))
	if fieldType == "FLOAT64" {
		return "FLOAT"
	}
	return fieldType
}

func rowMatchesQuery(row storedRow, query simpleSelectQuery) (bool, error) {
	if len(query.WhereConditionGroups) == 0 {
		if len(query.WhereConditions) > 0 {
			query.WhereConditionGroups = [][]whereCondition{query.WhereConditions}
		} else if query.WhereOperator != "" {
			query.WhereConditionGroups = [][]whereCondition{{
				{
					Field:    query.WhereField,
					Operator: query.WhereOperator,
					ValueRaw: query.WhereValueRaw,
				},
			}}
		}
	}
	if len(query.WhereConditionGroups) == 0 {
		return true, nil
	}
	for _, group := range query.WhereConditionGroups {
		matches, err := rowMatchesAllConditions(row, group)
		if err != nil {
			return false, err
		}
		if matches {
			return true, nil
		}
	}
	return false, nil
}

func rowMatchesAllConditions(row storedRow, conditions []whereCondition) (bool, error) {
	if len(conditions) == 0 {
		return true, nil
	}
	for _, condition := range conditions {
		raw, ok := row.JSON[condition.Field]
		if !ok || isJSONNull(raw) {
			return false, nil
		}
		cmp := compareRawValues(raw, condition.ValueRaw)
		var matches bool
		switch condition.Operator {
		case "=":
			matches = cmp == 0
		case "NOT =":
			matches = cmp != 0
		case "!=":
			matches = cmp != 0
		case "NOT !=":
			matches = cmp == 0
		case ">":
			matches = cmp > 0
		case "NOT >":
			matches = cmp <= 0
		case ">=":
			matches = cmp >= 0
		case "NOT >=":
			matches = cmp < 0
		case "<":
			matches = cmp < 0
		case "NOT <":
			matches = cmp >= 0
		case "<=":
			matches = cmp <= 0
		case "NOT <=":
			matches = cmp > 0
		default:
			return false, fmt.Errorf("unsupported WHERE operator")
		}
		if !matches {
			return false, nil
		}
	}
	return true, nil
}

func compareRawValues(left json.RawMessage, right json.RawMessage) int {
	leftNumber, leftNumberOK := rawFloat(left)
	rightNumber, rightNumberOK := rawFloat(right)
	if leftNumberOK && rightNumberOK {
		switch {
		case leftNumber < rightNumber:
			return -1
		case leftNumber > rightNumber:
			return 1
		default:
			return 0
		}
	}
	leftString := fmt.Sprint(rawValueForResponse(left))
	rightString := fmt.Sprint(rawValueForResponse(right))
	return strings.Compare(leftString, rightString)
}

func rawFloat(raw json.RawMessage) (float64, bool) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		value, err := strconv.ParseFloat(asString, 64)
		return value, err == nil
	}
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return 0, false
	}
	value, err := strconv.ParseFloat(number.String(), 64)
	return value, err == nil
}

func rawInt(raw json.RawMessage) (int64, bool) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		value, err := strconv.ParseInt(asString, 10, 64)
		return value, err == nil
	}
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return 0, false
	}
	value, err := strconv.ParseInt(number.String(), 10, 64)
	return value, err == nil
}
