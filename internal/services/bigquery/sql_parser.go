package bigquery

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func parseSimpleSelect(rawQuery string, defaultProjectID string) (simpleSelectQuery, error) {
	query := strings.TrimSpace(strings.TrimSuffix(rawQuery, ";"))
	if query == "" {
		return simpleSelectQuery{}, fmt.Errorf("query is required")
	}
	upper := strings.ToUpper(query)
	if !strings.HasPrefix(upper, "SELECT ") {
		return simpleSelectQuery{}, fmt.Errorf("only SELECT queries are supported")
	}
	fromIndex := strings.Index(upper, " FROM ")
	if fromIndex < 0 {
		return simpleSelectQuery{}, fmt.Errorf("SELECT query requires FROM")
	}
	selected := strings.TrimSpace(query[len("SELECT "):fromIndex])
	if selected == "" {
		return simpleSelectQuery{}, fmt.Errorf("SELECT list is required")
	}
	rest := strings.TrimSpace(query[fromIndex+len(" FROM "):])
	tableExpr, rest := nextQueryToken(rest)
	projectID, datasetID, tableID, err := parseTableIdentifier(tableExpr, defaultProjectID)
	if err != nil {
		return simpleSelectQuery{}, err
	}
	parsed := simpleSelectQuery{
		ProjectID:      projectID,
		DatasetID:      datasetID,
		TableID:        tableID,
		SelectedFields: parseSelectedFields(selected),
		Limit:          -1,
		Offset:         0,
		WhereOperator:  "",
		WhereField:     "",
		WhereValueRaw:  nil,
		OrderBy:        "",
	}
	if len(parsed.SelectedFields) == 0 {
		return simpleSelectQuery{}, fmt.Errorf("SELECT list is required")
	}
	if aggregate, ok, err := parseAggregateSelection(selected); err != nil {
		return simpleSelectQuery{}, err
	} else if ok {
		parsed.Aggregate = aggregate
		parsed.SelectedFields = nil
	} else if groupField, aggregate, ok, err := parseGroupedAggregateSelection(selected); err != nil {
		return simpleSelectQuery{}, err
	} else if ok {
		parsed.Aggregate = aggregate
		parsed.SelectedFields = []string{groupField}
	}
	rest = strings.TrimSpace(rest)
	for rest != "" {
		upperRest := strings.ToUpper(rest)
		switch {
		case strings.HasPrefix(upperRest, "WHERE "):
			conditionEnd := len(rest)
			for _, marker := range []string{" GROUP BY ", " ORDER BY ", " LIMIT ", " OFFSET "} {
				if idx := strings.Index(strings.ToUpper(rest), marker); idx >= 0 && idx < conditionEnd {
					conditionEnd = idx
				}
			}
			condition := strings.TrimSpace(rest[len("WHERE "):conditionEnd])
			conditionGroups, err := parseSimpleConditionGroups(condition)
			if err != nil {
				return simpleSelectQuery{}, err
			}
			conditions := flattenWhereConditionGroups(conditionGroups)
			parsed.WhereConditions = conditions
			parsed.WhereConditionGroups = conditionGroups
			if len(conditions) > 0 {
				parsed.WhereField = conditions[0].Field
				parsed.WhereOperator = conditions[0].Operator
				parsed.WhereValueRaw = conditions[0].ValueRaw
			}
			rest = strings.TrimSpace(rest[conditionEnd:])
		case strings.HasPrefix(upperRest, "GROUP BY "):
			value := strings.TrimSpace(rest[len("GROUP BY "):])
			field, suffix := nextQueryToken(value)
			if field == "" {
				return simpleSelectQuery{}, fmt.Errorf("GROUP BY field is required")
			}
			parsed.GroupBy = strings.Trim(field, "`")
			rest = strings.TrimSpace(suffix)
		case strings.HasPrefix(upperRest, "ORDER BY "):
			if parsed.Aggregate.Function != "" && parsed.GroupBy == "" {
				return simpleSelectQuery{}, fmt.Errorf("ORDER BY is not supported for aggregate queries")
			}
			value := strings.TrimSpace(rest[len("ORDER BY "):])
			field, suffix := nextQueryToken(value)
			if field == "" {
				return simpleSelectQuery{}, fmt.Errorf("ORDER BY field is required")
			}
			parsed.OrderBy = strings.Trim(field, "`")
			rest = strings.TrimSpace(suffix)
			direction, directionSuffix := nextQueryToken(rest)
			switch strings.ToUpper(direction) {
			case "ASC":
				rest = strings.TrimSpace(directionSuffix)
			case "DESC":
				parsed.OrderDesc = true
				rest = strings.TrimSpace(directionSuffix)
			}
		case strings.HasPrefix(upperRest, "LIMIT "):
			value, suffix := nextQueryToken(strings.TrimSpace(rest[len("LIMIT "):]))
			limit, err := strconv.Atoi(value)
			if err != nil || limit < 0 {
				return simpleSelectQuery{}, fmt.Errorf("LIMIT must be a non-negative integer")
			}
			parsed.Limit = limit
			rest = strings.TrimSpace(suffix)
		case strings.HasPrefix(upperRest, "OFFSET "):
			value, suffix := nextQueryToken(strings.TrimSpace(rest[len("OFFSET "):]))
			offset, err := strconv.Atoi(value)
			if err != nil || offset < 0 {
				return simpleSelectQuery{}, fmt.Errorf("OFFSET must be a non-negative integer")
			}
			parsed.Offset = offset
			rest = strings.TrimSpace(suffix)
		default:
			return simpleSelectQuery{}, fmt.Errorf("unsupported query clause")
		}
	}
	if parsed.GroupBy != "" {
		if parsed.Aggregate.Function == "" {
			return simpleSelectQuery{}, fmt.Errorf("GROUP BY requires an aggregate selection")
		}
		if len(parsed.SelectedFields) != 1 || parsed.SelectedFields[0] != parsed.GroupBy {
			return simpleSelectQuery{}, fmt.Errorf("GROUP BY field must be selected")
		}
	}
	return parsed, nil
}

func bindQueryParameters(rawQuery string, parameters []queryParameter) (string, error) {
	if len(parameters) == 0 {
		return rawQuery, nil
	}
	replacements := make(map[string]string, len(parameters))
	for _, parameter := range parameters {
		name := strings.TrimSpace(parameter.Name)
		if name == "" {
			return "", fmt.Errorf("named query parameter name is required")
		}
		if err := validateResourceID(name, "query parameter"); err != nil {
			return "", err
		}
		value, err := parameterSQLLiteral(parameter)
		if err != nil {
			return "", err
		}
		replacements[name] = value
	}
	bound, used, err := replaceNamedParameters(rawQuery, replacements)
	if err != nil {
		return "", err
	}
	for name := range replacements {
		if !used[name] {
			return "", fmt.Errorf("query parameter %q was not used", name)
		}
	}
	return bound, nil
}

func replaceNamedParameters(query string, replacements map[string]string) (string, map[string]bool, error) {
	var out strings.Builder
	used := make(map[string]bool, len(replacements))
	inSingleQuote := false
	inDoubleQuote := false
	for i := 0; i < len(query); {
		ch := query[i]
		switch ch {
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
			out.WriteByte(ch)
			i++
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
			out.WriteByte(ch)
			i++
		case '@':
			if inSingleQuote || inDoubleQuote {
				out.WriteByte(ch)
				i++
				continue
			}
			end := i + 1
			for end < len(query) && isParameterNameByte(query[end]) {
				end++
			}
			if end == i+1 {
				out.WriteByte(ch)
				i++
				continue
			}
			name := query[i+1 : end]
			value, ok := replacements[name]
			if !ok {
				return "", nil, fmt.Errorf("query parameter %q was not provided", name)
			}
			used[name] = true
			out.WriteString(value)
			i = end
		default:
			out.WriteByte(ch)
			i++
		}
	}
	return out.String(), used, nil
}

func isParameterNameByte(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_'
}

func parameterSQLLiteral(parameter queryParameter) (string, error) {
	value := parameter.ParameterValue.Value
	fieldType := strings.ToUpper(defaultString(parameter.ParameterType.Type, "STRING"))
	switch fieldType {
	case "STRING", "BYTES", "NUMERIC", "BIGNUMERIC", "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY", "JSON":
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	case "INTEGER", "INT64":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return "", fmt.Errorf("query parameter %q must be an integer", parameter.Name)
		}
		return value, nil
	case "FLOAT", "FLOAT64":
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return "", fmt.Errorf("query parameter %q must be a number", parameter.Name)
		}
		return value, nil
	case "BOOLEAN", "BOOL":
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return "", fmt.Errorf("query parameter %q must be a boolean", parameter.Name)
		}
		if parsed {
			return "true", nil
		}
		return "false", nil
	default:
		return "", fmt.Errorf("unsupported query parameter type %q", parameter.ParameterType.Type)
	}
}

func parseAggregateSelection(selected string) (aggregateSelection, bool, error) {
	expr := strings.TrimSpace(selected)
	if strings.Contains(expr, ",") {
		return aggregateSelection{}, false, nil
	}
	alias := ""
	for _, marker := range []string{" AS ", " as "} {
		if left, right, ok := strings.Cut(expr, marker); ok {
			expr = strings.TrimSpace(left)
			alias = strings.Trim(strings.TrimSpace(right), "`")
			if alias == "" {
				return aggregateSelection{}, true, fmt.Errorf("aggregate alias is empty")
			}
			break
		}
	}
	upper := strings.ToUpper(expr)
	function := ""
	for _, candidate := range []string{"COUNT", "SUM", "AVG", "MIN", "MAX"} {
		if strings.HasPrefix(upper, candidate+"(") {
			function = candidate
			break
		}
	}
	if function == "" || !strings.HasSuffix(expr, ")") {
		if strings.Contains(upper, "(") || strings.Contains(upper, ")") {
			return aggregateSelection{}, true, fmt.Errorf("unsupported aggregate expression")
		}
		return aggregateSelection{}, false, nil
	}
	field := strings.TrimSpace(expr[len(function)+1 : len(expr)-1])
	field = strings.Trim(field, "`")
	if field == "" {
		return aggregateSelection{}, true, fmt.Errorf("%s requires a field or *", function)
	}
	if field == "*" && function != "COUNT" {
		return aggregateSelection{}, true, fmt.Errorf("%s requires a field", function)
	}
	return aggregateSelection{
		Function: function,
		Field:    field,
		Alias:    alias,
	}, true, nil
}

func parseGroupedAggregateSelection(selected string) (string, aggregateSelection, bool, error) {
	parts := strings.Split(selected, ",")
	if len(parts) != 2 {
		return "", aggregateSelection{}, false, nil
	}
	var groupField string
	var aggregate aggregateSelection
	aggregateSeen := false
	groupFieldCount := 0
	for _, part := range parts {
		expr := strings.TrimSpace(part)
		parsedAggregate, ok, err := parseAggregateSelection(expr)
		if err != nil {
			return "", aggregateSelection{}, true, err
		}
		if ok {
			if aggregate.Function != "" {
				return "", aggregateSelection{}, true, fmt.Errorf("GROUP BY supports one aggregate expression")
			}
			aggregate = parsedAggregate
			aggregateSeen = true
			continue
		}
		if strings.ContainsAny(expr, "()") {
			return "", aggregateSelection{}, true, fmt.Errorf("unsupported grouped SELECT expression")
		}
		if groupFieldCount > 0 && aggregateSeen {
			return "", aggregateSelection{}, true, fmt.Errorf("GROUP BY supports one selected field")
		}
		groupField = strings.Trim(expr, "`")
		groupFieldCount++
	}
	if !aggregateSeen {
		return "", aggregateSelection{}, false, nil
	}
	if groupFieldCount != 1 {
		return "", aggregateSelection{}, true, fmt.Errorf("GROUP BY supports one selected field")
	}
	if groupField == "" || aggregate.Function == "" {
		return "", aggregateSelection{}, false, nil
	}
	return groupField, aggregate, true, nil
}

func aggregateField(schema tableSchema, aggregate aggregateSelection) (tableFieldSchema, bool) {
	if aggregate.Field == "*" {
		return tableFieldSchema{}, false
	}
	for _, field := range schema.Fields {
		if field.Name == aggregate.Field {
			return field, true
		}
	}
	return tableFieldSchema{}, false
}

func aggregateDryRunFields(schema tableSchema, aggregate aggregateSelection) ([]tableFieldSchema, error) {
	fieldName := aggregate.Alias
	if fieldName == "" {
		fieldName = "f0_"
	}
	if aggregate.Field == "*" {
		if aggregate.Function != "COUNT" {
			return nil, fmt.Errorf("%s requires a field", aggregate.Function)
		}
		return []tableFieldSchema{{Name: fieldName, Type: "INTEGER", Mode: "NULLABLE"}}, nil
	}
	field, ok := aggregateField(schema, aggregate)
	if !ok {
		return nil, fmt.Errorf("aggregate field %q does not exist", aggregate.Field)
	}
	switch aggregate.Function {
	case "COUNT":
		return []tableFieldSchema{{Name: fieldName, Type: "INTEGER", Mode: "NULLABLE"}}, nil
	case "SUM":
		if !isNumericField(field) {
			return nil, fmt.Errorf("SUM requires a numeric field")
		}
		return []tableFieldSchema{{Name: fieldName, Type: aggregateNumericType(field), Mode: "NULLABLE"}}, nil
	case "AVG":
		if !isNumericField(field) {
			return nil, fmt.Errorf("AVG requires a numeric field")
		}
		return []tableFieldSchema{{Name: fieldName, Type: "FLOAT", Mode: "NULLABLE"}}, nil
	case "MIN", "MAX":
		return []tableFieldSchema{{Name: fieldName, Type: defaultString(field.Type, "STRING"), Mode: "NULLABLE"}}, nil
	default:
		return nil, fmt.Errorf("unsupported aggregate function")
	}
}

func groupedAggregateDryRunFields(schema tableSchema, query simpleSelectQuery) ([]tableFieldSchema, error) {
	groupFields, err := fieldsForQuery(schema, []string{query.GroupBy})
	if err != nil {
		return nil, err
	}
	aggregateFields, err := aggregateDryRunFields(schema, query.Aggregate)
	if err != nil {
		return nil, err
	}
	return append(groupFields, aggregateFields...), nil
}

func nextQueryToken(value string) (string, string) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "`") {
		end := strings.Index(value[1:], "`")
		if end >= 0 {
			tokenEnd := end + 2
			return value[:tokenEnd], strings.TrimSpace(value[tokenEnd:])
		}
	}
	index := strings.IndexFunc(value, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' })
	if index < 0 {
		return value, ""
	}
	return value[:index], strings.TrimSpace(value[index:])
}

func parseTableIdentifier(identifier string, defaultProjectID string) (string, string, string, error) {
	trimmed := strings.Trim(strings.TrimSpace(identifier), "`")
	parts := strings.Split(trimmed, ".")
	if len(parts) == 2 {
		return defaultString(defaultProjectID, "devcloud"), parts[0], parts[1], nil
	}
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2], nil
	}
	return "", "", "", fmt.Errorf("FROM table must be dataset.table or project.dataset.table")
}

func parseSelectedFields(selected string) []string {
	if strings.TrimSpace(selected) == "*" {
		return []string{"*"}
	}
	parts := strings.Split(selected, ",")
	fields := make([]string, 0, len(parts))
	for _, part := range parts {
		field := strings.Trim(strings.TrimSpace(part), "`")
		if field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

func parseSimpleConditions(condition string) ([]whereCondition, error) {
	parts := splitANDConditions(condition)
	if len(parts) == 0 {
		return nil, fmt.Errorf("WHERE condition is required")
	}
	conditions := make([]whereCondition, 0, len(parts))
	for _, part := range parts {
		field, op, value, err := parseSimpleCondition(part)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, whereCondition{
			Field:    field,
			Operator: op,
			ValueRaw: value,
		})
	}
	return conditions, nil
}

func parseSimpleConditionGroups(condition string) ([][]whereCondition, error) {
	groups := splitORConditionGroups(condition)
	if len(groups) == 0 {
		return nil, fmt.Errorf("WHERE condition is required")
	}
	conditionGroups := make([][]whereCondition, 0, len(groups))
	for _, group := range groups {
		conditions, err := parseSimpleConditions(group)
		if err != nil {
			return nil, err
		}
		conditionGroups = append(conditionGroups, conditions)
	}
	return conditionGroups, nil
}

func flattenWhereConditionGroups(groups [][]whereCondition) []whereCondition {
	size := 0
	for _, group := range groups {
		size += len(group)
	}
	conditions := make([]whereCondition, 0, size)
	for _, group := range groups {
		conditions = append(conditions, group...)
	}
	return conditions
}

func splitORConditionGroups(condition string) []string {
	parts := strings.Fields(condition)
	if len(parts) == 0 {
		return nil
	}
	groups := make([]string, 0, 1)
	var current []string
	for _, part := range parts {
		if strings.EqualFold(part, "OR") {
			if len(current) == 0 {
				return nil
			}
			groups = append(groups, strings.Join(current, " "))
			current = nil
			continue
		}
		current = append(current, part)
	}
	if len(current) == 0 {
		return nil
	}
	groups = append(groups, strings.Join(current, " "))
	return groups
}

func splitANDConditions(condition string) []string {
	parts := strings.Fields(condition)
	if len(parts) == 0 {
		return nil
	}
	conditions := make([]string, 0, 1)
	var current []string
	for _, part := range parts {
		if strings.EqualFold(part, "AND") {
			if len(current) == 0 {
				return nil
			}
			conditions = append(conditions, strings.Join(current, " "))
			current = nil
			continue
		}
		current = append(current, part)
	}
	if len(current) == 0 {
		return nil
	}
	conditions = append(conditions, strings.Join(current, " "))
	return conditions
}

func parseSimpleCondition(condition string) (string, string, json.RawMessage, error) {
	negated := false
	trimmedCondition := strings.TrimSpace(condition)
	if strings.HasPrefix(strings.ToUpper(trimmedCondition), "NOT ") {
		negated = true
		trimmedCondition = strings.TrimSpace(trimmedCondition[len("NOT "):])
	}
	for _, op := range []string{">=", "<=", "!=", "=", ">", "<"} {
		if idx := strings.Index(trimmedCondition, op); idx >= 0 {
			field := strings.Trim(strings.TrimSpace(trimmedCondition[:idx]), "`")
			value := strings.TrimSpace(trimmedCondition[idx+len(op):])
			if field == "" || value == "" {
				return "", "", nil, fmt.Errorf("WHERE condition must compare a field to a literal")
			}
			raw, err := rawJSONLiteral(value)
			if err != nil {
				return "", "", nil, err
			}
			if negated {
				op = "NOT " + op
			}
			return field, op, raw, nil
		}
	}
	return "", "", nil, fmt.Errorf("WHERE supports simple comparisons only")
}

func rawJSONLiteral(value string) (json.RawMessage, error) {
	trimmed := strings.TrimSpace(value)
	if strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'") && len(trimmed) >= 2 {
		data, _ := json.Marshal(strings.Trim(trimmed, "'"))
		return data, nil
	}
	if strings.HasPrefix(trimmed, `"`) && strings.HasSuffix(trimmed, `"`) {
		var s string
		if err := json.Unmarshal([]byte(trimmed), &s); err != nil {
			return nil, fmt.Errorf("invalid string literal")
		}
		return []byte(trimmed), nil
	}
	switch strings.ToUpper(trimmed) {
	case "TRUE":
		return []byte("true"), nil
	case "FALSE":
		return []byte("false"), nil
	case "NULL":
		return []byte("null"), nil
	}
	var valueAny any
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&valueAny); err != nil {
		return nil, fmt.Errorf("WHERE literal must be a number, boolean, null, or quoted string")
	}
	return []byte(trimmed), nil
}

func fieldsForQuery(schema tableSchema, selected []string) ([]tableFieldSchema, error) {
	if len(selected) == 1 && selected[0] == "*" {
		return schema.Fields, nil
	}
	byName := make(map[string]tableFieldSchema, len(schema.Fields))
	for _, field := range schema.Fields {
		byName[field.Name] = field
	}
	fields := make([]tableFieldSchema, 0, len(selected))
	for _, name := range selected {
		field, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("selected field %q does not exist", name)
		}
		fields = append(fields, field)
	}
	return fields, nil
}
