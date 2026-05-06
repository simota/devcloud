package dynamodb

import (
	"errors"
	"net/http"
	"reflect"
	"strings"
)

func (s *Server) handleExecuteStatement(w http.ResponseWriter, r *http.Request) {
	var request executeStatementRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	statement, err := parsePartiQLSelect(request.Statement, request.Parameters)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tables[statement.tableName]
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "table not found")
		return
	}
	source := sortedItemsForQuery(state, "")
	items := []item{}
	for _, candidate := range source {
		if !partiQLConditionsMatch(candidate.value, statement.conditions) {
			continue
		}
		items = append(items, projectPartiQLItem(candidate.value, statement.projections))
		if request.Limit > 0 && len(items) == request.Limit {
			break
		}
	}
	response := map[string]any{"Items": items}
	addConsumedCapacity(response, statement.tableName, request.ReturnConsumedCapacity)
	writeJSON(w, http.StatusOK, response)
}

type partiQLSelectStatement struct {
	tableName   string
	projections []string
	conditions  []partiQLCondition
}

type partiQLCondition struct {
	attribute string
	value     attributeValue
}

func parsePartiQLSelect(statement string, parameters []attributeValue) (partiQLSelectStatement, error) {
	statement = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	if statement == "" {
		return partiQLSelectStatement{}, errors.New("statement is required")
	}
	upper := strings.ToUpper(statement)
	if !strings.HasPrefix(upper, "SELECT ") {
		return partiQLSelectStatement{}, errors.New("only SELECT statements are supported")
	}
	fromIndex := strings.Index(upper, " FROM ")
	if fromIndex < 0 {
		return partiQLSelectStatement{}, errors.New("SELECT statement must include FROM")
	}
	projectionPart := strings.TrimSpace(statement[len("SELECT "):fromIndex])
	afterFrom := strings.TrimSpace(statement[fromIndex+len(" FROM "):])
	whereIndex := strings.Index(strings.ToUpper(afterFrom), " WHERE ")
	tableName := afterFrom
	wherePart := ""
	if whereIndex >= 0 {
		tableName = strings.TrimSpace(afterFrom[:whereIndex])
		wherePart = strings.TrimSpace(afterFrom[whereIndex+len(" WHERE "):])
	}
	tableName = trimPartiQLIdentifier(tableName)
	if tableName == "" {
		return partiQLSelectStatement{}, errors.New("table name is required")
	}
	projections, err := parsePartiQLProjections(projectionPart)
	if err != nil {
		return partiQLSelectStatement{}, err
	}
	conditions, err := parsePartiQLWhere(wherePart, parameters)
	if err != nil {
		return partiQLSelectStatement{}, err
	}
	return partiQLSelectStatement{
		tableName:   tableName,
		projections: projections,
		conditions:  conditions,
	}, nil
}

func parsePartiQLProjections(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("SELECT projection is required")
	}
	if value == "*" {
		return nil, nil
	}
	projections := []string{}
	for _, token := range strings.Split(value, ",") {
		name := trimPartiQLIdentifier(strings.TrimSpace(token))
		if name == "" {
			return nil, errors.New("invalid SELECT projection")
		}
		projections = append(projections, name)
	}
	return projections, nil
}

func parsePartiQLWhere(value string, parameters []attributeValue) ([]partiQLCondition, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		if len(parameters) > 0 {
			return nil, errors.New("too many PartiQL parameters")
		}
		return nil, nil
	}
	parts := splitPartiQLAnd(value)
	conditions := make([]partiQLCondition, 0, len(parts))
	paramIndex := 0
	for _, part := range parts {
		left, right, ok := strings.Cut(part, "=")
		if !ok {
			return nil, errors.New("WHERE supports equality predicates only")
		}
		attribute := trimPartiQLIdentifier(strings.TrimSpace(left))
		if attribute == "" {
			return nil, errors.New("invalid WHERE attribute")
		}
		right = strings.TrimSpace(right)
		if right != "?" {
			return nil, errors.New("WHERE predicates must use positional parameters")
		}
		if paramIndex >= len(parameters) {
			return nil, errors.New("missing PartiQL parameter")
		}
		conditions = append(conditions, partiQLCondition{
			attribute: attribute,
			value:     cloneAttributeValue(parameters[paramIndex]),
		})
		paramIndex++
	}
	if paramIndex != len(parameters) {
		return nil, errors.New("too many PartiQL parameters")
	}
	return conditions, nil
}

func splitPartiQLAnd(value string) []string {
	fields := strings.Fields(value)
	parts := []string{}
	current := []string{}
	for _, field := range fields {
		if strings.EqualFold(field, "AND") {
			parts = append(parts, strings.Join(current, " "))
			current = nil
			continue
		}
		current = append(current, field)
	}
	if len(current) > 0 {
		parts = append(parts, strings.Join(current, " "))
	}
	return parts
}

func trimPartiQLIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		first := value[0]
		last := value[len(value)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') || (first == '`' && last == '`') {
			return value[1 : len(value)-1]
		}
	}
	return value
}

func partiQLConditionsMatch(value item, conditions []partiQLCondition) bool {
	for _, condition := range conditions {
		actual, ok := value[condition.attribute]
		if !ok || !reflect.DeepEqual(actual, condition.value) {
			return false
		}
	}
	return true
}

func projectPartiQLItem(value item, projections []string) item {
	if len(projections) == 0 {
		return cloneItem(value)
	}
	projected := item{}
	for _, name := range projections {
		if attr, ok := value[name]; ok {
			projected[name] = cloneAttributeValue(attr)
		}
	}
	return projected
}

func partiQLConditionsCoverKey(description tableDescription, conditions []partiQLCondition) bool {
	conditionAttributes := map[string]bool{}
	for _, condition := range conditions {
		conditionAttributes[condition.attribute] = true
	}
	for _, element := range description.KeySchema {
		if !conditionAttributes[element.AttributeName] {
			return false
		}
	}
	return true
}
