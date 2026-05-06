package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"
)

func matchKeyCondition(expression string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
	parts, err := splitConjunctivePredicates(expression)
	if err != nil {
		return false, err
	}
	for _, part := range parts {
		matched, err := matchPredicate(strings.TrimSpace(part), names, values, candidate)
		if err != nil || !matched {
			return matched, err
		}
	}
	return true, nil
}

func matchFilter(expression string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return true, nil
	}
	return matchConjunctiveExpression(expression, names, values, candidate)
}

func matchConjunctiveExpression(expression string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
	disjuncts, err := splitDisjunctivePredicates(expression)
	if err != nil {
		return false, err
	}
	for _, disjunct := range disjuncts {
		parts, err := splitConjunctivePredicates(disjunct)
		if err != nil {
			return false, err
		}
		matchedAll := true
		for _, part := range parts {
			matched, err := matchPredicate(strings.TrimSpace(part), names, values, candidate)
			if err != nil {
				return false, err
			}
			if !matched {
				matchedAll = false
				break
			}
		}
		if matchedAll {
			return true, nil
		}
	}
	return false, nil
}

func matchPredicate(expression string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
	if strings.HasPrefix(strings.ToUpper(expression), "NOT ") {
		matched, err := matchPredicate(strings.TrimSpace(expression[len("NOT "):]), names, values, candidate)
		if err != nil {
			return false, err
		}
		return !matched, nil
	}
	if strings.HasPrefix(expression, "attribute_exists(") && strings.HasSuffix(expression, ")") {
		attr := resolveAttributeName(strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(expression, "attribute_exists("), ")")), names)
		_, ok := candidate[attr]
		return ok, nil
	}
	if strings.HasPrefix(expression, "attribute_not_exists(") && strings.HasSuffix(expression, ")") {
		attr := resolveAttributeName(strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(expression, "attribute_not_exists("), ")")), names)
		_, ok := candidate[attr]
		return !ok, nil
	}
	if strings.HasPrefix(expression, "begins_with(") && strings.HasSuffix(expression, ")") {
		args := strings.Split(strings.TrimSuffix(strings.TrimPrefix(expression, "begins_with("), ")"), ",")
		if len(args) != 2 {
			return false, errors.New("invalid begins_with expression")
		}
		attr := resolveAttributeName(strings.TrimSpace(args[0]), names)
		expected, ok := values[strings.TrimSpace(args[1])]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", strings.TrimSpace(args[1]))
		}
		return attributeBeginsWith(candidate[attr], expected), nil
	}
	if strings.HasPrefix(expression, "contains(") && strings.HasSuffix(expression, ")") {
		args := splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(expression, "contains("), ")"))
		if len(args) != 2 {
			return false, errors.New("invalid contains expression")
		}
		attr := resolveAttributeName(strings.TrimSpace(args[0]), names)
		expected, ok := values[strings.TrimSpace(args[1])]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", strings.TrimSpace(args[1]))
		}
		return attributeContains(candidate[attr], expected), nil
	}
	if strings.HasPrefix(expression, "attribute_type(") && strings.HasSuffix(expression, ")") {
		args := splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(expression, "attribute_type("), ")"))
		if len(args) != 2 {
			return false, errors.New("invalid attribute_type expression")
		}
		attr := resolveAttributeName(strings.TrimSpace(args[0]), names)
		expected, ok := values[strings.TrimSpace(args[1])]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", strings.TrimSpace(args[1]))
		}
		return attributeHasType(candidate[attr], expected), nil
	}
	if attrToken, lowerToken, upperToken, ok := splitBetweenExpression(expression); ok {
		attr := resolveAttributeName(strings.TrimSpace(attrToken), names)
		lower, ok := values[strings.TrimSpace(lowerToken)]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", strings.TrimSpace(lowerToken))
		}
		upper, ok := values[strings.TrimSpace(upperToken)]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", strings.TrimSpace(upperToken))
		}
		actual, ok := candidate[attr]
		if !ok {
			return false, nil
		}
		return compareAttributeValues(actual, lower) >= 0 && compareAttributeValues(actual, upper) <= 0, nil
	}
	if attrToken, valueTokens, ok := splitInExpression(expression); ok {
		attr := resolveAttributeName(strings.TrimSpace(attrToken), names)
		actual, ok := candidate[attr]
		if !ok {
			return false, nil
		}
		if len(valueTokens) == 0 {
			return false, errors.New("IN expression requires at least one value")
		}
		for _, valueToken := range valueTokens {
			valueToken = strings.TrimSpace(valueToken)
			expected, ok := values[valueToken]
			if !ok {
				return false, fmt.Errorf("missing expression attribute value %s", valueToken)
			}
			if attributeValuesEqual(actual, expected) {
				return true, nil
			}
		}
		return false, nil
	}
	nameToken, operator, valueToken, ok := splitComparisonExpression(expression)
	if !ok {
		return false, errors.New("unsupported expression predicate")
	}
	if actualSize, ok, err := evaluateSizeOperand(strings.TrimSpace(nameToken), names, candidate); err != nil {
		return false, err
	} else if ok {
		valueToken = strings.TrimSpace(valueToken)
		expected, ok := values[valueToken]
		if !ok {
			return false, fmt.Errorf("missing expression attribute value %s", valueToken)
		}
		comparison := compareAttributeValues(attributeValue{"N": fmt.Sprintf("%d", actualSize)}, expected)
		switch operator {
		case "=":
			return comparison == 0, nil
		case "<>":
			return comparison != 0, nil
		case "<":
			return comparison < 0, nil
		case "<=":
			return comparison <= 0, nil
		case ">":
			return comparison > 0, nil
		case ">=":
			return comparison >= 0, nil
		default:
			return false, fmt.Errorf("unsupported comparison operator %s", operator)
		}
	}
	attr := resolveAttributeName(strings.TrimSpace(nameToken), names)
	valueToken = strings.TrimSpace(valueToken)
	expected, ok := values[valueToken]
	if !ok {
		return false, fmt.Errorf("missing expression attribute value %s", valueToken)
	}
	actual, ok := candidate[attr]
	if !ok {
		return false, nil
	}
	comparison := compareAttributeValues(actual, expected)
	switch operator {
	case "=":
		return reflect.DeepEqual(actual, expected), nil
	case "<>":
		return !reflect.DeepEqual(actual, expected), nil
	case "<":
		return comparison < 0, nil
	case "<=":
		return comparison <= 0, nil
	case ">":
		return comparison > 0, nil
	case ">=":
		return comparison >= 0, nil
	default:
		return false, fmt.Errorf("unsupported comparison operator %s", operator)
	}
}

func splitDisjunctivePredicates(expression string) ([]string, error) {
	fields := strings.Fields(expression)
	if len(fields) == 0 {
		return nil, errors.New("empty expression")
	}
	parts := []string{}
	var current []string
	for _, field := range fields {
		if strings.ToUpper(field) == "OR" {
			if len(current) == 0 {
				return nil, errors.New("invalid OR expression")
			}
			parts = append(parts, strings.Join(current, " "))
			current = nil
			continue
		}
		current = append(current, field)
	}
	if len(current) == 0 {
		return nil, errors.New("invalid OR expression")
	}
	parts = append(parts, strings.Join(current, " "))
	return parts, nil
}

func splitConjunctivePredicates(expression string) ([]string, error) {
	fields := strings.Fields(expression)
	if len(fields) == 0 {
		return nil, errors.New("empty expression")
	}
	parts := []string{}
	var current []string
	betweenNeedsAnd := false
	for _, field := range fields {
		upper := strings.ToUpper(field)
		if upper == "BETWEEN" {
			betweenNeedsAnd = true
			current = append(current, field)
			continue
		}
		if upper == "AND" && !betweenNeedsAnd {
			if len(current) == 0 {
				return nil, errors.New("invalid AND expression")
			}
			parts = append(parts, strings.Join(current, " "))
			current = nil
			continue
		}
		if upper == "AND" && betweenNeedsAnd {
			betweenNeedsAnd = false
		}
		current = append(current, field)
	}
	if len(current) == 0 {
		return nil, errors.New("invalid AND expression")
	}
	parts = append(parts, strings.Join(current, " "))
	return parts, nil
}

func splitBetweenExpression(expression string) (attr string, lower string, upper string, ok bool) {
	fields := strings.Fields(expression)
	if len(fields) != 5 || strings.ToUpper(fields[1]) != "BETWEEN" || strings.ToUpper(fields[3]) != "AND" {
		return "", "", "", false
	}
	return fields[0], fields[2], fields[4], true
}

func splitInExpression(expression string) (attr string, values []string, ok bool) {
	left, right, found := strings.Cut(expression, " IN ")
	if !found {
		left, right, found = strings.Cut(expression, " in ")
	}
	if !found {
		return "", nil, false
	}
	right = strings.TrimSpace(right)
	if !strings.HasPrefix(right, "(") || !strings.HasSuffix(right, ")") {
		return "", nil, false
	}
	return strings.TrimSpace(left), splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(right, "("), ")")), true
}

func splitComparisonExpression(expression string) (left string, operator string, right string, ok bool) {
	for _, op := range []string{"<=", ">=", "<>", "=", "<", ">"} {
		if left, right, ok := strings.Cut(expression, op); ok {
			return left, op, right, true
		}
	}
	return "", "", "", false
}

func attributeHasType(actual attributeValue, expected attributeValue) bool {
	expectedType, ok := expected["S"].(string)
	if !ok {
		return false
	}
	return attributeTypeName(actual) == expectedType
}

func evaluateSizeOperand(expression string, names map[string]string, candidate item) (int, bool, error) {
	if !strings.HasPrefix(expression, "size(") || !strings.HasSuffix(expression, ")") {
		return 0, false, nil
	}
	attr := resolveAttributeName(strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(expression, "size("), ")")), names)
	value, ok := candidate[attr]
	if !ok {
		return 0, true, nil
	}
	size, ok := attributeSize(value)
	if !ok {
		return 0, true, fmt.Errorf("size is not supported for attribute %s", attr)
	}
	return size, true, nil
}

func attributeSize(value attributeValue) (int, bool) {
	if text, ok := value["S"].(string); ok {
		return len(text), true
	}
	if binary, ok := value["B"].(string); ok {
		return len(binary), true
	}
	for _, setType := range []string{"SS", "NS", "BS"} {
		if values, ok := stringSliceAttribute(value, setType); ok {
			return len(values), true
		}
	}
	if entries := attributeValueList(value["L"]); entries != nil {
		return len(entries), true
	}
	if rawMap, ok := value["M"]; ok {
		switch values := rawMap.(type) {
		case map[string]attributeValue:
			return len(values), true
		case map[string]any:
			return len(values), true
		}
	}
	return 0, false
}

func attributeBeginsWith(actual attributeValue, expected attributeValue) bool {
	actualString, ok := actual["S"].(string)
	if !ok {
		return false
	}
	expectedString, ok := expected["S"].(string)
	if !ok {
		return false
	}
	return strings.HasPrefix(actualString, expectedString)
}

func attributeContains(actual attributeValue, expected attributeValue) bool {
	actualString, ok := actual["S"].(string)
	if ok {
		expectedString, ok := expected["S"].(string)
		return ok && strings.Contains(actualString, expectedString)
	}
	for _, setType := range []string{"SS", "NS", "BS"} {
		actualValues, ok := stringSliceAttribute(actual, setType)
		if !ok {
			continue
		}
		expectedValues, ok := stringSliceAttribute(expected, setType)
		if ok && len(expectedValues) == 1 {
			return stringSliceContains(actualValues, expectedValues[0])
		}
		if scalar, ok := expected[setElementScalarType(setType)].(string); ok {
			return stringSliceContains(actualValues, scalar)
		}
		return false
	}
	if rawList, ok := actual["L"]; ok {
		for _, entry := range attributeValueList(rawList) {
			if attributeValuesEqual(entry, expected) {
				return true
			}
		}
	}
	return false
}

func setElementScalarType(setType string) string {
	switch setType {
	case "SS":
		return "S"
	case "NS":
		return "N"
	case "BS":
		return "B"
	default:
		return ""
	}
}

func attributeValueList(raw any) []attributeValue {
	switch values := raw.(type) {
	case []attributeValue:
		return append([]attributeValue(nil), values...)
	case []any:
		result := make([]attributeValue, 0, len(values))
		for _, entry := range values {
			if value, ok := entry.(map[string]any); ok {
				result = append(result, attributeValue(value))
			}
		}
		return result
	default:
		return nil
	}
}

func attributeValuesEqual(left attributeValue, right attributeValue) bool {
	if reflect.DeepEqual(left, right) {
		return true
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func checkCondition(expression string, names map[string]string, values map[string]attributeValue, existing item, existed bool) error {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil
	}
	candidate := existing
	if !existed {
		candidate = item{}
	}
	matched, err := matchConjunctiveExpression(expression, names, values, candidate)
	if err != nil {
		return err
	}
	if !matched {
		return errors.New("condition check failed")
	}
	return nil
}

func applyUpdateExpression(target item, expression string, names map[string]string, values map[string]attributeValue) error {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return errors.New("update expression is required")
	}
	clauses, err := splitUpdateClauses(expression)
	if err != nil {
		return err
	}
	for _, clause := range clauses {
		switch clause.keyword {
		case "SET":
			assignments := splitCommaSeparated(clause.body)
			for _, assignment := range assignments {
				nameToken, valueToken, ok := strings.Cut(strings.TrimSpace(assignment), "=")
				if !ok {
					return errors.New("invalid SET assignment")
				}
				attr := resolveAttributeName(strings.TrimSpace(nameToken), names)
				value, err := evaluateUpdateValue(target, strings.TrimSpace(valueToken), names, values)
				if err != nil {
					return err
				}
				target[attr] = value
			}
		case "REMOVE":
			removals := splitCommaSeparated(clause.body)
			for _, removal := range removals {
				attr := resolveAttributeName(strings.TrimSpace(removal), names)
				if attr == "" {
					return errors.New("invalid REMOVE path")
				}
				delete(target, attr)
			}
		case "ADD":
			additions := splitCommaSeparated(clause.body)
			for _, addition := range additions {
				fields := strings.Fields(strings.TrimSpace(addition))
				if len(fields) != 2 {
					return errors.New("invalid ADD assignment")
				}
				attr := resolveAttributeName(fields[0], names)
				if attr == "" {
					return errors.New("invalid ADD path")
				}
				value, ok := values[fields[1]]
				if !ok {
					return fmt.Errorf("missing expression attribute value %s", fields[1])
				}
				updated, err := addAttributeValue(target[attr], value)
				if err != nil {
					return err
				}
				target[attr] = updated
			}
		case "DELETE":
			deletions := splitCommaSeparated(clause.body)
			for _, deletion := range deletions {
				fields := strings.Fields(strings.TrimSpace(deletion))
				if len(fields) != 2 {
					return errors.New("invalid DELETE assignment")
				}
				attr := resolveAttributeName(fields[0], names)
				if attr == "" {
					return errors.New("invalid DELETE path")
				}
				value, ok := values[fields[1]]
				if !ok {
					return fmt.Errorf("missing expression attribute value %s", fields[1])
				}
				updated, remove, err := deleteAttributeValue(target[attr], value)
				if err != nil {
					return err
				}
				if remove {
					delete(target, attr)
				} else if updated != nil {
					target[attr] = updated
				}
			}
		default:
			return fmt.Errorf("unsupported update expression clause %s", clause.keyword)
		}
	}
	return nil
}

func evaluateUpdateValue(target item, expression string, names map[string]string, values map[string]attributeValue) (attributeValue, error) {
	if left, operator, right, ok := splitArithmeticUpdateExpression(expression); ok {
		leftValue, err := evaluateUpdateValue(target, left, names, values)
		if err != nil {
			return nil, err
		}
		rightValue, err := evaluateUpdateValue(target, right, names, values)
		if err != nil {
			return nil, err
		}
		if operator == "-" {
			rightValue, err = negateNumberAttribute(rightValue)
			if err != nil {
				return nil, err
			}
		}
		return addAttributeValue(leftValue, rightValue)
	}
	if strings.HasPrefix(expression, "if_not_exists(") && strings.HasSuffix(expression, ")") {
		args := splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(expression, "if_not_exists("), ")"))
		if len(args) != 2 {
			return nil, errors.New("invalid if_not_exists expression")
		}
		attr := resolveAttributeName(strings.TrimSpace(args[0]), names)
		if current, ok := target[attr]; ok {
			return cloneAttributeValue(current), nil
		}
		return evaluateUpdateValue(target, strings.TrimSpace(args[1]), names, values)
	}
	if strings.HasPrefix(expression, "list_append(") && strings.HasSuffix(expression, ")") {
		args := splitCommaSeparated(strings.TrimSuffix(strings.TrimPrefix(expression, "list_append("), ")"))
		if len(args) != 2 {
			return nil, errors.New("invalid list_append expression")
		}
		leftValue, err := evaluateUpdateValue(target, strings.TrimSpace(args[0]), names, values)
		if err != nil {
			return nil, err
		}
		rightValue, err := evaluateUpdateValue(target, strings.TrimSpace(args[1]), names, values)
		if err != nil {
			return nil, err
		}
		return appendListAttributeValues(leftValue, rightValue)
	}
	if value, ok := values[expression]; ok {
		return cloneAttributeValue(value), nil
	}
	attr := resolveAttributeName(expression, names)
	if current, ok := target[attr]; ok {
		return cloneAttributeValue(current), nil
	}
	return nil, fmt.Errorf("missing expression attribute value %s", expression)
}

func splitArithmeticUpdateExpression(expression string) (left string, operator string, right string, ok bool) {
	depth := 0
	for index, char := range expression {
		switch char {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '+', '-':
			if depth == 0 {
				return strings.TrimSpace(expression[:index]), string(char), strings.TrimSpace(expression[index+1:]), true
			}
		}
	}
	return "", "", "", false
}

func appendListAttributeValues(left attributeValue, right attributeValue) (attributeValue, error) {
	leftEntries := attributeValueList(left["L"])
	rightEntries := attributeValueList(right["L"])
	if leftEntries == nil || rightEntries == nil {
		return nil, errors.New("list_append requires list attributes")
	}
	combined := make([]any, 0, len(leftEntries)+len(rightEntries))
	for _, entry := range leftEntries {
		combined = append(combined, map[string]any(cloneAttributeValue(entry)))
	}
	for _, entry := range rightEntries {
		combined = append(combined, map[string]any(cloneAttributeValue(entry)))
	}
	return attributeValue{"L": combined}, nil
}

func splitCommaSeparated(value string) []string {
	parts := []string{}
	depth := 0
	start := 0
	for index, char := range value {
		switch char {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(value[start:index]))
				start = index + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(value[start:]))
	return parts
}

type updateClause struct {
	keyword string
	body    string
}

func splitUpdateClauses(expression string) ([]updateClause, error) {
	type clauseStart struct {
		keyword string
		index   int
	}
	upper := strings.ToUpper(expression)
	starts := []clauseStart{}
	for _, keyword := range []string{"SET", "REMOVE", "ADD", "DELETE"} {
		for offset := 0; offset < len(upper); {
			index := strings.Index(upper[offset:], keyword)
			if index < 0 {
				break
			}
			absolute := offset + index
			if isUpdateClauseBoundary(upper, absolute, len(keyword)) {
				starts = append(starts, clauseStart{keyword: keyword, index: absolute})
			}
			offset = absolute + len(keyword)
		}
	}
	if len(starts) == 0 {
		return nil, errors.New("update expression must include SET, REMOVE, ADD, or DELETE")
	}
	sort.Slice(starts, func(i, j int) bool {
		return starts[i].index < starts[j].index
	})
	clauses := make([]updateClause, 0, len(starts))
	for i, start := range starts {
		next := len(expression)
		if i+1 < len(starts) {
			next = starts[i+1].index
		}
		body := strings.TrimSpace(expression[start.index+len(start.keyword) : next])
		if body == "" {
			return nil, fmt.Errorf("%s update expression clause is empty", start.keyword)
		}
		if start.keyword != "SET" && start.keyword != "REMOVE" && start.keyword != "ADD" && start.keyword != "DELETE" {
			return nil, fmt.Errorf("unsupported update expression clause %s", start.keyword)
		}
		clauses = append(clauses, updateClause{keyword: start.keyword, body: body})
	}
	return clauses, nil
}

func addAttributeValue(current attributeValue, increment attributeValue) (attributeValue, error) {
	if number, ok := increment["N"].(string); ok {
		if current == nil {
			return cloneAttributeValue(increment), nil
		}
		currentNumber, ok := current["N"].(string)
		if !ok {
			return nil, errors.New("ADD number requires existing number attribute")
		}
		sum, err := addNumberStrings(currentNumber, number)
		if err != nil {
			return nil, err
		}
		return attributeValue{"N": sum}, nil
	}
	for _, setType := range []string{"SS", "NS", "BS"} {
		valuesToAdd, ok := stringSliceAttribute(increment, setType)
		if !ok {
			continue
		}
		if current == nil {
			return cloneAttributeValue(increment), nil
		}
		currentValues, ok := stringSliceAttribute(current, setType)
		if !ok {
			return nil, fmt.Errorf("ADD %s requires existing %s attribute", setType, setType)
		}
		return attributeValue{setType: unionStrings(currentValues, valuesToAdd)}, nil
	}
	return nil, errors.New("ADD supports N, SS, NS, and BS values")
}

func negateNumberAttribute(value attributeValue) (attributeValue, error) {
	number, ok := value["N"].(string)
	if !ok {
		return nil, errors.New("subtraction requires number attributes")
	}
	parsed, ok := new(big.Rat).SetString(number)
	if !ok {
		return nil, fmt.Errorf("invalid number %q", number)
	}
	negated := new(big.Rat).Neg(parsed)
	if negated.IsInt() {
		return attributeValue{"N": negated.Num().String()}, nil
	}
	precision := decimalPlaces(number)
	if precision < 1 {
		precision = 1
	}
	return attributeValue{"N": strings.TrimRight(strings.TrimRight(negated.FloatString(precision), "0"), ".")}, nil
}

func deleteAttributeValue(current attributeValue, decrement attributeValue) (attributeValue, bool, error) {
	for _, setType := range []string{"SS", "NS", "BS"} {
		valuesToDelete, ok := stringSliceAttribute(decrement, setType)
		if !ok {
			continue
		}
		if len(valuesToDelete) == 0 {
			return nil, false, errors.New("DELETE set value must not be empty")
		}
		if current == nil {
			return nil, false, nil
		}
		currentValues, ok := stringSliceAttribute(current, setType)
		if !ok {
			return nil, false, fmt.Errorf("DELETE %s requires existing %s attribute", setType, setType)
		}
		remaining := subtractStrings(currentValues, valuesToDelete)
		if len(remaining) == 0 {
			return nil, true, nil
		}
		return attributeValue{setType: remaining}, false, nil
	}
	return nil, false, errors.New("DELETE supports SS, NS, and BS values")
}

func addNumberStrings(left string, right string) (string, error) {
	leftNumber, ok := new(big.Rat).SetString(left)
	if !ok {
		return "", fmt.Errorf("invalid number %q", left)
	}
	rightNumber, ok := new(big.Rat).SetString(right)
	if !ok {
		return "", fmt.Errorf("invalid number %q", right)
	}
	sum := new(big.Rat).Add(leftNumber, rightNumber)
	if sum.IsInt() {
		return sum.Num().String(), nil
	}
	precision := maxInt(decimalPlaces(left), decimalPlaces(right))
	if precision < 1 {
		precision = 1
	}
	return strings.TrimRight(strings.TrimRight(sum.FloatString(precision), "0"), "."), nil
}

func decimalPlaces(value string) int {
	if index := strings.IndexByte(value, '.'); index >= 0 {
		return len(value) - index - 1
	}
	return 0
}

func stringSliceAttribute(value attributeValue, key string) ([]string, bool) {
	raw, ok := value[key]
	if !ok {
		return nil, false
	}
	switch values := raw.(type) {
	case []string:
		return append([]string(nil), values...), true
	case []any:
		result := make([]string, 0, len(values))
		for _, entry := range values {
			text, ok := entry.(string)
			if !ok {
				return nil, false
			}
			result = append(result, text)
		}
		return result, true
	default:
		return nil, false
	}
}

func unionStrings(left []string, right []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(left)+len(right))
	for _, value := range append(append([]string(nil), left...), right...) {
		if seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}

func subtractStrings(left []string, right []string) []string {
	remove := map[string]bool{}
	for _, value := range right {
		remove[value] = true
	}
	result := make([]string, 0, len(left))
	for _, value := range left {
		if !remove[value] {
			result = append(result, value)
		}
	}
	return result
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func isUpdateClauseBoundary(expression string, index int, keywordLength int) bool {
	beforeOK := index == 0 || expression[index-1] == ' '
	after := index + keywordLength
	afterOK := after < len(expression) && expression[after] == ' '
	return beforeOK && afterOK
}

func resolveAttributeName(token string, names map[string]string) string {
	if strings.HasPrefix(token, "#") {
		if value, ok := names[token]; ok {
			return value
		}
	}
	return token
}
