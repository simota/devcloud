package dynamodb

import (
	"errors"
	"fmt"
	"reflect"
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
		return compareWithOperator(comparison, operator)
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
	switch operator {
	case "=":
		return reflect.DeepEqual(actual, expected), nil
	case "<>":
		return !reflect.DeepEqual(actual, expected), nil
	}
	return compareWithOperator(compareAttributeValues(actual, expected), operator)
}

func compareWithOperator(comparison int, operator string) (bool, error) {
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
