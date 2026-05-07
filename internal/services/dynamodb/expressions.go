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
	if matched, ok, err := evaluateExistencePredicate(expression, names, candidate); ok {
		return matched, err
	}
	if matched, ok, err := evaluateBinaryFunctionPredicate(expression, names, values, candidate); ok {
		return matched, err
	}
	if attrToken, lowerToken, upperToken, ok := splitBetweenExpression(expression); ok {
		return evaluateBetweenPredicate(attrToken, lowerToken, upperToken, names, values, candidate)
	}
	if attrToken, valueTokens, ok := splitInExpression(expression); ok {
		return evaluateInPredicate(attrToken, valueTokens, names, values, candidate)
	}
	return evaluateComparisonPredicate(expression, names, values, candidate)
}

func evaluateExistencePredicate(expression string, names map[string]string, candidate item) (bool, bool, error) {
	if body, ok := parseFunctionCall(expression, "attribute_exists"); ok {
		attr := resolveAttributeName(strings.TrimSpace(body), names)
		_, present := candidate[attr]
		return present, true, nil
	}
	if body, ok := parseFunctionCall(expression, "attribute_not_exists"); ok {
		attr := resolveAttributeName(strings.TrimSpace(body), names)
		_, present := candidate[attr]
		return !present, true, nil
	}
	return false, false, nil
}

type binaryFunctionPredicate struct {
	name      string
	evaluator func(actual attributeValue, expected attributeValue) bool
}

var binaryFunctionPredicates = []binaryFunctionPredicate{
	{name: "begins_with", evaluator: attributeBeginsWith},
	{name: "contains", evaluator: attributeContains},
	{name: "attribute_type", evaluator: attributeHasType},
}

func evaluateBinaryFunctionPredicate(expression string, names map[string]string, values map[string]attributeValue, candidate item) (bool, bool, error) {
	for _, predicate := range binaryFunctionPredicates {
		body, ok := parseFunctionCall(expression, predicate.name)
		if !ok {
			continue
		}
		args := splitCommaSeparated(body)
		if len(args) != 2 {
			return false, true, fmt.Errorf("invalid %s expression", predicate.name)
		}
		attr := resolveAttributeName(strings.TrimSpace(args[0]), names)
		valueToken := strings.TrimSpace(args[1])
		expected, ok := values[valueToken]
		if !ok {
			return false, true, fmt.Errorf("missing expression attribute value %s", valueToken)
		}
		return predicate.evaluator(candidate[attr], expected), true, nil
	}
	return false, false, nil
}

func evaluateBetweenPredicate(attrToken string, lowerToken string, upperToken string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
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

func evaluateInPredicate(attrToken string, valueTokens []string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
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

func evaluateComparisonPredicate(expression string, names map[string]string, values map[string]attributeValue, candidate item) (bool, error) {
	nameToken, operator, valueToken, ok := splitComparisonExpression(expression)
	if !ok {
		return false, errors.New("unsupported expression predicate")
	}
	if actualSize, isSize, err := evaluateSizeOperand(strings.TrimSpace(nameToken), names, candidate); err != nil {
		return false, err
	} else if isSize {
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
