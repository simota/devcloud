package dynamodb

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

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
