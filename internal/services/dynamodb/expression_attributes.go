package dynamodb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

func evaluateSizeOperand(expression string, names map[string]string, candidate item) (int, bool, error) {
	body, ok := parseFunctionCall(expression, "size")
	if !ok {
		return 0, false, nil
	}
	attr := resolveAttributeName(strings.TrimSpace(body), names)
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

func attributeHasType(actual attributeValue, expected attributeValue) bool {
	expectedType, ok := expected["S"].(string)
	if !ok {
		return false
	}
	return attributeTypeName(actual) == expectedType
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

func attributeValuesEqual(left attributeValue, right attributeValue) bool {
	if reflect.DeepEqual(left, right) {
		return true
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
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

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
