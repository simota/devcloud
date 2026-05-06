package sqs

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func validateMessageAttributes(attrs map[string]messageAttributeValue) error {
	for name, attr := range attrs {
		if err := validateMessageAttributeName(name); err != nil {
			return err
		}
		if err := validateMessageAttributeValue(name, attr); err != nil {
			return err
		}
	}
	return nil
}

func validateMessageAttributeName(name string) error {
	if name == "" {
		return errors.New("invalid attribute name: message attribute name is required")
	}
	if len(name) > 256 {
		return errors.New("invalid attribute name: message attribute name must be no longer than 256 characters")
	}
	if strings.HasPrefix(strings.ToLower(name), "aws.") || strings.HasPrefix(strings.ToLower(name), "amazon.") {
		return errors.New("invalid attribute name: message attribute name must not start with AWS. or Amazon.")
	}
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
		return errors.New("invalid attribute name: message attribute name must not start or end with a period or contain consecutive periods")
	}
	for _, r := range name {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			continue
		}
		return errors.New("invalid attribute name: message attribute name contains unsupported characters")
	}
	return nil
}

func validateMessageSystemAttributes(attrs map[string]messageAttributeValue) error {
	for name, attr := range attrs {
		if name != "AWSTraceHeader" {
			return fmt.Errorf("invalid attribute value: unsupported message system attribute %s", name)
		}
		if err := validateMessageAttributeValue(name, attr); err != nil {
			return err
		}
	}
	return nil
}

func validateMessageAttributeValue(name string, attr messageAttributeValue) error {
	if strings.TrimSpace(attr.DataType) == "" {
		return fmt.Errorf("invalid attribute value for %s: DataType is required", name)
	}
	dataType := strings.ToLower(attr.DataType)
	if isUnsupportedMessageAttributeListType(dataType) {
		return fmt.Errorf("invalid attribute value for %s: list DataType is not supported", name)
	}
	switch {
	case strings.HasPrefix(dataType, "string"):
		return nil
	case strings.HasPrefix(dataType, "number"):
		if _, err := strconv.ParseFloat(attr.StringValue, 64); err != nil {
			return fmt.Errorf("invalid attribute value for %s: Number attributes must be numeric", name)
		}
		return nil
	case strings.HasPrefix(dataType, "binary"):
		if attr.BinaryValue == "" {
			return fmt.Errorf("invalid attribute value for %s: BinaryValue is required", name)
		}
		if _, err := base64.StdEncoding.DecodeString(attr.BinaryValue); err != nil {
			return fmt.Errorf("invalid attribute value for %s: BinaryValue must be base64", name)
		}
		return nil
	default:
		return fmt.Errorf("invalid attribute value for %s: unsupported DataType", name)
	}
}

func isUnsupportedMessageAttributeListType(dataType string) bool {
	return dataType == "string.list" || strings.HasPrefix(dataType, "string.list.") ||
		dataType == "binary.list" || strings.HasPrefix(dataType, "binary.list.")
}

func validMessageBody(body string) bool {
	for _, r := range body {
		if r == '\uFFFD' {
			return false
		}
		if r == '\t' || r == '\n' || r == '\r' {
			continue
		}
		if r >= 0x20 && r <= 0xD7FF {
			continue
		}
		if r >= 0xE000 && r <= 0xFFFD {
			continue
		}
		if r >= 0x10000 && r <= 0x10FFFF {
			continue
		}
		return false
	}
	return true
}

func validBatchEntryID(id string) bool {
	return batchEntryIDPattern.MatchString(id)
}
