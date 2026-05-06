package redshift

import (
	"strconv"
	"strings"
)

func inferLiteralPGType(value string) (int32, int16) {
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return pgTypeInt4OID, 4
	}
	if strings.EqualFold(value, "true") || strings.EqualFold(value, "false") {
		return pgTypeBoolOID, 1
	}
	if strings.ContainsAny(value, ".eE") {
		if _, err := strconv.ParseFloat(value, 64); err == nil {
			return pgTypeFloat8OID, 8
		}
	}
	return pgTypeVarcharOID, -1
}

func pgTypeOID(dataType string) int32 {
	normalized := strings.ToLower(dataType)
	if strings.Contains(normalized, "int") {
		return pgTypeInt4OID
	}
	if normalized == "bool" || normalized == "boolean" {
		return pgTypeBoolOID
	}
	if strings.Contains(normalized, "double") || strings.Contains(normalized, "float") || normalized == "real" {
		return pgTypeFloat8OID
	}
	return pgTypeVarcharOID
}

func pgTypeSize(dataType string) int16 {
	switch pgTypeOID(dataType) {
	case pgTypeInt4OID:
		return 4
	case pgTypeBoolOID:
		return 1
	case pgTypeFloat8OID:
		return 8
	}
	return -1
}
