package redshift

import "strings"

type database struct {
	schemas map[string]*schema
}

type schema struct {
	tables map[string]*table
}

type table struct {
	name      qualifiedName
	columns   []column
	rows      [][]string
	kind      string
	viewSQL   string
	distStyle string
	distKey   string
	sortKeys  []string
}

type column struct {
	name         string
	dataType     string
	encoding     string
	defaultValue string
	identity     bool
	distKey      bool
	sortKey      bool
}

type qualifiedName struct {
	schema string
	table  string
}

func isView(tableState *table) bool {
	return tableState != nil && strings.EqualFold(tableState.kind, "VIEW")
}

func isMaterializedView(tableState *table) bool {
	return tableState != nil && strings.EqualFold(tableState.kind, "MATERIALIZED VIEW")
}

func isReadOnlyRelation(tableState *table) bool {
	return isView(tableState) || isMaterializedView(tableState)
}

func tableSnapshotType(tableState *table) string {
	if isMaterializedView(tableState) {
		return "MATERIALIZED_VIEW"
	}
	if isView(tableState) {
		return "VIEW"
	}
	return "TABLE"
}

func tableDataAPIType(tableState *table) string {
	if isMaterializedView(tableState) {
		return "MATERIALIZED_VIEW"
	}
	if isView(tableState) {
		return "VIEW"
	}
	return "TABLE"
}

func informationSchemaTableType(tableState *table) string {
	if isMaterializedView(tableState) {
		return "MATERIALIZED VIEW"
	}
	if isView(tableState) {
		return "VIEW"
	}
	return "BASE TABLE"
}

func pgClassRelKind(tableState *table) string {
	if isMaterializedView(tableState) {
		return "m"
	}
	if isView(tableState) {
		return "v"
	}
	return "r"
}

func columnsFromFields(fields []pgField) []column {
	columns := make([]column, 0, len(fields))
	for _, field := range fields {
		columns = append(columns, column{name: field.Name, dataType: pgFieldTypeName(field)})
	}
	return columns
}
