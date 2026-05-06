package redshift

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func isCatalogSelect(lower string) bool {
	return strings.HasPrefix(lower, "select ") &&
		(strings.Contains(lower, " information_schema.") ||
			strings.Contains(lower, " pg_catalog.") ||
			strings.Contains(lower, " svv_") ||
			strings.Contains(lower, " stl_") ||
			strings.Contains(lower, " stv_") ||
			strings.Contains(lower, " pg_table_def"))
}

func (s *Server) selectCatalog(statement string) (queryResult, error) {
	lower := strings.ToLower(statement)
	var result queryResult
	switch {
	case strings.Contains(lower, "information_schema.schemata"):
		result = s.catalogSchemata()
	case strings.Contains(lower, "information_schema.tables"):
		result = s.catalogTables()
	case strings.Contains(lower, "information_schema.columns"):
		result = s.catalogColumns()
	case strings.Contains(lower, "pg_catalog.pg_namespace"):
		result = s.catalogPGNamespace()
	case strings.Contains(lower, "pg_catalog.pg_database"):
		result = s.catalogPGDatabase()
	case strings.Contains(lower, "pg_catalog.pg_class"):
		result = s.catalogPGClass()
	case strings.Contains(lower, "pg_catalog.pg_attribute"):
		result = s.catalogPGAttribute()
	case strings.Contains(lower, "pg_catalog.pg_tables"):
		result = s.catalogPGTables()
	case strings.Contains(lower, "pg_catalog.pg_type"):
		result = s.catalogPGType()
	case strings.Contains(lower, "pg_catalog.pg_user"):
		result = s.catalogPGUser()
	case strings.Contains(lower, "pg_table_def"):
		result = s.catalogPGTableDef()
	case strings.Contains(lower, "svv_columns"):
		result = s.catalogSVVColumns()
	case strings.Contains(lower, "svv_mv_info"):
		result = s.catalogSVVMVInfo()
	case strings.Contains(lower, "svv_table_info"):
		result = s.catalogSVVTableInfo()
	case strings.Contains(lower, "stl_query"):
		result = s.catalogSTLQuery()
	case strings.Contains(lower, "stv_recents"):
		result = s.catalogSTVRecents()
	default:
		return queryResult{}, errors.New("unsupported Redshift catalog query in local MVP")
	}
	return shapeCatalogResult(statement, result)
}

func shapeCatalogResult(statement string, result queryResult) (queryResult, error) {
	lower := strings.ToLower(statement)
	fromIndex := strings.Index(lower, " from ")
	if fromIndex < 0 {
		return result, nil
	}
	columnPart := strings.TrimSpace(statement[len("select"):fromIndex])
	rest := strings.TrimSpace(statement[fromIndex+len(" from "):])
	_, clausePart := splitCatalogFromClause(rest)
	tableState := tableFromQueryResult(result)

	wherePart, orderPart, limitPart := splitSelectClauses(clausePart)
	wherePredicate, err := parseWherePredicate(tableState, wherePart)
	if err != nil {
		return queryResult{}, err
	}
	if countAlias, ok, err := parseCountProjection(tableState, columnPart); err != nil {
		return queryResult{}, err
	} else if ok {
		count := 0
		for _, row := range result.rows {
			if wherePredicate.matches(row) {
				count++
			}
		}
		return queryResult{
			fields: []pgField{{Name: countAlias, TypeOID: pgTypeInt4OID, TypeSize: 4}},
			rows:   [][]string{{strconv.Itoa(count)}},
			tag:    "SELECT 1",
		}, nil
	}
	selectedIndexes, fields, err := selectedColumns(tableState, columnPart)
	if err != nil {
		return queryResult{}, err
	}
	orderIndex, err := parseOrderBy(tableState, orderPart)
	if err != nil {
		return queryResult{}, err
	}
	limit, err := parseLimit(limitPart)
	if err != nil {
		return queryResult{}, err
	}

	rows := make([][]string, 0, len(result.rows))
	for _, sourceRow := range result.rows {
		if !wherePredicate.matches(sourceRow) {
			continue
		}
		row := make([]string, 0, len(selectedIndexes))
		for _, index := range selectedIndexes {
			row = append(row, sourceRow[index])
		}
		rows = append(rows, row)
	}
	if orderIndex >= 0 {
		sortRowsBySourceColumn(rows, selectedIndexes, orderIndex)
	}
	if limit >= 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return queryResult{fields: fields, rows: rows, tag: fmt.Sprintf("SELECT %d", len(rows))}, nil
}

func splitCatalogFromClause(rest string) (string, string) {
	for _, separator := range []string{" where ", " order by ", " limit "} {
		tablePart, clausePart := splitClause(rest, separator)
		if clausePart != "" {
			return firstCatalogToken(tablePart), separator + clausePart
		}
	}
	return firstCatalogToken(rest), ""
}

func firstCatalogToken(value string) string {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func splitSelectClauses(value string) (string, string, string) {
	clausePart := strings.TrimSpace(value)
	wherePart := ""
	orderPart := ""
	limitPart := ""
	if strings.HasPrefix(strings.ToLower(clausePart), "where ") {
		wherePart = strings.TrimSpace(clausePart[len("where "):])
		wherePart, orderPart = splitClause(wherePart, " order by ")
		wherePart, limitPart = splitClause(wherePart, " limit ")
	}
	if strings.HasPrefix(strings.ToLower(clausePart), "order by ") {
		orderPart = strings.TrimSpace(clausePart[len("order by "):])
	}
	if strings.HasPrefix(strings.ToLower(clausePart), "limit ") {
		limitPart = strings.TrimSpace(clausePart[len("limit "):])
	}
	if orderPart != "" {
		orderPart, limitPart = splitClause(orderPart, " limit ")
	}
	return wherePart, orderPart, limitPart
}

func tableFromQueryResult(result queryResult) *table {
	return &table{columns: columnsFromFields(result.fields)}
}

func (s *Server) catalogSchemata() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0, len(s.db.schemas))
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		rows = append(rows, []string{defaultString(s.config.Database, "dev"), schemaName, defaultString(s.config.User, "dev")})
	}
	return queryResult{
		fields: []pgField{
			{Name: "catalog_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "schema_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "schema_owner", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogTables() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.catalogTableRowsLocked()
	return queryResult{
		fields: []pgField{
			{Name: "table_catalog", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_schema", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_type", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogColumns() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.catalogColumnRowsLocked()
	return queryResult{
		fields: []pgField{
			{Name: "table_catalog", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_schema", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "column_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "ordinal_position", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "column_default", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "data_type", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "encoding", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGNamespace() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0, len(s.db.schemas))
	for i, schemaName := range sortedSchemaNames(s.db.schemas) {
		rows = append(rows, []string{strconv.Itoa(2200 + i), schemaName})
	}
	return queryResult{
		fields: []pgField{{Name: "oid", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "nspname", TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   rows,
		tag:    fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGDatabase() queryResult {
	return queryResult{
		fields: []pgField{
			{Name: "oid", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "datname", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "datdba", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "encoding", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "datistemplate", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "datallowconn", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: [][]string{{
			"1",
			defaultString(s.config.Database, "dev"),
			"10",
			"6",
			"false",
			"true",
		}},
		tag: "SELECT 1",
	}
}

func (s *Server) catalogPGClass() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			rows = append(rows, []string{catalogTableOID(schemaName, tableName), tableName, pgClassRelKind(tableState)})
		}
	}
	return queryResult{
		fields: []pgField{{Name: "oid", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "relname", TypeOID: pgTypeVarcharOID, TypeSize: -1}, {Name: "relkind", TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   rows,
		tag:    fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGAttribute() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			for i, column := range tableState.columns {
				rows = append(rows, []string{catalogTableOID(schemaName, tableName), strconv.Itoa(i + 1), column.name, strconv.Itoa(int(pgTypeOID(column.dataType)))})
			}
		}
	}
	return queryResult{
		fields: []pgField{{Name: "attrelid", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "attnum", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "attname", TypeOID: pgTypeVarcharOID, TypeSize: -1}, {Name: "atttypid", TypeOID: pgTypeInt4OID, TypeSize: 4}},
		rows:   rows,
		tag:    fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGTables() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, row := range s.catalogTableRowsLocked() {
		if row[3] != "BASE TABLE" {
			continue
		}
		rows = append(rows, []string{row[1], row[2], defaultString(s.config.User, "dev")})
	}
	return queryResult{
		fields: []pgField{{Name: "schemaname", TypeOID: pgTypeVarcharOID, TypeSize: -1}, {Name: "tablename", TypeOID: pgTypeVarcharOID, TypeSize: -1}, {Name: "tableowner", TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   rows,
		tag:    fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGType() queryResult {
	rows := [][]string{
		{strconv.Itoa(int(pgTypeInt4OID)), "int4", "4", "N"},
		{strconv.Itoa(int(pgTypeVarcharOID)), "varchar", "-1", "S"},
		{"25", "text", "-1", "S"},
		{"16", "bool", "1", "B"},
	}
	return queryResult{
		fields: []pgField{
			{Name: "oid", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "typname", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "typlen", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "typcategory", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGUser() queryResult {
	return queryResult{
		fields: []pgField{
			{Name: "usename", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "usesysid", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "usecreatedb", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "usesuper", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "passwd", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: [][]string{{
			defaultString(s.config.User, "dev"),
			"10",
			"true",
			"true",
			"********",
		}},
		tag: "SELECT 1",
	}
}

func (s *Server) catalogSVVTableInfo() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			if isView(tableState) {
				continue
			}
			rows = append(rows, []string{
				schemaName,
				tableName,
				defaultString(tableState.distStyle, "even"),
				tableState.distKey,
				strings.Join(tableState.sortKeys, ","),
				strconv.Itoa(len(tableState.rows)),
			})
		}
	}
	return queryResult{
		fields: []pgField{
			{Name: "schema", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "diststyle", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "distkey", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "sortkey1", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "tbl_rows", TypeOID: pgTypeInt4OID, TypeSize: 4},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogSVVColumns() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := s.catalogColumnRowsLocked()
	return queryResult{
		fields: []pgField{
			{Name: "table_catalog", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_schema", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "table_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "column_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "ordinal_position", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "column_default", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "data_type", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "encoding", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogSVVMVInfo() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			if !isMaterializedView(tableState) {
				continue
			}
			rows = append(rows, []string{
				schemaName,
				tableName,
				defaultString(s.config.User, "dev"),
				"1",
				"false",
				"false",
			})
		}
	}
	return queryResult{
		fields: []pgField{
			{Name: "schema", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "owner_user_name", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "state", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "autorefresh", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "is_stale", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogPGTableDef() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			if isView(tableState) {
				continue
			}
			for _, column := range tableState.columns {
				distKey := strconv.FormatBool(column.name == tableState.distKey)
				sortKey := "0"
				for sortIndex, sortColumn := range tableState.sortKeys {
					if column.name == sortColumn {
						sortKey = strconv.Itoa(sortIndex + 1)
						break
					}
				}
				rows = append(rows, []string{
					schemaName,
					tableName,
					column.name,
					column.dataType,
					column.encoding,
					distKey,
					sortKey,
					"false",
				})
			}
		}
	}
	return queryResult{
		fields: []pgField{
			{Name: "schemaname", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "tablename", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "column", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "type", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "encoding", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "distkey", TypeOID: pgTypeVarcharOID, TypeSize: -1},
			{Name: "sortkey", TypeOID: pgTypeInt4OID, TypeSize: 4},
			{Name: "notnull", TypeOID: pgTypeVarcharOID, TypeSize: -1},
		},
		rows: rows,
		tag:  fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogSTLQuery() queryResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([][]string, 0, len(s.statements))
	for _, stmt := range s.statements {
		preview, _, _ := safeSQLPreview(stmt.QueryString, 200)
		rows = append(rows, []string{strconv.FormatInt(redshiftQueryID(stmt.ID), 10), preview, stmt.Status})
	}
	return queryResult{
		fields: []pgField{{Name: "query", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "querytxt", TypeOID: pgTypeVarcharOID, TypeSize: -1}, {Name: "status", TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   rows,
		tag:    fmt.Sprintf("SELECT %d", len(rows)),
	}
}

func (s *Server) catalogSTVRecents() queryResult {
	return queryResult{
		fields: []pgField{{Name: "pid", TypeOID: pgTypeInt4OID, TypeSize: 4}, {Name: "status", TypeOID: pgTypeVarcharOID, TypeSize: -1}},
		rows:   [][]string{{strconv.Itoa(int(pgDefaultBackendPID)), "Idle"}},
		tag:    "SELECT 1",
	}
}

func (s *Server) catalogTableRowsLocked() [][]string {
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		for _, tableName := range sortedTableNames(s.db.schemas[schemaName].tables) {
			tableState := s.db.schemas[schemaName].tables[tableName]
			rows = append(rows, []string{defaultString(s.config.Database, "dev"), schemaName, tableName, informationSchemaTableType(tableState)})
		}
	}
	return rows
}

func (s *Server) catalogColumnRowsLocked() [][]string {
	rows := make([][]string, 0)
	for _, schemaName := range sortedSchemaNames(s.db.schemas) {
		schemaState := s.db.schemas[schemaName]
		for _, tableName := range sortedTableNames(schemaState.tables) {
			tableState := schemaState.tables[tableName]
			for i, column := range tableState.columns {
				rows = append(rows, []string{
					defaultString(s.config.Database, "dev"),
					schemaName,
					tableName,
					column.name,
					strconv.Itoa(i + 1),
					column.defaultValue,
					column.dataType,
					column.encoding,
				})
			}
		}
	}
	return rows
}

func sortedSchemaNames(schemas map[string]*schema) []string {
	names := make([]string, 0, len(schemas))
	for name := range schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedTableNames(tables map[string]*table) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func catalogTableOID(schemaName string, tableName string) string {
	var value int64 = 10000
	for _, ch := range schemaName + "." + tableName {
		value = value*31 + int64(ch)
		if value < 0 {
			value = -value
		}
	}
	return strconv.FormatInt(value%1000000000, 10)
}
