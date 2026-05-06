package redshift

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func (s *Server) executeSQLMemory(statement string) (queryResult, error) {
	normalized := strings.TrimSpace(strings.TrimRight(statement, ";"))
	if normalized == "" {
		return queryResult{tag: ""}, nil
	}
	if err := s.validateStatementSize(normalized); err != nil {
		return queryResult{}, err
	}
	lower := strings.ToLower(normalized)
	switch {
	case strings.EqualFold(normalized, "select 1"):
		return queryResult{
			fields: []pgField{{Name: "?column?", TypeOID: pgTypeInt4OID, TypeSize: 4}},
			rows:   [][]string{{"1"}},
			tag:    "SELECT 1",
		}, nil
	case strings.EqualFold(normalized, "select current_database()"):
		return stringFunctionResult("current_database", defaultString(s.config.Database, "dev")), nil
	case strings.EqualFold(normalized, "select current_schema()"):
		return stringFunctionResult("current_schema", "public"), nil
	case strings.EqualFold(normalized, "select current_user"):
		return stringFunctionResult("current_user", defaultString(s.config.User, "dev")), nil
	case strings.EqualFold(normalized, "select current_user()"):
		return stringFunctionResult("current_user", defaultString(s.config.User, "dev")), nil
	case strings.EqualFold(normalized, "select session_user"):
		return stringFunctionResult("session_user", defaultString(s.config.User, "dev")), nil
	case strings.EqualFold(normalized, "select session_user()"):
		return stringFunctionResult("session_user", defaultString(s.config.User, "dev")), nil
	case strings.EqualFold(normalized, "select pg_backend_pid()"):
		return queryResult{
			fields: []pgField{{Name: "pg_backend_pid", TypeOID: pgTypeInt4OID, TypeSize: 4}},
			rows:   [][]string{{strconv.Itoa(int(pgDefaultBackendPID))}},
			tag:    "SELECT 1",
		}, nil
	case strings.EqualFold(normalized, "select version()"):
		return stringFunctionResult("version", "PostgreSQL 8.0.2 on devcloud Redshift-compatible local server"), nil
	case strings.HasPrefix(lower, "set "):
		return queryResult{tag: "SET"}, nil
	case strings.HasPrefix(lower, "show "):
		return s.showParameter(normalized)
	case lower == "begin" || lower == "begin transaction" || lower == "start transaction":
		return queryResult{tag: "BEGIN"}, nil
	case lower == "commit" || lower == "end":
		return queryResult{tag: "COMMIT"}, nil
	case lower == "rollback":
		return queryResult{tag: "ROLLBACK"}, nil
	case strings.HasPrefix(lower, "create schema"):
		return s.createSchema(normalized)
	case strings.HasPrefix(lower, "drop schema"):
		return s.dropSchema(normalized)
	case strings.HasPrefix(lower, "drop materialized view"):
		return s.dropMaterializedView(normalized)
	case strings.HasPrefix(lower, "drop view"):
		return s.dropView(normalized)
	case strings.HasPrefix(lower, "drop table"):
		return s.dropTable(normalized)
	case strings.HasPrefix(lower, "create materialized view"):
		return s.createMaterializedView(normalized)
	case strings.HasPrefix(lower, "create view") || strings.HasPrefix(lower, "create or replace view"):
		return s.createView(normalized)
	case strings.HasPrefix(lower, "create table"):
		return s.createTable(normalized)
	case strings.HasPrefix(lower, "insert into"):
		return s.insertInto(normalized)
	case strings.HasPrefix(lower, "update "):
		return s.updateTable(normalized)
	case strings.HasPrefix(lower, "delete from "):
		return s.deleteFrom(normalized)
	case isCatalogSelect(lower):
		return s.selectCatalog(normalized)
	case strings.HasPrefix(lower, "select "):
		if !strings.Contains(lower, " from ") {
			return selectLiterals(normalized)
		}
		return s.selectFromTable(normalized)
	case strings.HasPrefix(lower, "copy "):
		return s.copyFromLocalCSV(normalized)
	case strings.HasPrefix(lower, "unload "):
		return s.unloadToLocalCSV(normalized)
	default:
		return queryResult{}, errors.New("unsupported Redshift SQL in local MVP")
	}
}

func selectLiterals(statement string) (queryResult, error) {
	columnPart := strings.TrimSpace(statement[len("select"):])
	if columnPart == "" {
		return queryResult{}, errors.New("SELECT requires at least one expression")
	}
	expressions := splitCommaSeparated(columnPart)
	fields := make([]pgField, 0, len(expressions))
	row := make([]string, 0, len(expressions))
	for i, expression := range expressions {
		value, alias, err := parseSelectLiteral(expression, i+1)
		if err != nil {
			return queryResult{}, err
		}
		typeOID, typeSize := inferLiteralPGType(value)
		fields = append(fields, pgField{Name: alias, TypeOID: typeOID, TypeSize: typeSize})
		row = append(row, value)
	}
	return queryResult{fields: fields, rows: [][]string{row}, tag: "SELECT 1"}, nil
}

func (s *Server) createSchema(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("create schema"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "if not exists ") {
		rest = strings.TrimSpace(rest[len("if not exists "):])
	}
	name := strings.Trim(rest, `"`)
	if name == "" {
		return queryResult{}, errors.New("CREATE SCHEMA requires a schema name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.db.schemas[name]; !ok {
		s.db.schemas[name] = &schema{tables: map[string]*table{}}
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "CREATE SCHEMA"}, nil
}

func (s *Server) dropSchema(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("drop schema"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "if exists ") {
		rest = strings.TrimSpace(rest[len("if exists "):])
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return queryResult{}, errors.New("DROP SCHEMA requires a schema name")
	}
	name := cleanIdentifier(fields[0])
	if name == "" {
		return queryResult{}, errors.New("DROP SCHEMA requires a schema name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.db.schemas, name)
	ensurePublicSchema(s.db)
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "DROP SCHEMA"}, nil
}

func (s *Server) dropTable(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("drop table"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "if exists ") {
		rest = strings.TrimSpace(rest[len("if exists "):])
	}
	name := parseQualifiedName(rest)
	s.mu.Lock()
	defer s.mu.Unlock()
	if schema := s.db.schemas[name.schema]; schema != nil {
		delete(schema.tables, name.table)
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "DROP TABLE"}, nil
}

func (s *Server) createTable(statement string) (queryResult, error) {
	if result, ok, err := s.createTableAs(statement); ok || err != nil {
		return result, err
	}
	open := strings.IndexByte(statement, '(')
	close := matchingParen(statement, open)
	if open < 0 || close < 0 {
		return queryResult{}, errors.New("CREATE TABLE requires a column list")
	}
	namePart := strings.TrimSpace(statement[len("create table"):open])
	if strings.HasPrefix(strings.ToLower(namePart), "if not exists ") {
		namePart = strings.TrimSpace(namePart[len("if not exists "):])
	}
	name := parseQualifiedName(namePart)
	columns, err := parseColumns(statement[open+1 : close])
	if err != nil {
		return queryResult{}, err
	}
	distStyle, distKey, sortKeys := parseTableAttributes(statement[close+1:])
	applyColumnTableAttributes(columns, &distStyle, &distKey, &sortKeys)
	s.mu.Lock()
	defer s.mu.Unlock()
	schemaState := s.db.schemas[name.schema]
	if schemaState == nil {
		schemaState = &schema{tables: map[string]*table{}}
		s.db.schemas[name.schema] = schemaState
	}
	schemaState.tables[name.table] = &table{name: name, columns: columns, distStyle: distStyle, distKey: distKey, sortKeys: sortKeys}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "CREATE TABLE"}, nil
}

func (s *Server) createTableAs(statement string) (queryResult, bool, error) {
	rest := strings.TrimSpace(statement[len("create table"):])
	if strings.HasPrefix(strings.ToLower(rest), "if not exists ") {
		rest = strings.TrimSpace(rest[len("if not exists "):])
	}
	namePart, queryPart := splitTopLevelClause(rest, " as ")
	if queryPart == "" {
		return queryResult{}, false, nil
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(queryPart)), "select ") {
		return queryResult{}, true, errors.New("CREATE TABLE AS requires SELECT")
	}
	nameToken := firstIdentifierToken(namePart)
	if strings.TrimSpace(nameToken) == "" {
		return queryResult{}, true, errors.New("CREATE TABLE AS requires a table name")
	}
	name := parseQualifiedName(nameToken)
	result, err := s.executeSQL(queryPart)
	if err != nil {
		return queryResult{}, true, err
	}
	columns := columnsFromFields(result.fields)
	if len(columns) == 0 {
		return queryResult{}, true, errors.New("CREATE TABLE AS SELECT must return columns")
	}
	attributes := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(namePart), nameToken))
	distStyle, distKey, sortKeys := parseTableAttributes(attributes)

	s.mu.Lock()
	defer s.mu.Unlock()
	schemaState := s.db.schemas[name.schema]
	if schemaState == nil {
		schemaState = &schema{tables: map[string]*table{}}
		s.db.schemas[name.schema] = schemaState
	}
	schemaState.tables[name.table] = &table{
		name:      name,
		columns:   columns,
		rows:      cloneRows(result.rows),
		distStyle: distStyle,
		distKey:   distKey,
		sortKeys:  sortKeys,
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, true, err
	}
	return queryResult{tag: fmt.Sprintf("SELECT %d", len(result.rows))}, true, nil
}

func (s *Server) createView(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("create "):])
	orReplace := false
	if strings.HasPrefix(strings.ToLower(rest), "or replace ") {
		orReplace = true
		rest = strings.TrimSpace(rest[len("or replace "):])
	}
	if !strings.HasPrefix(strings.ToLower(rest), "view ") {
		return queryResult{}, errors.New("CREATE VIEW requires VIEW")
	}
	rest = strings.TrimSpace(rest[len("view "):])
	namePart, queryPart := splitClause(rest, " as ")
	if namePart == "" || queryPart == "" {
		return queryResult{}, errors.New("CREATE VIEW requires name and AS SELECT")
	}
	name := parseQualifiedName(namePart)
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(queryPart)), "select ") {
		return queryResult{}, errors.New("CREATE VIEW requires SELECT")
	}
	result, err := s.executeSQL(queryPart)
	if err != nil {
		return queryResult{}, err
	}
	columns := columnsFromFields(result.fields)
	if len(columns) == 0 {
		return queryResult{}, errors.New("CREATE VIEW SELECT must return columns")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	schemaState := s.db.schemas[name.schema]
	if schemaState == nil {
		schemaState = &schema{tables: map[string]*table{}}
		s.db.schemas[name.schema] = schemaState
	}
	if existing := schemaState.tables[name.table]; existing != nil {
		if !isView(existing) {
			return queryResult{}, fmt.Errorf("relation %s.%s already exists", name.schema, name.table)
		}
		if !orReplace {
			return queryResult{}, fmt.Errorf("view %s.%s already exists", name.schema, name.table)
		}
	}
	schemaState.tables[name.table] = &table{name: name, columns: columns, kind: "VIEW", viewSQL: strings.TrimSpace(queryPart)}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "CREATE VIEW"}, nil
}

func (s *Server) createMaterializedView(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("create materialized view"):])
	if strings.HasPrefix(strings.ToLower(rest), "if not exists ") {
		rest = strings.TrimSpace(rest[len("if not exists "):])
	}
	namePart, queryPart := splitTopLevelClause(rest, " as ")
	if namePart == "" || queryPart == "" {
		return queryResult{}, errors.New("CREATE MATERIALIZED VIEW requires name and AS SELECT")
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(queryPart)), "select ") {
		return queryResult{}, errors.New("CREATE MATERIALIZED VIEW requires SELECT")
	}
	nameToken := firstIdentifierToken(namePart)
	if nameToken == "" {
		return queryResult{}, errors.New("CREATE MATERIALIZED VIEW requires a view name")
	}
	name := parseQualifiedName(nameToken)
	attributes := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(namePart), nameToken))
	distStyle, distKey, sortKeys := parseTableAttributes(attributes)
	result, err := s.executeSQL(queryPart)
	if err != nil {
		return queryResult{}, err
	}
	columns := columnsFromFields(result.fields)
	if len(columns) == 0 {
		return queryResult{}, errors.New("CREATE MATERIALIZED VIEW SELECT must return columns")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	schemaState := s.db.schemas[name.schema]
	if schemaState == nil {
		schemaState = &schema{tables: map[string]*table{}}
		s.db.schemas[name.schema] = schemaState
	}
	if existing := schemaState.tables[name.table]; existing != nil {
		if !isMaterializedView(existing) {
			return queryResult{}, fmt.Errorf("relation %s.%s already exists", name.schema, name.table)
		}
	}
	schemaState.tables[name.table] = &table{
		name:      name,
		columns:   columns,
		rows:      cloneRows(result.rows),
		kind:      "MATERIALIZED VIEW",
		viewSQL:   strings.TrimSpace(queryPart),
		distStyle: distStyle,
		distKey:   distKey,
		sortKeys:  sortKeys,
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "CREATE MATERIALIZED VIEW"}, nil
}

func (s *Server) dropView(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("drop view"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "if exists ") {
		rest = strings.TrimSpace(rest[len("if exists "):])
	}
	name := parseQualifiedName(rest)
	s.mu.Lock()
	defer s.mu.Unlock()
	if schema := s.db.schemas[name.schema]; schema != nil {
		if tableState := schema.tables[name.table]; tableState != nil && isView(tableState) {
			delete(schema.tables, name.table)
		}
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "DROP VIEW"}, nil
}

func (s *Server) dropMaterializedView(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("drop materialized view"):])
	restLower := strings.ToLower(rest)
	if strings.HasPrefix(restLower, "if exists ") {
		rest = strings.TrimSpace(rest[len("if exists "):])
	}
	name := parseQualifiedName(rest)
	s.mu.Lock()
	defer s.mu.Unlock()
	if schema := s.db.schemas[name.schema]; schema != nil {
		if tableState := schema.tables[name.table]; tableState != nil && isMaterializedView(tableState) {
			delete(schema.tables, name.table)
		}
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: "DROP MATERIALIZED VIEW"}, nil
}

func (s *Server) insertInto(statement string) (queryResult, error) {
	lower := strings.ToLower(statement)
	valuesIndex := strings.Index(lower, " values ")
	if valuesIndex < 0 {
		return queryResult{}, errors.New("INSERT requires VALUES")
	}
	namePart := strings.TrimSpace(statement[len("insert into"):valuesIndex])
	var insertColumns []string
	if columnListIndex := strings.IndexByte(namePart, '('); columnListIndex >= 0 {
		close := matchingParen(namePart, columnListIndex)
		if close < 0 {
			return queryResult{}, errors.New("INSERT column list is unterminated")
		}
		for _, columnName := range splitCommaSeparated(namePart[columnListIndex+1 : close]) {
			cleaned := cleanIdentifier(columnName)
			if cleaned == "" {
				return queryResult{}, errors.New("INSERT column list contains an empty column")
			}
			insertColumns = append(insertColumns, cleaned)
		}
		namePart = strings.TrimSpace(namePart[:columnListIndex])
	}
	name := parseQualifiedName(namePart)
	valueRows, err := parseValuesTuples(statement[valuesIndex+len(" values "):])
	if err != nil {
		return queryResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	table := s.lookupTableLocked(name)
	if table == nil {
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isReadOnlyRelation(table) {
		return queryResult{}, fmt.Errorf("cannot insert into view %s.%s", name.schema, name.table)
	}
	inserted := 0
	for _, values := range valueRows {
		row, err := buildInsertRow(table, insertColumns, values)
		if err != nil {
			return queryResult{}, err
		}
		table.rows = append(table.rows, row)
		inserted++
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: fmt.Sprintf("INSERT 0 %d", inserted)}, nil
}

func (s *Server) updateTable(statement string) (queryResult, error) {
	lower := strings.ToLower(statement)
	setIndex := strings.Index(lower, " set ")
	if setIndex < 0 {
		return queryResult{}, errors.New("UPDATE requires SET")
	}
	name := parseQualifiedName(statement[len("update "):setIndex])
	assignmentsPart, wherePart := splitClause(statement[setIndex+len(" set "):], " where ")

	s.mu.Lock()
	defer s.mu.Unlock()
	table := s.lookupTableLocked(name)
	if table == nil {
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isReadOnlyRelation(table) {
		return queryResult{}, fmt.Errorf("cannot update view %s.%s", name.schema, name.table)
	}
	assignments, err := parseAssignments(table, assignmentsPart)
	if err != nil {
		return queryResult{}, err
	}
	wherePredicate, err := parseWherePredicate(table, wherePart)
	if err != nil {
		return queryResult{}, err
	}
	updated := 0
	for rowIndex, stored := range table.rows {
		if !wherePredicate.matches(stored) {
			continue
		}
		next := append([]string(nil), stored...)
		for _, assignment := range assignments {
			next[assignment.index] = assignment.value
		}
		table.rows[rowIndex] = next
		updated++
	}
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: fmt.Sprintf("UPDATE %d", updated)}, nil
}

func (s *Server) deleteFrom(statement string) (queryResult, error) {
	rest := strings.TrimSpace(statement[len("delete from "):])
	tablePart, wherePart := splitClause(rest, " where ")
	name := parseQualifiedName(tablePart)

	s.mu.Lock()
	defer s.mu.Unlock()
	table := s.lookupTableLocked(name)
	if table == nil {
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isReadOnlyRelation(table) {
		return queryResult{}, fmt.Errorf("cannot delete from view %s.%s", name.schema, name.table)
	}
	wherePredicate, err := parseWherePredicate(table, wherePart)
	if err != nil {
		return queryResult{}, err
	}
	deleted := 0
	remaining := table.rows[:0]
	for _, stored := range table.rows {
		if wherePredicate.matches(stored) {
			deleted++
			continue
		}
		remaining = append(remaining, stored)
	}
	table.rows = remaining
	if err := s.persistLocked(); err != nil {
		return queryResult{}, err
	}
	return queryResult{tag: fmt.Sprintf("DELETE %d", deleted)}, nil
}

func (s *Server) selectFromTable(statement string) (queryResult, error) {
	lower := strings.ToLower(statement)
	fromIndex := strings.Index(lower, " from ")
	if fromIndex < 0 {
		return queryResult{}, errors.New("SELECT requires FROM")
	}
	columnPart := strings.TrimSpace(statement[len("select"):fromIndex])
	rest := strings.TrimSpace(statement[fromIndex+len(" from "):])
	tablePart, wherePart := splitClause(rest, " where ")
	tablePart, orderPart := splitClause(tablePart, " order by ")
	tablePart, limitPart := splitClause(tablePart, " limit ")
	if wherePart != "" {
		wherePart, orderPart = splitClause(wherePart, " order by ")
		wherePart, limitPart = splitClause(wherePart, " limit ")
	}
	if orderPart != "" {
		orderPart, limitPart = splitClause(orderPart, " limit ")
	}
	name := parseQualifiedName(tablePart)

	s.mu.Lock()
	tableState := s.lookupTableLocked(name)
	if tableState == nil {
		s.mu.Unlock()
		return queryResult{}, fmt.Errorf("table %s.%s does not exist", name.schema, name.table)
	}
	if isView(tableState) {
		viewSQL := tableState.viewSQL
		s.mu.Unlock()
		result, err := s.executeSQL(viewSQL)
		if err != nil {
			return queryResult{}, err
		}
		viewTable := tableFromQueryResult(result)
		viewTable.rows = cloneRows(result.rows)
		return selectFromResolvedTable(viewTable, columnPart, wherePart, orderPart, limitPart)
	}
	table := &table{
		name:      tableState.name,
		columns:   append([]column(nil), tableState.columns...),
		rows:      cloneRows(tableState.rows),
		kind:      tableState.kind,
		viewSQL:   tableState.viewSQL,
		distStyle: tableState.distStyle,
		distKey:   tableState.distKey,
		sortKeys:  append([]string(nil), tableState.sortKeys...),
	}
	s.mu.Unlock()
	return selectFromResolvedTable(table, columnPart, wherePart, orderPart, limitPart)
}

func selectFromResolvedTable(table *table, columnPart string, wherePart string, orderPart string, limitPart string) (queryResult, error) {
	wherePredicate, err := parseWherePredicate(table, wherePart)
	if err != nil {
		return queryResult{}, err
	}
	if countAlias, ok, err := parseCountProjection(table, columnPart); err != nil {
		return queryResult{}, err
	} else if ok {
		count := 0
		for _, stored := range table.rows {
			if wherePredicate.matches(stored) {
				count++
			}
		}
		return queryResult{
			fields: []pgField{{Name: countAlias, TypeOID: pgTypeInt4OID, TypeSize: 4}},
			rows:   [][]string{{strconv.Itoa(count)}},
			tag:    "SELECT 1",
		}, nil
	}
	selectedIndexes, fields, err := selectedColumns(table, columnPart)
	if err != nil {
		return queryResult{}, err
	}
	limit, err := parseLimit(limitPart)
	if err != nil {
		return queryResult{}, err
	}
	orderIndex, err := parseOrderBy(table, orderPart)
	if err != nil {
		return queryResult{}, err
	}
	rows := make([][]string, 0)
	for _, stored := range table.rows {
		if !wherePredicate.matches(stored) {
			continue
		}
		row := make([]string, 0, len(selectedIndexes))
		for _, index := range selectedIndexes {
			row = append(row, stored[index])
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

func parseCountProjection(table *table, columnPart string) (string, bool, error) {
	expression := strings.TrimSpace(columnPart)
	if expression == "" {
		return "", false, nil
	}
	fields := strings.Fields(expression)
	if len(fields) == 0 {
		return "", false, nil
	}
	countExpr := fields[0]
	lower := strings.ToLower(countExpr)
	if !strings.HasPrefix(lower, "count(") || !strings.HasSuffix(lower, ")") {
		return "", false, nil
	}
	argument := strings.TrimSpace(countExpr[len("count(") : len(countExpr)-1])
	switch {
	case argument == "*" || argument == "1":
	case columnIndex(table, cleanColumnIdentifier(argument)) >= 0:
	default:
		return "", false, fmt.Errorf("column %s does not exist", argument)
	}
	alias := "count"
	if len(fields) > 1 {
		rest := strings.TrimSpace(expression[len(countExpr):])
		parsedAlias := parseSelectAlias(rest)
		if parsedAlias == "" {
			return "", false, fmt.Errorf("unsupported SELECT count alias syntax: %s", rest)
		}
		alias = parsedAlias
	}
	return alias, true, nil
}
