package redshift

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type columnAssignment struct {
	index int
	value string
}

type wherePredicate struct {
	index int
	op    string
	value string
}

func (p wherePredicate) matches(row []string) bool {
	if p.index < 0 {
		return true
	}
	if p.index >= len(row) {
		return false
	}
	left := row[p.index]
	switch p.op {
	case "=":
		return left == p.value
	case "!=", "<>":
		return left != p.value
	case ">", ">=", "<", "<=":
		comparison := compareSQLValues(left, p.value)
		switch p.op {
		case ">":
			return comparison > 0
		case ">=":
			return comparison >= 0
		case "<":
			return comparison < 0
		case "<=":
			return comparison <= 0
		}
	}
	return false
}

func buildInsertRow(table *table, insertColumns []string, values []string) ([]string, error) {
	if len(insertColumns) == 0 {
		if len(values) != len(table.columns) {
			return nil, fmt.Errorf("INSERT has %d values for %d columns", len(values), len(table.columns))
		}
		row := make([]string, 0, len(values))
		for index, value := range values {
			resolved, err := resolveInsertValue(table, index, value)
			if err != nil {
				return nil, err
			}
			row = append(row, resolved)
		}
		return row, nil
	}
	if len(values) != len(insertColumns) {
		return nil, fmt.Errorf("INSERT has %d values for %d target columns", len(values), len(insertColumns))
	}
	row := make([]string, len(table.columns))
	assigned := make([]bool, len(table.columns))
	for valueIndex, columnName := range insertColumns {
		columnIndex := columnIndex(table, columnName)
		if columnIndex < 0 {
			return nil, fmt.Errorf("column %s does not exist", columnName)
		}
		if assigned[columnIndex] {
			return nil, fmt.Errorf("column %s specified more than once", columnName)
		}
		resolved, err := resolveInsertValue(table, columnIndex, values[valueIndex])
		if err != nil {
			return nil, err
		}
		row[columnIndex] = resolved
		assigned[columnIndex] = true
	}
	for index := range table.columns {
		if assigned[index] {
			continue
		}
		row[index] = defaultInsertValue(table, index)
	}
	return row, nil
}

func resolveInsertValue(table *table, columnIndex int, value string) (string, error) {
	if strings.EqualFold(strings.TrimSpace(value), "default") {
		return defaultInsertValue(table, columnIndex), nil
	}
	return value, nil
}

func defaultInsertValue(table *table, columnIndex int) string {
	column := table.columns[columnIndex]
	if column.defaultValue != "" {
		return strings.Trim(column.defaultValue, "'")
	}
	if column.identity {
		return strconv.Itoa(nextIdentityValue(table, columnIndex))
	}
	return ""
}

func nextIdentityValue(table *table, columnIndex int) int {
	next := 1
	for _, row := range table.rows {
		if columnIndex >= len(row) {
			continue
		}
		value, err := strconv.Atoi(row[columnIndex])
		if err == nil && value >= next {
			next = value + 1
		}
	}
	return next
}

func parseAssignments(table *table, assignmentsPart string) ([]columnAssignment, error) {
	parts := splitCommaSeparated(assignmentsPart)
	if len(parts) == 0 {
		return nil, errors.New("UPDATE requires at least one assignment")
	}
	assignments := make([]columnAssignment, 0, len(parts))
	for _, part := range parts {
		pieces := strings.SplitN(part, "=", 2)
		if len(pieces) != 2 {
			return nil, errors.New("UPDATE assignments must use column = literal")
		}
		name := cleanIdentifier(pieces[0])
		index := columnIndex(table, name)
		if index < 0 {
			return nil, fmt.Errorf("column %s does not exist", name)
		}
		value, err := parseLiteral(pieces[1])
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, columnAssignment{index: index, value: value})
	}
	return assignments, nil
}

func splitClause(value string, separator string) (string, string) {
	lower := strings.ToLower(value)
	index := strings.Index(lower, separator)
	if index < 0 {
		return strings.TrimSpace(value), ""
	}
	return strings.TrimSpace(value[:index]), strings.TrimSpace(value[index+len(separator):])
}

func selectedColumns(table *table, columnPart string) ([]int, []pgField, error) {
	if strings.TrimSpace(columnPart) == "*" {
		indexes := make([]int, 0, len(table.columns))
		fields := make([]pgField, 0, len(table.columns))
		for i, column := range table.columns {
			indexes = append(indexes, i)
			fields = append(fields, pgField{Name: column.name, TypeOID: pgTypeOID(column.dataType), TypeSize: pgTypeSize(column.dataType)})
		}
		return indexes, fields, nil
	}
	names := splitCommaSeparated(columnPart)
	indexes := make([]int, 0, len(names))
	fields := make([]pgField, 0, len(names))
	for _, name := range names {
		cleaned := cleanColumnIdentifier(name)
		index := columnIndex(table, cleaned)
		if index < 0 {
			return nil, nil, fmt.Errorf("column %s does not exist", cleaned)
		}
		column := table.columns[index]
		indexes = append(indexes, index)
		fields = append(fields, pgField{Name: column.name, TypeOID: pgTypeOID(column.dataType), TypeSize: pgTypeSize(column.dataType)})
	}
	return indexes, fields, nil
}

func parseWherePredicate(table *table, wherePart string) (wherePredicate, error) {
	if wherePart == "" {
		return wherePredicate{index: -1}, nil
	}
	left, op, right, ok := splitWhereComparison(wherePart)
	if !ok {
		return wherePredicate{}, errors.New("only simple WHERE comparison is supported")
	}
	name := cleanColumnIdentifier(left)
	index := columnIndex(table, name)
	if index < 0 {
		return wherePredicate{}, fmt.Errorf("column %s does not exist", name)
	}
	value, err := parseLiteral(right)
	if err != nil {
		return wherePredicate{}, err
	}
	return wherePredicate{index: index, op: op, value: value}, nil
}

func splitWhereComparison(wherePart string) (string, string, string, bool) {
	operators := []string{">=", "<=", "!=", "<>", "=", ">", "<"}
	inString := false
	for i := 0; i < len(wherePart); i++ {
		ch := wherePart[i]
		if ch == '\'' {
			if inString && i+1 < len(wherePart) && wherePart[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		for _, op := range operators {
			if strings.HasPrefix(wherePart[i:], op) {
				left := strings.TrimSpace(wherePart[:i])
				right := strings.TrimSpace(wherePart[i+len(op):])
				return left, op, right, left != "" && right != ""
			}
		}
	}
	return "", "", "", false
}

func compareSQLValues(left string, right string) int {
	leftInt, leftErr := strconv.ParseInt(left, 10, 64)
	rightInt, rightErr := strconv.ParseInt(right, 10, 64)
	if leftErr == nil && rightErr == nil {
		switch {
		case leftInt < rightInt:
			return -1
		case leftInt > rightInt:
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(left, right)
}

func parseOrderBy(table *table, orderPart string) (int, error) {
	if orderPart == "" {
		return -1, nil
	}
	fields := strings.Fields(orderPart)
	if len(fields) == 0 {
		return -1, nil
	}
	index := columnIndex(table, cleanColumnIdentifier(fields[0]))
	if index < 0 {
		return -1, fmt.Errorf("column %s does not exist", fields[0])
	}
	if len(fields) > 1 && !strings.EqualFold(fields[1], "asc") {
		return -1, errors.New("only ORDER BY column ASC is supported")
	}
	return index, nil
}

func sortRowsBySourceColumn(rows [][]string, selectedIndexes []int, orderIndex int) {
	selectedIndex := -1
	for i, sourceIndex := range selectedIndexes {
		if sourceIndex == orderIndex {
			selectedIndex = i
			break
		}
	}
	if selectedIndex < 0 {
		return
	}
	sort.SliceStable(rows, func(i, j int) bool {
		left := rows[i][selectedIndex]
		right := rows[j][selectedIndex]
		leftInt, leftErr := strconv.ParseInt(left, 10, 64)
		rightInt, rightErr := strconv.ParseInt(right, 10, 64)
		if leftErr == nil && rightErr == nil {
			return leftInt < rightInt
		}
		return left < right
	})
}

func parseLimit(value string) (int, error) {
	if value == "" {
		return -1, nil
	}
	limit, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || limit < 0 {
		return -1, errors.New("LIMIT must be a non-negative integer")
	}
	return limit, nil
}

func columnIndex(table *table, name string) int {
	for i, column := range table.columns {
		if strings.EqualFold(column.name, name) {
			return i
		}
	}
	return -1
}
