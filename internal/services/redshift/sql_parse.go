package redshift

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

func parseSelectLiteral(expression string, ordinal int) (string, string, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return "", "", errors.New("SELECT literal expression cannot be empty")
	}
	valuePart := expression
	alias := fmt.Sprintf("?column%d?", ordinal)
	if ordinal == 1 {
		alias = "?column?"
	}
	if value, rest, err := parseLeadingSQLStringLiteral(expression); err == nil {
		valuePart = "'" + strings.ReplaceAll(value, "'", "''") + "'"
		if parsedAlias := parseSelectAlias(rest); parsedAlias != "" {
			alias = parsedAlias
		} else if strings.TrimSpace(rest) != "" {
			return "", "", fmt.Errorf("unsupported SELECT literal alias syntax: %s", strings.TrimSpace(rest))
		}
		value, err := parseLiteral(valuePart)
		return value, alias, err
	}

	fields := strings.Fields(expression)
	if len(fields) == 0 {
		return "", "", errors.New("SELECT literal expression cannot be empty")
	}
	valuePart = fields[0]
	if len(fields) > 1 {
		rest := strings.TrimSpace(expression[len(fields[0]):])
		if parsedAlias := parseSelectAlias(rest); parsedAlias != "" {
			alias = parsedAlias
		} else {
			return "", "", fmt.Errorf("unsupported SELECT literal alias syntax: %s", rest)
		}
	}
	value, err := parseLiteral(valuePart)
	return value, alias, err
}

func parseSelectAlias(rest string) string {
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return ""
	}
	fields := strings.Fields(rest)
	if len(fields) == 2 && strings.EqualFold(fields[0], "as") {
		return cleanIdentifier(fields[1])
	}
	if len(fields) == 1 && !strings.EqualFold(fields[0], "as") {
		return cleanIdentifier(fields[0])
	}
	return ""
}

func splitTopLevelClause(value string, separator string) (string, string) {
	lower := strings.ToLower(value)
	depth := 0
	inString := false
	for i := 0; i <= len(value)-len(separator); i++ {
		ch := value[i]
		if ch == '\'' {
			if inString && i+1 < len(value) && value[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && strings.HasPrefix(lower[i:], separator) {
			return strings.TrimSpace(value[:i]), strings.TrimSpace(value[i+len(separator):])
		}
	}
	return strings.TrimSpace(value), ""
}

func splitSQLStatements(query string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		current.WriteByte(ch)
		if ch == '\'' {
			if inString && i+1 < len(query) && query[i+1] == '\'' {
				current.WriteByte(query[i+1])
				i++
				continue
			}
			inString = !inString
		}
		if ch == ';' && !inString {
			statement := strings.TrimSpace(current.String())
			if strings.Trim(statement, "; \t\r\n") != "" {
				statements = append(statements, statement)
			}
			current.Reset()
		}
	}
	if statement := strings.TrimSpace(current.String()); statement != "" {
		statements = append(statements, statement)
	}
	return statements
}

func parseQualifiedName(value string) qualifiedName {
	token := firstIdentifierToken(value)
	parts := strings.Split(token, ".")
	if len(parts) == 1 {
		return qualifiedName{schema: "public", table: cleanIdentifier(parts[0])}
	}
	return qualifiedName{schema: cleanIdentifier(parts[0]), table: cleanIdentifier(parts[1])}
}

func firstIdentifierToken(value string) string {
	value = strings.TrimSpace(value)
	for i, r := range value {
		if unicode.IsSpace(r) || r == '(' || r == ';' {
			return value[:i]
		}
	}
	return value
}

func cleanIdentifier(value string) string {
	return strings.Trim(strings.TrimSpace(value), `"`)
}

func cleanColumnIdentifier(value string) string {
	cleaned := cleanIdentifier(value)
	if dot := strings.LastIndex(cleaned, "."); dot >= 0 {
		cleaned = cleaned[dot+1:]
	}
	return cleanIdentifier(cleaned)
}

func matchingParen(value string, open int) int {
	if open < 0 || open >= len(value) || value[open] != '(' {
		return -1
	}
	depth := 0
	inString := false
	for i := open; i < len(value); i++ {
		ch := value[i]
		if ch == '\'' {
			if inString && i+1 < len(value) && value[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func parseColumns(value string) ([]column, error) {
	definitions := splitCommaSeparated(value)
	columns := make([]column, 0, len(definitions))
	for _, definition := range definitions {
		fields := strings.Fields(strings.TrimSpace(definition))
		if len(fields) < 2 {
			return nil, errors.New("CREATE TABLE column definition requires name and type")
		}
		name := cleanIdentifier(fields[0])
		if name == "" {
			return nil, errors.New("CREATE TABLE column name cannot be empty")
		}
		columns = append(columns, parseColumnDefinition(name, fields[1], fields[2:]))
	}
	if len(columns) == 0 {
		return nil, errors.New("CREATE TABLE requires at least one column")
	}
	return columns, nil
}

func parseColumnDefinition(name string, dataType string, attributes []string) column {
	column := column{name: name, dataType: strings.ToLower(dataType)}
	for i := 0; i < len(attributes); i++ {
		token := strings.ToLower(attributes[i])
		switch {
		case token == "encode":
			if i+1 < len(attributes) {
				column.encoding = cleanIdentifier(attributes[i+1])
				i++
			}
		case token == "default":
			if i+1 < len(attributes) && !strings.EqualFold(attributes[i+1], "as") {
				column.defaultValue = attributes[i+1]
				i++
			}
		case token == "identity" || strings.HasPrefix(token, "identity("):
			column.identity = true
		case token == "generated":
			for i+1 < len(attributes) {
				i++
				if next := strings.ToLower(attributes[i]); next == "identity" || strings.HasPrefix(next, "identity(") {
					column.identity = true
					break
				}
			}
		case token == "distkey":
			column.distKey = true
		case token == "sortkey":
			column.sortKey = true
		}
	}
	return column
}

func applyColumnTableAttributes(columns []column, distStyle *string, distKey *string, sortKeys *[]string) {
	for _, column := range columns {
		if column.distKey && *distKey == "" {
			*distKey = column.name
			if *distStyle == "" {
				*distStyle = "key"
			}
		}
		if column.sortKey && !containsIdentifier(*sortKeys, column.name) {
			*sortKeys = append(*sortKeys, column.name)
		}
	}
}

func containsIdentifier(values []string, value string) bool {
	for _, item := range values {
		if strings.EqualFold(item, value) {
			return true
		}
	}
	return false
}

func parseTableAttributes(value string) (string, string, []string) {
	fields := strings.Fields(value)
	distStyle := ""
	distKey := ""
	var sortKeys []string
	for i := 0; i < len(fields); i++ {
		token := strings.ToLower(fields[i])
		switch {
		case token == "diststyle" && i+1 < len(fields):
			distStyle = strings.ToLower(cleanIdentifier(fields[i+1]))
			i++
		case strings.HasPrefix(token, "diststyle") && strings.Contains(token, " "):
			distStyle = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(token, "diststyle")))
		case strings.HasPrefix(token, "distkey"):
			if key := parseParenthesizedIdentifier(fields[i], "distkey"); key != "" {
				distKey = key
			} else if i+1 < len(fields) {
				distKey = parseParenthesizedIdentifier(fields[i+1], "")
				i++
			}
		case strings.HasPrefix(token, "sortkey"):
			if keys := parseParenthesizedIdentifierList(fields[i], "sortkey"); len(keys) > 0 {
				sortKeys = keys
			} else if i+1 < len(fields) {
				sortKeys = parseParenthesizedIdentifierList(fields[i+1], "")
				i++
			}
		}
	}
	return distStyle, distKey, sortKeys
}

func parseParenthesizedIdentifier(value string, prefix string) string {
	values := parseParenthesizedIdentifierList(value, prefix)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func parseParenthesizedIdentifierList(value string, prefix string) []string {
	value = strings.TrimSpace(value)
	if prefix != "" {
		if !strings.HasPrefix(strings.ToLower(value), prefix) {
			return nil
		}
		value = strings.TrimSpace(value[len(prefix):])
	}
	value = strings.TrimSpace(value)
	if len(value) < 2 || value[0] != '(' || value[len(value)-1] != ')' {
		return nil
	}
	parts := splitCommaSeparated(value[1 : len(value)-1])
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if cleaned := cleanIdentifier(part); cleaned != "" {
			result = append(result, cleaned)
		}
	}
	return result
}

func splitCommaSeparated(value string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inString := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '\'' {
			if inString && i+1 < len(value) && value[i+1] == '\'' {
				current.WriteByte(ch)
				current.WriteByte(value[i+1])
				i++
				continue
			}
			inString = !inString
		}
		if !inString {
			switch ch {
			case '(':
				depth++
			case ')':
				depth--
			case ',':
				if depth == 0 {
					parts = append(parts, strings.TrimSpace(current.String()))
					current.Reset()
					continue
				}
			}
		}
		current.WriteByte(ch)
	}
	if part := strings.TrimSpace(current.String()); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func parseCSVishValues(value string) ([]string, error) {
	parts := splitCommaSeparated(value)
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		parsed, err := parseLiteral(part)
		if err != nil {
			return nil, err
		}
		values = append(values, parsed)
	}
	return values, nil
}

func parseValuesTuples(value string) ([][]string, error) {
	var rows [][]string
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("INSERT requires at least one VALUES row")
	}
	for {
		if value[0] != '(' {
			return nil, errors.New("INSERT requires parenthesized VALUES rows")
		}
		close := matchingParen(value, 0)
		if close < 0 {
			return nil, errors.New("INSERT has an unterminated row value list")
		}
		row, err := parseCSVishValues(value[1:close])
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		value = strings.TrimSpace(value[close+1:])
		if value == "" {
			break
		}
		if value[0] != ',' {
			return nil, errors.New("INSERT VALUES rows must be separated by commas")
		}
		value = strings.TrimSpace(value[1:])
		if value == "" {
			return nil, errors.New("INSERT requires a VALUES row after comma")
		}
	}
	if len(rows) == 0 {
		return nil, errors.New("INSERT requires at least one VALUES row")
	}
	return rows, nil
}

func parseLiteral(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		return strings.ReplaceAll(value[1:len(value)-1], "''", "'"), nil
	}
	if value == "" {
		return "", errors.New("empty literal")
	}
	return value, nil
}

func parseLeadingSQLStringLiteral(value string) (string, string, error) {
	value = strings.TrimSpace(value)
	if len(value) < 2 || value[0] != '\'' {
		return "", value, errors.New("expected SQL string literal")
	}
	var builder strings.Builder
	for i := 1; i < len(value); i++ {
		ch := value[i]
		if ch != '\'' {
			builder.WriteByte(ch)
			continue
		}
		if i+1 < len(value) && value[i+1] == '\'' {
			builder.WriteByte('\'')
			i++
			continue
		}
		return builder.String(), strings.TrimSpace(value[i+1:]), nil
	}
	return "", value, errors.New("unterminated SQL string literal")
}
