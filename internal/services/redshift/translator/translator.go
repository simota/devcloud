package translator

import (
	"context"
	"errors"
	"strings"
)

// RedshiftTranslator converts Redshift dialect SQL into backend SQL plus
// devcloud-owned metadata or side effects.
type RedshiftTranslator interface {
	Translate(ctx context.Context, session Session, sql string) (TranslationResult, error)
}

type Session struct {
	Database string
	User     string
	Schema   string
}

type Parameter struct {
	Name  string
	Value string
}

type TranslationResult struct {
	BackendSQL        string
	Parameters        []Parameter
	MetadataEffects   []MetadataEffect
	SideEffects       []SideEffect
	HandledByDevcloud bool
}

type MetadataEffect struct {
	Kind     string
	Schema   string
	Table    string
	Name     string
	Value    string
	Backup   string
	Columns  []ColumnMetadata
	SortKeys []string
}

type ColumnMetadata struct {
	Name         string
	DataType     string
	Encoding     string
	DefaultValue string
	Identity     bool
}

type SideEffect struct {
	Kind   string
	Source string
	Target string
}

type Passthrough struct{}

func NewPassthrough() Passthrough {
	return Passthrough{}
}

func (Passthrough) Translate(ctx context.Context, _ Session, sql string) (TranslationResult, error) {
	if err := ctx.Err(); err != nil {
		return TranslationResult{}, err
	}
	return TranslationResult{BackendSQL: sql}, nil
}

type RedshiftToPostgres struct{}

func NewRedshiftToPostgres() RedshiftToPostgres {
	return RedshiftToPostgres{}
}

func (RedshiftToPostgres) Translate(ctx context.Context, _ Session, sql string) (TranslationResult, error) {
	if err := ctx.Err(); err != nil {
		return TranslationResult{}, err
	}
	if translated, ok, err := translateCreateExternalSchema(sql); ok || err != nil {
		translated.BackendSQL = rewriteRedshiftFunctions(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateCreateExternalTable(sql); ok || err != nil {
		translated.BackendSQL = rewriteRedshiftFunctions(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateCreateMaterializedView(sql); ok || err != nil {
		translated.BackendSQL = rewriteRedshiftFunctions(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateAlterColumnEncode(sql); ok || err != nil {
		translated.BackendSQL = rewriteRedshiftFunctions(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateAlterAddColumnDefaultIdentity(sql); ok || err != nil {
		translated.BackendSQL = rewriteRedshiftFunctions(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateCreateTable(sql); ok || err != nil {
		translated.BackendSQL = rewriteRedshiftFunctions(translated.BackendSQL)
		return translated, err
	}
	return TranslationResult{BackendSQL: rewriteRedshiftFunctions(rewriteLateBindingView(sql))}, nil
}

func rewriteRedshiftFunctions(sql string) string {
	var out strings.Builder
	for i := 0; i < len(sql); {
		ch := sql[i]
		if ch == '\'' {
			next := copyQuotedString(&out, sql, i)
			i = next
			continue
		}
		if ch == '"' {
			next := copyQuotedIdentifier(&out, sql, i)
			i = next
			continue
		}
		if isIdentifierStart(ch) {
			start := i
			i++
			for i < len(sql) && isIdentifierPart(sql[i]) {
				i++
			}
			name := sql[start:i]
			lower := strings.ToLower(name)
			switch lower {
			case "boolean":
				next := skipSpaces(sql, i)
				if rewritten, literalEnd, ok := parseRedshiftBooleanLiteral(sql, next); ok {
					out.WriteString(rewritten)
					i = literalEnd
					continue
				}
			case "getdate":
				next := skipSpaces(sql, i)
				if next < len(sql) && sql[next] == '(' {
					close := matchingParen(sql, next)
					if close > next && strings.TrimSpace(sql[next+1:close]) == "" {
						out.WriteString(PostgresCurrentTimestamp)
						i = close + 1
						continue
					}
				}
			case "sysdate":
				out.WriteString(PostgresCurrentTimestamp)
				continue
			case "nvl":
				next := skipSpaces(sql, i)
				if next < len(sql) && sql[next] == '(' {
					close := matchingParen(sql, next)
					if close > next {
						out.WriteString(PostgresCoalesce)
						out.WriteString(sql[next : close+1])
						i = close + 1
						continue
					}
				}
			case "decode":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteDecode); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "dateadd":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteDateAdd); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "datediff":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteDateDiff); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "listagg":
				if rewritten, next, ok := rewriteListAgg(sql, i); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			}
			out.WriteString(name)
			continue
		}
		out.WriteByte(ch)
		i++
	}
	return out.String()
}

func rewriteLateBindingView(sql string) string {
	keywords := []string{"with", "no", "schema", "binding"}
	out := make([]byte, 0, len(sql))
	for i := 0; i < len(sql); {
		ch := sql[i]
		if ch == '\'' {
			var quoted strings.Builder
			next := copyQuotedString(&quoted, sql, i)
			out = append(out, quoted.String()...)
			i = next
			continue
		}
		if ch == '"' {
			var quoted strings.Builder
			next := copyQuotedIdentifier(&quoted, sql, i)
			out = append(out, quoted.String()...)
			i = next
			continue
		}
		if next, ok := matchKeywordSequence(sql, i, keywords); ok {
			out = trimRightSpaces(out)
			i = next
			continue
		}
		out = append(out, ch)
		i++
	}
	return strings.TrimSpace(string(out))
}

func matchKeywordSequence(sql string, index int, keywords []string) (int, bool) {
	if index > 0 && isIdentifierPart(sql[index-1]) {
		return index, false
	}
	next := index
	for keywordIndex, keyword := range keywords {
		if keywordIndex > 0 {
			next = skipSpaces(sql, next)
			if next >= len(sql) {
				return index, false
			}
		}
		if len(sql[next:]) < len(keyword) || !strings.EqualFold(sql[next:next+len(keyword)], keyword) {
			return index, false
		}
		afterKeyword := next + len(keyword)
		if afterKeyword < len(sql) && isIdentifierPart(sql[afterKeyword]) {
			return index, false
		}
		next = afterKeyword
	}
	return next, true
}

func trimRightSpaces(value []byte) []byte {
	for len(value) > 0 {
		switch value[len(value)-1] {
		case ' ', '\t', '\n', '\r':
			value = value[:len(value)-1]
		default:
			return value
		}
	}
	return value
}

func translateCreateExternalSchema(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	const prefix = "create external schema"
	if !strings.HasPrefix(strings.ToLower(statement), prefix) {
		return TranslationResult{}, false, nil
	}

	tokens := strings.Fields(strings.TrimSpace(statement[len(prefix):]))
	fromIndex := -1
	for i := 0; i+2 < len(tokens); i++ {
		if strings.EqualFold(tokens[i], "from") && strings.EqualFold(tokens[i+1], "data") && strings.EqualFold(tokens[i+2], "catalog") {
			fromIndex = i
			break
		}
	}
	if fromIndex < 0 {
		return TranslationResult{}, false, nil
	}
	if fromIndex == 0 {
		return TranslationResult{}, true, errors.New("CREATE EXTERNAL SCHEMA FROM DATA CATALOG requires a schema name")
	}

	backendSQL := "create schema " + strings.Join(tokens[:fromIndex], " ")
	return TranslationResult{BackendSQL: backendSQL}, true, nil
}

func parseRedshiftBooleanLiteral(sql string, index int) (string, int, bool) {
	if index >= len(sql) {
		return "", index, false
	}
	if sql[index] == '\'' {
		value, next, ok := readQuotedStringValue(sql, index)
		if !ok {
			return "", index, false
		}
		rewritten, ok := postgresBooleanLiteral(value)
		return rewritten, next, ok
	}
	if sql[index] == '0' || sql[index] == '1' {
		if index+1 < len(sql) && isIdentifierPart(sql[index+1]) {
			return "", index, false
		}
		rewritten, ok := postgresBooleanLiteral(sql[index : index+1])
		return rewritten, index + 1, ok
	}
	if !isIdentifierStart(sql[index]) {
		return "", index, false
	}
	start := index
	index++
	for index < len(sql) && isIdentifierPart(sql[index]) {
		index++
	}
	rewritten, ok := postgresBooleanLiteral(sql[start:index])
	return rewritten, index, ok
}

func readQuotedStringValue(value string, start int) (string, int, bool) {
	var out strings.Builder
	for i := start + 1; i < len(value); i++ {
		if value[i] != '\'' {
			out.WriteByte(value[i])
			continue
		}
		if i+1 < len(value) && value[i+1] == '\'' {
			out.WriteByte(value[i+1])
			i++
			continue
		}
		return out.String(), i + 1, true
	}
	return "", start, false
}

func postgresBooleanLiteral(value string) (string, bool) {
	normalized := strings.Trim(strings.TrimSpace(value), `"'`)
	switch strings.ToLower(normalized) {
	case "1", "t", "true", "y", "yes":
		return "TRUE", true
	case "0", "f", "false", "n", "no":
		return "FALSE", true
	default:
		return "", false
	}
}

func rewriteParenFunction(sql string, index int, rewrite func([]string) (string, bool)) (string, int, bool) {
	open := skipSpaces(sql, index)
	if open >= len(sql) || sql[open] != '(' {
		return "", index, false
	}
	close := matchingParen(sql, open)
	if close < 0 {
		return "", index, false
	}
	args := splitCommaSeparated(sql[open+1 : close])
	rewritten, ok := rewrite(args)
	if !ok {
		return "", index, false
	}
	return rewritten, close + 1, true
}

func rewriteDecode(args []string) (string, bool) {
	if len(args) < 3 {
		return "", false
	}
	var out strings.Builder
	out.WriteString("CASE ")
	out.WriteString(strings.TrimSpace(args[0]))
	for i := 1; i+1 < len(args); i += 2 {
		out.WriteString(" WHEN ")
		out.WriteString(strings.TrimSpace(args[i]))
		out.WriteString(" THEN ")
		out.WriteString(strings.TrimSpace(args[i+1]))
	}
	if len(args)%2 == 0 {
		out.WriteString(" ELSE ")
		out.WriteString(strings.TrimSpace(args[len(args)-1]))
	}
	out.WriteString(" END")
	return out.String(), true
}

func rewriteDateAdd(args []string) (string, bool) {
	if len(args) != 3 {
		return "", false
	}
	part, ok := postgresIntervalPart(args[0])
	if !ok {
		return "", false
	}
	return strings.TrimSpace(args[2]) + " + (" + strings.TrimSpace(args[1]) + " * interval '1 " + part + "')", true
}

func rewriteDateDiff(args []string) (string, bool) {
	if len(args) != 3 {
		return "", false
	}
	part, ok := postgresDatePart(args[0])
	if !ok {
		return "", false
	}
	start := strings.TrimSpace(args[1])
	end := strings.TrimSpace(args[2])
	switch part {
	case "year":
		return "date_part('year', age(" + end + ", " + start + "))::int", true
	case "month":
		return "(date_part('year', age(" + end + ", " + start + "))::int * 12 + date_part('month', age(" + end + ", " + start + "))::int)", true
	case "day":
		return "(" + end + "::date - " + start + "::date)", true
	case "hour":
		return "floor(extract(epoch from (" + end + " - " + start + ")) / 3600)::int", true
	case "minute":
		return "floor(extract(epoch from (" + end + " - " + start + ")) / 60)::int", true
	case "second":
		return "floor(extract(epoch from (" + end + " - " + start + ")))::int", true
	default:
		return "", false
	}
}

func rewriteListAgg(sql string, index int) (string, int, bool) {
	rewritten, next, ok := rewriteParenFunction(sql, index, func(args []string) (string, bool) {
		if len(args) != 2 {
			return "", false
		}
		return PostgresStringAgg + "(" + strings.TrimSpace(args[0]) + ", " + strings.TrimSpace(args[1]), true
	})
	if !ok {
		return "", index, false
	}
	withinStart := skipSpaces(sql, next)
	if !strings.HasPrefix(strings.ToLower(sql[withinStart:]), "within") {
		return rewritten + ")", next, true
	}
	groupStart := skipSpaces(sql, withinStart+len("within"))
	if !strings.HasPrefix(strings.ToLower(sql[groupStart:]), "group") {
		return rewritten + ")", next, true
	}
	open := skipSpaces(sql, groupStart+len("group"))
	if open >= len(sql) || sql[open] != '(' {
		return rewritten + ")", next, true
	}
	close := matchingParen(sql, open)
	if close < 0 {
		return rewritten + ")", next, true
	}
	orderBy := strings.TrimSpace(sql[open+1 : close])
	if strings.HasPrefix(strings.ToLower(orderBy), "order by") {
		orderBy = strings.TrimSpace(orderBy[len("order by"):])
		if orderBy != "" {
			return rewritten + " ORDER BY " + orderBy + ")", close + 1, true
		}
	}
	return rewritten + ")", next, true
}

func postgresIntervalPart(value string) (string, bool) {
	return postgresDatePart(value)
}

func postgresDatePart(value string) (string, bool) {
	switch strings.ToLower(cleanIdentifier(value)) {
	case "year", "yy", "yyyy":
		return "year", true
	case "month", "mon", "mm":
		return "month", true
	case "day", "d", "dd":
		return "day", true
	case "hour", "h", "hh":
		return "hour", true
	case "minute", "m", "mi", "n":
		return "minute", true
	case "second", "s", "sec", "ss":
		return "second", true
	default:
		return "", false
	}
}

func copyQuotedString(out *strings.Builder, value string, start int) int {
	for i := start; i < len(value); i++ {
		out.WriteByte(value[i])
		if value[i] != '\'' {
			continue
		}
		if i > start && i+1 < len(value) && value[i+1] == '\'' {
			out.WriteByte(value[i+1])
			i++
			continue
		}
		if i > start {
			return i + 1
		}
	}
	return len(value)
}

func copyQuotedIdentifier(out *strings.Builder, value string, start int) int {
	for i := start; i < len(value); i++ {
		out.WriteByte(value[i])
		if value[i] != '"' {
			continue
		}
		if i > start && i+1 < len(value) && value[i+1] == '"' {
			out.WriteByte(value[i+1])
			i++
			continue
		}
		if i > start {
			return i + 1
		}
	}
	return len(value)
}

func skipSpaces(value string, index int) int {
	for index < len(value) && (value[index] == ' ' || value[index] == '\t' || value[index] == '\n' || value[index] == '\r') {
		index++
	}
	return index
}

func isIdentifierStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || (ch >= '0' && ch <= '9') || ch == '$'
}

func translateCreateExternalTable(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	lower := strings.ToLower(statement)
	if !strings.HasPrefix(lower, "create external table") {
		return TranslationResult{}, false, nil
	}
	open := strings.IndexByte(statement, '(')
	if open < 0 {
		return TranslationResult{BackendSQL: statement[:len("create ")] + statement[len("create external "):]}, true, nil
	}
	close := matchingParen(statement, open)
	if close < 0 {
		return TranslationResult{}, true, errors.New("CREATE EXTERNAL TABLE has an unterminated column list")
	}

	namePart := strings.TrimSpace(statement[len("create external table"):open])
	if strings.HasPrefix(strings.ToLower(namePart), "if not exists ") {
		namePart = strings.TrimSpace(namePart[len("if not exists "):])
	}
	schemaName, tableName := parseQualifiedName(namePart)
	cleanColumns, columns, columnDistKey, columnSortKeys, err := translateColumnDefinitions(statement[open+1 : close])
	if err != nil {
		return TranslationResult{}, true, err
	}

	effect := MetadataEffect{
		Kind:     MetadataEffectCreateTable,
		Schema:   schemaName,
		Table:    tableName,
		Name:     columnDistKey,
		Columns:  columns,
		SortKeys: columnSortKeys,
	}
	prefix := statement[:len("create ")] + statement[len("create external "):open+1]
	backendSQL := strings.TrimSpace(prefix + strings.Join(cleanColumns, ", ") + ")")
	return TranslationResult{BackendSQL: backendSQL, MetadataEffects: []MetadataEffect{effect}}, true, nil
}

func translateCreateMaterializedView(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	const prefix = "create materialized view"
	if !strings.HasPrefix(strings.ToLower(statement), prefix) {
		return TranslationResult{}, false, nil
	}

	asIndex := findTopLevelKeyword(statement, "as", len(prefix))
	if asIndex < 0 {
		return TranslationResult{BackendSQL: statement}, true, nil
	}

	header, removed := removeKeywordSequence(statement[:asIndex], []string{"auto", "refresh", "yes"})
	if !removed {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	backendSQL := strings.TrimSpace(header + " " + strings.TrimSpace(statement[asIndex:]))
	return TranslationResult{BackendSQL: backendSQL}, true, nil
}

func translateAlterColumnEncode(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	tokens := strings.Fields(statement)
	if len(tokens) < 7 || !strings.EqualFold(tokens[0], "alter") || !strings.EqualFold(tokens[1], "table") {
		return TranslationResult{}, false, nil
	}

	tableIndex := 2
	if len(tokens) > tableIndex+2 && strings.EqualFold(tokens[tableIndex], "if") && strings.EqualFold(tokens[tableIndex+1], "exists") {
		tableIndex += 2
	}
	alterIndex := tableIndex + 1
	if alterIndex >= len(tokens) || !strings.EqualFold(tokens[alterIndex], "alter") {
		return TranslationResult{}, false, nil
	}

	columnIndex := alterIndex + 1
	if columnIndex < len(tokens) && strings.EqualFold(tokens[columnIndex], "column") {
		columnIndex++
	}
	encodeIndex := columnIndex + 1
	if encodeIndex+1 >= len(tokens) || !strings.EqualFold(tokens[encodeIndex], "encode") {
		return TranslationResult{}, false, nil
	}
	if encodeIndex+2 != len(tokens) {
		return TranslationResult{}, false, nil
	}

	tablePrefix := "alter table "
	if tableIndex == 4 {
		tablePrefix += "if exists "
	}
	backendSQL := tablePrefix + tokens[tableIndex] + " alter column " + tokens[columnIndex] + " set statistics -1"
	return TranslationResult{BackendSQL: backendSQL}, true, nil
}

func translateAlterAddColumnDefaultIdentity(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	tokens := strings.Fields(statement)
	if len(tokens) < 7 || !strings.EqualFold(tokens[0], "alter") || !strings.EqualFold(tokens[1], "table") {
		return TranslationResult{}, false, nil
	}

	tableIndex := 2
	if len(tokens) > tableIndex+2 && strings.EqualFold(tokens[tableIndex], "if") && strings.EqualFold(tokens[tableIndex+1], "exists") {
		tableIndex += 2
	}
	addIndex := tableIndex + 1
	if addIndex >= len(tokens) || !strings.EqualFold(tokens[addIndex], "add") {
		return TranslationResult{}, false, nil
	}

	columnIndex := addIndex + 1
	if columnIndex < len(tokens) && strings.EqualFold(tokens[columnIndex], "column") {
		columnIndex++
	}
	if columnIndex+3 > len(tokens) {
		return TranslationResult{}, false, nil
	}

	definitionTokens := tokens[columnIndex:]
	defaultIndex := -1
	for i := 2; i < len(definitionTokens); i++ {
		if strings.EqualFold(definitionTokens[i], "default") {
			defaultIndex = i
			break
		}
	}
	if defaultIndex < 0 {
		return TranslationResult{}, false, nil
	}

	identityClause, consumed, ok := parseDefaultIdentityClause(definitionTokens, defaultIndex)
	if !ok {
		return TranslationResult{}, false, nil
	}

	cleanDefinition := []string{definitionTokens[0], postgresColumnType(definitionTokens[1])}
	cleanDefinition = append(cleanDefinition, definitionTokens[2:defaultIndex]...)
	cleanDefinition = append(cleanDefinition, identityClause)
	cleanDefinition = append(cleanDefinition, definitionTokens[defaultIndex+consumed:]...)

	tablePrefix := "alter table "
	if tableIndex == 4 {
		tablePrefix += "if exists "
	}
	backendSQL := tablePrefix + tokens[tableIndex] + " add column " + strings.Join(cleanDefinition, " ")
	return TranslationResult{BackendSQL: backendSQL}, true, nil
}

func parseDefaultIdentityClause(tokens []string, defaultIndex int) (string, int, bool) {
	if defaultIndex+1 >= len(tokens) || !strings.EqualFold(tokens[defaultIndex], "default") {
		return "", 0, false
	}

	var identityText string
	for i := defaultIndex + 1; i < len(tokens); i++ {
		if identityText != "" {
			identityText += " "
		}
		identityText += tokens[i]
		trimmed := strings.TrimSpace(identityText)
		lower := strings.ToLower(trimmed)
		if !strings.HasPrefix(lower, "identity") {
			return "", 0, false
		}

		open := strings.IndexByte(trimmed, '(')
		if open < 0 {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(trimmed[:open]), "identity") {
			return "", 0, false
		}
		close := matchingParen(trimmed, open)
		if close < 0 {
			continue
		}
		if strings.TrimSpace(trimmed[close+1:]) != "" {
			return "", 0, false
		}

		args := splitCommaSeparated(trimmed[open+1 : close])
		if len(args) != 2 {
			return "", 0, false
		}
		start := strings.TrimSpace(args[0])
		increment := strings.TrimSpace(args[1])
		if start == "" || increment == "" {
			return "", 0, false
		}
		clause := "generated by default as identity (start with " + start + " increment by " + increment + ")"
		return clause, i - defaultIndex + 1, true
	}
	return "", 0, false
}

func translateCreateTable(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	if !strings.HasPrefix(strings.ToLower(statement), "create table") {
		return TranslationResult{}, false, nil
	}
	open := strings.IndexByte(statement, '(')
	if open < 0 {
		return TranslationResult{BackendSQL: sql}, true, nil
	}
	close := matchingParen(statement, open)
	if close < 0 {
		return TranslationResult{}, true, errors.New("CREATE TABLE has an unterminated column list")
	}

	namePart := strings.TrimSpace(statement[len("create table"):open])
	if strings.HasPrefix(strings.ToLower(namePart), "if not exists ") {
		namePart = strings.TrimSpace(namePart[len("if not exists "):])
	}
	schemaName, tableName := parseQualifiedName(namePart)
	cleanColumns, columns, columnDistKey, columnSortKeys, err := translateColumnDefinitions(statement[open+1 : close])
	if err != nil {
		return TranslationResult{}, true, err
	}
	cleanRest, distStyle, distKey, sortKeys, backup := translateTableAttributes(statement[close+1:])
	if distKey == "" {
		distKey = columnDistKey
	}
	for _, key := range columnSortKeys {
		if !containsIdentifier(sortKeys, key) {
			sortKeys = append(sortKeys, key)
		}
	}
	if distStyle == "" && distKey != "" {
		distStyle = "key"
	}

	effect := MetadataEffect{
		Kind:     MetadataEffectCreateTable,
		Schema:   schemaName,
		Table:    tableName,
		Name:     distKey,
		Value:    distStyle,
		Backup:   backup,
		Columns:  columns,
		SortKeys: sortKeys,
	}
	backendSQL := strings.TrimSpace(statement[:open+1] + strings.Join(cleanColumns, ", ") + ")" + cleanRest)
	return TranslationResult{BackendSQL: backendSQL, MetadataEffects: []MetadataEffect{effect}}, true, nil
}

func translateColumnDefinitions(value string) ([]string, []ColumnMetadata, string, []string, error) {
	definitions := splitCommaSeparated(value)
	cleaned := make([]string, 0, len(definitions))
	columns := make([]ColumnMetadata, 0, len(definitions))
	distKey := ""
	var sortKeys []string
	for _, definition := range definitions {
		tokens := strings.Fields(strings.TrimSpace(definition))
		if len(tokens) < 2 {
			return nil, nil, "", nil, errors.New("CREATE TABLE column definition requires name and type")
		}
		columnName := cleanIdentifier(tokens[0])
		if columnName == "" {
			return nil, nil, "", nil, errors.New("CREATE TABLE column name cannot be empty")
		}
		column := ColumnMetadata{Name: columnName, DataType: strings.ToLower(tokens[1])}
		columnType := postgresColumnType(tokens[1])
		cleanTokens := []string{tokens[0], columnType}
		if byteLimit, ok := redshiftByteLimitedStringType(tokens[1]); ok {
			cleanTokens = append(cleanTokens, "check", "(octet_length("+tokens[0]+") <= "+byteLimit+")")
		}
		for i := 2; i < len(tokens); i++ {
			token := strings.ToLower(tokens[i])
			switch {
			case token == "encode" && i+1 < len(tokens):
				column.Encoding = cleanIdentifier(tokens[i+1])
				i++
			case token == "default" && i+1 < len(tokens) && !strings.EqualFold(tokens[i+1], "as"):
				defaultValue := tokens[i+1]
				if isBooleanColumnType(column.DataType) {
					if rewritten, ok := postgresBooleanLiteral(defaultValue); ok {
						defaultValue = rewritten
					}
				}
				column.DefaultValue = tokens[i+1]
				cleanTokens = append(cleanTokens, tokens[i], defaultValue)
				i++
			case token == "identity" || strings.HasPrefix(token, "identity("):
				column.Identity = true
				cleanTokens = append(cleanTokens, "generated", "by", "default", "as", "identity")
			case token == "generated":
				column.Identity = hasIdentityToken(tokens[i+1:])
				cleanTokens = append(cleanTokens, tokens[i:]...)
				i = len(tokens)
			case token == "distkey":
				distKey = columnName
			case token == "sortkey":
				if !containsIdentifier(sortKeys, columnName) {
					sortKeys = append(sortKeys, columnName)
				}
			default:
				cleanTokens = append(cleanTokens, tokens[i])
			}
		}
		cleaned = append(cleaned, strings.Join(cleanTokens, " "))
		columns = append(columns, column)
	}
	return cleaned, columns, distKey, sortKeys, nil
}

func postgresColumnType(value string) string {
	switch {
	case strings.EqualFold(value, "timestamp"):
		return "timestamp(6) without time zone"
	case strings.EqualFold(value, "timestamptz"):
		return "timestamp(6) without time zone"
	case strings.EqualFold(value, "time"):
		return "time(6) without time zone"
	case strings.EqualFold(value, "timetz"):
		return "time(6) with time zone"
	case strings.EqualFold(value, "super"):
		return "jsonb"
	case strings.EqualFold(value, "hllsketch"):
		return "bytea"
	case strings.EqualFold(value, "varbyte"):
		return "bytea"
	case strings.EqualFold(value, "geometry"), strings.EqualFold(value, "geography"):
		return "text"
	}
	if _, ok := redshiftByteLimitedStringType(value); ok {
		return "text"
	}
	return value
}

func redshiftByteLimitedStringType(value string) (string, bool) {
	lower := strings.ToLower(strings.TrimSpace(value))
	for _, prefix := range []string{"varchar", "char"} {
		if !strings.HasPrefix(lower, prefix+"(") || !strings.HasSuffix(lower, ")") {
			continue
		}
		limit := strings.TrimSpace(lower[len(prefix)+1 : len(lower)-1])
		if limit == "" {
			return "", false
		}
		for i := 0; i < len(limit); i++ {
			if limit[i] < '0' || limit[i] > '9' {
				return "", false
			}
		}
		return limit, true
	}
	return "", false
}

func isBooleanColumnType(value string) bool {
	return strings.EqualFold(value, "bool") || strings.EqualFold(value, "boolean")
}

func translateTableAttributes(value string) (string, string, string, []string, string) {
	tokens := strings.Fields(value)
	cleanTokens := make([]string, 0, len(tokens))
	distStyle := ""
	distKey := ""
	var sortKeys []string
	backup := ""
	for i := 0; i < len(tokens); i++ {
		token := strings.ToLower(tokens[i])
		switch {
		case token == "diststyle" && i+1 < len(tokens):
			distStyle = strings.ToLower(cleanIdentifier(tokens[i+1]))
			i++
		case strings.HasPrefix(token, "distkey"):
			if key := parseParenthesizedIdentifier(tokens[i], "distkey"); key != "" {
				distKey = key
			} else if i+1 < len(tokens) {
				distKey = parseParenthesizedIdentifier(tokens[i+1], "")
				i++
			}
		case strings.HasPrefix(token, "sortkey"):
			if keys := parseParenthesizedIdentifierList(tokens[i], "sortkey"); len(keys) > 0 {
				sortKeys = keys
			} else if i+1 < len(tokens) {
				sortKeys = parseParenthesizedIdentifierList(tokens[i+1], "")
				i++
			}
		case token == "backup" && i+1 < len(tokens):
			backup = strings.ToLower(cleanIdentifier(tokens[i+1]))
			i++
		default:
			cleanTokens = append(cleanTokens, tokens[i])
		}
	}
	if len(cleanTokens) == 0 {
		return "", distStyle, distKey, sortKeys, backup
	}
	return " " + strings.Join(cleanTokens, " "), distStyle, distKey, sortKeys, backup
}

func hasIdentityToken(tokens []string) bool {
	for _, token := range tokens {
		lower := strings.ToLower(token)
		if lower == "identity" || strings.HasPrefix(lower, "identity(") {
			return true
		}
	}
	return false
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

func findTopLevelKeyword(value string, keyword string, start int) int {
	depth := 0
	inString := false
	inQuotedIdentifier := false
	lowerKeyword := strings.ToLower(keyword)
	for i := start; i < len(value); i++ {
		ch := value[i]
		if ch == '\'' && !inQuotedIdentifier {
			if inString && i+1 < len(value) && value[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if ch == '"' && !inString {
			if inQuotedIdentifier && i+1 < len(value) && value[i+1] == '"' {
				i++
				continue
			}
			inQuotedIdentifier = !inQuotedIdentifier
			continue
		}
		if inString || inQuotedIdentifier {
			continue
		}
		switch ch {
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || i+len(keyword) > len(value) {
			continue
		}
		if strings.ToLower(value[i:i+len(keyword)]) != lowerKeyword {
			continue
		}
		beforeOK := i == 0 || !isIdentifierPart(value[i-1])
		afterOK := i+len(keyword) == len(value) || !isIdentifierPart(value[i+len(keyword)])
		if beforeOK && afterOK {
			return i
		}
	}
	return -1
}

func removeKeywordSequence(value string, sequence []string) (string, bool) {
	tokens := strings.Fields(value)
	for i := 0; i+len(sequence) <= len(tokens); i++ {
		matched := true
		for j, keyword := range sequence {
			if !strings.EqualFold(tokens[i+j], keyword) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		cleaned := append(append([]string{}, tokens[:i]...), tokens[i+len(sequence):]...)
		return strings.Join(cleaned, " "), true
	}
	return value, false
}

func parseQualifiedName(value string) (string, string) {
	value = strings.TrimSpace(value)
	fields := strings.Fields(value)
	if len(fields) > 0 {
		value = fields[0]
	}
	parts := strings.Split(value, ".")
	if len(parts) == 1 {
		return "public", cleanIdentifier(parts[0])
	}
	return cleanIdentifier(parts[len(parts)-2]), cleanIdentifier(parts[len(parts)-1])
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

func cleanIdentifier(value string) string {
	return strings.Trim(strings.TrimSpace(value), `"`)
}

func containsIdentifier(values []string, value string) bool {
	for _, item := range values {
		if strings.EqualFold(item, value) {
			return true
		}
	}
	return false
}

const (
	MetadataEffectCreateTable = "CREATE_TABLE"
	MetadataEffectDistStyle   = "DISTSTYLE"
	MetadataEffectDistKey     = "DISTKEY"
	MetadataEffectSortKey     = "SORTKEY"
	MetadataEffectEncode      = "ENCODE"
	MetadataEffectBackup      = "BACKUP"
	MetadataEffectIdentity    = "IDENTITY"
	MetadataEffectDefault     = "DEFAULT"

	SideEffectCopy   = "COPY"
	SideEffectUnload = "UNLOAD"

	RewriteGetDate  = "GETDATE"
	RewriteSysdate  = "SYSDATE"
	RewriteNVL      = "NVL"
	RewriteDecode   = "DECODE"
	RewriteDateAdd  = "DATEADD"
	RewriteDateDiff = "DATEDIFF"
	RewriteListAgg  = "LISTAGG"

	PostgresCoalesce         = "COALESCE"
	PostgresCurrentTimestamp = "CURRENT_TIMESTAMP"
	PostgresStringAgg        = "string_agg"
)
