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
	if translated, ok, err := translateCreateTable(sql); ok || err != nil {
		translated.BackendSQL = rewriteRedshiftFunctions(translated.BackendSQL)
		return translated, err
	}
	return TranslationResult{BackendSQL: rewriteRedshiftFunctions(sql)}, nil
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
		cleanTokens := []string{tokens[0], postgresColumnType(tokens[1])}
		for i := 2; i < len(tokens); i++ {
			token := strings.ToLower(tokens[i])
			switch {
			case token == "encode" && i+1 < len(tokens):
				column.Encoding = cleanIdentifier(tokens[i+1])
				i++
			case token == "default" && i+1 < len(tokens) && !strings.EqualFold(tokens[i+1], "as"):
				column.DefaultValue = tokens[i+1]
				cleanTokens = append(cleanTokens, tokens[i], tokens[i+1])
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
	case strings.EqualFold(value, "super"):
		return "jsonb"
	case strings.EqualFold(value, "hllsketch"):
		return "bytea"
	case strings.EqualFold(value, "varbyte"):
		return "bytea"
	case strings.EqualFold(value, "geometry"), strings.EqualFold(value, "geography"):
		return "text"
	}
	return value
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
