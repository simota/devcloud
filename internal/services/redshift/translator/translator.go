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
	sql = translateSelectTopLimit(sql)
	sql = rewriteLateralColumnAliases(sql)
	sql = rewriteNullOrderingDefaults(sql)
	if translated, ok, err := translateCreateExternalSchema(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateCreateExternalTable(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateCreateMaterializedView(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateMergeInto(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateInsertSelectReturning(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateInsertValuesDefault(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateAlterColumnEncode(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateAlterAddColumnDefaultIdentity(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateTruncateImmediateCommit(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateQualifySelect(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateGrantAssumeRole(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	if translated, ok, err := translateCreateTable(sql); ok || err != nil {
		translated.BackendSQL = rewritePostgresCompatibility(translated.BackendSQL)
		return translated, err
	}
	return TranslationResult{BackendSQL: rewritePostgresCompatibility(rewriteLateBindingView(sql))}, nil
}

func translateSelectTopLimit(sql string) string {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	selectEnd, ok := matchKeywordSequence(statement, 0, []string{"select"})
	if !ok {
		return sql
	}
	topStart := skipSpaces(statement, selectEnd)
	topEnd, ok := matchKeywordSequence(statement, topStart, []string{"top"})
	if !ok {
		return sql
	}
	limitStart := skipSpaces(statement, topEnd)
	limit, limitEnd, ok := parseTopLimit(statement, limitStart)
	if !ok {
		return sql
	}
	selectList := strings.TrimSpace(statement[limitEnd:])
	if selectList == "" {
		return sql
	}
	return strings.TrimSpace(statement[:selectEnd]) + " " + selectList + " limit " + limit
}

func parseTopLimit(sql string, index int) (string, int, bool) {
	if index >= len(sql) {
		return "", index, false
	}
	if sql[index] == '(' {
		close := matchingParen(sql, index)
		if close < 0 {
			return "", index, false
		}
		limit := strings.TrimSpace(sql[index+1 : close])
		if !isUnsignedInteger(limit) {
			return "", index, false
		}
		return limit, close + 1, true
	}
	start := index
	for index < len(sql) && sql[index] >= '0' && sql[index] <= '9' {
		index++
	}
	if index == start || (index < len(sql) && isIdentifierPart(sql[index])) {
		return "", start, false
	}
	return sql[start:index], index, true
}

func isUnsignedInteger(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func rewriteNullOrderingDefaults(sql string) string {
	orderStart, orderEnd := findTopLevelKeywordSequence(sql, []string{"order", "by"}, 0)
	if orderStart < 0 {
		return sql
	}
	orderListEnd := findTopLevelOrderByEnd(sql, orderEnd)
	orderList := strings.TrimSpace(sql[orderEnd:orderListEnd])
	if orderList == "" {
		return sql
	}

	items := splitCommaSeparated(orderList)
	rewrittenItems := make([]string, 0, len(items))
	changed := false
	for _, item := range items {
		rewritten, itemChanged := rewriteOrderByNullDefault(item)
		rewrittenItems = append(rewrittenItems, rewritten)
		changed = changed || itemChanged
	}
	if !changed {
		return sql
	}

	prefix := strings.TrimRight(sql[:orderEnd], " \t\n\r")
	suffix := sql[orderListEnd:]
	separator := ""
	if suffix != "" && !strings.ContainsAny(suffix[:1], " \t\n\r;,)") {
		separator = " "
	}
	return prefix + " " + strings.Join(rewrittenItems, ", ") + separator + suffix
}

func rewriteOrderByNullDefault(item string) (string, bool) {
	trimmed := strings.TrimSpace(item)
	if trimmed == "" {
		return item, false
	}
	if _, end := findTopLevelKeywordSequence(trimmed, []string{"nulls", "first"}, 0); end >= 0 {
		return trimmed, false
	}
	if _, end := findTopLevelKeywordSequence(trimmed, []string{"nulls", "last"}, 0); end >= 0 {
		return trimmed, false
	}
	if _, end := findTopLevelKeywordSequence(trimmed, []string{"desc"}, 0); end >= 0 {
		return trimmed + " NULLS FIRST", true
	}
	return trimmed + " NULLS LAST", true
}

func findTopLevelOrderByEnd(sql string, start int) int {
	end := len(sql)
	if semicolon := findTopLevelKeywordTerminator(sql, start, []string{";"}); semicolon >= 0 {
		end = semicolon
	}
	for _, sequence := range [][]string{
		{"limit"},
		{"offset"},
		{"fetch"},
		{"for"},
	} {
		if sequenceStart, _ := findTopLevelKeywordSequence(sql, sequence, start); sequenceStart >= 0 && sequenceStart < end {
			end = sequenceStart
		}
	}
	return end
}

func findTopLevelKeywordTerminator(sql string, start int, terminators []string) int {
	depth := 0
	inString := false
	inQuotedIdentifier := false
	for i := start; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' && !inQuotedIdentifier {
			if inString && i+1 < len(sql) && sql[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if ch == '"' && !inString {
			if inQuotedIdentifier && i+1 < len(sql) && sql[i+1] == '"' {
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
		if depth != 0 {
			continue
		}
		for _, terminator := range terminators {
			if sql[i:i+1] == terminator {
				return i
			}
		}
	}
	return -1
}

func rewriteLateralColumnAliases(sql string) string {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	selectEnd, ok := matchKeywordSequence(statement, 0, []string{"select"})
	if !ok {
		return sql
	}

	selectListEnd := len(statement)
	fromStart, _ := findTopLevelKeywordSequence(statement, []string{"from"}, selectEnd)
	if fromStart >= 0 {
		selectListEnd = fromStart
	}
	selectList := strings.TrimSpace(statement[selectEnd:selectListEnd])
	if selectList == "" {
		return sql
	}
	selectModifier := ""
	for _, keyword := range []string{"all", "distinct"} {
		if modifierEnd, ok := matchKeywordSequence(selectList, 0, []string{keyword}); ok {
			selectModifier = strings.TrimSpace(selectList[:modifierEnd])
			selectList = strings.TrimSpace(selectList[modifierEnd:])
			break
		}
	}

	aliases := map[string]string{}
	items := splitCommaSeparated(selectList)
	rewrittenItems := make([]string, 0, len(items))
	changed := false
	for _, item := range items {
		expression, alias, hasAlias := splitSelectAlias(item)
		rewrittenExpression, expressionChanged := replaceLateralAliasReferences(expression, aliases)
		changed = changed || expressionChanged
		if hasAlias {
			rewrittenItems = append(rewrittenItems, strings.TrimSpace(rewrittenExpression)+" as "+alias)
			if cleaned := cleanIdentifier(alias); cleaned != "" {
				aliases[strings.ToLower(cleaned)] = strings.TrimSpace(rewrittenExpression)
			}
			continue
		}
		rewrittenItems = append(rewrittenItems, strings.TrimSpace(rewrittenExpression))
	}
	if !changed {
		return sql
	}

	suffix := strings.TrimSpace(statement[selectListEnd:])
	selectPrefix := "select "
	if selectModifier != "" {
		selectPrefix += selectModifier + " "
	}
	backendSQL := selectPrefix + strings.Join(rewrittenItems, ", ")
	if suffix != "" {
		backendSQL += " " + suffix
	}
	return backendSQL
}

func splitSelectAlias(item string) (string, string, bool) {
	asStart, asEnd := findTopLevelKeywordSequence(item, []string{"as"}, 0)
	if asStart < 0 {
		return splitImplicitSelectAlias(item)
	}
	expression := strings.TrimSpace(item[:asStart])
	alias := strings.TrimSpace(item[asEnd:])
	if expression == "" || alias == "" || strings.ContainsAny(alias, " \t\n\r") {
		return item, "", false
	}
	return expression, alias, true
}

func splitImplicitSelectAlias(item string) (string, string, bool) {
	end := len(item)
	for end > 0 && (item[end-1] == ' ' || item[end-1] == '\t' || item[end-1] == '\n' || item[end-1] == '\r') {
		end--
	}
	if end == 0 {
		return item, "", false
	}

	start := end
	if item[start-1] == '"' {
		start--
		for start > 0 {
			start--
			if item[start] != '"' {
				continue
			}
			if start > 0 && item[start-1] == '"' {
				start--
				continue
			}
			break
		}
	} else {
		for start > 0 && isIdentifierPart(item[start-1]) {
			start--
		}
		if start == end || !isIdentifierStart(item[start]) {
			return item, "", false
		}
	}

	if start == 0 || (item[start-1] != ' ' && item[start-1] != '\t' && item[start-1] != '\n' && item[start-1] != '\r') {
		return item, "", false
	}
	expression := strings.TrimSpace(item[:start])
	alias := strings.TrimSpace(item[start:end])
	if expression == "" || alias == "" {
		return item, "", false
	}
	return expression, alias, true
}

func replaceLateralAliasReferences(value string, aliases map[string]string) (string, bool) {
	if len(aliases) == 0 {
		return value, false
	}
	var out strings.Builder
	changed := false
	for i := 0; i < len(value); {
		ch := value[i]
		if ch == '\'' {
			next := copyQuotedString(&out, value, i)
			i = next
			continue
		}
		if ch == '"' {
			next := copyQuotedIdentifier(&out, value, i)
			i = next
			continue
		}
		if !isIdentifierStart(ch) {
			out.WriteByte(ch)
			i++
			continue
		}

		start := i
		i++
		for i < len(value) && isIdentifierPart(value[i]) {
			i++
		}
		identifier := value[start:i]
		if expression, ok := aliases[strings.ToLower(identifier)]; ok && !isQualifiedIdentifierPart(value, start, i) {
			out.WriteByte('(')
			out.WriteString(expression)
			out.WriteByte(')')
			changed = true
			continue
		}
		out.WriteString(identifier)
	}
	if !changed {
		return value, false
	}
	return out.String(), true
}

func isQualifiedIdentifierPart(value string, start, end int) bool {
	before := start - 1
	for before >= 0 && (value[before] == ' ' || value[before] == '\t' || value[before] == '\n' || value[before] == '\r') {
		before--
	}
	if before >= 0 && value[before] == '.' {
		return true
	}
	after := skipSpaces(value, end)
	return after < len(value) && value[after] == '.'
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
			if rewritten, next, ok := rewritePartiQLNavigation(sql, start, i); ok {
				out.WriteString(rewritten)
				i = next
				continue
			}
			lower := strings.ToLower(name)
			switch lower {
			case "approximate":
				if rewritten, next, ok := rewriteApproximateCountDistinct(sql, i); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "bit_and":
				next := skipSpaces(sql, i)
				if next < len(sql) && sql[next] == '(' && matchingParen(sql, next) > next {
					out.WriteString(PostgresBoolAnd)
					continue
				}
			case "bit_or":
				next := skipSpaces(sql, i)
				if next < len(sql) && sql[next] == '(' && matchingParen(sql, next) > next {
					out.WriteString(PostgresBoolOr)
					continue
				}
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
			case "timeofday":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteTimeOfDay); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "rand":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteRand); ok {
					out.WriteString(rewritten)
					i = next
					continue
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
			case "nvl2":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteNVL2); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "len":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteLen); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "charindex":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteCharIndex); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "substring":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteSubstring); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "split_part":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteSplitPart); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "strtol":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteStrtol); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "crc32":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteCRC32); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "md5_digest":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteMD5Digest); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "func_sha1":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteFuncSHA1); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "regexp_substr":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteRegexpSubstr); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "regexp_count":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteRegexpCount); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "regexp_instr":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteRegexpInstr); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "decode":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteDecode); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "greatest":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteGreatest); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "least":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteLeast); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "round":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteRound); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "json_extract_path_text":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteJSONExtractPathText); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "json_extract_array_element_text":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteJSONExtractArrayElementText); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "json_array_length":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteJSONArrayLength); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "json_parse":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteJSONParse); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "is_valid_json":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteIsValidJSON); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "is_valid_json_array":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteIsValidJSONArray); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "object_transform":
				if rewritten, next, ok := rewriteObjectTransformCall(sql, i); ok {
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
			case "convert_timezone":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteConvertTimezone); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "date_part":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteDatePartFunction); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "date_trunc":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteDateTruncFunction); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "last_day":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteLastDay); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "months_between":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteMonthsBetween); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "add_months":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteAddMonths); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "next_day":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteNextDay); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "to_date":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteToDate); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "to_timestamp":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteToTimestamp); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "to_char":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteToChar); ok {
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
			case "median":
				if rewritten, next, ok := rewriteParenFunction(sql, i, rewriteMedian); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "ratio_to_report":
				if rewritten, next, ok := rewriteRatioToReport(sql, i); ok {
					out.WriteString(rewritten)
					i = next
					continue
				}
			case "like":
				if rewritten, next, ok := rewriteLikeDefaultEscape(sql, start, i); ok {
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

type partiQLNavigationStep struct {
	value     string
	subscript bool
}

func rewritePartiQLNavigation(sql string, start, end int) (string, int, bool) {
	next := end
	steps := []partiQLNavigationStep{}
	hasSubscript := false
	for next < len(sql) {
		switch sql[next] {
		case '.':
			keyStart := next + 1
			if keyStart >= len(sql) || !isIdentifierStart(sql[keyStart]) {
				return "", start, false
			}
			keyEnd := keyStart + 1
			for keyEnd < len(sql) && isIdentifierPart(sql[keyEnd]) {
				keyEnd++
			}
			steps = append(steps, partiQLNavigationStep{value: sql[keyStart:keyEnd]})
			next = keyEnd
		case '[':
			close := matchingBracket(sql, next)
			if close < 0 {
				return "", start, false
			}
			index := strings.TrimSpace(sql[next+1 : close])
			if index == "" {
				return "", start, false
			}
			steps = append(steps, partiQLNavigationStep{value: index, subscript: true})
			hasSubscript = true
			next = close + 1
		default:
			if len(steps) == 0 || (!hasSubscript && len(steps) < 2) {
				return "", start, false
			}
			return postgresPartiQLNavigation(sql[start:end], steps), next, true
		}
	}
	if len(steps) == 0 || (!hasSubscript && len(steps) < 2) {
		return "", start, false
	}
	return postgresPartiQLNavigation(sql[start:end], steps), next, true
}

func postgresPartiQLNavigation(base string, steps []partiQLNavigationStep) string {
	var out strings.Builder
	out.WriteByte('(')
	out.WriteString(base)
	out.WriteString(")::jsonb")
	for i, step := range steps {
		if i == len(steps)-1 {
			out.WriteString(" ->> ")
		} else {
			out.WriteString(" -> ")
		}
		if step.subscript {
			out.WriteString(step.value)
			continue
		}
		out.WriteString(sqlStringLiteral(step.value))
	}
	return out.String()
}

func matchingBracket(value string, open int) int {
	if open < 0 || open >= len(value) || value[open] != '[' {
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
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func rewriteApproximateCountDistinct(sql string, index int) (string, int, bool) {
	countStart := skipSpaces(sql, index)
	countEnd, ok := matchKeywordSequence(sql, countStart, []string{"count"})
	if !ok {
		return "", index, false
	}
	open := skipSpaces(sql, countEnd)
	if open >= len(sql) || sql[open] != '(' {
		return "", index, false
	}
	close := matchingParen(sql, open)
	if close < 0 {
		return "", index, false
	}
	args := strings.TrimSpace(sql[open+1 : close])
	distinctEnd, ok := matchKeywordSequence(args, 0, []string{"distinct"})
	if !ok || strings.TrimSpace(args[distinctEnd:]) == "" {
		return "", index, false
	}
	return sql[countStart : close+1], close + 1, true
}

func rewriteLikeDefaultEscape(sql string, keywordStart, keywordEnd int) (string, int, bool) {
	patternStart := skipSpaces(sql, keywordEnd)
	if patternStart >= len(sql) || sql[patternStart] != '\'' {
		return "", keywordStart, false
	}
	patternValue, patternEnd, ok := readQuotedStringValue(sql, patternStart)
	if !ok || !strings.Contains(patternValue, `\`) {
		return "", keywordStart, false
	}
	next := skipSpaces(sql, patternEnd)
	if _, ok := matchKeywordSequence(sql, next, []string{"escape"}); ok {
		return "", keywordStart, false
	}
	return sql[keywordStart:patternEnd] + " ESCAPE '\\'", patternEnd, true
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

func rewritePostgresCompatibility(sql string) string {
	return rewriteRedshiftFunctions(rewriteRedshiftSystemTables(rewriteBeginTransactionModes(rewriteResetCommand(sql))))
}

func rewriteResetCommand(sql string) string {
	start := skipSpaces(sql, 0)
	resetEnd, ok := matchKeywordSequence(sql, start, []string{"reset"})
	if !ok {
		return sql
	}

	targetStart := skipSpaces(sql, resetEnd)
	target, targetEnd, ok := parseResetTarget(sql, targetStart)
	if !ok {
		return sql
	}
	next := skipSpaces(sql, targetEnd)
	suffix := ""
	if next < len(sql) {
		if sql[next] != ';' || strings.TrimSpace(sql[next+1:]) != "" {
			return sql
		}
		suffix = ";"
	}

	switch {
	case strings.EqualFold(target, "all"):
		return sql
	case strings.EqualFold(target, "query_group"):
		return sql[:start] + "RESET application_name" + suffix
	case strings.Contains(target, "."):
		return sql[:start] + "SELECT set_config(" + sqlStringLiteral(target) + ", NULL, false)" + suffix
	default:
		return sql
	}
}

func parseResetTarget(sql string, index int) (string, int, bool) {
	if end, ok := matchKeywordSequence(sql, index, []string{"all"}); ok {
		return sql[index:end], end, true
	}

	var parts []string
	next := index
	for {
		part, partEnd, ok := parseResetIdentifier(sql, next)
		if !ok {
			return "", index, false
		}
		parts = append(parts, part)
		next = partEnd
		if next >= len(sql) || sql[next] != '.' {
			break
		}
		next++
	}
	return strings.Join(parts, "."), next, true
}

func parseResetIdentifier(sql string, index int) (string, int, bool) {
	if index >= len(sql) {
		return "", index, false
	}
	if sql[index] == '"' {
		var out strings.Builder
		for i := index + 1; i < len(sql); i++ {
			if sql[i] != '"' {
				out.WriteByte(sql[i])
				continue
			}
			if i+1 < len(sql) && sql[i+1] == '"' {
				out.WriteByte('"')
				i++
				continue
			}
			return out.String(), i + 1, true
		}
		return "", index, false
	}
	if !isIdentifierStart(sql[index]) {
		return "", index, false
	}
	end := index + 1
	for end < len(sql) && isIdentifierPart(sql[end]) {
		end++
	}
	return strings.ToLower(sql[index:end]), end, true
}

func rewriteBeginTransactionModes(sql string) string {
	start := skipSpaces(sql, 0)
	beginEnd, ok := matchKeywordSequence(sql, start, []string{"begin"})
	if !ok {
		return sql
	}

	next := skipSpaces(sql, beginEnd)
	readStart := next
	readEnd, hasReadMode := matchBeginReadMode(sql, readStart)
	if hasReadMode {
		next = skipSpaces(sql, readEnd)
	}

	isolationStart := next
	isolationEnd, hasIsolationMode := matchKeywordSequence(sql, isolationStart, []string{"isolation", "level", "serializable"})
	if hasIsolationMode {
		next = skipSpaces(sql, isolationEnd)
	}
	if !hasReadMode || !hasIsolationMode {
		return sql
	}

	suffix := ""
	if next < len(sql) {
		if sql[next] != ';' || strings.TrimSpace(sql[next+1:]) != "" {
			return sql
		}
		suffix = ";"
	}

	readMode := strings.TrimSpace(sql[readStart:readEnd])
	isolationMode := strings.TrimSpace(sql[isolationStart:isolationEnd])
	return sql[:start] + sql[start:beginEnd] + " " + readMode + ", " + isolationMode + suffix
}

func matchBeginReadMode(sql string, index int) (int, bool) {
	if end, ok := matchKeywordSequence(sql, index, []string{"read", "only"}); ok {
		return end, true
	}
	if end, ok := matchKeywordSequence(sql, index, []string{"read", "write"}); ok {
		return end, true
	}
	return index, false
}

func rewriteRedshiftSystemTables(sql string) string {
	var out strings.Builder
	for i := 0; i < len(sql); {
		ch := sql[i]
		if ch == '\'' {
			i = copyQuotedString(&out, sql, i)
			continue
		}
		if ch == '"' {
			i = copyQuotedIdentifier(&out, sql, i)
			continue
		}
		if end, ok := matchKeywordSequence(sql, i, []string{"from"}); ok {
			out.WriteString(sql[i:end])
			next := copySpaces(&out, sql, end)
			if rewritten, rewrittenEnd, ok := rewriteRedshiftSystemTableReference(sql, next); ok {
				out.WriteString(rewritten)
				i = rewrittenEnd
				continue
			}
			i = next
			continue
		}
		if end, ok := matchKeywordSequence(sql, i, []string{"join"}); ok {
			out.WriteString(sql[i:end])
			next := copySpaces(&out, sql, end)
			if rewritten, rewrittenEnd, ok := rewriteRedshiftSystemTableReference(sql, next); ok {
				out.WriteString(rewritten)
				i = rewrittenEnd
				continue
			}
			i = next
			continue
		}
		out.WriteByte(ch)
		i++
	}
	return out.String()
}

func rewriteRedshiftSystemTableReference(sql string, index int) (string, int, bool) {
	reference, referenceEnd, tableName, ok := readRelationIdentifier(sql, index)
	if !ok {
		return "", index, false
	}
	replacement := ""
	if isRedshiftReadOnlySystemTable(tableName) {
		replacement = postgresRedshiftSystemTable(tableName)
	} else if isRedshiftInformationSchemaRelation(reference) {
		replacement = postgresRedshiftInformationSchemaRelation(reference)
	} else {
		return "", index, false
	}
	afterReferenceSpaces := skipSpaces(sql, referenceEnd)
	alias := tableName
	next := referenceEnd
	if asEnd, ok := matchKeywordSequence(sql, afterReferenceSpaces, []string{"as"}); ok {
		aliasStart := skipSpaces(sql, asEnd)
		parsedAlias, aliasEnd, ok := readAliasIdentifier(sql, aliasStart)
		if !ok {
			return "", index, false
		}
		alias = parsedAlias
		next = aliasEnd
	} else if parsedAlias, aliasEnd, ok := readAliasIdentifier(sql, afterReferenceSpaces); ok && !isRelationAliasStopWord(parsedAlias) {
		alias = parsedAlias
		next = aliasEnd
	}

	return replacement + " as " + alias, next, true
}

func isRedshiftReadOnlySystemTable(tableName string) bool {
	normalized := strings.ToLower(tableName)
	return strings.HasPrefix(normalized, "stv_") ||
		strings.HasPrefix(normalized, "stl_") ||
		strings.HasPrefix(normalized, "svv_") ||
		strings.HasPrefix(normalized, "svl_") ||
		strings.HasPrefix(normalized, "sys_") ||
		normalized == "pg_table_def" ||
		normalized == "pg_table_info"
}

func readRelationIdentifier(sql string, index int) (string, int, string, bool) {
	if index >= len(sql) || !isIdentifierStart(sql[index]) {
		return "", index, "", false
	}
	start := index
	lastStart := index
	for {
		if index >= len(sql) || !isIdentifierStart(sql[index]) {
			return "", start, "", false
		}
		partStart := index
		index++
		for index < len(sql) && isIdentifierPart(sql[index]) {
			index++
		}
		lastStart = partStart
		if index >= len(sql) || sql[index] != '.' {
			break
		}
		index++
	}
	return sql[start:index], index, sql[lastStart:index], true
}

func readAliasIdentifier(sql string, index int) (string, int, bool) {
	if index >= len(sql) {
		return "", index, false
	}
	if sql[index] == '"' {
		var alias strings.Builder
		next := copyQuotedIdentifier(&alias, sql, index)
		if next <= index {
			return "", index, false
		}
		return alias.String(), next, true
	}
	if !isIdentifierStart(sql[index]) {
		return "", index, false
	}
	start := index
	index++
	for index < len(sql) && isIdentifierPart(sql[index]) {
		index++
	}
	return sql[start:index], index, true
}

func isRelationAliasStopWord(value string) bool {
	switch strings.ToLower(strings.Trim(value, `"`)) {
	case "cross", "except", "fetch", "full", "group", "having", "inner", "intersect", "join", "left", "limit", "offset", "on", "order", "outer", "qualify", "right", "union", "using", "where":
		return true
	default:
		return false
	}
}

func postgresRedshiftSystemTableStub() string {
	return "(select null::integer as node, null::integer as slice, null::integer as userid, null::text as user_name, null::integer as pid, null::bigint as xid, null::bigint as query, null::text as label, null::timestamp as starttime, null::timestamp as endtime, null::text as status, null::text as text, null::bigint as rows, null::bigint as bytes, null::bigint as cpu_time, null::boolean as is_diskbased, null::bigint as workmem, null::text as type, null::text as name, null::text as value where false)"
}

func postgresRedshiftSystemTable(tableName string) string {
	switch strings.ToLower(tableName) {
	case "pg_table_def":
		return postgresPGTableDef()
	case "pg_table_info":
		return postgresPGTableInfo()
	default:
		return postgresRedshiftSystemTableStub()
	}
}

func postgresPGTableDef() string {
	return "(select table_schema::text as schemaname, table_name::text as tablename, column_name::text as \"column\", data_type::text as type, null::text as encoding, false as distkey, 0::integer as sortkey, (is_nullable = 'NO') as notnull from information_schema.columns)"
}

func postgresPGTableInfo() string {
	return "(select current_database()::text as database, n.nspname::text as schema, c.relname::text as \"table\", c.oid::integer as table_id, 'N'::text as encoded, null::text as diststyle, 0::integer as sortkey1, 0::integer as max_varchar, null::text as sortkey1_enc, 0::integer as sortkey_num, 0::bigint as size, 0::numeric as pct_used, 0::bigint as empty, 0::numeric as unsorted, 0::numeric as stats_off, c.reltuples::bigint as tbl_rows, 0::numeric as skew_sortkey1, 0::numeric as skew_rows, c.reltuples::bigint as estimated_visible_rows, null::text as risk_event, 0::numeric as vacuum_sort_benefit from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace where c.relkind in ('r', 'p'))"
}

func isRedshiftInformationSchemaRelation(reference string) bool {
	return strings.EqualFold(reference, "information_schema.columns")
}

func postgresRedshiftInformationSchemaRelation(reference string) string {
	switch strings.ToLower(reference) {
	case "information_schema.columns":
		return postgresInformationSchemaColumns()
	default:
		return ""
	}
}

func postgresInformationSchemaColumns() string {
	return "(select table_catalog::text as table_catalog, table_schema::text as table_schema, table_name::text as table_name, column_name::text as column_name, ordinal_position::integer as ordinal_position, column_default::text as column_default, is_nullable::text as is_nullable, data_type::text as data_type, character_maximum_length::integer as character_maximum_length, numeric_precision::integer as numeric_precision, numeric_precision_radix::integer as numeric_precision_radix, numeric_scale::integer as numeric_scale, datetime_precision::integer as datetime_precision, interval_type::text as interval_type, interval_precision::text as interval_precision, character_set_catalog::text as character_set_catalog, character_set_schema::text as character_set_schema, character_set_name::text as character_set_name, collation_catalog::text as collation_catalog, collation_schema::text as collation_schema, collation_name::text as collation_name, domain_name::text as domain_name, null::text as remarks from information_schema.columns)"
}

func copySpaces(out *strings.Builder, value string, index int) int {
	for index < len(value) {
		switch value[index] {
		case ' ', '\t', '\n', '\r':
			out.WriteByte(value[index])
			index++
		default:
			return index
		}
	}
	return index
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

func rewriteGreatest(args []string) (string, bool) {
	return rewriteNullIgnoringExtremum("max", "greatest_value", args)
}

func rewriteLeast(args []string) (string, bool) {
	return rewriteNullIgnoringExtremum("min", "least_value", args)
}

func rewriteNullIgnoringExtremum(aggregate string, column string, args []string) (string, bool) {
	if len(args) == 0 {
		return "", false
	}
	values := make([]string, 0, len(args))
	for _, arg := range args {
		value := strings.TrimSpace(arg)
		if value == "" {
			return "", false
		}
		values = append(values, "("+value+")")
	}
	return "(select " + aggregate + "(" + column + ") from (values " + strings.Join(values, ", ") + ") as redshift_" + column + "s(" + column + "))", true
}

func rewriteRound(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	scale := strings.TrimSpace(args[1])
	if value == "" || scale == "" {
		return "", false
	}
	magnitude, ok := negativeIntegerLiteralMagnitude(scale)
	if !ok {
		return "", false
	}
	return "round((" + value + ")::numeric, -" + magnitude + ")", true
}

func rewriteJSONExtractPathText(args []string) (string, bool) {
	if len(args) < 2 {
		return "", false
	}
	rewrittenArgs := make([]string, 0, len(args))
	for _, arg := range args {
		trimmed := strings.TrimSpace(arg)
		if trimmed == "" {
			return "", false
		}
		rewrittenArgs = append(rewrittenArgs, trimmed)
	}
	if _, ok := postgresBooleanLiteral(rewrittenArgs[len(rewrittenArgs)-1]); ok {
		rewrittenArgs = rewrittenArgs[:len(rewrittenArgs)-1]
	}
	if len(rewrittenArgs) < 2 {
		return "", false
	}
	return "jsonb_extract_path_text((" + rewrittenArgs[0] + ")::jsonb, " + strings.Join(rewrittenArgs[1:], ", ") + ")", true
}

func rewriteJSONExtractArrayElementText(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	index := strings.TrimSpace(args[1])
	if value == "" || index == "" {
		return "", false
	}
	return "((" + value + ")::jsonb -> " + index + ")::text", true
}

func rewriteJSONArrayLength(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	return "jsonb_array_length((" + value + ")::jsonb)", true
}

func rewriteJSONParse(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	return "(" + value + ")::jsonb", true
}

func rewriteIsValidJSON(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	return "coalesce(json_valid((" + value + ")::text), false)", true
}

func rewriteIsValidJSONArray(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	validJSON := "coalesce(json_valid((" + value + ")::text), false)"
	return "(case when " + validJSON + " then jsonb_typeof((" + value + ")::jsonb) = 'array' else false end)", true
}

func rewriteObjectTransformCall(sql string, index int) (string, int, bool) {
	open := skipSpaces(sql, index)
	if open >= len(sql) || sql[open] != '(' {
		return "", index, false
	}
	close := matchingParen(sql, open)
	if close < 0 {
		return "", index, false
	}
	rewritten, ok := rewriteObjectTransform(sql[open+1 : close])
	if !ok {
		return "", index, false
	}
	return rewritten, close + 1, true
}

func rewriteObjectTransform(value string) (string, bool) {
	keepStart, keepEnd := findTopLevelKeywordSequence(value, []string{"keep"}, 0)
	setStart, setEnd := findTopLevelKeywordSequence(value, []string{"set"}, 0)
	if keepStart >= 0 && setStart >= 0 && setStart < keepStart {
		return "", false
	}

	inputEnd := len(value)
	if keepStart >= 0 {
		inputEnd = keepStart
	}
	if setStart >= 0 && setStart < inputEnd {
		inputEnd = setStart
	}
	input := strings.TrimSpace(value[:inputEnd])
	if input == "" {
		return "", false
	}
	inputJSON := "(" + input + ")::jsonb"
	if keepStart < 0 && setStart < 0 {
		return inputJSON, true
	}

	current := "'{}'::jsonb"
	ensuredPaths := make(map[string]bool)
	if keepStart >= 0 {
		keepEndAt := len(value)
		if setStart >= 0 {
			keepEndAt = setStart
		}
		keepPaths := splitCommaSeparated(value[keepEnd:keepEndAt])
		if len(keepPaths) == 0 {
			return "", false
		}
		for _, keepPath := range keepPaths {
			path, components, ok := objectTransformPath(keepPath)
			if !ok {
				return "", false
			}
			current = ensureObjectTransformPath(current, components, ensuredPaths)
			current = "jsonb_set(" + current + ", " + path + ", (" + inputJSON + " #> " + path + "), true)"
		}
	}
	if setStart >= 0 {
		setArgs := splitCommaSeparated(value[setEnd:])
		if len(setArgs) == 0 || len(setArgs)%2 != 0 {
			return "", false
		}
		for i := 0; i < len(setArgs); i += 2 {
			path, components, ok := objectTransformPath(setArgs[i])
			setValue := strings.TrimSpace(setArgs[i+1])
			if !ok || setValue == "" {
				return "", false
			}
			current = ensureObjectTransformPath(current, components, ensuredPaths)
			current = "jsonb_set(" + current + ", " + path + ", to_jsonb(" + setValue + "), true)"
		}
	}
	return current, true
}

func ensureObjectTransformPath(current string, components []string, ensuredPaths map[string]bool) string {
	for i := 1; i < len(components); i++ {
		key := strings.Join(components[:i], "\x00")
		if ensuredPaths[key] {
			continue
		}
		path := objectTransformPathArray(components[:i])
		current = "jsonb_set(" + current + ", " + path + ", coalesce(" + current + " #> " + path + ", '{}'::jsonb), true)"
		ensuredPaths[key] = true
	}
	return current
}

func objectTransformPath(value string) (string, []string, bool) {
	path, ok := sqlStringLiteralValue(value)
	if !ok {
		return "", nil, false
	}
	components, ok := objectTransformPathComponents(path)
	if !ok {
		return "", nil, false
	}
	return objectTransformPathArray(components), components, true
}

func objectTransformPathArray(components []string) string {
	literals := make([]string, 0, len(components))
	for _, component := range components {
		literals = append(literals, sqlStringLiteral(component))
	}
	return "ARRAY[" + strings.Join(literals, ", ") + "]"
}

func objectTransformPathComponents(path string) ([]string, bool) {
	var components []string
	for i := 0; i < len(path); {
		if path[i] != '"' {
			return nil, false
		}
		i++
		var component strings.Builder
		for i < len(path) {
			if path[i] != '"' {
				component.WriteByte(path[i])
				i++
				continue
			}
			if i+1 < len(path) && path[i+1] == '"' {
				component.WriteByte(path[i+1])
				i += 2
				continue
			}
			break
		}
		if i >= len(path) || path[i] != '"' || component.Len() == 0 {
			return nil, false
		}
		components = append(components, component.String())
		i++
		if i == len(path) {
			break
		}
		if path[i] != '.' {
			return nil, false
		}
		i++
	}
	if len(components) == 0 {
		return nil, false
	}
	return components, true
}

func rewriteNVL2(args []string) (string, bool) {
	if len(args) != 3 {
		return "", false
	}
	expr := strings.TrimSpace(args[0])
	valIfNotNull := strings.TrimSpace(args[1])
	valIfNull := strings.TrimSpace(args[2])
	if expr == "" || valIfNotNull == "" || valIfNull == "" {
		return "", false
	}
	return "CASE WHEN " + expr + " IS NOT NULL THEN " + valIfNotNull + " ELSE " + valIfNull + " END", true
}

func rewriteLen(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	return "length(" + value + ")", true
}

func rewriteCharIndex(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	substring := strings.TrimSpace(args[0])
	value := strings.TrimSpace(args[1])
	if substring == "" || value == "" {
		return "", false
	}
	return "position(" + substring + " in " + value + ")", true
}

func rewriteSubstring(args []string) (string, bool) {
	if len(args) != 3 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	start := strings.TrimSpace(args[1])
	length := strings.TrimSpace(args[2])
	if value == "" || start == "" || length == "" {
		return "", false
	}

	postgresStart := "(case when " + start + " < 1 then 1 else " + start + " end)"
	redshiftLength := start + " + " + length + " - 1"
	postgresLength := "(case when " + start + " <= 0 then case when " + redshiftLength + " <= 0 then 0 else " + redshiftLength + " end else " + length + " end)"
	return "substring(" + value + " from " + postgresStart + " for " + postgresLength + ")", true
}

func rewriteSplitPart(args []string) (string, bool) {
	if len(args) != 3 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	separator := strings.TrimSpace(args[1])
	position := strings.TrimSpace(args[2])
	if value == "" || separator == "" || position == "" {
		return "", false
	}
	magnitude, ok := negativeIntegerLiteralMagnitude(position)
	if !ok {
		return "", false
	}
	return "reverse(split_part(reverse(" + value + "), reverse(" + separator + "), " + magnitude + "))", true
}

func negativeIntegerLiteralMagnitude(value string) (string, bool) {
	if len(value) < 2 || value[0] != '-' {
		return "", false
	}
	magnitude := value[1:]
	if !isUnsignedInteger(magnitude) || strings.TrimLeft(magnitude, "0") == "" {
		return "", false
	}
	return magnitude, true
}

func rewriteStrtol(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	base := strings.TrimSpace(args[1])
	if value == "" || base == "" {
		return "", false
	}

	normalized := "regexp_replace(trim(" + value + "), '^[+-]', '')"
	return "(select (case when left(trim(" + value + "), 1) = '-' then -1 else 1 end) * coalesce(sum((strpos('0123456789abcdefghijklmnopqrstuvwxyz', digit) - 1)::numeric * power((" + base + ")::numeric, (length(" + normalized + ") - ordinality)::numeric)), 0)::bigint from regexp_split_to_table(lower(" + normalized + "), '') with ordinality as strtol_digits(digit, ordinality))", true
}

func rewriteCRC32(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}

	seed := "4294967295"
	polynomial := "3988292384"
	crcInput := "(case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end)"
	crcStep := "(case when (" + crcInput + " & 1) = 1 then ((" + crcInput + " >> 1) # " + polynomial + ") else (" + crcInput + " >> 1) end)"
	return "(with recursive crc32_input(data) as (select convert_to((" + value + ")::text, 'UTF8')), crc32_state(step, crc) as (select 0, " + seed + "::bigint union all select step + 1, " + crcStep + " from crc32_state, crc32_input where step < length(data) * 8) select case when data is null then null else (crc # " + seed + ")::bigint end from crc32_state, crc32_input order by step desc limit 1)", true
}

func rewriteMD5Digest(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	return "md5((" + value + ")::text)", true
}

func rewriteFuncSHA1(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	return "encode(digest((" + value + ")::text, 'sha1'), 'hex')", true
}

func rewriteRegexpSubstr(args []string) (string, bool) {
	if len(args) != 4 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	pattern := strings.TrimSpace(args[1])
	start := strings.TrimSpace(args[2])
	occurrence := strings.TrimSpace(args[3])
	if value == "" || pattern == "" || start == "" || occurrence == "" {
		return "", false
	}
	if start == "1" && occurrence == "1" {
		return "regexp_match(" + value + ", " + pattern + ")", true
	}
	return "(select regexp_substr_match from regexp_matches(substring(" + value + " from " + start + "), " + pattern + ", 'g') with ordinality as regexp_substr_matches(regexp_substr_match, regexp_substr_ordinality) where regexp_substr_ordinality = " + occurrence + ")", true
}

func rewriteRegexpCount(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	pattern := strings.TrimSpace(args[1])
	if value == "" || pattern == "" {
		return "", false
	}
	return "(case when " + value + " is null or " + pattern + " is null then null else (select count(*)::int from regexp_matches(" + value + ", " + pattern + ", 'g')) end)", true
}

func rewriteRegexpInstr(args []string) (string, bool) {
	if len(args) < 2 || len(args) > 6 {
		return "", false
	}
	rewrittenArgs := make([]string, 0, len(args)+1)
	for _, arg := range args {
		trimmed := strings.TrimSpace(arg)
		if trimmed == "" {
			return "", false
		}
		rewrittenArgs = append(rewrittenArgs, trimmed)
	}
	if len(rewrittenArgs) == 6 {
		parameters, ok := sqlStringLiteralValue(rewrittenArgs[5])
		if ok && strings.ContainsAny(parameters, "eE") {
			pgParameters := strings.Map(func(ch rune) rune {
				if ch == 'e' || ch == 'E' {
					return -1
				}
				return ch
			}, parameters)
			rewrittenArgs[5] = sqlStringLiteral(pgParameters)
			rewrittenArgs = append(rewrittenArgs, "1")
		}
	}
	return "regexp_instr(" + strings.Join(rewrittenArgs, ", ") + ")", true
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

func rewriteTimeOfDay(args []string) (string, bool) {
	if len(args) != 0 {
		return "", false
	}
	return PostgresClockTimestamp + "::text", true
}

func rewriteRand(args []string) (string, bool) {
	if len(args) != 0 {
		return "", false
	}
	return PostgresRandom, true
}

func rewriteConvertTimezone(args []string) (string, bool) {
	if len(args) != 3 {
		return "", false
	}
	source, ok := sqlStringLiteralValue(args[0])
	if !ok || !strings.EqualFold(source, "UTC") {
		return "", false
	}
	target, ok := sqlStringLiteralValue(args[1])
	if !ok || !strings.EqualFold(target, "JST") {
		return "", false
	}
	timestamp := strings.TrimSpace(args[2])
	if timestamp == "" {
		return "", false
	}
	return timestamp + " AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Tokyo'", true
}

func sqlStringLiteralValue(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed[0] != '\'' {
		return "", false
	}
	unquoted, end, ok := readQuotedStringValue(trimmed, 0)
	if !ok || strings.TrimSpace(trimmed[end:]) != "" {
		return "", false
	}
	return unquoted, true
}

func rewriteDatePartFunction(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	part, ok := postgresDatePartFunctionPart(args[0])
	if !ok {
		return "", false
	}
	return "date_part('" + part + "', " + strings.TrimSpace(args[1]) + ")", true
}

func rewriteDateTruncFunction(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	part, ok := postgresDateTruncPart(args[0])
	if !ok {
		return "", false
	}
	return "date_trunc('" + part + "', " + strings.TrimSpace(args[1]) + ")", true
}

func rewriteLastDay(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	return "(date_trunc('month', " + value + ") + interval '1 month - 1 day')::date", true
}

func rewriteMonthsBetween(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	end := strings.TrimSpace(args[0])
	start := strings.TrimSpace(args[1])
	if end == "" || start == "" {
		return "", false
	}
	return "(extract(year from age(" + end + ", " + start + ")) * 12 + extract(month from age(" + end + ", " + start + ")))", true
}

func rewriteAddMonths(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	months := strings.TrimSpace(args[1])
	if value == "" || months == "" {
		return "", false
	}
	return value + " + (" + months + " * interval '1 month')", true
}

func rewriteNextDay(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	day, ok := sqlStringLiteralValue(args[1])
	if !ok {
		return "", false
	}
	targetDow, ok := redshiftNextDayNumber(day)
	if !ok {
		return "", false
	}
	dateValue := "(" + value + ")::date"
	return "(" + dateValue + " + ((" + targetDow + " - extract(dow from " + dateValue + ")::int + 6) % 7 + 1))::date", true
}

func redshiftNextDayNumber(value string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "su", "sun", "sunday":
		return "0", true
	case "m", "mo", "mon", "monday":
		return "1", true
	case "tu", "tue", "tues", "tuesday":
		return "2", true
	case "w", "we", "wed", "wednesday":
		return "3", true
	case "th", "thu", "thurs", "thursday":
		return "4", true
	case "f", "fr", "fri", "friday":
		return "5", true
	case "sa", "sat", "saturday":
		return "6", true
	default:
		return "", false
	}
}

func rewriteToDate(args []string) (string, bool) {
	return rewriteDateTimeFormatFunction("to_date", args)
}

func rewriteToTimestamp(args []string) (string, bool) {
	return rewriteDateTimeFormatFunction("to_timestamp", args)
}

func rewriteToChar(args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	format, ok := sqlStringLiteralValue(args[1])
	if !ok {
		return "", false
	}
	rewrittenFormat, ok := rewriteRedshiftToCharFormat(format)
	if !ok {
		return "", false
	}
	return "to_char(" + value + ", " + sqlStringLiteral(rewrittenFormat) + ")", true
}

func rewriteDateTimeFormatFunction(functionName string, args []string) (string, bool) {
	if len(args) != 2 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	format, ok := sqlStringLiteralValue(args[1])
	if !ok {
		return "", false
	}
	rewrittenFormat, ok := removeTrailingRedshiftTimezoneFormat(format)
	if !ok {
		return "", false
	}
	return functionName + "(regexp_replace(" + value + ", '[[:space:]]*([[:alpha:]_/]+|[+-][0-9]{2}(:?[0-9]{2})?)$', ''), " + sqlStringLiteral(rewrittenFormat) + ")", true
}

func rewriteRedshiftToCharFormat(format string) (string, bool) {
	var out strings.Builder
	changed := false
	for i := 0; i < len(format); {
		if format[i] == '"' {
			start := i
			i++
			for i < len(format) {
				if format[i] == '\\' && i+1 < len(format) {
					i += 2
					continue
				}
				if format[i] == '"' {
					i++
					break
				}
				i++
			}
			out.WriteString(format[start:i])
			continue
		}
		if hasFormatToken(format, i, "TZ") {
			out.WriteString(`"UTC"`)
			i += len("TZ")
			changed = true
			continue
		}
		if hasFormatToken(format, i, "tz") {
			out.WriteString(`"utc"`)
			i += len("tz")
			changed = true
			continue
		}
		if hasFormatToken(format, i, "OF") {
			out.WriteString(`"+00"`)
			i += len("OF")
			changed = true
			continue
		}
		out.WriteByte(format[i])
		i++
	}
	if !changed {
		return format, false
	}
	return out.String(), true
}

func hasFormatToken(format string, index int, token string) bool {
	if index > 0 && isFormatLetter(format[index-1]) {
		return false
	}
	if !strings.HasPrefix(format[index:], token) {
		return false
	}
	end := index + len(token)
	return end == len(format) || !isFormatLetter(format[end])
}

func removeTrailingRedshiftTimezoneFormat(format string) (string, bool) {
	trimmed := strings.TrimRight(format, " \t\n\r")
	lower := strings.ToLower(trimmed)
	for _, token := range []string{"tz", "of"} {
		if !strings.HasSuffix(lower, token) {
			continue
		}
		start := len(trimmed) - len(token)
		if start > 0 && isFormatLetter(trimmed[start-1]) {
			continue
		}
		end := strings.TrimRight(trimmed[:start], " \t\n\r")
		return end + format[len(trimmed):], true
	}
	return format, false
}

func isFormatLetter(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
}

func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func rewriteMedian(args []string) (string, bool) {
	if len(args) != 1 {
		return "", false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", false
	}
	return "percentile_cont(0.5) WITHIN GROUP (ORDER BY " + value + ")", true
}

func rewriteRatioToReport(sql string, index int) (string, int, bool) {
	open := skipSpaces(sql, index)
	if open >= len(sql) || sql[open] != '(' {
		return "", index, false
	}
	close := matchingParen(sql, open)
	if close < 0 {
		return "", index, false
	}
	args := splitCommaSeparated(sql[open+1 : close])
	if len(args) != 1 {
		return "", index, false
	}
	value := strings.TrimSpace(args[0])
	if value == "" {
		return "", index, false
	}

	overStart := skipSpaces(sql, close+1)
	overEnd, ok := matchKeywordSequence(sql, overStart, []string{"over"})
	if !ok {
		return "", index, false
	}
	overOpen := skipSpaces(sql, overEnd)
	if overOpen >= len(sql) || sql[overOpen] != '(' {
		return "", index, false
	}
	overClose := matchingParen(sql, overOpen)
	if overClose < 0 {
		return "", index, false
	}

	return value + " / SUM(" + value + ") OVER " + sql[overOpen:overClose+1], overClose + 1, true
}

func rewriteListAgg(sql string, index int) (string, int, bool) {
	var listAggExpression string
	var listAggDelimiter string
	rewritten, next, ok := rewriteParenFunction(sql, index, func(args []string) (string, bool) {
		if len(args) != 2 {
			return "", false
		}
		listAggExpression = strings.TrimSpace(args[0])
		listAggDelimiter = strings.TrimSpace(args[1])
		return PostgresStringAgg + "(" + listAggExpression + ", " + listAggDelimiter, true
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
			overStart := skipSpaces(sql, close+1)
			if overEnd, ok := matchKeywordSequence(sql, overStart, []string{"over"}); ok {
				overOpen := skipSpaces(sql, overEnd)
				if overOpen < len(sql) && sql[overOpen] == '(' {
					overClose := matchingParen(sql, overOpen)
					if overClose > overOpen {
						overClause := addOrderToListAggWindow(strings.TrimSpace(sql[overOpen+1:overClose]), orderBy)
						return "array_to_string(array_agg(" + listAggExpression + ") OVER (" + overClause + "), " + listAggDelimiter + ")", overClose + 1, true
					}
				}
			}
			return rewritten + " ORDER BY " + orderBy + ")", close + 1, true
		}
	}
	return rewritten + ")", next, true
}

func addOrderToListAggWindow(overClause string, orderBy string) string {
	if overClause == "" {
		return "ORDER BY " + orderBy + " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
	}
	return overClause + " ORDER BY " + orderBy + " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
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

func postgresDatePartFunctionPart(value string) (string, bool) {
	switch strings.ToLower(cleanDatePartIdentifier(value)) {
	case "millennium", "millennia":
		return "millennium", true
	case "century", "c", "centuries":
		return "century", true
	case "decade", "dec", "decades":
		return "decade", true
	case "year", "y", "yr", "yrs", "yy", "yyyy":
		return "year", true
	case "quarter", "qtr", "q":
		return "quarter", true
	case "month", "mon", "mons", "mm":
		return "month", true
	case "week", "w":
		return "week", true
	case "day", "d", "dd":
		return "day", true
	case "dayofweek", "dow", "dw", "weekday":
		return "dow", true
	case "dayofyear", "doy":
		return "doy", true
	case "hour", "h", "hr", "hrs", "hh":
		return "hour", true
	case "minute", "m", "min", "mins", "mi", "n":
		return "minute", true
	case "second", "s", "sec", "secs", "ss":
		return "second", true
	case "millisecond", "milliseconds", "msec", "msecs", "ms":
		return "milliseconds", true
	case "microsecond", "microseconds", "usec", "usecs", "us":
		return "microseconds", true
	case "epoch":
		return "epoch", true
	default:
		return "", false
	}
}

func postgresDateTruncPart(value string) (string, bool) {
	switch strings.ToLower(cleanDatePartIdentifier(value)) {
	case "millennium", "millennia":
		return "millennium", true
	case "century", "c", "centuries":
		return "century", true
	case "decade", "dec", "decades":
		return "decade", true
	case "year", "y", "yr", "yrs", "yy", "yyyy":
		return "year", true
	case "quarter", "qtr", "q":
		return "quarter", true
	case "month", "mon", "mons", "mm":
		return "month", true
	case "week", "w":
		return "week", true
	case "day", "d", "dd":
		return "day", true
	case "hour", "h", "hr", "hrs", "hh":
		return "hour", true
	case "minute", "m", "min", "mins", "mi", "n":
		return "minute", true
	case "second", "s", "sec", "secs", "ss":
		return "second", true
	case "millisecond", "milliseconds", "msec", "msecs", "ms":
		return "milliseconds", true
	case "microsecond", "microseconds", "usec", "usecs", "us":
		return "microseconds", true
	default:
		return "", false
	}
}

func cleanDatePartIdentifier(value string) string {
	return strings.Trim(strings.TrimSpace(value), `"'`)
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

func translateMergeInto(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	prefixEnd, ok := matchKeywordSequence(statement, 0, []string{"merge", "into"})
	if !ok {
		return TranslationResult{}, false, nil
	}

	usingStart, usingEnd := findTopLevelKeywordSequence(statement, []string{"using"}, prefixEnd)
	if usingStart < 0 {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	onStart, onEnd := findTopLevelKeywordSequence(statement, []string{"on"}, usingEnd)
	if onStart < 0 {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	matchedStart, matchedEnd := findTopLevelKeywordSequence(statement, []string{"when", "matched", "then", "update", "set"}, onEnd)
	if matchedStart < 0 {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	notMatchedStart, notMatchedEnd := findTopLevelKeywordSequence(statement, []string{"when", "not", "matched", "then", "insert"}, matchedEnd)
	if notMatchedStart < 0 {
		return TranslationResult{BackendSQL: statement}, true, nil
	}

	target := strings.TrimSpace(statement[prefixEnd:usingStart])
	source := strings.TrimSpace(statement[usingEnd:onStart])
	onCondition := strings.TrimSpace(statement[onEnd:matchedStart])
	updateAssignments := strings.TrimSpace(statement[matchedEnd:notMatchedStart])
	if target == "" || source == "" || onCondition == "" || updateAssignments == "" {
		return TranslationResult{BackendSQL: statement}, true, nil
	}

	insertColumns, insertValues, ok := parseMergeInsertClause(statement[notMatchedEnd:])
	if !ok {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	insertTarget := firstSQLToken(target)
	if insertTarget == "" {
		return TranslationResult{BackendSQL: statement}, true, nil
	}

	backendSQL := "with updated as (update " + target +
		" set " + updateAssignments +
		" from " + source +
		" where " + onCondition +
		" returning 1) insert into " + insertTarget +
		" " + insertColumns +
		" select " + insertValues +
		" from " + source +
		" where not exists (select 1 from " + target +
		" where " + onCondition + ")"
	return TranslationResult{BackendSQL: backendSQL}, true, nil
}

func parseMergeInsertClause(value string) (string, string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed[0] != '(' {
		return "", "", false
	}
	columnsClose := matchingParen(trimmed, 0)
	if columnsClose < 0 {
		return "", "", false
	}
	columns := strings.TrimSpace(trimmed[:columnsClose+1])
	valuesStart, valuesEnd := findTopLevelKeywordSequence(trimmed, []string{"values"}, columnsClose+1)
	if valuesStart < 0 || strings.TrimSpace(trimmed[columnsClose+1:valuesStart]) != "" {
		return "", "", false
	}
	values := strings.TrimSpace(trimmed[valuesEnd:])
	if len(values) < 2 || values[0] != '(' {
		return "", "", false
	}
	valuesClose := matchingParen(values, 0)
	if valuesClose < 0 || strings.TrimSpace(values[valuesClose+1:]) != "" {
		return "", "", false
	}
	return columns, strings.TrimSpace(values[1:valuesClose]), true
}

func translateInsertSelectReturning(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	prefixEnd, ok := matchKeywordSequence(statement, 0, []string{"insert", "into"})
	if !ok {
		return TranslationResult{}, false, nil
	}
	selectStart, selectEnd := findTopLevelKeywordSequence(statement, []string{"select"}, prefixEnd)
	if selectStart < 0 {
		return TranslationResult{}, false, nil
	}
	if valuesStart, _ := findTopLevelKeywordSequence(statement, []string{"values"}, prefixEnd); valuesStart >= 0 && valuesStart < selectStart {
		return TranslationResult{}, false, nil
	}
	returningStart, _ := findTopLevelKeywordSequence(statement, []string{"returning"}, selectEnd)
	if returningStart < 0 {
		return TranslationResult{}, false, nil
	}
	backendSQL := strings.TrimSpace(statement[:returningStart])
	return TranslationResult{BackendSQL: backendSQL}, true, nil
}

func translateInsertValuesDefault(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	prefixEnd, ok := matchKeywordSequence(statement, 0, []string{"insert", "into"})
	if !ok {
		return TranslationResult{}, false, nil
	}
	valuesStart, valuesEnd := findTopLevelKeywordSequence(statement, []string{"values"}, prefixEnd)
	if valuesStart < 0 {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	open := skipSpaces(statement, valuesEnd)
	if open >= len(statement) || statement[open] != '(' {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	close := matchingParen(statement, open)
	if close < 0 || strings.TrimSpace(statement[close+1:]) != "" {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	values := splitCommaSeparated(statement[open+1 : close])
	if len(values) != 1 || !strings.EqualFold(strings.TrimSpace(values[0]), "default") {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	backendSQL := strings.TrimSpace(statement[:valuesStart]) + " default values"
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

func translateTruncateImmediateCommit(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	prefixEnd, ok := matchKeywordSequence(statement, 0, []string{"truncate"})
	if !ok {
		return TranslationResult{}, false, nil
	}
	if strings.TrimSpace(statement[prefixEnd:]) == "" {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	return TranslationResult{BackendSQL: "commit; " + statement}, true, nil
}

func translateQualifySelect(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	prefixEnd, ok := matchKeywordSequence(statement, 0, []string{"select"})
	if !ok {
		return TranslationResult{}, false, nil
	}
	qualifyStart, qualifyEnd := findTopLevelKeywordSequence(statement, []string{"qualify"}, prefixEnd)
	if qualifyStart < 0 {
		return TranslationResult{}, false, nil
	}

	qualifyClauseEnd := len(statement)
	suffixStart := -1
	for _, sequence := range [][]string{
		{"order", "by"},
		{"limit"},
		{"offset"},
		{"fetch"},
	} {
		start, _ := findTopLevelKeywordSequence(statement, sequence, qualifyEnd)
		if start >= 0 && start < qualifyClauseEnd {
			qualifyClauseEnd = start
			suffixStart = start
		}
	}

	innerSQL := strings.TrimSpace(statement[:qualifyStart])
	condition := strings.TrimSpace(statement[qualifyEnd:qualifyClauseEnd])
	if innerSQL == "" || condition == "" {
		return TranslationResult{BackendSQL: statement}, true, nil
	}

	outerSelect := "*"
	if rewrittenOuterSelect, rewrittenInnerSQL, rewrittenCondition, ok := rewriteQualifyWindowPredicate(innerSQL, condition); ok {
		outerSelect = rewrittenOuterSelect
		innerSQL = rewrittenInnerSQL
		condition = rewrittenCondition
	}

	backendSQL := "select " + outerSelect + " from (" + innerSQL + ") as devcloud_qualify where " + condition
	if suffixStart >= 0 {
		backendSQL += " " + strings.TrimSpace(statement[suffixStart:])
	}
	return TranslationResult{BackendSQL: backendSQL}, true, nil
}

func translateGrantAssumeRole(sql string) (TranslationResult, bool, error) {
	statement := strings.TrimSpace(strings.TrimRight(sql, ";"))
	prefixEnd, ok := matchKeywordSequence(statement, 0, []string{"grant", "assumerole", "on"})
	if !ok {
		return TranslationResult{}, false, nil
	}

	roleStart := skipSpaces(statement, prefixEnd)
	_, roleEnd, ok := readQuotedStringValue(statement, roleStart)
	if !ok {
		return TranslationResult{BackendSQL: statement}, true, nil
	}
	toEnd, ok := matchKeywordSequence(statement, skipSpaces(statement, roleEnd), []string{"to"})
	if !ok || strings.TrimSpace(statement[toEnd:]) == "" {
		return TranslationResult{BackendSQL: statement}, true, nil
	}

	return TranslationResult{BackendSQL: "select 1"}, true, nil
}

func rewriteQualifyWindowPredicate(innerSQL string, condition string) (string, string, string, bool) {
	windowStart, windowEnd := findFirstWindowFunctionExpression(condition)
	if windowStart < 0 {
		return "", "", "", false
	}

	selectEnd, ok := matchKeywordSequence(innerSQL, 0, []string{"select"})
	if !ok {
		return "", "", "", false
	}
	fromStart, _ := findTopLevelKeywordSequence(innerSQL, []string{"from"}, selectEnd)
	if fromStart < 0 {
		return "", "", "", false
	}

	selectList := strings.TrimSpace(innerSQL[selectEnd:fromStart])
	fromClause := strings.TrimSpace(innerSQL[fromStart:])
	if selectList == "" || fromClause == "" {
		return "", "", "", false
	}

	alias := "__devcloud_qualify_1"
	windowExpression := strings.TrimSpace(condition[windowStart:windowEnd])
	outerSelect := qualifyOuterSelectList(selectList)
	rewrittenInnerSQL := "select " + selectList + ", " + windowExpression + " as " + alias + " " + fromClause
	rewrittenCondition := condition[:windowStart] + alias + condition[windowEnd:]
	return outerSelect, rewrittenInnerSQL, rewrittenCondition, true
}

func findFirstWindowFunctionExpression(value string) (int, int) {
	inString := false
	inQuotedIdentifier := false
	for i := 0; i < len(value); i++ {
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
		if inString || inQuotedIdentifier || !isIdentifierStart(ch) {
			continue
		}

		nameStart := i
		i++
		for i < len(value) && isIdentifierPart(value[i]) {
			i++
		}
		open := skipSpaces(value, i)
		if open >= len(value) || value[open] != '(' {
			continue
		}
		argsClose := matchingParen(value, open)
		if argsClose < 0 {
			continue
		}
		overStart := skipSpaces(value, argsClose+1)
		overEnd, ok := matchKeywordSequence(value, overStart, []string{"over"})
		if !ok {
			continue
		}
		overOpen := skipSpaces(value, overEnd)
		if overOpen >= len(value) || value[overOpen] != '(' {
			continue
		}
		overClose := matchingParen(value, overOpen)
		if overClose < 0 {
			continue
		}
		return nameStart, overClose + 1
	}
	return -1, -1
}

func qualifyOuterSelectList(selectList string) string {
	items := splitCommaSeparated(selectList)
	outer := make([]string, 0, len(items))
	for _, item := range items {
		outer = append(outer, qualifyOuterSelectItem(item))
	}
	return strings.Join(outer, ", ")
}

func qualifyOuterSelectItem(item string) string {
	item = strings.TrimSpace(item)
	if item == "*" || strings.HasSuffix(item, ".*") {
		return item
	}
	if asStart, asEnd := findTopLevelKeywordSequence(item, []string{"as"}, 0); asStart >= 0 {
		alias := strings.TrimSpace(item[asEnd:])
		if alias != "" {
			return alias
		}
	}

	fields := strings.Fields(item)
	if len(fields) > 1 {
		alias := fields[len(fields)-1]
		if cleanIdentifier(alias) != "" {
			return alias
		}
	}
	return item
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
	prefixEnd, temporary, ok := parseCreateTablePrefix(statement)
	if !ok {
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

	namePart := strings.TrimSpace(statement[prefixEnd:open])
	if strings.HasPrefix(strings.ToLower(namePart), "if not exists ") {
		namePart = strings.TrimSpace(namePart[len("if not exists "):])
	}
	schemaName, tableName := parseQualifiedName(namePart)
	if cleanLike, ok := translateCreateTableLikeClause(statement[open+1 : close]); ok {
		cleanRest, distStyle, distKey, sortKeys, backup := translateTableAttributes(statement[close+1:])
		cleanRest = ensureTemporaryTableScope(cleanRest, temporary)
		effect := MetadataEffect{
			Kind:     MetadataEffectCreateTable,
			Schema:   schemaName,
			Table:    tableName,
			Name:     distKey,
			Value:    distStyle,
			Backup:   backup,
			SortKeys: sortKeys,
		}
		backendSQL := strings.TrimSpace(statement[:open+1] + cleanLike + ")" + cleanRest)
		if temporary {
			return TranslationResult{BackendSQL: backendSQL}, true, nil
		}
		return TranslationResult{BackendSQL: backendSQL, MetadataEffects: []MetadataEffect{effect}}, true, nil
	}
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
	cleanRest = ensureTemporaryTableScope(cleanRest, temporary)

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
	if temporary {
		return TranslationResult{BackendSQL: backendSQL}, true, nil
	}
	return TranslationResult{BackendSQL: backendSQL, MetadataEffects: []MetadataEffect{effect}}, true, nil
}

func parseCreateTablePrefix(statement string) (int, bool, bool) {
	if next, ok := matchKeywordSequence(statement, 0, []string{"create", "temporary", "table"}); ok {
		return next, true, true
	}
	if next, ok := matchKeywordSequence(statement, 0, []string{"create", "temp", "table"}); ok {
		return next, true, true
	}
	if next, ok := matchKeywordSequence(statement, 0, []string{"create", "table"}); ok {
		return next, false, true
	}
	return 0, false, false
}

func ensureTemporaryTableScope(cleanRest string, temporary bool) string {
	if !temporary || strings.Contains(strings.ToLower(cleanRest), "on commit") {
		return cleanRest
	}
	if strings.TrimSpace(cleanRest) == "" {
		return " on commit preserve rows"
	}
	return cleanRest + " on commit preserve rows"
}

func translateCreateTableLikeClause(value string) (string, bool) {
	tokens := strings.Fields(strings.TrimSpace(value))
	if len(tokens) != 2 && len(tokens) != 4 {
		return "", false
	}
	if !strings.EqualFold(tokens[0], "like") {
		return "", false
	}
	if len(tokens) == 2 {
		return "LIKE " + tokens[1], true
	}
	if !strings.EqualFold(tokens[3], "defaults") {
		return "", false
	}
	switch {
	case strings.EqualFold(tokens[2], "including"):
		return "LIKE " + tokens[1] + " INCLUDING DEFAULTS", true
	case strings.EqualFold(tokens[2], "excluding"):
		return "LIKE " + tokens[1] + " EXCLUDING DEFAULTS", true
	default:
		return "", false
	}
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

func findTopLevelKeywordSequence(value string, keywords []string, start int) (int, int) {
	if start < 0 {
		return -1, -1
	}
	depth := 0
	inString := false
	inQuotedIdentifier := false
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
		if depth != 0 {
			continue
		}
		if end, ok := matchKeywordSequence(value, i, keywords); ok {
			return i, end
		}
	}
	return -1, -1
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

func firstSQLToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	inQuotedIdentifier := false
	for i := 0; i < len(value); i++ {
		if value[i] == '"' {
			if inQuotedIdentifier && i+1 < len(value) && value[i+1] == '"' {
				i++
				continue
			}
			inQuotedIdentifier = !inQuotedIdentifier
			continue
		}
		if inQuotedIdentifier {
			continue
		}
		if value[i] == ' ' || value[i] == '\t' || value[i] == '\n' || value[i] == '\r' {
			return value[:i]
		}
	}
	return value
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
	PostgresClockTimestamp   = "clock_timestamp()"
	PostgresRandom           = "random()"
	PostgresBoolAnd          = "bool_and"
	PostgresBoolOr           = "bool_or"
	PostgresStringAgg        = "string_agg"
)
