//! SQL statement tokenizing/parsing helpers.
//!
//! Parity: `internal/services/redshift/sql_parse.rs`, quirks included — these
//! are byte-scanning helpers (single-quote string awareness, paren depth), not
//! a real SQL grammar. All scans index bytes exactly like legacy; slicing only ever
//! happens at positions whose byte is ASCII, so `&str` boundaries are safe.

use crate::errors::SqlError;
use crate::model::Column;

/// Mirrors `parseSelectLiteral`: parses one `SELECT <literal> [AS] [alias]`
/// expression, returning (value, alias).
pub fn parse_select_literal(
    expression: &str,
    ordinal: usize,
) -> Result<(String, String), SqlError> {
    let expression = expression.trim();
    if expression.is_empty() {
        return Err(SqlError::new("SELECT literal expression cannot be empty"));
    }
    let mut alias = format!("?column{ordinal}?");
    if ordinal == 1 {
        alias = "?column?".to_string();
    }
    if let Ok((value, rest)) = parse_leading_sql_string_literal(expression) {
        let value_part = format!("'{}'", value.replace('\'', "''"));
        let parsed_alias = parse_select_alias(&rest);
        if !parsed_alias.is_empty() {
            alias = parsed_alias;
        } else if !rest.trim().is_empty() {
            return Err(SqlError::new(format!(
                "unsupported SELECT literal alias syntax: {}",
                rest.trim()
            )));
        }
        let value = parse_literal(&value_part)?;
        return Ok((value, alias));
    }

    let fields: Vec<&str> = expression.split_whitespace().collect();
    if fields.is_empty() {
        return Err(SqlError::new("SELECT literal expression cannot be empty"));
    }
    let value_part = fields[0];
    if fields.len() > 1 {
        let rest = expression[fields[0].len()..].trim();
        let parsed_alias = parse_select_alias(rest);
        if !parsed_alias.is_empty() {
            alias = parsed_alias;
        } else {
            return Err(SqlError::new(format!(
                "unsupported SELECT literal alias syntax: {rest}"
            )));
        }
    }
    let value = parse_literal(value_part)?;
    Ok((value, alias))
}

/// Mirrors `parseSelectAlias`.
pub fn parse_select_alias(rest: &str) -> String {
    let rest = rest.trim();
    if rest.is_empty() {
        return String::new();
    }
    let fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() == 2 && fields[0].eq_ignore_ascii_case("as") {
        return clean_identifier(fields[1]);
    }
    if fields.len() == 1 && !fields[0].eq_ignore_ascii_case("as") {
        return clean_identifier(fields[0]);
    }
    String::new()
}

/// Mirrors `splitTopLevelClause`: splits at the first occurrence of
/// `separator` (matched case-insensitively) that is outside string literals
/// and at paren depth zero.
pub fn split_top_level_clause(value: &str, separator: &str) -> (String, String) {
    let lower = value.to_ascii_lowercase();
    let bytes = value.as_bytes();
    let lower_bytes = lower.as_bytes();
    let sep = separator.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    if bytes.len() >= sep.len() {
        let mut i = 0usize;
        while i <= bytes.len() - sep.len() {
            let ch = bytes[i];
            if ch == b'\'' {
                if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_string = !in_string;
                i += 1;
                continue;
            }
            if in_string {
                i += 1;
                continue;
            }
            match ch {
                b'(' => depth += 1,
                b')' => {
                    if depth > 0 {
                        depth -= 1;
                    }
                }
                _ => {}
            }
            if depth == 0 && lower_bytes[i..].starts_with(sep) {
                return (
                    value[..i].trim().to_string(),
                    value[i + sep.len()..].trim().to_string(),
                );
            }
            i += 1;
        }
    }
    (value.trim().to_string(), String::new())
}

/// Mirrors `splitClause`: splits at the first case-insensitive occurrence of
/// `separator` anywhere (no string/paren awareness).
pub fn split_clause(value: &str, separator: &str) -> (String, String) {
    let lower = value.to_ascii_lowercase();
    match lower.find(separator) {
        None => (value.trim().to_string(), String::new()),
        Some(index) => (
            value[..index].trim().to_string(),
            value[index + separator.len()..].trim().to_string(),
        ),
    }
}

/// Mirrors `splitSQLStatements`: splits on `;` outside string literals; each
/// emitted statement keeps its trailing semicolon (legacy appends before checking).
pub fn split_sql_statements(query: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let bytes = query.as_bytes();
    let mut current: Vec<u8> = Vec::new();
    let mut in_string = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i];
        current.push(ch);
        if ch == b'\'' {
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                current.push(bytes[i + 1]);
                i += 2;
                continue;
            }
            in_string = !in_string;
        }
        if ch == b';' && !in_string {
            let statement = String::from_utf8(current.clone()).unwrap_or_default();
            let statement = statement.trim();
            if !statement
                .trim_matches(|c| matches!(c, ';' | ' ' | '\t' | '\r' | '\n'))
                .is_empty()
            {
                statements.push(statement.to_string());
            }
            current.clear();
        }
        i += 1;
    }
    let statement = String::from_utf8(current).unwrap_or_default();
    let statement = statement.trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
    }
    statements
}

/// Mirrors `parseQualifiedName`: `a.b` → (a, b); bare `t` → (public, t). Extra
/// dotted segments beyond the second are ignored, as in legacy.
pub fn parse_qualified_name(value: &str) -> crate::model::QualifiedName {
    let token = first_identifier_token(value);
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() == 1 {
        return crate::model::QualifiedName {
            schema: "public".to_string(),
            table: clean_identifier(parts[0]),
        };
    }
    crate::model::QualifiedName {
        schema: clean_identifier(parts[0]),
        table: clean_identifier(parts[1]),
    }
}

/// Mirrors `firstIdentifierToken`.
pub fn first_identifier_token(value: &str) -> &str {
    let value = value.trim();
    for (i, r) in value.char_indices() {
        if r.is_whitespace() || r == '(' || r == ';' {
            return &value[..i];
        }
    }
    value
}

/// Mirrors `cleanIdentifier`: trim whitespace then surrounding double quotes.
pub fn clean_identifier(value: &str) -> String {
    value.trim().trim_matches('"').to_string()
}

/// Mirrors `cleanColumnIdentifier`: drops any `alias.` qualifier.
pub fn clean_column_identifier(value: &str) -> String {
    let mut cleaned = clean_identifier(value);
    if let Some(dot) = cleaned.rfind('.') {
        cleaned = cleaned[dot + 1..].to_string();
    }
    clean_identifier(&cleaned)
}

/// Mirrors `matchingParen`: index of the `)` matching the `(` at `open`.
pub fn matching_paren(value: &str, open: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    if open >= bytes.len() || bytes[open] != b'(' {
        return None;
    }
    let mut depth = 0i32;
    let mut in_string = false;
    let mut i = open;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_string = !in_string;
        }
        if in_string {
            i += 1;
            continue;
        }
        match ch {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Mirrors `parseColumns` (CREATE TABLE column list).
pub fn parse_columns(value: &str) -> Result<Vec<Column>, SqlError> {
    let definitions = split_comma_separated(value);
    let mut columns = Vec::with_capacity(definitions.len());
    for definition in &definitions {
        let fields: Vec<&str> = definition.trim().split_whitespace().collect();
        if fields.len() < 2 {
            return Err(SqlError::new(
                "CREATE TABLE column definition requires name and type",
            ));
        }
        let name = clean_identifier(fields[0]);
        if name.is_empty() {
            return Err(SqlError::new("CREATE TABLE column name cannot be empty"));
        }
        columns.push(parse_column_definition(name, fields[1], &fields[2..]));
    }
    if columns.is_empty() {
        return Err(SqlError::new("CREATE TABLE requires at least one column"));
    }
    Ok(columns)
}

/// Mirrors `parseColumnDefinition`: scans column attributes (ENCODE, DEFAULT,
/// IDENTITY, GENERATED ... AS IDENTITY, DISTKEY, SORTKEY) by token.
pub fn parse_column_definition(name: String, data_type: &str, attributes: &[&str]) -> Column {
    let mut column = Column {
        name,
        data_type: data_type.to_lowercase(),
        ..Column::default()
    };
    let mut i = 0usize;
    while i < attributes.len() {
        let token = attributes[i].to_lowercase();
        if token == "encode" {
            if i + 1 < attributes.len() {
                column.encoding = clean_identifier(attributes[i + 1]);
                i += 1;
            }
        } else if token == "default" {
            if i + 1 < attributes.len() && !attributes[i + 1].eq_ignore_ascii_case("as") {
                column.default_value = attributes[i + 1].to_string();
                i += 1;
            }
        } else if token == "identity" || token.starts_with("identity(") {
            column.identity = true;
        } else if token == "generated" {
            while i + 1 < attributes.len() {
                i += 1;
                let next = attributes[i].to_lowercase();
                if next == "identity" || next.starts_with("identity(") {
                    column.identity = true;
                    break;
                }
            }
        } else if token == "distkey" {
            column.dist_key = true;
        } else if token == "sortkey" {
            column.sort_key = true;
        }
        i += 1;
    }
    column
}

/// Mirrors `applyColumnTableAttributes`: column-level DISTKEY/SORTKEY markers
/// flow into the table-level attributes when not already set.
pub fn apply_column_table_attributes(
    columns: &[Column],
    dist_style: &mut String,
    dist_key: &mut String,
    sort_keys: &mut Vec<String>,
) {
    for column in columns {
        if column.dist_key && dist_key.is_empty() {
            *dist_key = column.name.clone();
            if dist_style.is_empty() {
                *dist_style = "key".to_string();
            }
        }
        if column.sort_key && !contains_identifier(sort_keys, &column.name) {
            sort_keys.push(column.name.clone());
        }
    }
}

/// Mirrors `containsIdentifier`.
pub fn contains_identifier(values: &[String], value: &str) -> bool {
    values.iter().any(|item| item.eq_ignore_ascii_case(value))
}

/// Mirrors `parseTableAttributes`: extracts DISTSTYLE / DISTKEY(...) /
/// SORTKEY(...) from the trailing CREATE TABLE attribute text.
pub fn parse_table_attributes(value: &str) -> (String, String, Vec<String>) {
    let fields: Vec<&str> = value.split_whitespace().collect();
    let mut dist_style = String::new();
    let mut dist_key = String::new();
    let mut sort_keys: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < fields.len() {
        let token = fields[i].to_lowercase();
        if token == "diststyle" && i + 1 < fields.len() {
            dist_style = clean_identifier(fields[i + 1]).to_lowercase();
            i += 1;
        } else if token.starts_with("distkey") {
            if let Some(key) = parse_parenthesized_identifier(fields[i], "distkey") {
                dist_key = key;
            } else if i + 1 < fields.len() {
                dist_key = parse_parenthesized_identifier(fields[i + 1], "").unwrap_or_default();
                i += 1;
            }
        } else if token.starts_with("sortkey") {
            let keys = parse_parenthesized_identifier_list(fields[i], "sortkey");
            if !keys.is_empty() {
                sort_keys = keys;
            } else if i + 1 < fields.len() {
                sort_keys = parse_parenthesized_identifier_list(fields[i + 1], "");
                i += 1;
            }
        }
        i += 1;
    }
    (dist_style, dist_key, sort_keys)
}

/// Mirrors `parseParenthesizedIdentifier` (first identifier or none).
pub fn parse_parenthesized_identifier(value: &str, prefix: &str) -> Option<String> {
    let values = parse_parenthesized_identifier_list(value, prefix);
    values.into_iter().next()
}

/// Mirrors `parseParenthesizedIdentifierList`.
pub fn parse_parenthesized_identifier_list(value: &str, prefix: &str) -> Vec<String> {
    let mut value = value.trim();
    if !prefix.is_empty() {
        if !value.to_lowercase().starts_with(prefix) {
            return Vec::new();
        }
        value = value[prefix.len()..].trim();
    }
    let value = value.trim();
    let bytes = value.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'(' || bytes[bytes.len() - 1] != b')' {
        return Vec::new();
    }
    let parts = split_comma_separated(&value[1..value.len() - 1]);
    parts
        .iter()
        .map(|part| clean_identifier(part))
        .filter(|cleaned| !cleaned.is_empty())
        .collect()
}

/// Mirrors `splitCommaSeparated`: splits on commas outside string literals at
/// paren depth zero. Parts are trimmed; only the trailing part is dropped when
/// empty (intermediate empty parts are kept, as in legacy).
pub fn split_comma_separated(value: &str) -> Vec<String> {
    let bytes = value.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_string = !in_string;
            i += 1;
            continue;
        }
        if !in_string {
            match ch {
                b'(' => depth += 1,
                b')' => depth -= 1,
                b',' if depth == 0 => {
                    parts.push(value[start..i].trim().to_string());
                    start = i + 1;
                }
                _ => {}
            }
        }
        i += 1;
    }
    let last = value[start..].trim();
    if !last.is_empty() {
        parts.push(last.to_string());
    }
    parts
}

/// Mirrors `parseCSVishValues`.
pub fn parse_csvish_values(value: &str) -> Result<Vec<String>, SqlError> {
    let parts = split_comma_separated(value);
    let mut values = Vec::with_capacity(parts.len());
    for part in &parts {
        values.push(parse_literal(part)?);
    }
    Ok(values)
}

/// Mirrors `parseValuesTuples`: parses `(...), (...), ...` row value lists.
pub fn parse_values_tuples(value: &str) -> Result<Vec<Vec<String>>, SqlError> {
    let mut rows = Vec::new();
    let mut value = value.trim();
    if value.is_empty() {
        return Err(SqlError::new("INSERT requires at least one VALUES row"));
    }
    loop {
        if value.as_bytes()[0] != b'(' {
            return Err(SqlError::new("INSERT requires parenthesized VALUES rows"));
        }
        let close = matching_paren(value, 0)
            .ok_or_else(|| SqlError::new("INSERT has an unterminated row value list"))?;
        rows.push(parse_csvish_values(&value[1..close])?);
        value = value[close + 1..].trim();
        if value.is_empty() {
            break;
        }
        if value.as_bytes()[0] != b',' {
            return Err(SqlError::new(
                "INSERT VALUES rows must be separated by commas",
            ));
        }
        value = value[1..].trim();
        if value.is_empty() {
            return Err(SqlError::new("INSERT requires a VALUES row after comma"));
        }
    }
    if rows.is_empty() {
        return Err(SqlError::new("INSERT requires at least one VALUES row"));
    }
    Ok(rows)
}

/// Mirrors `parseLiteral`: unquotes `'...'` (with `''` escapes) or passes the
/// trimmed token through; empty unquoted tokens are an error.
pub fn parse_literal(value: &str) -> Result<String, SqlError> {
    let value = value.trim();
    let bytes = value.as_bytes();
    if bytes.len() >= 2 && bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\'' {
        return Ok(value[1..value.len() - 1].replace("''", "'"));
    }
    if value.is_empty() {
        return Err(SqlError::new("empty literal"));
    }
    Ok(value.to_string())
}

/// Mirrors `parseLeadingSQLStringLiteral`: consumes a leading `'...'` literal
/// and returns (unquoted value, trimmed remainder).
pub fn parse_leading_sql_string_literal(value: &str) -> Result<(String, String), SqlError> {
    let value = value.trim();
    let bytes = value.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'\'' {
        return Err(SqlError::new("expected SQL string literal"));
    }
    let mut builder: Vec<u8> = Vec::new();
    let mut i = 1usize;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch != b'\'' {
            builder.push(ch);
            i += 1;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
            builder.push(b'\'');
            i += 2;
            continue;
        }
        let content =
            String::from_utf8(builder).map_err(|_| SqlError::new("expected SQL string literal"))?;
        return Ok((content, value[i + 1..].trim().to_string()));
    }
    Err(SqlError::new("unterminated SQL string literal"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- split_top_level_clause ----

    #[test]
    fn split_top_level_clause_splits_at_top_level_separator() {
        let (left, right) = split_top_level_clause("SELECT a FROM t WHERE x = 1", "where");
        assert_eq!(left, "SELECT a FROM t");
        assert_eq!(right, "x = 1");
    }

    #[test]
    fn split_top_level_clause_ignores_separator_inside_parens() {
        let (left, right) =
            split_top_level_clause("SELECT a FROM (SELECT b FROM t WHERE b > 0) s", "where");
        assert_eq!(left, "SELECT a FROM (SELECT b FROM t WHERE b > 0) s");
        assert_eq!(right, "");
    }

    #[test]
    fn split_top_level_clause_ignores_separator_inside_string() {
        let (left, right) =
            split_top_level_clause("SELECT 'no where here' FROM t WHERE x = 1", "where");
        assert_eq!(left, "SELECT 'no where here' FROM t");
        assert_eq!(right, "x = 1");
    }

    #[test]
    fn split_top_level_clause_separator_not_found_returns_empty_right() {
        let (left, right) = split_top_level_clause("SELECT a FROM t", "where");
        assert_eq!(left, "SELECT a FROM t");
        assert_eq!(right, "");
    }

    #[test]
    fn split_top_level_clause_is_case_insensitive() {
        // The separator is matched case-insensitively against the *lowercased* value;
        // the separator argument itself must be lowercase (the function compares
        // lower_bytes[i..].starts_with(sep) where sep = separator.as_bytes() as-is).
        let (left, right) = split_top_level_clause("a FROM b", "from");
        assert_eq!(left, "a");
        assert_eq!(right, "b");
    }

    #[test]
    fn split_top_level_clause_nested_parens_skipped() {
        // nested parens: (a (b) c) — the separator inside should be skipped
        let (left, right) = split_top_level_clause("OUTER(a (b WHERE c) d) WHERE e = 1", "where");
        assert_eq!(left, "OUTER(a (b WHERE c) d)");
        assert_eq!(right, "e = 1");
    }

    // ---- split_clause ----

    #[test]
    fn split_clause_splits_at_first_occurrence() {
        let (left, right) = split_clause("name = 'it''s'", "=");
        assert_eq!(left, "name");
        assert_eq!(right, "'it''s'");
    }

    #[test]
    fn split_clause_no_match_returns_empty_right() {
        let (left, right) = split_clause("SELECT * FROM t", "WHERE");
        assert_eq!(left, "SELECT * FROM t");
        assert_eq!(right, "");
    }

    #[test]
    fn split_clause_is_case_insensitive() {
        // split_clause does not skip parens/strings — it's simpler
        let (left, right) = split_clause("a WHERE b", "where");
        assert_eq!(left, "a");
        assert_eq!(right, "b");
    }

    // ---- matching_paren ----

    #[test]
    fn matching_paren_simple() {
        assert_eq!(matching_paren("(abc)", 0), Some(4));
    }

    #[test]
    fn matching_paren_nested() {
        assert_eq!(matching_paren("(a(b)c)", 0), Some(6));
    }

    #[test]
    fn matching_paren_string_with_paren_inside() {
        // '(' inside a string literal should not count as opening paren;
        // matching close for the outer '(' at 0 is at index 4, not 5.
        // Input: ( ' ( ' )  — indexes 0..4; the ')' at 4 closes the outer '('.
        assert_eq!(matching_paren("('(')", 0), Some(4));
    }

    #[test]
    fn matching_paren_unbalanced_returns_none() {
        assert_eq!(matching_paren("(abc", 0), None);
    }

    #[test]
    fn matching_paren_not_open_paren_returns_none() {
        assert_eq!(matching_paren("abc)", 1), None);
    }

    #[test]
    fn matching_paren_index_out_of_range_returns_none() {
        assert_eq!(matching_paren("(abc)", 10), None);
    }

    #[test]
    fn matching_paren_escaped_quote_inside_string() {
        // 'it''s' — the escaped quote should not end the string early
        assert_eq!(matching_paren("('it''s')", 0), Some(8));
    }

    // ---- split_sql_statements ----

    #[test]
    fn split_sql_statements_single_statement_with_semicolon() {
        let stmts = split_sql_statements("SELECT 1;");
        assert_eq!(stmts, vec!["SELECT 1;"]);
    }

    #[test]
    fn split_sql_statements_multiple() {
        let stmts = split_sql_statements("SELECT 1; SELECT 2;");
        assert_eq!(stmts, vec!["SELECT 1;", "SELECT 2;"]);
    }

    #[test]
    fn split_sql_statements_semicolon_inside_string_does_not_split() {
        let stmts = split_sql_statements("SELECT 'a;b' FROM t;");
        assert_eq!(stmts, vec!["SELECT 'a;b' FROM t;"]);
    }

    #[test]
    fn split_sql_statements_no_trailing_semicolon() {
        let stmts = split_sql_statements("SELECT 1");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn split_sql_statements_empty_input() {
        let stmts = split_sql_statements("");
        assert!(stmts.is_empty());
    }

    #[test]
    fn split_sql_statements_only_semicolons_are_empty() {
        // bare ";" alone should produce nothing meaningful
        let stmts = split_sql_statements(";");
        assert!(stmts.is_empty(), "got: {:?}", stmts);
    }

    #[test]
    fn split_sql_statements_escaped_quote_in_string() {
        // 'it''s' — escaped quote should not break string tracking
        let stmts = split_sql_statements("SELECT 'it''s a test; not a split'; SELECT 2;");
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 'it''s a test; not a split';");
        assert_eq!(stmts[1], "SELECT 2;");
    }

    // ---- parse_qualified_name ----

    #[test]
    fn parse_qualified_name_bare_table_defaults_to_public() {
        let qn = parse_qualified_name("mytable");
        assert_eq!(qn.schema, "public");
        assert_eq!(qn.table, "mytable");
    }

    #[test]
    fn parse_qualified_name_schema_dot_table() {
        let qn = parse_qualified_name("myschema.mytable");
        assert_eq!(qn.schema, "myschema");
        assert_eq!(qn.table, "mytable");
    }

    #[test]
    fn parse_qualified_name_quoted_identifiers() {
        let qn = parse_qualified_name("\"MySchema\".\"MyTable\"");
        assert_eq!(qn.schema, "MySchema");
        assert_eq!(qn.table, "MyTable");
    }

    #[test]
    fn parse_qualified_name_stops_at_whitespace() {
        // first_identifier_token stops at whitespace
        let qn = parse_qualified_name("t WHERE x = 1");
        assert_eq!(qn.schema, "public");
        assert_eq!(qn.table, "t");
    }

    // ---- clean_identifier / clean_column_identifier ----

    #[test]
    fn clean_identifier_trims_whitespace_and_quotes() {
        assert_eq!(clean_identifier("  \"MyCol\"  "), "MyCol");
    }

    #[test]
    fn clean_identifier_unquoted_stays_as_is() {
        assert_eq!(clean_identifier("mycol"), "mycol");
    }

    #[test]
    fn clean_identifier_empty_returns_empty() {
        assert_eq!(clean_identifier(""), "");
    }

    #[test]
    fn clean_column_identifier_drops_alias_qualifier() {
        assert_eq!(clean_column_identifier("t.col"), "col");
    }

    #[test]
    fn clean_column_identifier_quoted_with_alias() {
        assert_eq!(clean_column_identifier("\"alias\".\"Col\""), "Col");
    }

    #[test]
    fn clean_column_identifier_bare_column_unchanged() {
        assert_eq!(clean_column_identifier("mycol"), "mycol");
    }

    // ---- split_comma_separated ----

    #[test]
    fn split_comma_separated_simple() {
        assert_eq!(split_comma_separated("a, b, c"), vec!["a", "b", "c"]);
    }

    #[test]
    fn split_comma_separated_ignores_comma_inside_parens() {
        let parts = split_comma_separated("f(a, b), c");
        assert_eq!(parts, vec!["f(a, b)", "c"]);
    }

    #[test]
    fn split_comma_separated_ignores_comma_inside_string() {
        let parts = split_comma_separated("'a,b', c");
        assert_eq!(parts, vec!["'a,b'", "c"]);
    }

    #[test]
    fn split_comma_separated_empty_string_returns_empty_vec() {
        let parts = split_comma_separated("");
        assert!(parts.is_empty());
    }

    #[test]
    fn split_comma_separated_single_element_no_comma() {
        let parts = split_comma_separated("abc");
        assert_eq!(parts, vec!["abc"]);
    }

    // ---- parse_csvish_values / parse_values_tuples ----

    #[test]
    fn parse_csvish_values_strings_and_numbers() {
        let vals = parse_csvish_values("'hello', 42, NULL").unwrap();
        assert_eq!(vals, vec!["hello", "42", "NULL"]);
    }

    #[test]
    fn parse_csvish_values_escaped_quote_in_string() {
        let vals = parse_csvish_values("'it''s'").unwrap();
        assert_eq!(vals, vec!["it's"]);
    }

    #[test]
    fn parse_csvish_values_comma_inside_string_stays_intact() {
        let vals = parse_csvish_values("'a,b', 2").unwrap();
        assert_eq!(vals, vec!["a,b", "2"]);
    }

    #[test]
    fn parse_values_tuples_single_row() {
        let rows = parse_values_tuples("(1, 'a')").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec!["1", "a"]);
    }

    #[test]
    fn parse_values_tuples_multiple_rows() {
        let rows = parse_values_tuples("(1, 'a'), (2, 'b')").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1], vec!["2", "b"]);
    }

    #[test]
    fn parse_values_tuples_unterminated_paren_is_err() {
        assert!(parse_values_tuples("(1, 'a'").is_err());
    }

    #[test]
    fn parse_values_tuples_missing_row_after_comma_is_err() {
        assert!(parse_values_tuples("(1),").is_err());
    }

    #[test]
    fn parse_values_tuples_empty_is_err() {
        assert!(parse_values_tuples("").is_err());
    }

    #[test]
    fn parse_values_tuples_not_parenthesized_is_err() {
        assert!(parse_values_tuples("1, 2").is_err());
    }

    // ---- parse_select_literal / parse_select_alias ----

    #[test]
    fn parse_select_literal_bare_number() {
        let (val, alias) = parse_select_literal("42", 1).unwrap();
        assert_eq!(val, "42");
        assert_eq!(alias, "?column?");
    }

    #[test]
    fn parse_select_literal_ordinal_2_alias() {
        let (val, alias) = parse_select_literal("42", 2).unwrap();
        assert_eq!(val, "42");
        assert_eq!(alias, "?column2?");
    }

    #[test]
    fn parse_select_literal_with_as_alias() {
        let (val, alias) = parse_select_literal("42 AS answer", 1).unwrap();
        assert_eq!(val, "42");
        assert_eq!(alias, "answer");
    }

    #[test]
    fn parse_select_literal_with_implicit_alias() {
        let (val, alias) = parse_select_literal("42 myalias", 1).unwrap();
        assert_eq!(val, "42");
        assert_eq!(alias, "myalias");
    }

    #[test]
    fn parse_select_literal_string_literal_no_alias() {
        let (val, alias) = parse_select_literal("'hello'", 1).unwrap();
        assert_eq!(val, "hello");
        assert_eq!(alias, "?column?");
    }

    #[test]
    fn parse_select_literal_string_literal_with_alias() {
        let (val, alias) = parse_select_literal("'hello' AS greeting", 1).unwrap();
        assert_eq!(val, "hello");
        assert_eq!(alias, "greeting");
    }

    #[test]
    fn parse_select_literal_empty_is_err() {
        assert!(parse_select_literal("", 1).is_err());
    }

    #[test]
    fn parse_select_alias_as_keyword() {
        assert_eq!(parse_select_alias("AS myalias"), "myalias");
    }

    #[test]
    fn parse_select_alias_implicit_single_word() {
        assert_eq!(parse_select_alias("myalias"), "myalias");
    }

    #[test]
    fn parse_select_alias_empty() {
        assert_eq!(parse_select_alias(""), "");
    }

    #[test]
    fn parse_select_alias_as_alone_returns_empty() {
        // just "AS" with no name — ambiguous, should return empty
        assert_eq!(parse_select_alias("AS"), "");
    }

    // ---- first_identifier_token ----

    #[test]
    fn first_identifier_token_stops_at_whitespace() {
        assert_eq!(first_identifier_token("abc def"), "abc");
    }

    #[test]
    fn first_identifier_token_stops_at_paren() {
        assert_eq!(first_identifier_token("func(a, b)"), "func");
    }

    #[test]
    fn first_identifier_token_stops_at_semicolon() {
        assert_eq!(first_identifier_token("t;"), "t");
    }

    #[test]
    fn first_identifier_token_entire_value_when_no_delimiter() {
        assert_eq!(first_identifier_token("mytable"), "mytable");
    }

    // ---- contains_identifier ----

    #[test]
    fn contains_identifier_case_insensitive_match() {
        let vals = vec!["Col1".to_string(), "Col2".to_string()];
        assert!(contains_identifier(&vals, "col1"));
        assert!(contains_identifier(&vals, "COL2"));
    }

    #[test]
    fn contains_identifier_no_match() {
        let vals = vec!["Col1".to_string()];
        assert!(!contains_identifier(&vals, "Col3"));
    }

    // ---- parse_columns / parse_column_definition / parse_table_attributes ----

    #[test]
    fn parse_columns_basic() {
        let cols = parse_columns("id integer, name varchar").unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, "integer");
        assert_eq!(cols[1].name, "name");
    }

    #[test]
    fn parse_columns_empty_is_err() {
        assert!(parse_columns("").is_err());
    }

    #[test]
    fn parse_columns_missing_type_is_err() {
        assert!(parse_columns("id").is_err());
    }

    #[test]
    fn parse_column_definition_encode_attribute() {
        let col = parse_column_definition("c".to_string(), "integer", &["ENCODE", "lzo"]);
        assert_eq!(col.encoding, "lzo");
    }

    #[test]
    fn parse_column_definition_default_attribute() {
        let col = parse_column_definition("c".to_string(), "varchar", &["DEFAULT", "'foo'"]);
        assert_eq!(col.default_value, "'foo'");
    }

    #[test]
    fn parse_column_definition_identity_attribute() {
        let col = parse_column_definition("id".to_string(), "bigint", &["IDENTITY"]);
        assert!(col.identity);
    }

    #[test]
    fn parse_column_definition_generated_as_identity() {
        let col = parse_column_definition(
            "id".to_string(),
            "bigint",
            &["GENERATED", "ALWAYS", "AS", "IDENTITY"],
        );
        assert!(col.identity);
    }

    #[test]
    fn parse_column_definition_distkey_sortkey() {
        let col = parse_column_definition("k".to_string(), "integer", &["DISTKEY", "SORTKEY"]);
        assert!(col.dist_key);
        assert!(col.sort_key);
    }

    #[test]
    fn parse_table_attributes_diststyle() {
        let (style, key, sort) = parse_table_attributes("DISTSTYLE KEY DISTKEY(col1)");
        assert_eq!(style, "key");
        assert_eq!(key, "col1");
        assert!(sort.is_empty());
    }

    #[test]
    fn parse_table_attributes_sortkey_multi() {
        // parse_table_attributes tokenizes by whitespace, so the SORTKEY list must
        // have no internal spaces — "SORTKEY(a,b,c)" is one token.
        let (_, _, sort) = parse_table_attributes("SORTKEY(a,b,c)");
        assert_eq!(sort, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_table_attributes_empty() {
        let (style, key, sort) = parse_table_attributes("");
        assert_eq!(style, "");
        assert_eq!(key, "");
        assert!(sort.is_empty());
    }

    // ---- apply_column_table_attributes ----

    #[test]
    fn apply_column_table_attributes_propagates_distkey() {
        let cols = vec![Column {
            name: "mykey".to_string(),
            data_type: "integer".to_string(),
            dist_key: true,
            ..Column::default()
        }];
        let mut style = String::new();
        let mut key = String::new();
        let mut sort: Vec<String> = Vec::new();
        apply_column_table_attributes(&cols, &mut style, &mut key, &mut sort);
        assert_eq!(key, "mykey");
        assert_eq!(style, "key");
    }

    #[test]
    fn apply_column_table_attributes_distkey_not_overwritten_when_set() {
        let cols = vec![Column {
            name: "col".to_string(),
            data_type: "integer".to_string(),
            dist_key: true,
            ..Column::default()
        }];
        let mut style = "even".to_string();
        let mut key = "existing".to_string();
        let mut sort: Vec<String> = Vec::new();
        apply_column_table_attributes(&cols, &mut style, &mut key, &mut sort);
        // dist_key was already set, so no overwrite
        assert_eq!(key, "existing");
    }

    #[test]
    fn apply_column_table_attributes_propagates_sortkey() {
        let cols = vec![Column {
            name: "sortcol".to_string(),
            data_type: "integer".to_string(),
            sort_key: true,
            ..Column::default()
        }];
        let mut style = String::new();
        let mut key = String::new();
        let mut sort: Vec<String> = Vec::new();
        apply_column_table_attributes(&cols, &mut style, &mut key, &mut sort);
        assert_eq!(sort, vec!["sortcol"]);
    }

    // ---- parse_parenthesized_identifier(_list) ----

    #[test]
    fn parse_parenthesized_identifier_simple() {
        assert_eq!(
            parse_parenthesized_identifier("DISTKEY(col1)", "distkey"),
            Some("col1".to_string())
        );
    }

    #[test]
    fn parse_parenthesized_identifier_no_prefix_match() {
        assert_eq!(
            parse_parenthesized_identifier("SORTKEY(col1)", "distkey"),
            None
        );
    }

    #[test]
    fn parse_parenthesized_identifier_list_multiple() {
        let list = parse_parenthesized_identifier_list("SORTKEY(a, b, c)", "sortkey");
        assert_eq!(list, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_parenthesized_identifier_list_no_parens_returns_empty() {
        let list = parse_parenthesized_identifier_list("col", "");
        assert!(list.is_empty());
    }
}
