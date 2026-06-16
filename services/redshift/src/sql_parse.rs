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
