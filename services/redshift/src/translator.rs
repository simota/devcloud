//! Redshift→PostgreSQL SQL translator.
//!
//! Parity: `internal/services/redshift/translator/translator.rs`, quirks
//! included — the rewrites are byte-scanning string surgery (single-quote /
//! double-quote awareness, paren depth), not a SQL grammar. Every scan indexes
//! bytes exactly like legacy; slices only ever split at positions whose byte is
//! ASCII, so `&str` boundaries are safe.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::errors::SqlError;

/// Mirrors `RedshiftTranslator`: converts Redshift dialect SQL into backend
/// SQL plus devcloud-owned metadata or side effects.
pub trait RedshiftTranslator: Send + Sync {
    fn translate(&self, session: &Session, sql: &str) -> Result<TranslationResult, SqlError>;
}

#[derive(Debug, Clone, Default)]
pub struct Session {
    pub database: String,
    pub user: String,
    pub schema: String,
}

#[derive(Debug, Clone, Default)]
pub struct Parameter {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Default)]
pub struct TranslationResult {
    pub backend_sql: String,
    pub parameters: Vec<Parameter>,
    pub metadata_effects: Vec<MetadataEffect>,
    pub side_effects: Vec<SideEffect>,
    pub handled_by_devcloud: bool,
}

/// Mirrors legacy `MetadataEffect.Kind` string constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataEffectKind {
    CreateTable,
    DistStyle,
    DistKey,
    SortKey,
    Encode,
    Backup,
    Identity,
    Default,
}

impl fmt::Display for MetadataEffectKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            MetadataEffectKind::CreateTable => "CREATE_TABLE",
            MetadataEffectKind::DistStyle => "DISTSTYLE",
            MetadataEffectKind::DistKey => "DISTKEY",
            MetadataEffectKind::SortKey => "SORTKEY",
            MetadataEffectKind::Encode => "ENCODE",
            MetadataEffectKind::Backup => "BACKUP",
            MetadataEffectKind::Identity => "IDENTITY",
            MetadataEffectKind::Default => "DEFAULT",
        })
    }
}

#[derive(Debug, Clone)]
pub struct MetadataEffect {
    pub kind: MetadataEffectKind,
    pub schema: String,
    pub table: String,
    pub name: String,
    pub value: String,
    pub backup: String,
    pub columns: Vec<ColumnMetadata>,
    pub sort_keys: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: String,
    pub encoding: String,
    pub default_value: String,
    pub identity: bool,
}

/// Mirrors legacy `SideEffect.Kind` string constants ("COPY" / "UNLOAD").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SideEffectKind {
    Copy,
    Unload,
}

impl fmt::Display for SideEffectKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SideEffectKind::Copy => "COPY",
            SideEffectKind::Unload => "UNLOAD",
        })
    }
}

#[derive(Debug, Clone)]
pub struct SideEffect {
    pub kind: SideEffectKind,
    pub source: String,
    pub target: String,
}

pub const POSTGRES_COALESCE: &str = "COALESCE";
pub const POSTGRES_CURRENT_TIMESTAMP: &str = "CURRENT_TIMESTAMP";
pub const POSTGRES_CLOCK_TIMESTAMP: &str = "clock_timestamp()";
pub const POSTGRES_RANDOM: &str = "random()";
pub const POSTGRES_BOOL_AND: &str = "bool_and";
pub const POSTGRES_BOOL_OR: &str = "bool_or";
pub const POSTGRES_STRING_AGG: &str = "string_agg";

/// Mirrors `Passthrough`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Passthrough;

impl RedshiftTranslator for Passthrough {
    fn translate(&self, _session: &Session, sql: &str) -> Result<TranslationResult, SqlError> {
        Ok(TranslationResult {
            backend_sql: sql.to_string(),
            ..TranslationResult::default()
        })
    }
}

/// Mirrors `RedshiftToPostgres` (`NewRedshiftToPostgres`).
#[derive(Debug, Clone, Copy, Default)]
pub struct RedshiftToPostgres;

type StatementTranslation = Option<Result<TranslationResult, SqlError>>;

impl RedshiftTranslator for RedshiftToPostgres {
    fn translate(&self, _session: &Session, sql: &str) -> Result<TranslationResult, SqlError> {
        let sql = translate_select_top_limit(sql);
        let sql = rewrite_lateral_column_aliases(&sql);
        let sql = rewrite_null_ordering_defaults(&sql);
        let translators: [fn(&str) -> StatementTranslation; 17] = [
            translate_create_external_schema,
            translate_create_external_table,
            translate_create_materialized_view,
            translate_merge_into,
            translate_insert_select_returning,
            translate_insert_values_default,
            translate_alter_column_encode,
            translate_alter_add_column_default_identity,
            translate_truncate_immediate_commit,
            translate_qualify_select,
            translate_create_model,
            translate_create_external_function,
            translate_datashare,
            translate_masking_policy,
            translate_row_access_policy,
            translate_grant_assume_role,
            translate_create_table,
        ];
        for translate in translators {
            if let Some(result) = translate(&sql) {
                let mut translated = result?;
                translated.backend_sql = rewrite_postgres_compatibility(&translated.backend_sql);
                return Ok(translated);
            }
        }
        Ok(TranslationResult {
            backend_sql: rewrite_postgres_compatibility(&rewrite_late_binding_view(&sql)),
            ..TranslationResult::default()
        })
    }
}

fn passthrough_statement(statement: &str) -> TranslationResult {
    TranslationResult {
        backend_sql: statement.to_string(),
        ..TranslationResult::default()
    }
}

fn select_one() -> TranslationResult {
    TranslationResult {
        backend_sql: "select 1".to_string(),
        ..TranslationResult::default()
    }
}

/// Mirrors `translateSelectTopLimit`.
fn translate_select_top_limit(sql: &str) -> String {
    let statement = sql.trim_end_matches(';').trim();
    let Some(select_end) = match_keyword_sequence(statement, 0, &["select"]) else {
        return sql.to_string();
    };
    let top_start = skip_spaces(statement, select_end);
    let Some(top_end) = match_keyword_sequence(statement, top_start, &["top"]) else {
        return sql.to_string();
    };
    let limit_start = skip_spaces(statement, top_end);
    let Some((limit, limit_end)) = parse_top_limit(statement, limit_start) else {
        return sql.to_string();
    };
    let select_list = statement[limit_end..].trim();
    if select_list.is_empty() {
        return sql.to_string();
    }
    format!(
        "{} {select_list} limit {limit}",
        statement[..select_end].trim()
    )
}

/// Mirrors `parseTopLimit`.
fn parse_top_limit(sql: &str, index: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    if index >= bytes.len() {
        return None;
    }
    if bytes[index] == b'(' {
        let close = matching_paren(sql, index)?;
        let limit = sql[index + 1..close].trim();
        if !is_unsigned_integer(limit) {
            return None;
        }
        return Some((limit.to_string(), close + 1));
    }
    let start = index;
    let mut index = index;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        index += 1;
    }
    if index == start || (index < bytes.len() && is_identifier_part(bytes[index])) {
        return None;
    }
    Some((sql[start..index].to_string(), index))
}

/// Mirrors `isUnsignedInteger`.
fn is_unsigned_integer(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit())
}

/// Mirrors `rewriteNullOrderingDefaults`.
fn rewrite_null_ordering_defaults(sql: &str) -> String {
    let Some((_, order_end)) = find_top_level_keyword_sequence(sql, &["order", "by"], 0) else {
        return sql.to_string();
    };
    let order_list_end = find_top_level_order_by_end(sql, order_end);
    let order_list = sql[order_end..order_list_end].trim();
    if order_list.is_empty() {
        return sql.to_string();
    }

    let items = split_comma_separated(order_list);
    let mut rewritten_items = Vec::with_capacity(items.len());
    let mut changed = false;
    for item in &items {
        let (rewritten, item_changed) = rewrite_order_by_null_default(item);
        rewritten_items.push(rewritten);
        changed = changed || item_changed;
    }
    if !changed {
        return sql.to_string();
    }

    let prefix = sql[..order_end].trim_end_matches([' ', '\t', '\n', '\r']);
    let suffix = &sql[order_list_end..];
    let mut separator = "";
    if !suffix.is_empty()
        && !matches!(
            suffix.as_bytes()[0],
            b' ' | b'\t' | b'\n' | b'\r' | b';' | b',' | b')'
        )
    {
        separator = " ";
    }
    format!("{prefix} {}{separator}{suffix}", rewritten_items.join(", "))
}

/// Mirrors `rewriteOrderByNullDefault`.
fn rewrite_order_by_null_default(item: &str) -> (String, bool) {
    let trimmed = item.trim();
    if trimmed.is_empty() {
        return (item.to_string(), false);
    }
    if find_top_level_keyword_sequence(trimmed, &["nulls", "first"], 0).is_some() {
        return (trimmed.to_string(), false);
    }
    if find_top_level_keyword_sequence(trimmed, &["nulls", "last"], 0).is_some() {
        return (trimmed.to_string(), false);
    }
    if find_top_level_keyword_sequence(trimmed, &["desc"], 0).is_some() {
        return (format!("{trimmed} NULLS FIRST"), true);
    }
    (format!("{trimmed} NULLS LAST"), true)
}

/// Mirrors `findTopLevelOrderByEnd`.
fn find_top_level_order_by_end(sql: &str, start: usize) -> usize {
    let mut end = sql.len();
    if let Some(semicolon) = find_top_level_byte(sql, start, b';') {
        end = semicolon;
    }
    for sequence in [&["limit"][..], &["offset"], &["fetch"], &["for"]] {
        if let Some((sequence_start, _)) = find_top_level_keyword_sequence(sql, sequence, start) {
            if sequence_start < end {
                end = sequence_start;
            }
        }
    }
    end
}

/// Mirrors `findTopLevelKeywordTerminator` (only ever used with ";").
fn find_top_level_byte(sql: &str, start: usize, target: u8) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut in_quoted_identifier = false;
    let mut i = start;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' && !in_quoted_identifier {
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_string = !in_string;
            i += 1;
            continue;
        }
        if ch == b'"' && !in_string {
            if in_quoted_identifier && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            in_quoted_identifier = !in_quoted_identifier;
            i += 1;
            continue;
        }
        if in_string || in_quoted_identifier {
            i += 1;
            continue;
        }
        match ch {
            b'(' => {
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                if depth > 0 {
                    depth -= 1;
                }
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth != 0 {
            i += 1;
            continue;
        }
        if ch == target {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Mirrors `rewriteLateralColumnAliases`.
fn rewrite_lateral_column_aliases(sql: &str) -> String {
    let statement = sql.trim_end_matches(';').trim();
    let Some(select_end) = match_keyword_sequence(statement, 0, &["select"]) else {
        return sql.to_string();
    };

    let mut select_list_end = statement.len();
    if let Some((from_start, _)) = find_top_level_keyword_sequence(statement, &["from"], select_end)
    {
        select_list_end = from_start;
    }
    let mut select_list = statement[select_end..select_list_end].trim();
    if select_list.is_empty() {
        return sql.to_string();
    }
    let mut select_modifier = "";
    for keyword in ["all", "distinct"] {
        if let Some(modifier_end) = match_keyword_sequence(select_list, 0, &[keyword]) {
            select_modifier = select_list[..modifier_end].trim();
            select_list = select_list[modifier_end..].trim();
            break;
        }
    }

    let mut aliases: HashMap<String, String> = HashMap::new();
    let items = split_comma_separated(select_list);
    let mut rewritten_items: Vec<String> = Vec::with_capacity(items.len());
    let mut changed = false;
    for item in &items {
        let (expression, alias, has_alias) = split_select_alias(item);
        let (rewritten_expression, expression_changed) =
            replace_lateral_alias_references(&expression, &aliases);
        changed = changed || expression_changed;
        if has_alias {
            rewritten_items.push(format!("{} as {alias}", rewritten_expression.trim()));
            let cleaned = clean_identifier(&alias);
            if !cleaned.is_empty() {
                aliases.insert(
                    cleaned.to_lowercase(),
                    rewritten_expression.trim().to_string(),
                );
            }
            continue;
        }
        rewritten_items.push(rewritten_expression.trim().to_string());
    }
    if !changed {
        return sql.to_string();
    }

    let suffix = statement[select_list_end..].trim();
    let mut backend_sql = String::from("select ");
    if !select_modifier.is_empty() {
        backend_sql.push_str(select_modifier);
        backend_sql.push(' ');
    }
    backend_sql.push_str(&rewritten_items.join(", "));
    if !suffix.is_empty() {
        backend_sql.push(' ');
        backend_sql.push_str(suffix);
    }
    backend_sql
}

/// Mirrors `splitSelectAlias`.
fn split_select_alias(item: &str) -> (String, String, bool) {
    let Some((as_start, as_end)) = find_top_level_keyword_sequence(item, &["as"], 0) else {
        return split_implicit_select_alias(item);
    };
    let expression = item[..as_start].trim();
    let alias = item[as_end..].trim();
    if expression.is_empty()
        || alias.is_empty()
        || alias
            .bytes()
            .any(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
    {
        return (item.to_string(), String::new(), false);
    }
    (expression.to_string(), alias.to_string(), true)
}

/// Mirrors `splitImplicitSelectAlias`.
fn split_implicit_select_alias(item: &str) -> (String, String, bool) {
    let bytes = item.as_bytes();
    let mut end = bytes.len();
    while end > 0 && matches!(bytes[end - 1], b' ' | b'\t' | b'\n' | b'\r') {
        end -= 1;
    }
    if end == 0 {
        return (item.to_string(), String::new(), false);
    }

    let mut start = end;
    if bytes[start - 1] == b'"' {
        start -= 1;
        while start > 0 {
            start -= 1;
            if bytes[start] != b'"' {
                continue;
            }
            if start > 0 && bytes[start - 1] == b'"' {
                start -= 1;
                continue;
            }
            break;
        }
    } else {
        while start > 0 && is_identifier_part(bytes[start - 1]) {
            start -= 1;
        }
        if start == end || !is_identifier_start(bytes[start]) {
            return (item.to_string(), String::new(), false);
        }
    }

    if start == 0 || !matches!(bytes[start - 1], b' ' | b'\t' | b'\n' | b'\r') {
        return (item.to_string(), String::new(), false);
    }
    let expression = item[..start].trim();
    let alias = item[start..end].trim();
    if expression.is_empty() || alias.is_empty() {
        return (item.to_string(), String::new(), false);
    }
    (expression.to_string(), alias.to_string(), true)
}

/// Mirrors `replaceLateralAliasReferences`.
fn replace_lateral_alias_references(
    value: &str,
    aliases: &HashMap<String, String>,
) -> (String, bool) {
    if aliases.is_empty() {
        return (value.to_string(), false);
    }
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut changed = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            i = copy_quoted_string(&mut out, value, i);
            continue;
        }
        if ch == b'"' {
            i = copy_quoted_identifier(&mut out, value, i);
            continue;
        }
        if !is_identifier_start(ch) {
            out.push(ch);
            i += 1;
            continue;
        }

        let start = i;
        i += 1;
        while i < bytes.len() && is_identifier_part(bytes[i]) {
            i += 1;
        }
        let identifier = &value[start..i];
        if let Some(expression) = aliases.get(&identifier.to_lowercase()) {
            if !is_qualified_identifier_part(value, start, i) {
                out.push(b'(');
                out.extend_from_slice(expression.as_bytes());
                out.push(b')');
                changed = true;
                continue;
            }
        }
        out.extend_from_slice(identifier.as_bytes());
    }
    if !changed {
        return (value.to_string(), false);
    }
    (
        String::from_utf8(out).expect("rewrites preserve UTF-8"),
        true,
    )
}

/// Mirrors `isQualifiedIdentifierPart`.
fn is_qualified_identifier_part(value: &str, start: usize, end: usize) -> bool {
    let bytes = value.as_bytes();
    let mut before = start as isize - 1;
    while before >= 0 && matches!(bytes[before as usize], b' ' | b'\t' | b'\n' | b'\r') {
        before -= 1;
    }
    if before >= 0 && bytes[before as usize] == b'.' {
        return true;
    }
    let after = skip_spaces(value, end);
    after < bytes.len() && bytes[after] == b'.'
}

/// Mirrors `rewriteLateBindingView`.
fn rewrite_late_binding_view(sql: &str) -> String {
    let keywords = ["with", "no", "schema", "binding"];
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            i = copy_quoted_string(&mut out, sql, i);
            continue;
        }
        if ch == b'"' {
            i = copy_quoted_identifier(&mut out, sql, i);
            continue;
        }
        if let Some(next) = match_keyword_sequence(sql, i, &keywords) {
            trim_right_spaces(&mut out);
            i = next;
            continue;
        }
        out.push(ch);
        i += 1;
    }
    String::from_utf8(out)
        .expect("rewrites preserve UTF-8")
        .trim()
        .to_string()
}

/// Mirrors `rewritePostgresCompatibility`.
fn rewrite_postgres_compatibility(sql: &str) -> String {
    rewrite_redshift_functions(&rewrite_redshift_system_tables(
        &rewrite_begin_transaction_modes(&rewrite_reset_command(
            &rewrite_create_procedure_argument_modes(&rewrite_create_function_sql_stable(
                &rewrite_create_function_plpython_language(&rewrite_create_user_password_clauses(
                    &rewrite_explain_verbose(sql),
                )),
            )),
        )),
    ))
}

/// Mirrors `rewriteExplainVerbose`.
fn rewrite_explain_verbose(sql: &str) -> String {
    let start = skip_spaces(sql, 0);
    let Some(explain_end) = match_keyword_sequence(sql, start, &["explain"]) else {
        return sql.to_string();
    };
    let verbose_start = skip_spaces(sql, explain_end);
    let Some(verbose_end) = match_keyword_sequence(sql, verbose_start, &["verbose"]) else {
        return sql.to_string();
    };
    if sql[verbose_end..].trim().is_empty() {
        return sql.to_string();
    }
    format!("{} (VERBOSE){}", &sql[..explain_end], &sql[verbose_end..])
}

/// Mirrors `rewriteCreateFunctionPLPythonLanguage`.
fn rewrite_create_function_plpython_language(sql: &str) -> String {
    let start = skip_spaces(sql, 0);
    let function_end = match_keyword_sequence(sql, start, &["create", "or", "replace", "function"])
        .or_else(|| match_keyword_sequence(sql, start, &["create", "function"]));
    let Some(function_end) = function_end else {
        return sql.to_string();
    };

    let Some((_, language_end)) = find_create_function_language(sql, function_end) else {
        return sql.to_string();
    };
    let language_name_start = skip_spaces(sql, language_end);
    let Some((language_name, language_name_end)) = read_sql_identifier(sql, language_name_start)
    else {
        return sql.to_string();
    };
    if !language_name.eq_ignore_ascii_case("plpythonu") {
        return sql.to_string();
    }
    format!(
        "{}plpython3u{}",
        &sql[..language_name_start],
        &sql[language_name_end..]
    )
}

/// Mirrors `rewriteCreateFunctionSQLStable`.
fn rewrite_create_function_sql_stable(sql: &str) -> String {
    let start = skip_spaces(sql, 0);
    let function_end = match_keyword_sequence(sql, start, &["create", "or", "replace", "function"])
        .or_else(|| match_keyword_sequence(sql, start, &["create", "function"]));
    let Some(function_end) = function_end else {
        return sql.to_string();
    };

    let Some((language_start, language_end)) = find_create_function_language(sql, function_end)
    else {
        return sql.to_string();
    };
    let language_name_start = skip_spaces(sql, language_end);
    let Some((language_name, language_name_end)) = read_sql_identifier(sql, language_name_start)
    else {
        return sql.to_string();
    };
    if !language_name.eq_ignore_ascii_case("sql") {
        return sql.to_string();
    }
    if find_create_function_top_level_keyword(sql, language_name_end, sql.len(), &["stable"])
        .is_some()
    {
        return sql.to_string();
    }

    let Some((as_start, _)) =
        find_create_function_top_level_keyword(sql, function_end, language_start, &["as"])
    else {
        return sql.to_string();
    };
    let Some((stable_start, stable_end)) =
        find_create_function_top_level_keyword(sql, function_end, as_start, &["stable"])
    else {
        return sql.to_string();
    };

    format!(
        "{}{} STABLE{}",
        &sql[..stable_start],
        sql[stable_end..language_name_end].trim_start_matches([' ', '\t', '\n', '\r']),
        &sql[language_name_end..]
    )
}

/// Mirrors `findCreateFunctionTopLevelKeyword`.
fn find_create_function_top_level_keyword(
    sql: &str,
    start: usize,
    end: usize,
    keywords: &[&str],
) -> Option<(usize, usize)> {
    let bytes = sql.as_bytes();
    let end = end.min(bytes.len());
    let mut depth = 0i32;
    let mut i = start;
    while i < end {
        match bytes[i] {
            b'\'' => {
                i = skip_quoted_string(sql, i);
                continue;
            }
            b'"' => {
                i = skip_quoted_identifier(sql, i);
                continue;
            }
            b'$' => {
                if let Some(next) = skip_dollar_quoted_string(sql, i) {
                    i = next;
                    continue;
                }
            }
            b'-' => {
                if i + 1 < end && bytes[i + 1] == b'-' {
                    i = skip_line_comment(sql, i + 2);
                    continue;
                }
            }
            b'/' => {
                if i + 1 < end && bytes[i + 1] == b'*' {
                    i = skip_block_comment(sql, i + 2);
                    continue;
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                if depth > 0 {
                    depth -= 1;
                }
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0 {
            if let Some(keyword_end) = match_keyword_sequence(sql, i, keywords) {
                if keyword_end <= end {
                    return Some((i, keyword_end));
                }
            }
        }
        i += 1;
    }
    None
}

/// Mirrors `findCreateFunctionLanguage`.
fn find_create_function_language(sql: &str, start: usize) -> Option<(usize, usize)> {
    let bytes = sql.as_bytes();
    let mut depth = 0i32;
    let mut i = start;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = skip_quoted_string(sql, i);
                continue;
            }
            b'"' => {
                i = skip_quoted_identifier(sql, i);
                continue;
            }
            b'$' => {
                if let Some(next) = skip_dollar_quoted_string(sql, i) {
                    i = next;
                    continue;
                }
            }
            b'-' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    i = skip_line_comment(sql, i + 2);
                    continue;
                }
            }
            b'/' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    i = skip_block_comment(sql, i + 2);
                    continue;
                }
            }
            b'(' => {
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                if depth > 0 {
                    depth -= 1;
                }
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth == 0 {
            if let Some(end) = match_keyword_sequence(sql, i, &["language"]) {
                return Some((i, end));
            }
        }
        i += 1;
    }
    None
}

/// Mirrors `readSQLIdentifier`.
fn read_sql_identifier(sql: &str, index: usize) -> Option<(&str, usize)> {
    let bytes = sql.as_bytes();
    if index >= bytes.len() || !is_identifier_start(bytes[index]) {
        return None;
    }
    let start = index;
    let mut index = index + 1;
    while index < bytes.len() && is_identifier_part(bytes[index]) {
        index += 1;
    }
    Some((&sql[start..index], index))
}

/// Mirrors `skipDollarQuotedString` (quirks included: because `$` itself is an
/// identifier-part byte, the tag scan always overruns the closing `$`, so this
/// never actually matches — exactly like legacy).
fn skip_dollar_quoted_string(sql: &str, start: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if start >= bytes.len() || bytes[start] != b'$' {
        return None;
    }
    let mut end_tag = start + 1;
    while end_tag < bytes.len() && is_identifier_part(bytes[end_tag]) {
        end_tag += 1;
    }
    if end_tag >= bytes.len() || bytes[end_tag] != b'$' {
        return None;
    }
    let tag = &sql[start..=end_tag];
    match sql[end_tag + 1..].find(tag) {
        None => Some(bytes.len()),
        Some(close) => Some(end_tag + 1 + close + tag.len()),
    }
}

/// Mirrors `skipLineComment`.
fn skip_line_comment(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut start = start;
    while start < bytes.len() && bytes[start] != b'\n' {
        start += 1;
    }
    start
}

/// Mirrors `skipBlockComment`.
fn skip_block_comment(sql: &str, start: usize) -> usize {
    match sql[start.min(sql.len())..].find("*/") {
        None => sql.len(),
        Some(close) => start + close + "*/".len(),
    }
}

/// Mirrors `rewriteCreateProcedureArgumentModes`.
fn rewrite_create_procedure_argument_modes(sql: &str) -> String {
    let start = skip_spaces(sql, 0);
    let procedure_end =
        match_keyword_sequence(sql, start, &["create", "or", "replace", "procedure"])
            .or_else(|| match_keyword_sequence(sql, start, &["create", "procedure"]));
    let Some(procedure_end) = procedure_end else {
        return sql.to_string();
    };

    let Some(open) = find_create_procedure_arguments_open(sql, procedure_end) else {
        return sql.to_string();
    };
    let Some(close) = matching_paren(sql, open) else {
        return sql.to_string();
    };

    let Some(args) = rewrite_create_procedure_argument_list(&sql[open + 1..close]) else {
        return sql.to_string();
    };
    format!("{}{args}{}", &sql[..=open], &sql[close..])
}

/// Mirrors `findCreateProcedureArgumentsOpen`.
fn find_create_procedure_arguments_open(sql: &str, index: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut i = index;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_quoted_string(sql, i),
            b'"' => i = skip_quoted_identifier(sql, i),
            b'(' => return Some(i),
            _ => i += 1,
        }
    }
    None
}

/// Mirrors `rewriteCreateProcedureArgumentList` (None = unchanged).
fn rewrite_create_procedure_argument_list(value: &str) -> Option<String> {
    let args = split_comma_separated(value);
    if args.is_empty() {
        return None;
    }

    let mut rewritten_args = Vec::with_capacity(args.len());
    let mut changed = false;
    for arg in &args {
        let (rewritten, arg_changed) = rewrite_create_procedure_argument(arg);
        rewritten_args.push(rewritten);
        changed = changed || arg_changed;
    }
    if !changed {
        return None;
    }
    Some(rewritten_args.join(", "))
}

/// Mirrors `rewriteCreateProcedureArgument`.
fn rewrite_create_procedure_argument(value: &str) -> (String, bool) {
    let trimmed = value.trim();
    let Some((name, name_end)) = read_sql_token(trimmed, 0) else {
        return (trimmed.to_string(), false);
    };
    if is_procedure_argument_mode(name) {
        let rest = trimmed[name_end..].trim();
        if name.eq_ignore_ascii_case("out") && !procedure_argument_has_default(rest) {
            return (format!("{} {rest} DEFAULT NULL", name.to_uppercase()), true);
        }
        return (trimmed.to_string(), false);
    }
    let mode_start = skip_spaces(trimmed, name_end);
    let Some((mode, mode_end)) = read_sql_token(trimmed, mode_start) else {
        return (trimmed.to_string(), false);
    };
    if !is_procedure_argument_mode(mode) {
        return (trimmed.to_string(), false);
    }
    let arg_type = trimmed[mode_end..].trim();
    if arg_type.is_empty() {
        return (trimmed.to_string(), false);
    }
    let mut rewritten = format!("{} {name} {arg_type}", mode.to_uppercase());
    if mode.eq_ignore_ascii_case("out") && !procedure_argument_has_default(arg_type) {
        rewritten.push_str(" DEFAULT NULL");
    }
    (rewritten, true)
}

/// Mirrors `readSQLToken`.
fn read_sql_token(value: &str, index: usize) -> Option<(&str, usize)> {
    let bytes = value.as_bytes();
    let index = skip_spaces(value, index);
    if index >= bytes.len() {
        return None;
    }
    if bytes[index] == b'"' {
        let mut i = index + 1;
        while i < bytes.len() {
            if bytes[i] != b'"' {
                i += 1;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            return Some((&value[index..=i], i + 1));
        }
        return None;
    }
    let start = index;
    let mut index = index;
    while index < bytes.len() && !matches!(bytes[index], b' ' | b'\t' | b'\n' | b'\r') {
        index += 1;
    }
    Some((&value[start..index], index))
}

/// Mirrors `isProcedureArgumentMode`.
fn is_procedure_argument_mode(value: &str) -> bool {
    if value.starts_with('"') {
        return false;
    }
    matches!(value.to_lowercase().as_str(), "in" | "out" | "inout")
}

/// Mirrors `procedureArgumentHasDefault`.
fn procedure_argument_has_default(value: &str) -> bool {
    if find_top_level_keyword_sequence(value, &["default"], 0).is_some() {
        return true;
    }
    find_top_level_equals(value).is_some()
}

/// Mirrors `findTopLevelEquals`.
fn find_top_level_equals(value: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut in_quoted_identifier = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' && !in_quoted_identifier {
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_string = !in_string;
            i += 1;
            continue;
        }
        if ch == b'"' && !in_string {
            if in_quoted_identifier && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            in_quoted_identifier = !in_quoted_identifier;
            i += 1;
            continue;
        }
        if in_string || in_quoted_identifier {
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
            b'=' => {
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

/// Mirrors `rewriteCreateUserPasswordClauses`.
fn rewrite_create_user_password_clauses(sql: &str) -> String {
    let start = skip_spaces(sql, 0);
    let Some(create_user_end) = match_keyword_sequence(sql, start, &["create", "user"]) else {
        return sql.to_string();
    };

    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len() + 4);
    out.extend_from_slice(&bytes[..create_user_end]);
    let mut changed = false;
    let mut i = create_user_end;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            i = copy_quoted_string(&mut out, sql, i);
            continue;
        }
        if ch == b'"' {
            i = copy_quoted_identifier(&mut out, sql, i);
            continue;
        }
        let Some(password_end) = match_keyword_sequence(sql, i, &["password"]) else {
            out.push(ch);
            i += 1;
            continue;
        };
        out.extend_from_slice(&bytes[i..password_end]);
        let next = copy_spaces(&mut out, sql, password_end);
        let Some(disable_end) = match_keyword_sequence(sql, next, &["disable"]) else {
            i = next;
            continue;
        };
        out.extend_from_slice(b"NULL");
        i = disable_end;
        changed = true;
    }
    if !changed {
        return sql.to_string();
    }
    String::from_utf8(out).expect("rewrites preserve UTF-8")
}

/// Mirrors `rewriteResetCommand`.
fn rewrite_reset_command(sql: &str) -> String {
    let start = skip_spaces(sql, 0);
    let Some(reset_end) = match_keyword_sequence(sql, start, &["reset"]) else {
        return sql.to_string();
    };

    let target_start = skip_spaces(sql, reset_end);
    let Some((target, target_end)) = parse_reset_target(sql, target_start) else {
        return sql.to_string();
    };
    let bytes = sql.as_bytes();
    let next = skip_spaces(sql, target_end);
    let mut suffix = "";
    if next < bytes.len() {
        if bytes[next] != b';' || !sql[next + 1..].trim().is_empty() {
            return sql.to_string();
        }
        suffix = ";";
    }

    if target.eq_ignore_ascii_case("all") {
        sql.to_string()
    } else if target.eq_ignore_ascii_case("query_group") {
        format!("{}RESET application_name{suffix}", &sql[..start])
    } else if target.contains('.') {
        format!(
            "{}SELECT set_config({}, NULL, false){suffix}",
            &sql[..start],
            sql_string_literal(&target)
        )
    } else {
        sql.to_string()
    }
}

/// Mirrors `parseResetTarget`.
fn parse_reset_target(sql: &str, index: usize) -> Option<(String, usize)> {
    if let Some(end) = match_keyword_sequence(sql, index, &["all"]) {
        return Some((sql[index..end].to_string(), end));
    }

    let bytes = sql.as_bytes();
    let mut parts: Vec<String> = Vec::new();
    let mut next = index;
    loop {
        let (part, part_end) = parse_reset_identifier(sql, next)?;
        parts.push(part);
        next = part_end;
        if next >= bytes.len() || bytes[next] != b'.' {
            break;
        }
        next += 1;
    }
    Some((parts.join("."), next))
}

/// Mirrors `parseResetIdentifier`.
fn parse_reset_identifier(sql: &str, index: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    if index >= bytes.len() {
        return None;
    }
    if bytes[index] == b'"' {
        let mut out: Vec<u8> = Vec::new();
        let mut i = index + 1;
        while i < bytes.len() {
            if bytes[i] != b'"' {
                out.push(bytes[i]);
                i += 1;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                out.push(b'"');
                i += 2;
                continue;
            }
            return Some((
                String::from_utf8(out).expect("rewrites preserve UTF-8"),
                i + 1,
            ));
        }
        return None;
    }
    if !is_identifier_start(bytes[index]) {
        return None;
    }
    let mut end = index + 1;
    while end < bytes.len() && is_identifier_part(bytes[end]) {
        end += 1;
    }
    Some((sql[index..end].to_lowercase(), end))
}

/// Mirrors `rewriteBeginTransactionModes`.
fn rewrite_begin_transaction_modes(sql: &str) -> String {
    let start = skip_spaces(sql, 0);
    let Some(begin_end) = match_keyword_sequence(sql, start, &["begin"]) else {
        return sql.to_string();
    };

    let bytes = sql.as_bytes();
    let mut next = skip_spaces(sql, begin_end);
    let read_start = next;
    let read_mode = match_begin_read_mode(sql, read_start);
    if let Some(read_end) = read_mode {
        next = skip_spaces(sql, read_end);
    }

    let isolation_start = next;
    let isolation = match_keyword_sequence(
        sql,
        isolation_start,
        &["isolation", "level", "serializable"],
    );
    if let Some(isolation_end) = isolation {
        next = skip_spaces(sql, isolation_end);
    }
    let (Some(read_end), Some(isolation_end)) = (read_mode, isolation) else {
        return sql.to_string();
    };

    let mut suffix = "";
    if next < bytes.len() {
        if bytes[next] != b';' || !sql[next + 1..].trim().is_empty() {
            return sql.to_string();
        }
        suffix = ";";
    }

    let read_mode = sql[read_start..read_end].trim();
    let isolation_mode = sql[isolation_start..isolation_end].trim();
    format!(
        "{} {read_mode}, {isolation_mode}{suffix}",
        &sql[..begin_end]
    )
}

/// Mirrors `matchBeginReadMode`.
fn match_begin_read_mode(sql: &str, index: usize) -> Option<usize> {
    if let Some(end) = match_keyword_sequence(sql, index, &["read", "only"]) {
        return Some(end);
    }
    match_keyword_sequence(sql, index, &["read", "write"])
}

/// Mirrors `rewriteRedshiftSystemTables`.
fn rewrite_redshift_system_tables(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            i = copy_quoted_string(&mut out, sql, i);
            continue;
        }
        if ch == b'"' {
            i = copy_quoted_identifier(&mut out, sql, i);
            continue;
        }
        let matched = match_keyword_sequence(sql, i, &["from"])
            .or_else(|| match_keyword_sequence(sql, i, &["join"]));
        if let Some(end) = matched {
            out.extend_from_slice(&bytes[i..end]);
            let next = copy_spaces(&mut out, sql, end);
            if let Some((rewritten, rewritten_end)) =
                rewrite_redshift_system_table_reference(sql, next)
            {
                out.extend_from_slice(rewritten.as_bytes());
                i = rewritten_end;
                continue;
            }
            i = next;
            continue;
        }
        out.push(ch);
        i += 1;
    }
    String::from_utf8(out).expect("rewrites preserve UTF-8")
}

/// Mirrors `rewriteRedshiftSystemTableReference`.
fn rewrite_redshift_system_table_reference(sql: &str, index: usize) -> Option<(String, usize)> {
    let (reference, reference_end, table_name) = read_relation_identifier(sql, index)?;
    let replacement = if is_redshift_read_only_system_table(table_name) {
        postgres_redshift_system_table(table_name)
    } else if is_redshift_information_schema_relation(reference) {
        postgres_redshift_information_schema_relation(reference)
    } else {
        return None;
    };
    let after_reference_spaces = skip_spaces(sql, reference_end);
    let mut alias = table_name.to_string();
    let mut next = reference_end;
    if let Some(as_end) = match_keyword_sequence(sql, after_reference_spaces, &["as"]) {
        let alias_start = skip_spaces(sql, as_end);
        let (parsed_alias, alias_end) = read_alias_identifier(sql, alias_start)?;
        alias = parsed_alias;
        next = alias_end;
    } else if let Some((parsed_alias, alias_end)) =
        read_alias_identifier(sql, after_reference_spaces)
    {
        if !is_relation_alias_stop_word(&parsed_alias) {
            alias = parsed_alias;
            next = alias_end;
        }
    }

    Some((format!("{replacement} as {alias}"), next))
}

/// Mirrors `isRedshiftReadOnlySystemTable`.
fn is_redshift_read_only_system_table(table_name: &str) -> bool {
    let normalized = table_name.to_lowercase();
    normalized.starts_with("stv_")
        || normalized.starts_with("stl_")
        || normalized.starts_with("svv_")
        || normalized.starts_with("svl_")
        || normalized.starts_with("sys_")
        || normalized == "pg_table_def"
        || normalized == "pg_table_info"
}

/// Mirrors `readRelationIdentifier`: returns (reference, end, last component).
fn read_relation_identifier(sql: &str, index: usize) -> Option<(&str, usize, &str)> {
    let bytes = sql.as_bytes();
    let start = index;
    let mut index = index;
    let mut last_start;
    loop {
        if index >= bytes.len() || !is_identifier_start(bytes[index]) {
            return None;
        }
        last_start = index;
        index += 1;
        while index < bytes.len() && is_identifier_part(bytes[index]) {
            index += 1;
        }
        if index >= bytes.len() || bytes[index] != b'.' {
            break;
        }
        index += 1;
    }
    Some((&sql[start..index], index, &sql[last_start..index]))
}

/// Mirrors `readAliasIdentifier`.
fn read_alias_identifier(sql: &str, index: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    if index >= bytes.len() {
        return None;
    }
    if bytes[index] == b'"' {
        let mut alias: Vec<u8> = Vec::new();
        let next = copy_quoted_identifier(&mut alias, sql, index);
        if next <= index {
            return None;
        }
        return Some((
            String::from_utf8(alias).expect("rewrites preserve UTF-8"),
            next,
        ));
    }
    if !is_identifier_start(bytes[index]) {
        return None;
    }
    let start = index;
    let mut index = index + 1;
    while index < bytes.len() && is_identifier_part(bytes[index]) {
        index += 1;
    }
    Some((sql[start..index].to_string(), index))
}

/// Mirrors `isRelationAliasStopWord`.
fn is_relation_alias_stop_word(value: &str) -> bool {
    matches!(
        value.trim_matches('"').to_lowercase().as_str(),
        "cross"
            | "except"
            | "fetch"
            | "full"
            | "group"
            | "having"
            | "inner"
            | "intersect"
            | "join"
            | "left"
            | "limit"
            | "offset"
            | "on"
            | "order"
            | "outer"
            | "qualify"
            | "right"
            | "union"
            | "using"
            | "where"
    )
}

/// Mirrors `postgresRedshiftSystemTableStub`.
pub fn postgres_redshift_system_table_stub() -> &'static str {
    "(select null::integer as node, null::integer as slice, null::integer as userid, null::text as user_name, null::integer as pid, null::bigint as xid, null::bigint as query, null::text as label, null::timestamp as starttime, null::timestamp as endtime, null::text as status, null::text as text, null::bigint as rows, null::bigint as bytes, null::bigint as cpu_time, null::boolean as is_diskbased, null::bigint as workmem, null::text as type, null::text as name, null::text as value where false)"
}

/// Mirrors `postgresRedshiftSystemTable`.
fn postgres_redshift_system_table(table_name: &str) -> &'static str {
    match table_name.to_lowercase().as_str() {
        "pg_table_def" => postgres_pg_table_def(),
        "pg_table_info" => postgres_pg_table_info(),
        _ => postgres_redshift_system_table_stub(),
    }
}

/// Mirrors `postgresPGTableDef`.
pub fn postgres_pg_table_def() -> &'static str {
    "(select table_schema::text as schemaname, table_name::text as tablename, column_name::text as \"column\", data_type::text as type, null::text as encoding, false as distkey, 0::integer as sortkey, (is_nullable = 'NO') as notnull from information_schema.columns)"
}

/// Mirrors `postgresPGTableInfo`.
pub fn postgres_pg_table_info() -> &'static str {
    "(select current_database()::text as database, n.nspname::text as schema, c.relname::text as \"table\", c.oid::integer as table_id, 'N'::text as encoded, null::text as diststyle, 0::integer as sortkey1, 0::integer as max_varchar, null::text as sortkey1_enc, 0::integer as sortkey_num, 0::bigint as size, 0::numeric as pct_used, 0::bigint as empty, 0::numeric as unsorted, 0::numeric as stats_off, c.reltuples::bigint as tbl_rows, 0::numeric as skew_sortkey1, 0::numeric as skew_rows, c.reltuples::bigint as estimated_visible_rows, null::text as risk_event, 0::numeric as vacuum_sort_benefit from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace where c.relkind in ('r', 'p'))"
}

/// Mirrors `isRedshiftInformationSchemaRelation`.
fn is_redshift_information_schema_relation(reference: &str) -> bool {
    reference.eq_ignore_ascii_case("information_schema.columns")
}

/// Mirrors `postgresRedshiftInformationSchemaRelation`.
fn postgres_redshift_information_schema_relation(reference: &str) -> &'static str {
    match reference.to_lowercase().as_str() {
        "information_schema.columns" => postgres_information_schema_columns(),
        _ => "",
    }
}

/// Mirrors `postgresInformationSchemaColumns`.
pub fn postgres_information_schema_columns() -> &'static str {
    "(select table_catalog::text as table_catalog, table_schema::text as table_schema, table_name::text as table_name, column_name::text as column_name, ordinal_position::integer as ordinal_position, column_default::text as column_default, is_nullable::text as is_nullable, data_type::text as data_type, character_maximum_length::integer as character_maximum_length, numeric_precision::integer as numeric_precision, numeric_precision_radix::integer as numeric_precision_radix, numeric_scale::integer as numeric_scale, datetime_precision::integer as datetime_precision, interval_type::text as interval_type, interval_precision::text as interval_precision, character_set_catalog::text as character_set_catalog, character_set_schema::text as character_set_schema, character_set_name::text as character_set_name, collation_catalog::text as collation_catalog, collation_schema::text as collation_schema, collation_name::text as collation_name, domain_name::text as domain_name, null::text as remarks from information_schema.columns)"
}

/// Mirrors `rewriteRedshiftFunctions`.
fn rewrite_redshift_functions(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            i = copy_quoted_string(&mut out, sql, i);
            continue;
        }
        if ch == b'"' {
            i = copy_quoted_identifier(&mut out, sql, i);
            continue;
        }
        if is_identifier_start(ch) {
            let start = i;
            i += 1;
            while i < bytes.len() && is_identifier_part(bytes[i]) {
                i += 1;
            }
            if let Some((rewritten, next)) = rewrite_partiql_navigation(sql, start, i) {
                out.extend_from_slice(rewritten.as_bytes());
                i = next;
                continue;
            }
            if let Some((rewritten, next)) = rewrite_redshift_function_call(sql, start, i) {
                out.extend_from_slice(rewritten.as_bytes());
                i = next;
                continue;
            }
            out.extend_from_slice(&bytes[start..i]);
            continue;
        }
        out.push(ch);
        i += 1;
    }
    String::from_utf8(out).expect("rewrites preserve UTF-8")
}

/// Mirrors the `redshiftFunctionRewrites` dispatch map: lower-cased
/// identifiers route to their per-construct rewriter. Adding a translation is
/// a single match arm.
fn rewrite_redshift_function_call(sql: &str, start: usize, end: usize) -> Option<(String, usize)> {
    match sql[start..end].to_lowercase().as_str() {
        "approximate" => rewrite_approximate_count_distinct(sql, end),
        "bit_and" => emit_when_paren_follows(sql, end, POSTGRES_BOOL_AND),
        "bit_or" => emit_when_paren_follows(sql, end, POSTGRES_BOOL_OR),
        "boolean" => rewrite_boolean_literal_after_keyword(sql, end),
        "getdate" => rewrite_getdate_empty_parens(sql, end),
        "timeofday" => rewrite_paren_function(sql, end, rewrite_time_of_day),
        "rand" => rewrite_paren_function(sql, end, rewrite_rand),
        "sysdate" => Some((POSTGRES_CURRENT_TIMESTAMP.to_string(), end)),
        "nvl" => rewrite_nvl_as_coalesce(sql, end),
        "nvl2" => rewrite_paren_function(sql, end, rewrite_nvl2),
        "len" => rewrite_paren_function(sql, end, rewrite_len),
        "charindex" => rewrite_paren_function(sql, end, rewrite_char_index),
        "substring" => rewrite_paren_function(sql, end, rewrite_substring),
        "split_part" => rewrite_paren_function(sql, end, rewrite_split_part),
        "strtol" => rewrite_paren_function(sql, end, rewrite_strtol),
        "crc32" => rewrite_paren_function(sql, end, rewrite_crc32),
        "md5_digest" => rewrite_paren_function(sql, end, rewrite_md5_digest),
        "func_sha1" => rewrite_paren_function(sql, end, rewrite_func_sha1),
        "regexp_substr" => rewrite_paren_function(sql, end, rewrite_regexp_substr),
        "regexp_count" => rewrite_paren_function(sql, end, rewrite_regexp_count),
        "regexp_instr" => rewrite_paren_function(sql, end, rewrite_regexp_instr),
        "decode" => rewrite_paren_function(sql, end, rewrite_decode),
        "greatest" => rewrite_paren_function(sql, end, rewrite_greatest),
        "least" => rewrite_paren_function(sql, end, rewrite_least),
        "round" => rewrite_paren_function(sql, end, rewrite_round),
        "json_extract_path_text" => {
            rewrite_paren_function(sql, end, rewrite_json_extract_path_text)
        }
        "json_extract_array_element_text" => {
            rewrite_paren_function(sql, end, rewrite_json_extract_array_element_text)
        }
        "json_array_length" => rewrite_paren_function(sql, end, rewrite_json_array_length),
        "json_parse" => rewrite_paren_function(sql, end, rewrite_json_parse),
        "is_valid_json" => rewrite_paren_function(sql, end, rewrite_is_valid_json),
        "is_valid_json_array" => rewrite_paren_function(sql, end, rewrite_is_valid_json_array),
        "object_transform" => rewrite_object_transform_call(sql, end),
        "dateadd" => rewrite_paren_function(sql, end, rewrite_date_add),
        "datediff" => rewrite_paren_function(sql, end, rewrite_date_diff),
        "convert_timezone" => rewrite_paren_function(sql, end, rewrite_convert_timezone),
        "date_part" => rewrite_paren_function(sql, end, rewrite_date_part_function),
        "date_trunc" => rewrite_paren_function(sql, end, rewrite_date_trunc_function),
        "last_day" => rewrite_paren_function(sql, end, rewrite_last_day),
        "months_between" => rewrite_paren_function(sql, end, rewrite_months_between),
        "add_months" => rewrite_paren_function(sql, end, rewrite_add_months),
        "next_day" => rewrite_paren_function(sql, end, rewrite_next_day),
        "to_date" => rewrite_paren_function(sql, end, rewrite_to_date),
        "to_timestamp" => rewrite_paren_function(sql, end, rewrite_to_timestamp),
        "to_char" => rewrite_paren_function(sql, end, rewrite_to_char),
        "listagg" => rewrite_list_agg(sql, end),
        "median" => rewrite_paren_function(sql, end, rewrite_median),
        "ratio_to_report" => rewrite_ratio_to_report(sql, end),
        "like" => rewrite_like_default_escape(sql, start, end),
        _ => None,
    }
}

/// Mirrors `rewriteParenFunction` (`adaptParenFn` callers).
fn rewrite_paren_function(
    sql: &str,
    index: usize,
    rewrite: fn(&[String]) -> Option<String>,
) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let open = skip_spaces(sql, index);
    if open >= bytes.len() || bytes[open] != b'(' {
        return None;
    }
    let close = matching_paren(sql, open)?;
    let args = split_comma_separated(&sql[open + 1..close]);
    let rewritten = rewrite(&args)?;
    Some((rewritten, close + 1))
}

/// Mirrors `emitWhenParenFollows`: replaces the identifier with `name` when an
/// argument list follows; the arguments are left for the outer loop.
fn emit_when_paren_follows(sql: &str, end: usize, name: &str) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let next = skip_spaces(sql, end);
    if next < bytes.len()
        && bytes[next] == b'('
        && matching_paren(sql, next).is_some_and(|close| close > next)
    {
        return Some((name.to_string(), end));
    }
    None
}

/// Mirrors `rewriteGetDateEmptyParens`: GETDATE() -> CURRENT_TIMESTAMP; only
/// the empty-parens form matches.
fn rewrite_getdate_empty_parens(sql: &str, end: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let next = skip_spaces(sql, end);
    if next >= bytes.len() || bytes[next] != b'(' {
        return None;
    }
    let close = matching_paren(sql, next)?;
    if close > next && sql[next + 1..close].trim().is_empty() {
        return Some((POSTGRES_CURRENT_TIMESTAMP.to_string(), close + 1));
    }
    None
}

/// Mirrors `rewriteNVLAsCoalesce`: swaps NVL(...) for COALESCE(...) preserving
/// the original argument list.
fn rewrite_nvl_as_coalesce(sql: &str, end: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let next = skip_spaces(sql, end);
    if next >= bytes.len() || bytes[next] != b'(' {
        return None;
    }
    let close = matching_paren(sql, next)?;
    Some((
        format!("{POSTGRES_COALESCE}{}", &sql[next..=close]),
        close + 1,
    ))
}

/// Mirrors `rewriteBooleanLiteralAfterKeyword`.
fn rewrite_boolean_literal_after_keyword(sql: &str, end: usize) -> Option<(String, usize)> {
    let next = skip_spaces(sql, end);
    parse_redshift_boolean_literal(sql, next)
}

/// Mirrors `partiQLNavigationStep`.
struct PartiQlNavigationStep {
    value: String,
    subscript: bool,
}

/// Mirrors `rewritePartiQLNavigation`.
fn rewrite_partiql_navigation(sql: &str, start: usize, end: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let mut next = end;
    let mut steps: Vec<PartiQlNavigationStep> = Vec::new();
    let mut has_subscript = false;
    while next < bytes.len() {
        match bytes[next] {
            b'.' => {
                let key_start = next + 1;
                if key_start >= bytes.len() || !is_identifier_start(bytes[key_start]) {
                    return None;
                }
                let mut key_end = key_start + 1;
                while key_end < bytes.len() && is_identifier_part(bytes[key_end]) {
                    key_end += 1;
                }
                steps.push(PartiQlNavigationStep {
                    value: sql[key_start..key_end].to_string(),
                    subscript: false,
                });
                next = key_end;
            }
            b'[' => {
                let close = matching_bracket(sql, next)?;
                let index = sql[next + 1..close].trim();
                if index.is_empty() {
                    return None;
                }
                steps.push(PartiQlNavigationStep {
                    value: index.to_string(),
                    subscript: true,
                });
                has_subscript = true;
                next = close + 1;
            }
            _ => {
                if steps.is_empty() || (!has_subscript && steps.len() < 2) {
                    return None;
                }
                return Some((postgres_partiql_navigation(&sql[start..end], &steps), next));
            }
        }
    }
    if steps.is_empty() || (!has_subscript && steps.len() < 2) {
        return None;
    }
    Some((postgres_partiql_navigation(&sql[start..end], &steps), next))
}

/// Mirrors `postgresPartiQLNavigation`.
fn postgres_partiql_navigation(base: &str, steps: &[PartiQlNavigationStep]) -> String {
    let mut out = String::new();
    out.push('(');
    out.push_str(base);
    out.push_str(")::jsonb");
    for (i, step) in steps.iter().enumerate() {
        if i == steps.len() - 1 {
            out.push_str(" ->> ");
        } else {
            out.push_str(" -> ");
        }
        if step.subscript {
            out.push_str(&step.value);
            continue;
        }
        out.push_str(&sql_string_literal(&step.value));
    }
    out
}

/// Mirrors `matchingBracket`.
fn matching_bracket(value: &str, open: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    if open >= bytes.len() || bytes[open] != b'[' {
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
        if !in_string {
            match ch {
                b'[' => depth += 1,
                b']' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Mirrors `rewriteApproximateCountDistinct`.
fn rewrite_approximate_count_distinct(sql: &str, index: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let count_start = skip_spaces(sql, index);
    let count_end = match_keyword_sequence(sql, count_start, &["count"])?;
    let open = skip_spaces(sql, count_end);
    if open >= bytes.len() || bytes[open] != b'(' {
        return None;
    }
    let close = matching_paren(sql, open)?;
    let args = sql[open + 1..close].trim();
    let distinct_end = match_keyword_sequence(args, 0, &["distinct"])?;
    if args[distinct_end..].trim().is_empty() {
        return None;
    }
    Some((sql[count_start..=close].to_string(), close + 1))
}

/// Mirrors `rewriteLikeDefaultEscape`.
fn rewrite_like_default_escape(
    sql: &str,
    keyword_start: usize,
    keyword_end: usize,
) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let pattern_start = skip_spaces(sql, keyword_end);
    if pattern_start >= bytes.len() || bytes[pattern_start] != b'\'' {
        return None;
    }
    let (pattern_value, pattern_end) = read_quoted_string_value(sql, pattern_start)?;
    if !pattern_value.contains('\\') {
        return None;
    }
    let next = skip_spaces(sql, pattern_end);
    if match_keyword_sequence(sql, next, &["escape"]).is_some() {
        return None;
    }
    Some((
        format!("{} ESCAPE '\\'", &sql[keyword_start..pattern_end]),
        pattern_end,
    ))
}

/// Mirrors `rewriteListAgg`.
fn rewrite_list_agg(sql: &str, index: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let open = skip_spaces(sql, index);
    if open >= bytes.len() || bytes[open] != b'(' {
        return None;
    }
    let close = matching_paren(sql, open)?;
    let args = split_comma_separated(&sql[open + 1..close]);
    if args.len() != 2 {
        return None;
    }
    let expression = args[0].trim();
    let delimiter = args[1].trim();
    let rewritten = format!("{POSTGRES_STRING_AGG}({expression}, {delimiter}");
    let next = close + 1;

    let within_start = skip_spaces(sql, next);
    if !has_prefix_fold(&sql[within_start..], "within") {
        return Some((format!("{rewritten})"), next));
    }
    let group_start = skip_spaces(sql, within_start + "within".len());
    if !has_prefix_fold(&sql[group_start..], "group") {
        return Some((format!("{rewritten})"), next));
    }
    let group_open = skip_spaces(sql, group_start + "group".len());
    if group_open >= bytes.len() || bytes[group_open] != b'(' {
        return Some((format!("{rewritten})"), next));
    }
    let Some(group_close) = matching_paren(sql, group_open) else {
        return Some((format!("{rewritten})"), next));
    };
    let order_by_clause = sql[group_open + 1..group_close].trim();
    if has_prefix_fold(order_by_clause, "order by") {
        let order_by = order_by_clause["order by".len()..].trim();
        if !order_by.is_empty() {
            let over_start = skip_spaces(sql, group_close + 1);
            if let Some(over_end) = match_keyword_sequence(sql, over_start, &["over"]) {
                let over_open = skip_spaces(sql, over_end);
                if over_open < bytes.len() && bytes[over_open] == b'(' {
                    if let Some(over_close) = matching_paren(sql, over_open) {
                        if over_close > over_open {
                            let over_clause = add_order_to_list_agg_window(
                                sql[over_open + 1..over_close].trim(),
                                order_by,
                            );
                            return Some((
                                format!(
                                    "array_to_string(array_agg({expression}) OVER ({over_clause}), {delimiter})"
                                ),
                                over_close + 1,
                            ));
                        }
                    }
                }
            }
            return Some((format!("{rewritten} ORDER BY {order_by})"), group_close + 1));
        }
    }
    Some((format!("{rewritten})"), next))
}

/// Mirrors `addOrderToListAggWindow`.
fn add_order_to_list_agg_window(over_clause: &str, order_by: &str) -> String {
    if over_clause.is_empty() {
        return format!(
            "ORDER BY {order_by} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
        );
    }
    format!(
        "{over_clause} ORDER BY {order_by} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
    )
}

/// Mirrors `rewriteRatioToReport`.
fn rewrite_ratio_to_report(sql: &str, index: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let open = skip_spaces(sql, index);
    if open >= bytes.len() || bytes[open] != b'(' {
        return None;
    }
    let close = matching_paren(sql, open)?;
    let args = split_comma_separated(&sql[open + 1..close]);
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }

    let over_start = skip_spaces(sql, close + 1);
    let over_end = match_keyword_sequence(sql, over_start, &["over"])?;
    let over_open = skip_spaces(sql, over_end);
    if over_open >= bytes.len() || bytes[over_open] != b'(' {
        return None;
    }
    let over_close = matching_paren(sql, over_open)?;

    Some((
        format!(
            "{value} / SUM({value}) OVER {}",
            &sql[over_open..=over_close]
        ),
        over_close + 1,
    ))
}

/// Mirrors `rewriteObjectTransformCall`.
fn rewrite_object_transform_call(sql: &str, index: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let open = skip_spaces(sql, index);
    if open >= bytes.len() || bytes[open] != b'(' {
        return None;
    }
    let close = matching_paren(sql, open)?;
    let rewritten = rewrite_object_transform(&sql[open + 1..close])?;
    Some((rewritten, close + 1))
}

/// Mirrors `rewriteObjectTransform`.
fn rewrite_object_transform(value: &str) -> Option<String> {
    let keep = find_top_level_keyword_sequence(value, &["keep"], 0);
    let set = find_top_level_keyword_sequence(value, &["set"], 0);
    if let (Some((keep_start, _)), Some((set_start, _))) = (keep, set) {
        if set_start < keep_start {
            return None;
        }
    }

    let mut input_end = value.len();
    if let Some((keep_start, _)) = keep {
        input_end = keep_start;
    }
    if let Some((set_start, _)) = set {
        if set_start < input_end {
            input_end = set_start;
        }
    }
    let input = value[..input_end].trim();
    if input.is_empty() {
        return None;
    }
    let input_json = format!("({input})::jsonb");
    if keep.is_none() && set.is_none() {
        return Some(input_json);
    }

    let mut current = "'{}'::jsonb".to_string();
    let mut ensured_paths: HashSet<String> = HashSet::new();
    if let Some((_, keep_end)) = keep {
        let mut keep_end_at = value.len();
        if let Some((set_start, _)) = set {
            keep_end_at = set_start;
        }
        let keep_paths = split_comma_separated(&value[keep_end..keep_end_at]);
        if keep_paths.is_empty() {
            return None;
        }
        for keep_path in &keep_paths {
            let (path, components) = object_transform_path(keep_path)?;
            current = ensure_object_transform_path(current, &components, &mut ensured_paths);
            current = format!("jsonb_set({current}, {path}, ({input_json} #> {path}), true)");
        }
    }
    if let Some((_, set_end)) = set {
        let set_args = split_comma_separated(&value[set_end..]);
        if set_args.is_empty() || set_args.len() % 2 != 0 {
            return None;
        }
        let mut i = 0;
        while i < set_args.len() {
            let (path, components) = object_transform_path(&set_args[i])?;
            let set_value = set_args[i + 1].trim();
            if set_value.is_empty() {
                return None;
            }
            current = ensure_object_transform_path(current, &components, &mut ensured_paths);
            current = format!("jsonb_set({current}, {path}, to_jsonb({set_value}), true)");
            i += 2;
        }
    }
    Some(current)
}

/// Mirrors `ensureObjectTransformPath`.
fn ensure_object_transform_path(
    current: String,
    components: &[String],
    ensured_paths: &mut HashSet<String>,
) -> String {
    let mut current = current;
    for i in 1..components.len() {
        let key = components[..i].join("\x00");
        if ensured_paths.contains(&key) {
            continue;
        }
        let path = object_transform_path_array(&components[..i]);
        current = format!(
            "jsonb_set({current}, {path}, coalesce({current} #> {path}, '{{}}'::jsonb), true)"
        );
        ensured_paths.insert(key);
    }
    current
}

/// Mirrors `objectTransformPath`.
fn object_transform_path(value: &str) -> Option<(String, Vec<String>)> {
    let path = sql_string_literal_value(value)?;
    let components = object_transform_path_components(&path)?;
    Some((object_transform_path_array(&components), components))
}

/// Mirrors `objectTransformPathArray`.
fn object_transform_path_array(components: &[String]) -> String {
    let literals: Vec<String> = components
        .iter()
        .map(|component| sql_string_literal(component))
        .collect();
    format!("ARRAY[{}]", literals.join(", "))
}

/// Mirrors `objectTransformPathComponents`.
fn object_transform_path_components(path: &str) -> Option<Vec<String>> {
    let bytes = path.as_bytes();
    let mut components: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'"' {
            return None;
        }
        i += 1;
        let mut component: Vec<u8> = Vec::new();
        while i < bytes.len() {
            if bytes[i] != b'"' {
                component.push(bytes[i]);
                i += 1;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                component.push(bytes[i + 1]);
                i += 2;
                continue;
            }
            break;
        }
        if i >= bytes.len() || bytes[i] != b'"' || component.is_empty() {
            return None;
        }
        components.push(String::from_utf8(component).expect("rewrites preserve UTF-8"));
        i += 1;
        if i == bytes.len() {
            break;
        }
        if bytes[i] != b'.' {
            return None;
        }
        i += 1;
    }
    if components.is_empty() {
        return None;
    }
    Some(components)
}

/// Mirrors `rewriteDecode`.
fn rewrite_decode(args: &[String]) -> Option<String> {
    if args.len() < 3 {
        return None;
    }
    let mut out = String::from("CASE ");
    out.push_str(args[0].trim());
    let mut i = 1;
    while i + 1 < args.len() {
        out.push_str(" WHEN ");
        out.push_str(args[i].trim());
        out.push_str(" THEN ");
        out.push_str(args[i + 1].trim());
        i += 2;
    }
    if args.len() % 2 == 0 {
        out.push_str(" ELSE ");
        out.push_str(args[args.len() - 1].trim());
    }
    out.push_str(" END");
    Some(out)
}

/// Mirrors `rewriteGreatest`.
fn rewrite_greatest(args: &[String]) -> Option<String> {
    rewrite_null_ignoring_extremum("max", "greatest_value", args)
}

/// Mirrors `rewriteLeast`.
fn rewrite_least(args: &[String]) -> Option<String> {
    rewrite_null_ignoring_extremum("min", "least_value", args)
}

/// Mirrors `rewriteNullIgnoringExtremum`.
fn rewrite_null_ignoring_extremum(
    aggregate: &str,
    column: &str,
    args: &[String],
) -> Option<String> {
    if args.is_empty() {
        return None;
    }
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        let value = arg.trim();
        if value.is_empty() {
            return None;
        }
        values.push(format!("({value})"));
    }
    Some(format!(
        "(select {aggregate}({column}) from (values {}) as redshift_{column}s({column}))",
        values.join(", ")
    ))
}

/// Mirrors `rewriteRound`.
fn rewrite_round(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let value = args[0].trim();
    let scale = args[1].trim();
    if value.is_empty() || scale.is_empty() {
        return None;
    }
    let magnitude = negative_integer_literal_magnitude(scale)?;
    Some(format!("round(({value})::numeric, -{magnitude})"))
}

/// Mirrors `rewriteJSONExtractPathText`.
fn rewrite_json_extract_path_text(args: &[String]) -> Option<String> {
    if args.len() < 2 {
        return None;
    }
    let mut rewritten_args = Vec::with_capacity(args.len());
    for arg in args {
        let trimmed = arg.trim();
        if trimmed.is_empty() {
            return None;
        }
        rewritten_args.push(trimmed);
    }
    if postgres_boolean_literal(rewritten_args[rewritten_args.len() - 1]).is_some() {
        rewritten_args.pop();
    }
    if rewritten_args.len() < 2 {
        return None;
    }
    Some(format!(
        "jsonb_extract_path_text(({})::jsonb, {})",
        rewritten_args[0],
        rewritten_args[1..].join(", ")
    ))
}

/// Mirrors `rewriteJSONExtractArrayElementText`.
fn rewrite_json_extract_array_element_text(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let value = args[0].trim();
    let index = args[1].trim();
    if value.is_empty() || index.is_empty() {
        return None;
    }
    Some(format!("(({value})::jsonb -> {index})::text"))
}

/// Mirrors `rewriteJSONArrayLength`.
fn rewrite_json_array_length(args: &[String]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    Some(format!("jsonb_array_length(({value})::jsonb)"))
}

/// Mirrors `rewriteJSONParse`.
fn rewrite_json_parse(args: &[String]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    Some(format!("({value})::jsonb"))
}

/// Mirrors `rewriteIsValidJSON`.
fn rewrite_is_valid_json(args: &[String]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    Some(format!("coalesce(json_valid(({value})::text), false)"))
}

/// Mirrors `rewriteIsValidJSONArray`.
fn rewrite_is_valid_json_array(args: &[String]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    let valid_json = format!("coalesce(json_valid(({value})::text), false)");
    Some(format!(
        "(case when {valid_json} then jsonb_typeof(({value})::jsonb) = 'array' else false end)"
    ))
}

/// Mirrors `rewriteNVL2`.
fn rewrite_nvl2(args: &[String]) -> Option<String> {
    if args.len() != 3 {
        return None;
    }
    let expr = args[0].trim();
    let val_if_not_null = args[1].trim();
    let val_if_null = args[2].trim();
    if expr.is_empty() || val_if_not_null.is_empty() || val_if_null.is_empty() {
        return None;
    }
    Some(format!(
        "CASE WHEN {expr} IS NOT NULL THEN {val_if_not_null} ELSE {val_if_null} END"
    ))
}

/// Mirrors `rewriteLen`.
fn rewrite_len(args: &[String]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    Some(format!("length({value})"))
}

/// Mirrors `rewriteCharIndex`.
fn rewrite_char_index(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let substring = args[0].trim();
    let value = args[1].trim();
    if substring.is_empty() || value.is_empty() {
        return None;
    }
    Some(format!("position({substring} in {value})"))
}

/// Mirrors `rewriteSubstring`.
fn rewrite_substring(args: &[String]) -> Option<String> {
    if args.len() != 3 {
        return None;
    }
    let value = args[0].trim();
    let start = args[1].trim();
    let length = args[2].trim();
    if value.is_empty() || start.is_empty() || length.is_empty() {
        return None;
    }

    let postgres_start = format!("(case when {start} < 1 then 1 else {start} end)");
    let redshift_length = format!("{start} + {length} - 1");
    let postgres_length = format!(
        "(case when {start} <= 0 then case when {redshift_length} <= 0 then 0 else {redshift_length} end else {length} end)"
    );
    Some(format!(
        "substring({value} from {postgres_start} for {postgres_length})"
    ))
}

/// Mirrors `rewriteSplitPart`.
fn rewrite_split_part(args: &[String]) -> Option<String> {
    if args.len() != 3 {
        return None;
    }
    let value = args[0].trim();
    let separator = args[1].trim();
    let position = args[2].trim();
    if value.is_empty() || separator.is_empty() || position.is_empty() {
        return None;
    }
    let magnitude = negative_integer_literal_magnitude(position)?;
    Some(format!(
        "reverse(split_part(reverse({value}), reverse({separator}), {magnitude}))"
    ))
}

/// Mirrors `negativeIntegerLiteralMagnitude`.
fn negative_integer_literal_magnitude(value: &str) -> Option<&str> {
    if value.len() < 2 || !value.starts_with('-') {
        return None;
    }
    let magnitude = &value[1..];
    if !is_unsigned_integer(magnitude) || magnitude.trim_start_matches('0').is_empty() {
        return None;
    }
    Some(magnitude)
}

/// Mirrors `rewriteStrtol`.
fn rewrite_strtol(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let value = args[0].trim();
    let base = args[1].trim();
    if value.is_empty() || base.is_empty() {
        return None;
    }

    let normalized = format!("regexp_replace(trim({value}), '^[+-]', '')");
    Some(format!(
        "(select (case when left(trim({value}), 1) = '-' then -1 else 1 end) * coalesce(sum((strpos('0123456789abcdefghijklmnopqrstuvwxyz', digit) - 1)::numeric * power(({base})::numeric, (length({normalized}) - ordinality)::numeric)), 0)::bigint from regexp_split_to_table(lower({normalized}), '') with ordinality as strtol_digits(digit, ordinality))"
    ))
}

/// Mirrors `rewriteCRC32`.
fn rewrite_crc32(args: &[String]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }

    let seed = "4294967295";
    let polynomial = "3988292384";
    let crc_input = "(case when step % 8 = 0 then crc # get_byte(data, step / 8) else crc end)";
    let crc_step = format!(
        "(case when ({crc_input} & 1) = 1 then (({crc_input} >> 1) # {polynomial}) else ({crc_input} >> 1) end)"
    );
    Some(format!(
        "(with recursive crc32_input(data) as (select convert_to(({value})::text, 'UTF8')), crc32_state(step, crc) as (select 0, {seed}::bigint union all select step + 1, {crc_step} from crc32_state, crc32_input where step < length(data) * 8) select case when data is null then null else (crc # {seed})::bigint end from crc32_state, crc32_input order by step desc limit 1)"
    ))
}

/// Mirrors `rewriteMD5Digest`.
fn rewrite_md5_digest(args: &[String]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    Some(format!("md5(({value})::text)"))
}

/// Mirrors `rewriteFuncSHA1`.
fn rewrite_func_sha1(args: &[String]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    Some(format!("encode(digest(({value})::text, 'sha1'), 'hex')"))
}

/// Mirrors `rewriteRegexpSubstr`.
fn rewrite_regexp_substr(args: &[String]) -> Option<String> {
    if args.len() != 4 {
        return None;
    }
    let value = args[0].trim();
    let pattern = args[1].trim();
    let start = args[2].trim();
    let occurrence = args[3].trim();
    if value.is_empty() || pattern.is_empty() || start.is_empty() || occurrence.is_empty() {
        return None;
    }
    if start == "1" && occurrence == "1" {
        return Some(format!("regexp_match({value}, {pattern})"));
    }
    Some(format!(
        "(select regexp_substr_match from regexp_matches(substring({value} from {start}), {pattern}, 'g') with ordinality as regexp_substr_matches(regexp_substr_match, regexp_substr_ordinality) where regexp_substr_ordinality = {occurrence})"
    ))
}

/// Mirrors `rewriteRegexpCount`.
fn rewrite_regexp_count(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let value = args[0].trim();
    let pattern = args[1].trim();
    if value.is_empty() || pattern.is_empty() {
        return None;
    }
    Some(format!(
        "(case when {value} is null or {pattern} is null then null else (select count(*)::int from regexp_matches({value}, {pattern}, 'g')) end)"
    ))
}

/// Mirrors `rewriteRegexpInstr`.
fn rewrite_regexp_instr(args: &[String]) -> Option<String> {
    if args.len() < 2 || args.len() > 6 {
        return None;
    }
    let mut rewritten_args: Vec<String> = Vec::with_capacity(args.len() + 1);
    for arg in args {
        let trimmed = arg.trim();
        if trimmed.is_empty() {
            return None;
        }
        rewritten_args.push(trimmed.to_string());
    }
    if rewritten_args.len() == 6 {
        if let Some(parameters) = sql_string_literal_value(&rewritten_args[5]) {
            if parameters.contains(['e', 'E']) {
                let pg_parameters: String = parameters
                    .chars()
                    .filter(|ch| *ch != 'e' && *ch != 'E')
                    .collect();
                rewritten_args[5] = sql_string_literal(&pg_parameters);
                rewritten_args.push("1".to_string());
            }
        }
    }
    Some(format!("regexp_instr({})", rewritten_args.join(", ")))
}

/// Mirrors `rewriteDateAdd`.
fn rewrite_date_add(args: &[String]) -> Option<String> {
    if args.len() != 3 {
        return None;
    }
    let part = postgres_interval_part(&args[0])?;
    Some(format!(
        "{} + ({} * interval '1 {part}')",
        args[2].trim(),
        args[1].trim()
    ))
}

/// Mirrors `rewriteDateDiff`.
fn rewrite_date_diff(args: &[String]) -> Option<String> {
    if args.len() != 3 {
        return None;
    }
    let part = postgres_date_part(&args[0])?;
    let start = args[1].trim();
    let end = args[2].trim();
    match part {
        "year" => Some(format!("date_part('year', age({end}, {start}))::int")),
        "month" => Some(format!(
            "(date_part('year', age({end}, {start}))::int * 12 + date_part('month', age({end}, {start}))::int)"
        )),
        "day" => Some(format!("({end}::date - {start}::date)")),
        "hour" => Some(format!(
            "floor(extract(epoch from ({end} - {start})) / 3600)::int"
        )),
        "minute" => Some(format!(
            "floor(extract(epoch from ({end} - {start})) / 60)::int"
        )),
        "second" => Some(format!("floor(extract(epoch from ({end} - {start})))::int")),
        _ => None,
    }
}

/// Mirrors `rewriteTimeOfDay`.
fn rewrite_time_of_day(args: &[String]) -> Option<String> {
    if !args.is_empty() {
        return None;
    }
    Some(format!("{POSTGRES_CLOCK_TIMESTAMP}::text"))
}

/// Mirrors `rewriteRand`.
fn rewrite_rand(args: &[String]) -> Option<String> {
    if !args.is_empty() {
        return None;
    }
    Some(POSTGRES_RANDOM.to_string())
}

/// Mirrors `rewriteConvertTimezone`.
fn rewrite_convert_timezone(args: &[String]) -> Option<String> {
    if args.len() != 3 {
        return None;
    }
    let source = normalize_timezone_arg(&args[0]);
    let target = normalize_timezone_arg(&args[1]);
    let timestamp = args[2].trim();
    if source.is_empty() || target.is_empty() || timestamp.is_empty() {
        return None;
    }
    Some(format!(
        "{timestamp} AT TIME ZONE {source} AT TIME ZONE {target}"
    ))
}

/// Mirrors `normalizeTimezoneArg`: trims whitespace and remaps timezone
/// abbreviations that PostgreSQL does not always recognize (e.g. JST) to their
/// IANA equivalents so AT TIME ZONE resolves correctly.
fn normalize_timezone_arg(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(literal) = sql_string_literal_value(trimmed) {
        if literal.eq_ignore_ascii_case("JST") {
            return "'Asia/Tokyo'".to_string();
        }
    }
    trimmed.to_string()
}

/// Mirrors `sqlStringLiteralValue`.
fn sql_string_literal_value(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || !trimmed.starts_with('\'') {
        return None;
    }
    let (unquoted, end) = read_quoted_string_value(trimmed, 0)?;
    if !trimmed[end..].trim().is_empty() {
        return None;
    }
    Some(unquoted)
}

/// Mirrors `rewriteDatePartFunction`.
fn rewrite_date_part_function(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let part = postgres_date_part_function_part(&args[0])?;
    Some(format!("date_part('{part}', {})", args[1].trim()))
}

/// Mirrors `rewriteDateTruncFunction`.
fn rewrite_date_trunc_function(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let part = postgres_date_trunc_part(&args[0])?;
    Some(format!("date_trunc('{part}', {})", args[1].trim()))
}

/// Mirrors `rewriteLastDay`.
fn rewrite_last_day(args: &[String]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    Some(format!(
        "(date_trunc('month', {value}) + interval '1 month - 1 day')::date"
    ))
}

/// Mirrors `rewriteMonthsBetween`.
fn rewrite_months_between(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let end = args[0].trim();
    let start = args[1].trim();
    if end.is_empty() || start.is_empty() {
        return None;
    }
    Some(format!(
        "(extract(year from age({end}, {start})) * 12 + extract(month from age({end}, {start})))"
    ))
}

/// Mirrors `rewriteAddMonths`.
fn rewrite_add_months(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let value = args[0].trim();
    let months = args[1].trim();
    if value.is_empty() || months.is_empty() {
        return None;
    }
    Some(format!("{value} + ({months} * interval '1 month')"))
}

/// Mirrors `rewriteNextDay`.
fn rewrite_next_day(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    let day = sql_string_literal_value(&args[1])?;
    let target_dow = redshift_next_day_number(&day)?;
    let date_value = format!("({value})::date");
    Some(format!(
        "({date_value} + (({target_dow} - extract(dow from {date_value})::int + 6) % 7 + 1))::date"
    ))
}

/// Mirrors `redshiftNextDayNumber`.
fn redshift_next_day_number(value: &str) -> Option<&'static str> {
    match value.trim().to_lowercase().as_str() {
        "su" | "sun" | "sunday" => Some("0"),
        "m" | "mo" | "mon" | "monday" => Some("1"),
        "tu" | "tue" | "tues" | "tuesday" => Some("2"),
        "w" | "we" | "wed" | "wednesday" => Some("3"),
        "th" | "thu" | "thurs" | "thursday" => Some("4"),
        "f" | "fr" | "fri" | "friday" => Some("5"),
        "sa" | "sat" | "saturday" => Some("6"),
        _ => None,
    }
}

/// Mirrors `rewriteToDate`.
fn rewrite_to_date(args: &[String]) -> Option<String> {
    rewrite_date_time_format_function("to_date", args)
}

/// Mirrors `rewriteToTimestamp`.
fn rewrite_to_timestamp(args: &[String]) -> Option<String> {
    rewrite_date_time_format_function("to_timestamp", args)
}

/// Mirrors `rewriteToChar`.
fn rewrite_to_char(args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    let format = sql_string_literal_value(&args[1])?;
    let rewritten_format = rewrite_redshift_to_char_format(&format)?;
    Some(format!(
        "to_char({value}, {})",
        sql_string_literal(&rewritten_format)
    ))
}

/// Mirrors `rewriteDateTimeFormatFunction`.
fn rewrite_date_time_format_function(function_name: &str, args: &[String]) -> Option<String> {
    if args.len() != 2 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    let format = sql_string_literal_value(&args[1])?;
    let rewritten_format = remove_trailing_redshift_timezone_format(&format)?;
    Some(format!(
        "{function_name}(regexp_replace({value}, '[[:space:]]*([[:alpha:]_/]+|[+-][0-9]{{2}}(:?[0-9]{{2}})?)$', ''), {})",
        sql_string_literal(&rewritten_format)
    ))
}

/// Mirrors `rewriteRedshiftToCharFormat` (None = unchanged).
fn rewrite_redshift_to_char_format(format: &str) -> Option<String> {
    let bytes = format.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut changed = false;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.extend_from_slice(&bytes[start..i]);
            continue;
        }
        if has_format_token(format, i, "TZ") {
            out.extend_from_slice(b"\"UTC\"");
            i += "TZ".len();
            changed = true;
            continue;
        }
        if has_format_token(format, i, "tz") {
            out.extend_from_slice(b"\"utc\"");
            i += "tz".len();
            changed = true;
            continue;
        }
        if has_format_token(format, i, "OF") {
            out.extend_from_slice(b"\"+00\"");
            i += "OF".len();
            changed = true;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    if !changed {
        return None;
    }
    Some(String::from_utf8(out).expect("rewrites preserve UTF-8"))
}

/// Mirrors `hasFormatToken` (case-sensitive token match, like legacy).
fn has_format_token(format: &str, index: usize, token: &str) -> bool {
    let bytes = format.as_bytes();
    if index > 0 && is_format_letter(bytes[index - 1]) {
        return false;
    }
    let token_bytes = token.as_bytes();
    if bytes.len() < index + token_bytes.len()
        || &bytes[index..index + token_bytes.len()] != token_bytes
    {
        return false;
    }
    let end = index + token_bytes.len();
    end == bytes.len() || !is_format_letter(bytes[end])
}

/// Mirrors `removeTrailingRedshiftTimezoneFormat` (None = unchanged).
fn remove_trailing_redshift_timezone_format(format: &str) -> Option<String> {
    let trimmed = format.trim_end_matches([' ', '\t', '\n', '\r']);
    let trimmed_bytes = trimmed.as_bytes();
    for token in ["tz", "of"] {
        if trimmed_bytes.len() < token.len() {
            continue;
        }
        let start = trimmed_bytes.len() - token.len();
        if !trimmed_bytes[start..].eq_ignore_ascii_case(token.as_bytes()) {
            continue;
        }
        if start > 0 && is_format_letter(trimmed_bytes[start - 1]) {
            continue;
        }
        let end = trimmed[..start].trim_end_matches([' ', '\t', '\n', '\r']);
        return Some(format!("{end}{}", &format[trimmed.len()..]));
    }
    None
}

/// Mirrors `isFormatLetter`.
fn is_format_letter(ch: u8) -> bool {
    ch.is_ascii_alphabetic()
}

/// Mirrors `sqlStringLiteral`.
fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Mirrors `rewriteMedian`.
fn rewrite_median(args: &[String]) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    let value = args[0].trim();
    if value.is_empty() {
        return None;
    }
    Some(format!(
        "percentile_cont(0.5) WITHIN GROUP (ORDER BY {value})"
    ))
}

/// Mirrors `postgresIntervalPart`.
fn postgres_interval_part(value: &str) -> Option<&'static str> {
    postgres_date_part(value)
}

/// Mirrors `postgresDatePart`.
fn postgres_date_part(value: &str) -> Option<&'static str> {
    match clean_identifier(value).to_lowercase().as_str() {
        "year" | "yy" | "yyyy" => Some("year"),
        "month" | "mon" | "mm" => Some("month"),
        "day" | "d" | "dd" => Some("day"),
        "hour" | "h" | "hh" => Some("hour"),
        "minute" | "m" | "mi" | "n" => Some("minute"),
        "second" | "s" | "sec" | "ss" => Some("second"),
        _ => None,
    }
}

/// Mirrors `postgresDatePartFunctionPart`.
fn postgres_date_part_function_part(value: &str) -> Option<&'static str> {
    match clean_date_part_identifier(value).to_lowercase().as_str() {
        "millennium" | "millennia" => Some("millennium"),
        "century" | "c" | "centuries" => Some("century"),
        "decade" | "dec" | "decades" => Some("decade"),
        "year" | "y" | "yr" | "yrs" | "yy" | "yyyy" => Some("year"),
        "quarter" | "qtr" | "q" => Some("quarter"),
        "month" | "mon" | "mons" | "mm" => Some("month"),
        "week" | "w" => Some("week"),
        "day" | "d" | "dd" => Some("day"),
        "dayofweek" | "dow" | "dw" | "weekday" => Some("dow"),
        "dayofyear" | "doy" => Some("doy"),
        "hour" | "h" | "hr" | "hrs" | "hh" => Some("hour"),
        "minute" | "m" | "min" | "mins" | "mi" | "n" => Some("minute"),
        "second" | "s" | "sec" | "secs" | "ss" => Some("second"),
        "millisecond" | "milliseconds" | "msec" | "msecs" | "ms" => Some("milliseconds"),
        "microsecond" | "microseconds" | "usec" | "usecs" | "us" => Some("microseconds"),
        "epoch" => Some("epoch"),
        _ => None,
    }
}

/// Mirrors `postgresDateTruncPart`.
fn postgres_date_trunc_part(value: &str) -> Option<&'static str> {
    match clean_date_part_identifier(value).to_lowercase().as_str() {
        "millennium" | "millennia" => Some("millennium"),
        "century" | "c" | "centuries" => Some("century"),
        "decade" | "dec" | "decades" => Some("decade"),
        "year" | "y" | "yr" | "yrs" | "yy" | "yyyy" => Some("year"),
        "quarter" | "qtr" | "q" => Some("quarter"),
        "month" | "mon" | "mons" | "mm" => Some("month"),
        "week" | "w" => Some("week"),
        "day" | "d" | "dd" => Some("day"),
        "hour" | "h" | "hr" | "hrs" | "hh" => Some("hour"),
        "minute" | "m" | "min" | "mins" | "mi" | "n" => Some("minute"),
        "second" | "s" | "sec" | "secs" | "ss" => Some("second"),
        "millisecond" | "milliseconds" | "msec" | "msecs" | "ms" => Some("milliseconds"),
        "microsecond" | "microseconds" | "usec" | "usecs" | "us" => Some("microseconds"),
        _ => None,
    }
}

/// Mirrors `cleanDatePartIdentifier`.
fn clean_date_part_identifier(value: &str) -> &str {
    value.trim().trim_matches(|ch| ch == '"' || ch == '\'')
}

/// Mirrors `parseRedshiftBooleanLiteral`.
fn parse_redshift_boolean_literal(sql: &str, index: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    if index >= bytes.len() {
        return None;
    }
    if bytes[index] == b'\'' {
        let (value, next) = read_quoted_string_value(sql, index)?;
        let rewritten = postgres_boolean_literal(&value)?;
        return Some((rewritten, next));
    }
    if bytes[index] == b'0' || bytes[index] == b'1' {
        if index + 1 < bytes.len() && is_identifier_part(bytes[index + 1]) {
            return None;
        }
        let rewritten = postgres_boolean_literal(&sql[index..index + 1])?;
        return Some((rewritten, index + 1));
    }
    if !is_identifier_start(bytes[index]) {
        return None;
    }
    let start = index;
    let mut index = index + 1;
    while index < bytes.len() && is_identifier_part(bytes[index]) {
        index += 1;
    }
    let rewritten = postgres_boolean_literal(&sql[start..index])?;
    Some((rewritten, index))
}

/// Mirrors `readQuotedStringValue`.
fn read_quoted_string_value(value: &str, start: usize) -> Option<(String, usize)> {
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::new();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] != b'\'' {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
            out.push(bytes[i + 1]);
            i += 2;
            continue;
        }
        return Some((
            String::from_utf8(out).expect("rewrites preserve UTF-8"),
            i + 1,
        ));
    }
    None
}

/// Mirrors `postgresBooleanLiteral`.
fn postgres_boolean_literal(value: &str) -> Option<String> {
    let normalized = value.trim().trim_matches(|ch| ch == '"' || ch == '\'');
    match normalized.to_lowercase().as_str() {
        "1" | "t" | "true" | "y" | "yes" => Some("TRUE".to_string()),
        "0" | "f" | "false" | "n" | "no" => Some("FALSE".to_string()),
        _ => None,
    }
}

/// Mirrors `translateCreateExternalSchema`.
fn translate_create_external_schema(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    const PREFIX: &str = "create external schema";
    if !has_prefix_fold(statement, PREFIX) {
        return None;
    }

    let tokens: Vec<&str> = statement[PREFIX.len()..].split_whitespace().collect();
    let mut from_index: Option<usize> = None;
    let mut i = 0usize;
    while i + 2 < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("from")
            && tokens[i + 1].eq_ignore_ascii_case("data")
            && tokens[i + 2].eq_ignore_ascii_case("catalog")
        {
            from_index = Some(i);
            break;
        }
        i += 1;
    }
    let from_index = from_index?;
    if from_index == 0 {
        return Some(Err(SqlError::new(
            "CREATE EXTERNAL SCHEMA FROM DATA CATALOG requires a schema name",
        )));
    }

    Some(Ok(TranslationResult {
        backend_sql: format!("create schema {}", tokens[..from_index].join(" ")),
        ..TranslationResult::default()
    }))
}

/// Mirrors `translateCreateExternalTable`.
fn translate_create_external_table(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    if !has_prefix_fold(statement, "create external table") {
        return None;
    }
    let Some(open) = statement.find('(') else {
        return Some(Ok(TranslationResult {
            backend_sql: format!("{}{}", &statement[..7], &statement[16..]),
            ..TranslationResult::default()
        }));
    };
    let Some(close) = matching_paren(statement, open) else {
        return Some(Err(SqlError::new(
            "CREATE EXTERNAL TABLE has an unterminated column list",
        )));
    };

    let mut name_part = statement[22..open].trim();
    if has_prefix_fold(name_part, "if not exists ") {
        name_part = name_part["if not exists ".len()..].trim();
    }
    let (schema_name, table_name) = parse_qualified_name(name_part);
    let (clean_columns, columns, column_dist_key, column_sort_keys) =
        match translate_column_definitions(&statement[open + 1..close]) {
            Ok(parts) => parts,
            Err(err) => return Some(Err(err)),
        };

    let effect = MetadataEffect {
        kind: MetadataEffectKind::CreateTable,
        schema: schema_name,
        table: table_name,
        name: column_dist_key,
        value: String::new(),
        backup: String::new(),
        columns,
        sort_keys: column_sort_keys,
    };
    let prefix = format!("{}{}", &statement[..7], &statement[16..=open]);
    let backend_sql = format!("{prefix}{})", clean_columns.join(", "))
        .trim()
        .to_string();
    Some(Ok(TranslationResult {
        backend_sql,
        metadata_effects: vec![effect],
        ..TranslationResult::default()
    }))
}

/// Mirrors `translateCreateMaterializedView`.
fn translate_create_materialized_view(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    const PREFIX: &str = "create materialized view";
    if !has_prefix_fold(statement, PREFIX) {
        return None;
    }

    let Some(as_index) = find_top_level_keyword(statement, "as", PREFIX.len()) else {
        return Some(Ok(passthrough_statement(statement)));
    };

    let Some(header) = remove_keyword_sequence(&statement[..as_index], &["auto", "refresh", "yes"])
    else {
        return Some(Ok(passthrough_statement(statement)));
    };
    let backend_sql = format!("{header} {}", statement[as_index..].trim())
        .trim()
        .to_string();
    Some(Ok(TranslationResult {
        backend_sql,
        ..TranslationResult::default()
    }))
}

/// Mirrors `translateMergeInto`. Emits INSERT first, then UPDATE, as two
/// semicolon-separated statements — see the legacy comment for why INSERT must run
/// before UPDATE (the NOT EXISTS membership check has to see the pre-update
/// state when the MATCHED branch rewrites a column used in the ON condition).
fn translate_merge_into(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let prefix_end = match_keyword_sequence(statement, 0, &["merge", "into"])?;

    let Some((using_start, using_end)) =
        find_top_level_keyword_sequence(statement, &["using"], prefix_end)
    else {
        return Some(Ok(passthrough_statement(statement)));
    };
    let Some((on_start, on_end)) = find_top_level_keyword_sequence(statement, &["on"], using_end)
    else {
        return Some(Ok(passthrough_statement(statement)));
    };
    let Some((matched_start, matched_end)) = find_top_level_keyword_sequence(
        statement,
        &["when", "matched", "then", "update", "set"],
        on_end,
    ) else {
        return Some(Ok(passthrough_statement(statement)));
    };
    let Some((not_matched_start, not_matched_end)) = find_top_level_keyword_sequence(
        statement,
        &["when", "not", "matched", "then", "insert"],
        matched_end,
    ) else {
        return Some(Ok(passthrough_statement(statement)));
    };

    let target = statement[prefix_end..using_start].trim();
    let source = statement[using_end..on_start].trim();
    let on_condition = statement[on_end..matched_start].trim();
    let update_assignments = statement[matched_end..not_matched_start].trim();
    if target.is_empty()
        || source.is_empty()
        || on_condition.is_empty()
        || update_assignments.is_empty()
    {
        return Some(Ok(passthrough_statement(statement)));
    }

    let Some((insert_columns, insert_values)) =
        parse_merge_insert_clause(&statement[not_matched_end..])
    else {
        return Some(Ok(passthrough_statement(statement)));
    };
    let insert_target = first_sql_token(target);
    if insert_target.is_empty() {
        return Some(Ok(passthrough_statement(statement)));
    }

    let insert_stmt = format!(
        "insert into {insert_target} {insert_columns} select {insert_values} from {source} where not exists (select 1 from {target} where {on_condition})"
    );
    let update_stmt =
        format!("update {target} set {update_assignments} from {source} where {on_condition}");
    Some(Ok(TranslationResult {
        backend_sql: format!("{insert_stmt}; {update_stmt}"),
        ..TranslationResult::default()
    }))
}

/// Mirrors `parseMergeInsertClause`.
fn parse_merge_insert_clause(value: &str) -> Option<(String, String)> {
    let trimmed = value.trim();
    if trimmed.is_empty() || !trimmed.starts_with('(') {
        return None;
    }
    let columns_close = matching_paren(trimmed, 0)?;
    let columns = trimmed[..=columns_close].trim().to_string();
    let (values_start, values_end) =
        find_top_level_keyword_sequence(trimmed, &["values"], columns_close + 1)?;
    if !trimmed[columns_close + 1..values_start].trim().is_empty() {
        return None;
    }
    let values = trimmed[values_end..].trim();
    if values.len() < 2 || !values.starts_with('(') {
        return None;
    }
    let values_close = matching_paren(values, 0)?;
    if !values[values_close + 1..].trim().is_empty() {
        return None;
    }
    Some((columns, values[1..values_close].trim().to_string()))
}

/// Mirrors `translateInsertSelectReturning`.
fn translate_insert_select_returning(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let prefix_end = match_keyword_sequence(statement, 0, &["insert", "into"])?;
    let (select_start, select_end) =
        find_top_level_keyword_sequence(statement, &["select"], prefix_end)?;
    if let Some((values_start, _)) =
        find_top_level_keyword_sequence(statement, &["values"], prefix_end)
    {
        if values_start < select_start {
            return None;
        }
    }
    let (returning_start, _) =
        find_top_level_keyword_sequence(statement, &["returning"], select_end)?;
    Some(Ok(TranslationResult {
        backend_sql: statement[..returning_start].trim().to_string(),
        ..TranslationResult::default()
    }))
}

/// Mirrors `translateInsertValuesDefault`.
fn translate_insert_values_default(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let bytes = statement.as_bytes();
    let prefix_end = match_keyword_sequence(statement, 0, &["insert", "into"])?;
    let Some((values_start, values_end)) =
        find_top_level_keyword_sequence(statement, &["values"], prefix_end)
    else {
        return Some(Ok(passthrough_statement(statement)));
    };
    let open = skip_spaces(statement, values_end);
    if open >= bytes.len() || bytes[open] != b'(' {
        return Some(Ok(passthrough_statement(statement)));
    }
    let Some(close) = matching_paren(statement, open) else {
        return Some(Ok(passthrough_statement(statement)));
    };
    if !statement[close + 1..].trim().is_empty() {
        return Some(Ok(passthrough_statement(statement)));
    }
    let values = split_comma_separated(&statement[open + 1..close]);
    if values.len() != 1 || !values[0].trim().eq_ignore_ascii_case("default") {
        return Some(Ok(passthrough_statement(statement)));
    }
    Some(Ok(TranslationResult {
        backend_sql: format!("{} default values", statement[..values_start].trim()),
        ..TranslationResult::default()
    }))
}

/// Mirrors `translateAlterColumnEncode`.
fn translate_alter_column_encode(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let tokens: Vec<&str> = statement.split_whitespace().collect();
    if tokens.len() < 7
        || !tokens[0].eq_ignore_ascii_case("alter")
        || !tokens[1].eq_ignore_ascii_case("table")
    {
        return None;
    }

    let mut table_index = 2;
    if tokens.len() > table_index + 2
        && tokens[table_index].eq_ignore_ascii_case("if")
        && tokens[table_index + 1].eq_ignore_ascii_case("exists")
    {
        table_index += 2;
    }
    let alter_index = table_index + 1;
    if alter_index >= tokens.len() || !tokens[alter_index].eq_ignore_ascii_case("alter") {
        return None;
    }

    let mut column_index = alter_index + 1;
    if column_index < tokens.len() && tokens[column_index].eq_ignore_ascii_case("column") {
        column_index += 1;
    }
    let encode_index = column_index + 1;
    if encode_index + 1 >= tokens.len() || !tokens[encode_index].eq_ignore_ascii_case("encode") {
        return None;
    }
    if encode_index + 2 != tokens.len() {
        return None;
    }

    let mut table_prefix = String::from("alter table ");
    if table_index == 4 {
        table_prefix.push_str("if exists ");
    }
    Some(Ok(TranslationResult {
        backend_sql: format!(
            "{table_prefix}{} alter column {} set statistics -1",
            tokens[table_index], tokens[column_index]
        ),
        ..TranslationResult::default()
    }))
}

/// Mirrors `translateAlterAddColumnDefaultIdentity`.
fn translate_alter_add_column_default_identity(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let tokens: Vec<&str> = statement.split_whitespace().collect();
    if tokens.len() < 7
        || !tokens[0].eq_ignore_ascii_case("alter")
        || !tokens[1].eq_ignore_ascii_case("table")
    {
        return None;
    }

    let mut table_index = 2;
    if tokens.len() > table_index + 2
        && tokens[table_index].eq_ignore_ascii_case("if")
        && tokens[table_index + 1].eq_ignore_ascii_case("exists")
    {
        table_index += 2;
    }
    let add_index = table_index + 1;
    if add_index >= tokens.len() || !tokens[add_index].eq_ignore_ascii_case("add") {
        return None;
    }

    let mut column_index = add_index + 1;
    if column_index < tokens.len() && tokens[column_index].eq_ignore_ascii_case("column") {
        column_index += 1;
    }
    if column_index + 3 > tokens.len() {
        return None;
    }

    let definition_tokens = &tokens[column_index..];
    let mut default_index: Option<usize> = None;
    for (i, token) in definition_tokens.iter().enumerate().skip(2) {
        if token.eq_ignore_ascii_case("default") {
            default_index = Some(i);
            break;
        }
    }
    let default_index = default_index?;

    let (identity_clause, consumed) =
        parse_default_identity_clause(definition_tokens, default_index)?;

    let mut clean_definition: Vec<String> = vec![
        definition_tokens[0].to_string(),
        postgres_column_type(definition_tokens[1]),
    ];
    clean_definition.extend(
        definition_tokens[2..default_index]
            .iter()
            .map(|token| token.to_string()),
    );
    clean_definition.push(identity_clause);
    clean_definition.extend(
        definition_tokens[default_index + consumed..]
            .iter()
            .map(|token| token.to_string()),
    );

    let mut table_prefix = String::from("alter table ");
    if table_index == 4 {
        table_prefix.push_str("if exists ");
    }
    Some(Ok(TranslationResult {
        backend_sql: format!(
            "{table_prefix}{} add column {}",
            tokens[table_index],
            clean_definition.join(" ")
        ),
        ..TranslationResult::default()
    }))
}

/// Mirrors `parseDefaultIdentityClause`.
fn parse_default_identity_clause(tokens: &[&str], default_index: usize) -> Option<(String, usize)> {
    if default_index + 1 >= tokens.len() || !tokens[default_index].eq_ignore_ascii_case("default") {
        return None;
    }

    let mut identity_text = String::new();
    for i in default_index + 1..tokens.len() {
        if !identity_text.is_empty() {
            identity_text.push(' ');
        }
        identity_text.push_str(tokens[i]);
        let trimmed = identity_text.trim().to_string();
        let lower = trimmed.to_lowercase();
        if !lower.starts_with("identity") {
            return None;
        }

        let Some(open) = trimmed.find('(') else {
            continue;
        };
        if !trimmed[..open].trim().eq_ignore_ascii_case("identity") {
            return None;
        }
        let Some(close) = matching_paren(&trimmed, open) else {
            continue;
        };
        if !trimmed[close + 1..].trim().is_empty() {
            return None;
        }

        let args = split_comma_separated(&trimmed[open + 1..close]);
        if args.len() != 2 {
            return None;
        }
        let start = args[0].trim();
        let increment = args[1].trim();
        if start.is_empty() || increment.is_empty() {
            return None;
        }
        return Some((
            format!(
                "generated by default as identity (start with {start} increment by {increment})"
            ),
            i - default_index + 1,
        ));
    }
    None
}

/// Mirrors `translateTruncateImmediateCommit`.
fn translate_truncate_immediate_commit(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let prefix_end = match_keyword_sequence(statement, 0, &["truncate"])?;
    if statement[prefix_end..].trim().is_empty() {
        return Some(Ok(passthrough_statement(statement)));
    }
    Some(Ok(TranslationResult {
        backend_sql: format!("commit; {statement}"),
        ..TranslationResult::default()
    }))
}

/// Mirrors `translateQualifySelect`.
fn translate_qualify_select(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let prefix_end = match_keyword_sequence(statement, 0, &["select"])?;
    let (qualify_start, qualify_end) =
        find_top_level_keyword_sequence(statement, &["qualify"], prefix_end)?;

    let mut qualify_clause_end = statement.len();
    let mut suffix_start: Option<usize> = None;
    for sequence in [&["order", "by"][..], &["limit"], &["offset"], &["fetch"]] {
        if let Some((start, _)) = find_top_level_keyword_sequence(statement, sequence, qualify_end)
        {
            if start < qualify_clause_end {
                qualify_clause_end = start;
                suffix_start = Some(start);
            }
        }
    }

    let mut inner_sql = statement[..qualify_start].trim().to_string();
    let mut condition = statement[qualify_end..qualify_clause_end]
        .trim()
        .to_string();
    if inner_sql.is_empty() || condition.is_empty() {
        return Some(Ok(passthrough_statement(statement)));
    }

    let mut outer_select = "*".to_string();
    if let Some((rewritten_outer_select, rewritten_inner_sql, rewritten_condition)) =
        rewrite_qualify_window_predicate(&inner_sql, &condition)
    {
        outer_select = rewritten_outer_select;
        inner_sql = rewritten_inner_sql;
        condition = rewritten_condition;
    }

    let mut backend_sql =
        format!("select {outer_select} from ({inner_sql}) as devcloud_qualify where {condition}");
    if let Some(start) = suffix_start {
        backend_sql.push(' ');
        backend_sql.push_str(statement[start..].trim());
    }
    Some(Ok(TranslationResult {
        backend_sql,
        ..TranslationResult::default()
    }))
}

/// Mirrors `translateGrantAssumeRole`.
fn translate_grant_assume_role(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let prefix_end = match_keyword_sequence(statement, 0, &["grant", "assumerole", "on"])?;

    let role_start = skip_spaces(statement, prefix_end);
    let Some((_, role_end)) = read_quoted_string_value(statement, role_start) else {
        return Some(Ok(passthrough_statement(statement)));
    };
    let Some(to_end) = match_keyword_sequence(statement, skip_spaces(statement, role_end), &["to"])
    else {
        return Some(Ok(passthrough_statement(statement)));
    };
    if statement[to_end..].trim().is_empty() {
        return Some(Ok(passthrough_statement(statement)));
    }

    Some(Ok(select_one()))
}

/// Mirrors `translateCreateModel`.
fn translate_create_model(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let prefix_end = match_keyword_sequence(statement, 0, &["create", "model"])?;

    let from = find_top_level_keyword_sequence(statement, &["from"], prefix_end);
    let target =
        from.and_then(|(_, end)| find_top_level_keyword_sequence(statement, &["target"], end));
    let function =
        target.and_then(|(_, end)| find_top_level_keyword_sequence(statement, &["function"], end));
    let iam_role = function
        .and_then(|(_, end)| find_top_level_keyword_sequence(statement, &["iam_role"], end));
    let (
        Some((from_start, from_end)),
        Some((target_start, target_end)),
        Some((function_start, function_end)),
        Some((iam_role_start, iam_role_end)),
    ) = (from, target, function, iam_role)
    else {
        return Some(Ok(passthrough_statement(statement)));
    };
    if statement[prefix_end..from_start].trim().is_empty()
        || statement[from_end..target_start].trim().is_empty()
        || statement[target_end..function_start].trim().is_empty()
        || statement[function_end..iam_role_start].trim().is_empty()
        || statement[iam_role_end..].trim().is_empty()
    {
        return Some(Ok(passthrough_statement(statement)));
    }
    Some(Ok(select_one()))
}

/// Mirrors `translateCreateExternalFunction`.
fn translate_create_external_function(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let prefix_end = match_keyword_sequence(statement, 0, &["create", "external", "function"])?;

    let Some((lambda_start, lambda_end)) =
        find_top_level_keyword_sequence(statement, &["lambda"], prefix_end)
    else {
        return Some(Ok(passthrough_statement(statement)));
    };
    if statement[prefix_end..lambda_start].trim().is_empty() {
        return Some(Ok(passthrough_statement(statement)));
    }

    let arn_start = skip_spaces(statement, lambda_end);
    match read_quoted_string_value(statement, arn_start) {
        Some((arn, _)) if has_prefix_fold(&arn, "arn:") => Some(Ok(select_one())),
        _ => Some(Ok(passthrough_statement(statement))),
    }
}

/// Mirrors `translateDatashare`.
fn translate_datashare(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    for keywords in [&["create", "datashare"][..], &["alter", "datashare"]] {
        let Some(prefix_end) = match_keyword_sequence(statement, 0, keywords) else {
            continue;
        };
        if statement[prefix_end..].trim().is_empty() {
            return Some(Ok(passthrough_statement(statement)));
        }
        return Some(Ok(select_one()));
    }

    let prefix_end = match_keyword_sequence(statement, 0, &["grant", "usage", "on", "datashare"])?;
    let Some((to_start, to_end)) = find_top_level_keyword_sequence(statement, &["to"], prefix_end)
    else {
        return Some(Ok(passthrough_statement(statement)));
    };
    if statement[prefix_end..to_start].trim().is_empty() || statement[to_end..].trim().is_empty() {
        return Some(Ok(passthrough_statement(statement)));
    }
    Some(Ok(select_one()))
}

/// Mirrors `translateMaskingPolicy`.
fn translate_masking_policy(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    for keywords in [
        &["create", "masking", "policy"][..],
        &["attach", "masking", "policy"],
    ] {
        let Some(prefix_end) = match_keyword_sequence(statement, 0, keywords) else {
            continue;
        };
        if statement[prefix_end..].trim().is_empty() {
            return Some(Ok(passthrough_statement(statement)));
        }
        return Some(Ok(select_one()));
    }
    None
}

/// Mirrors `translateRowAccessPolicy`.
fn translate_row_access_policy(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let prefix_end = match_keyword_sequence(statement, 0, &["create", "row", "access", "policy"])?;
    if statement[prefix_end..].trim().is_empty() {
        return Some(Ok(passthrough_statement(statement)));
    }
    Some(Ok(select_one()))
}

/// Mirrors `rewriteQualifyWindowPredicate`.
fn rewrite_qualify_window_predicate(
    inner_sql: &str,
    condition: &str,
) -> Option<(String, String, String)> {
    let (window_start, window_end) = find_first_window_function_expression(condition)?;

    let select_end = match_keyword_sequence(inner_sql, 0, &["select"])?;
    let (from_start, _) = find_top_level_keyword_sequence(inner_sql, &["from"], select_end)?;

    let select_list = inner_sql[select_end..from_start].trim();
    let from_clause = inner_sql[from_start..].trim();
    if select_list.is_empty() || from_clause.is_empty() {
        return None;
    }

    let alias = "__devcloud_qualify_1";
    let window_expression = condition[window_start..window_end].trim();
    let outer_select = qualify_outer_select_list(select_list);
    let rewritten_inner_sql =
        format!("select {select_list}, {window_expression} as {alias} {from_clause}");
    let rewritten_condition = format!(
        "{}{alias}{}",
        &condition[..window_start],
        &condition[window_end..]
    );
    Some((outer_select, rewritten_inner_sql, rewritten_condition))
}

/// Mirrors `findFirstWindowFunctionExpression`.
fn find_first_window_function_expression(value: &str) -> Option<(usize, usize)> {
    let bytes = value.as_bytes();
    let mut in_string = false;
    let mut in_quoted_identifier = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' && !in_quoted_identifier {
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_string = !in_string;
            i += 1;
            continue;
        }
        if ch == b'"' && !in_string {
            if in_quoted_identifier && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            in_quoted_identifier = !in_quoted_identifier;
            i += 1;
            continue;
        }
        if in_string || in_quoted_identifier || !is_identifier_start(ch) {
            i += 1;
            continue;
        }

        let name_start = i;
        i += 1;
        while i < bytes.len() && is_identifier_part(bytes[i]) {
            i += 1;
        }
        let open = skip_spaces(value, i);
        if open >= bytes.len() || bytes[open] != b'(' {
            i += 1;
            continue;
        }
        let Some(args_close) = matching_paren(value, open) else {
            i += 1;
            continue;
        };
        let over_start = skip_spaces(value, args_close + 1);
        let Some(over_end) = match_keyword_sequence(value, over_start, &["over"]) else {
            i += 1;
            continue;
        };
        let over_open = skip_spaces(value, over_end);
        if over_open >= bytes.len() || bytes[over_open] != b'(' {
            i += 1;
            continue;
        }
        let Some(over_close) = matching_paren(value, over_open) else {
            i += 1;
            continue;
        };
        return Some((name_start, over_close + 1));
    }
    None
}

/// Mirrors `qualifyOuterSelectList`.
fn qualify_outer_select_list(select_list: &str) -> String {
    let items = split_comma_separated(select_list);
    let outer: Vec<String> = items
        .iter()
        .map(|item| qualify_outer_select_item(item))
        .collect();
    outer.join(", ")
}

/// Mirrors `qualifyOuterSelectItem`.
fn qualify_outer_select_item(item: &str) -> String {
    let item = item.trim();
    if item == "*" || item.ends_with(".*") {
        return item.to_string();
    }
    if let Some((_, as_end)) = find_top_level_keyword_sequence(item, &["as"], 0) {
        let alias = item[as_end..].trim();
        if !alias.is_empty() {
            return alias.to_string();
        }
    }

    let fields: Vec<&str> = item.split_whitespace().collect();
    if fields.len() > 1 {
        let alias = fields[fields.len() - 1];
        if !clean_identifier(alias).is_empty() {
            return alias.to_string();
        }
    }
    item.to_string()
}

/// Mirrors `translateCreateTable`.
fn translate_create_table(sql: &str) -> StatementTranslation {
    let statement = sql.trim_end_matches(';').trim();
    let (prefix_end, temporary) = parse_create_table_prefix(statement)?;
    let Some(open) = statement.find('(') else {
        return Some(Ok(TranslationResult {
            backend_sql: sql.to_string(),
            ..TranslationResult::default()
        }));
    };
    let Some(close) = matching_paren(statement, open) else {
        return Some(Err(SqlError::new(
            "CREATE TABLE has an unterminated column list",
        )));
    };

    let mut name_part = statement[prefix_end..open].trim();
    if has_prefix_fold(name_part, "if not exists ") {
        name_part = name_part["if not exists ".len()..].trim();
    }
    let (schema_name, table_name) = parse_qualified_name(name_part);
    if let Some(clean_like) = translate_create_table_like_clause(&statement[open + 1..close]) {
        let (clean_rest, dist_style, dist_key, sort_keys, backup) =
            translate_table_attributes(&statement[close + 1..]);
        let clean_rest = ensure_temporary_table_scope(clean_rest, temporary);
        let effect = MetadataEffect {
            kind: MetadataEffectKind::CreateTable,
            schema: schema_name,
            table: table_name,
            name: dist_key,
            value: dist_style,
            backup,
            columns: Vec::new(),
            sort_keys,
        };
        let backend_sql = format!("{}{clean_like}){clean_rest}", &statement[..=open])
            .trim()
            .to_string();
        if temporary {
            return Some(Ok(TranslationResult {
                backend_sql,
                ..TranslationResult::default()
            }));
        }
        return Some(Ok(TranslationResult {
            backend_sql,
            metadata_effects: vec![effect],
            ..TranslationResult::default()
        }));
    }
    let (clean_columns, columns, column_dist_key, column_sort_keys) =
        match translate_column_definitions(&statement[open + 1..close]) {
            Ok(parts) => parts,
            Err(err) => return Some(Err(err)),
        };
    let (clean_rest, mut dist_style, mut dist_key, mut sort_keys, backup) =
        translate_table_attributes(&statement[close + 1..]);
    if dist_key.is_empty() {
        dist_key = column_dist_key;
    }
    for key in &column_sort_keys {
        if !contains_identifier(&sort_keys, key) {
            sort_keys.push(key.clone());
        }
    }
    if dist_style.is_empty() && !dist_key.is_empty() {
        dist_style = "key".to_string();
    }
    let clean_rest = ensure_temporary_table_scope(clean_rest, temporary);

    let effect = MetadataEffect {
        kind: MetadataEffectKind::CreateTable,
        schema: schema_name,
        table: table_name,
        name: dist_key,
        value: dist_style,
        backup,
        columns,
        sort_keys,
    };
    let backend_sql = format!(
        "{}{}){clean_rest}",
        &statement[..=open],
        clean_columns.join(", ")
    )
    .trim()
    .to_string();
    if temporary {
        return Some(Ok(TranslationResult {
            backend_sql,
            ..TranslationResult::default()
        }));
    }
    Some(Ok(TranslationResult {
        backend_sql,
        metadata_effects: vec![effect],
        ..TranslationResult::default()
    }))
}

/// Mirrors `parseCreateTablePrefix`.
fn parse_create_table_prefix(statement: &str) -> Option<(usize, bool)> {
    if let Some(next) = match_keyword_sequence(statement, 0, &["create", "temporary", "table"]) {
        return Some((next, true));
    }
    if let Some(next) = match_keyword_sequence(statement, 0, &["create", "temp", "table"]) {
        return Some((next, true));
    }
    if let Some(next) = match_keyword_sequence(statement, 0, &["create", "table"]) {
        return Some((next, false));
    }
    None
}

/// Mirrors `ensureTemporaryTableScope`.
fn ensure_temporary_table_scope(clean_rest: String, temporary: bool) -> String {
    if !temporary || clean_rest.to_lowercase().contains("on commit") {
        return clean_rest;
    }
    if clean_rest.trim().is_empty() {
        return " on commit preserve rows".to_string();
    }
    format!("{clean_rest} on commit preserve rows")
}

/// Mirrors `translateCreateTableLikeClause`.
fn translate_create_table_like_clause(value: &str) -> Option<String> {
    let tokens: Vec<&str> = value.trim().split_whitespace().collect();
    if tokens.len() != 2 && tokens.len() != 4 {
        return None;
    }
    if !tokens[0].eq_ignore_ascii_case("like") {
        return None;
    }
    if tokens.len() == 2 {
        return Some(format!("LIKE {}", tokens[1]));
    }
    if !tokens[3].eq_ignore_ascii_case("defaults") {
        return None;
    }
    if tokens[2].eq_ignore_ascii_case("including") {
        return Some(format!("LIKE {} INCLUDING DEFAULTS", tokens[1]));
    }
    if tokens[2].eq_ignore_ascii_case("excluding") {
        return Some(format!("LIKE {} EXCLUDING DEFAULTS", tokens[1]));
    }
    None
}

/// Mirrors `translateColumnDefinitions`: returns (clean column SQL, column
/// metadata, column-level DISTKEY, column-level SORTKEYs).
#[allow(clippy::type_complexity)]
fn translate_column_definitions(
    value: &str,
) -> Result<(Vec<String>, Vec<ColumnMetadata>, String, Vec<String>), SqlError> {
    let definitions = split_comma_separated(value);
    let mut cleaned = Vec::with_capacity(definitions.len());
    let mut columns = Vec::with_capacity(definitions.len());
    let mut dist_key = String::new();
    let mut sort_keys: Vec<String> = Vec::new();
    for definition in &definitions {
        let tokens: Vec<&str> = definition.trim().split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(SqlError::new(
                "CREATE TABLE column definition requires name and type",
            ));
        }
        let column_name = clean_identifier(tokens[0]).to_string();
        if column_name.is_empty() {
            return Err(SqlError::new("CREATE TABLE column name cannot be empty"));
        }
        let mut column = ColumnMetadata {
            name: column_name.clone(),
            data_type: tokens[1].to_lowercase(),
            ..ColumnMetadata::default()
        };
        let column_type = postgres_column_type(tokens[1]);
        let mut clean_tokens: Vec<String> = vec![tokens[0].to_string(), column_type];
        if let Some(byte_limit) = redshift_byte_limited_string_type(tokens[1]) {
            clean_tokens.push("check".to_string());
            clean_tokens.push(format!("(octet_length({}) <= {byte_limit})", tokens[0]));
        }
        let mut i = 2;
        while i < tokens.len() {
            let token = tokens[i].to_lowercase();
            if token == "encode" && i + 1 < tokens.len() {
                column.encoding = clean_identifier(tokens[i + 1]).to_string();
                i += 1;
            } else if token == "default"
                && i + 1 < tokens.len()
                && !tokens[i + 1].eq_ignore_ascii_case("as")
            {
                let mut default_value = tokens[i + 1].to_string();
                if is_boolean_column_type(&column.data_type) {
                    if let Some(rewritten) = postgres_boolean_literal(&default_value) {
                        default_value = rewritten;
                    }
                }
                column.default_value = tokens[i + 1].to_string();
                clean_tokens.push(tokens[i].to_string());
                clean_tokens.push(default_value);
                i += 1;
            } else if token == "identity" || token.starts_with("identity(") {
                column.identity = true;
                for part in ["generated", "by", "default", "as", "identity"] {
                    clean_tokens.push(part.to_string());
                }
            } else if token == "generated" {
                column.identity = has_identity_token(&tokens[i + 1..]);
                clean_tokens.extend(tokens[i..].iter().map(|t| t.to_string()));
                i = tokens.len();
                continue;
            } else if token == "distkey" {
                dist_key = column_name.clone();
            } else if token == "sortkey" {
                if !contains_identifier(&sort_keys, &column_name) {
                    sort_keys.push(column_name.clone());
                }
            } else {
                clean_tokens.push(tokens[i].to_string());
            }
            i += 1;
        }
        cleaned.push(clean_tokens.join(" "));
        columns.push(column);
    }
    Ok((cleaned, columns, dist_key, sort_keys))
}

/// Mirrors `postgresColumnType`.
fn postgres_column_type(value: &str) -> String {
    if value.eq_ignore_ascii_case("timestamp") || value.eq_ignore_ascii_case("timestamptz") {
        return "timestamp(6) without time zone".to_string();
    }
    if value.eq_ignore_ascii_case("time") {
        return "time(6) without time zone".to_string();
    }
    if value.eq_ignore_ascii_case("timetz") {
        return "time(6) with time zone".to_string();
    }
    if value.eq_ignore_ascii_case("super") {
        return "jsonb".to_string();
    }
    if value.eq_ignore_ascii_case("hllsketch") || value.eq_ignore_ascii_case("varbyte") {
        return "bytea".to_string();
    }
    if value.eq_ignore_ascii_case("geometry") || value.eq_ignore_ascii_case("geography") {
        return "text".to_string();
    }
    if redshift_byte_limited_string_type(value).is_some() {
        return "text".to_string();
    }
    value.to_string()
}

/// Mirrors `redshiftByteLimitedStringType`.
fn redshift_byte_limited_string_type(value: &str) -> Option<String> {
    let lower = value.trim().to_lowercase();
    for prefix in ["varchar", "char"] {
        if !lower.starts_with(&format!("{prefix}(")) || !lower.ends_with(')') {
            continue;
        }
        let limit = lower[prefix.len() + 1..lower.len() - 1].trim();
        if limit.is_empty() {
            return None;
        }
        if limit.bytes().any(|b| !b.is_ascii_digit()) {
            return None;
        }
        return Some(limit.to_string());
    }
    None
}

/// Mirrors `isBooleanColumnType`.
fn is_boolean_column_type(value: &str) -> bool {
    value.eq_ignore_ascii_case("bool") || value.eq_ignore_ascii_case("boolean")
}

/// Mirrors `translateTableAttributes`: returns (clean rest SQL, DISTSTYLE,
/// DISTKEY, SORTKEYs, BACKUP).
fn translate_table_attributes(value: &str) -> (String, String, String, Vec<String>, String) {
    let tokens: Vec<&str> = value.split_whitespace().collect();
    let mut clean_tokens: Vec<&str> = Vec::with_capacity(tokens.len());
    let mut dist_style = String::new();
    let mut dist_key = String::new();
    let mut sort_keys: Vec<String> = Vec::new();
    let mut backup = String::new();
    let mut i = 0usize;
    while i < tokens.len() {
        let token = tokens[i].to_lowercase();
        if token == "diststyle" && i + 1 < tokens.len() {
            dist_style = clean_identifier(tokens[i + 1]).to_lowercase();
            i += 1;
        } else if token.starts_with("distkey") {
            let key = parse_parenthesized_identifier(tokens[i], "distkey");
            if !key.is_empty() {
                dist_key = key;
            } else if i + 1 < tokens.len() {
                dist_key = parse_parenthesized_identifier(tokens[i + 1], "");
                i += 1;
            }
        } else if token.starts_with("sortkey") {
            let keys = parse_parenthesized_identifier_list(tokens[i], "sortkey");
            if !keys.is_empty() {
                sort_keys = keys;
            } else if i + 1 < tokens.len() {
                sort_keys = parse_parenthesized_identifier_list(tokens[i + 1], "");
                i += 1;
            }
        } else if token == "backup" && i + 1 < tokens.len() {
            backup = clean_identifier(tokens[i + 1]).to_lowercase();
            i += 1;
        } else {
            clean_tokens.push(tokens[i]);
        }
        i += 1;
    }
    let clean_rest = if clean_tokens.is_empty() {
        String::new()
    } else {
        format!(" {}", clean_tokens.join(" "))
    };
    (clean_rest, dist_style, dist_key, sort_keys, backup)
}

/// Mirrors `hasIdentityToken`.
fn has_identity_token(tokens: &[&str]) -> bool {
    tokens.iter().any(|token| {
        let lower = token.to_lowercase();
        lower == "identity" || lower.starts_with("identity(")
    })
}

/// Mirrors `matchingParen`.
fn matching_paren(value: &str, open: usize) -> Option<usize> {
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
        if !in_string {
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
        }
        i += 1;
    }
    None
}

/// Mirrors `splitCommaSeparated`.
fn split_comma_separated(value: &str) -> Vec<String> {
    let bytes = value.as_bytes();
    let mut parts: Vec<String> = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' {
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                current.push(ch);
                current.push(bytes[i + 1]);
                i += 2;
                continue;
            }
            in_string = !in_string;
        }
        if !in_string {
            match ch {
                b'(' => depth += 1,
                b')' => depth -= 1,
                b',' if depth == 0 => {
                    let part = String::from_utf8(std::mem::take(&mut current))
                        .expect("rewrites preserve UTF-8");
                    parts.push(part.trim().to_string());
                    i += 1;
                    continue;
                }
                _ => {}
            }
        }
        current.push(ch);
        i += 1;
    }
    let last = String::from_utf8(current).expect("rewrites preserve UTF-8");
    let last = last.trim();
    if !last.is_empty() {
        parts.push(last.to_string());
    }
    parts
}

/// Mirrors `findTopLevelKeywordSequence`.
fn find_top_level_keyword_sequence(
    value: &str,
    keywords: &[&str],
    start: usize,
) -> Option<(usize, usize)> {
    let bytes = value.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut in_quoted_identifier = false;
    let mut i = start;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' && !in_quoted_identifier {
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_string = !in_string;
            i += 1;
            continue;
        }
        if ch == b'"' && !in_string {
            if in_quoted_identifier && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            in_quoted_identifier = !in_quoted_identifier;
            i += 1;
            continue;
        }
        if in_string || in_quoted_identifier {
            i += 1;
            continue;
        }
        match ch {
            b'(' => {
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                if depth > 0 {
                    depth -= 1;
                }
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth != 0 {
            i += 1;
            continue;
        }
        if let Some(end) = match_keyword_sequence(value, i, keywords) {
            return Some((i, end));
        }
        i += 1;
    }
    None
}

/// Mirrors `findTopLevelKeyword`.
fn find_top_level_keyword(value: &str, keyword: &str, start: usize) -> Option<usize> {
    let bytes = value.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut in_quoted_identifier = false;
    let mut i = start;
    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'\'' && !in_quoted_identifier {
            if in_string && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
                continue;
            }
            in_string = !in_string;
            i += 1;
            continue;
        }
        if ch == b'"' && !in_string {
            if in_quoted_identifier && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            in_quoted_identifier = !in_quoted_identifier;
            i += 1;
            continue;
        }
        if in_string || in_quoted_identifier {
            i += 1;
            continue;
        }
        match ch {
            b'(' => {
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                if depth > 0 {
                    depth -= 1;
                }
                i += 1;
                continue;
            }
            _ => {}
        }
        if depth != 0 || i + keyword_bytes.len() > bytes.len() {
            i += 1;
            continue;
        }
        if !bytes[i..i + keyword_bytes.len()].eq_ignore_ascii_case(keyword_bytes) {
            i += 1;
            continue;
        }
        let before_ok = i == 0 || !is_identifier_part(bytes[i - 1]);
        let after_ok = i + keyword_bytes.len() == bytes.len()
            || !is_identifier_part(bytes[i + keyword_bytes.len()]);
        if before_ok && after_ok {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Mirrors `firstSQLToken`.
fn first_sql_token(value: &str) -> &str {
    let value = value.trim();
    if value.is_empty() {
        return value;
    }
    let bytes = value.as_bytes();
    let mut in_quoted_identifier = false;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            if in_quoted_identifier && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
                continue;
            }
            in_quoted_identifier = !in_quoted_identifier;
            i += 1;
            continue;
        }
        if in_quoted_identifier {
            i += 1;
            continue;
        }
        if matches!(bytes[i], b' ' | b'\t' | b'\n' | b'\r') {
            return &value[..i];
        }
        i += 1;
    }
    value
}

/// Mirrors `removeKeywordSequence` (None = sequence absent / unchanged).
fn remove_keyword_sequence(value: &str, sequence: &[&str]) -> Option<String> {
    let tokens: Vec<&str> = value.split_whitespace().collect();
    let mut i = 0usize;
    while i + sequence.len() <= tokens.len() {
        let matched = sequence
            .iter()
            .enumerate()
            .all(|(j, keyword)| tokens[i + j].eq_ignore_ascii_case(keyword));
        if !matched {
            i += 1;
            continue;
        }
        let mut cleaned: Vec<&str> = tokens[..i].to_vec();
        cleaned.extend_from_slice(&tokens[i + sequence.len()..]);
        return Some(cleaned.join(" "));
    }
    None
}

/// Mirrors `parseQualifiedName`.
fn parse_qualified_name(value: &str) -> (String, String) {
    let mut value = value.trim();
    if let Some(first) = value.split_whitespace().next() {
        value = first;
    }
    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() == 1 {
        return ("public".to_string(), clean_identifier(parts[0]).to_string());
    }
    (
        clean_identifier(parts[parts.len() - 2]).to_string(),
        clean_identifier(parts[parts.len() - 1]).to_string(),
    )
}

/// Mirrors `parseParenthesizedIdentifier`.
fn parse_parenthesized_identifier(value: &str, prefix: &str) -> String {
    parse_parenthesized_identifier_list(value, prefix)
        .into_iter()
        .next()
        .unwrap_or_default()
}

/// Mirrors `parseParenthesizedIdentifierList`.
fn parse_parenthesized_identifier_list(value: &str, prefix: &str) -> Vec<String> {
    let mut value = value.trim();
    if !prefix.is_empty() {
        if !has_prefix_fold(value, prefix) {
            return Vec::new();
        }
        value = value[prefix.len()..].trim();
    }
    let bytes = value.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'(' || bytes[bytes.len() - 1] != b')' {
        return Vec::new();
    }
    split_comma_separated(&value[1..value.len() - 1])
        .iter()
        .map(|part| clean_identifier(part).to_string())
        .filter(|cleaned| !cleaned.is_empty())
        .collect()
}

/// Mirrors `cleanIdentifier`.
fn clean_identifier(value: &str) -> &str {
    value.trim().trim_matches('"')
}

/// Mirrors `containsIdentifier`.
fn contains_identifier(values: &[String], value: &str) -> bool {
    values.iter().any(|item| item.eq_ignore_ascii_case(value))
}

/// Mirrors `matchKeywordSequence`.
fn match_keyword_sequence(sql: &str, index: usize, keywords: &[&str]) -> Option<usize> {
    let bytes = sql.as_bytes();
    if index > 0 && is_identifier_part(bytes[index - 1]) {
        return None;
    }
    let mut next = index;
    for (keyword_index, keyword) in keywords.iter().enumerate() {
        if keyword_index > 0 {
            next = skip_spaces(sql, next);
            if next >= bytes.len() {
                return None;
            }
        }
        let keyword_bytes = keyword.as_bytes();
        if bytes.len() < next + keyword_bytes.len()
            || !bytes[next..next + keyword_bytes.len()].eq_ignore_ascii_case(keyword_bytes)
        {
            return None;
        }
        let after_keyword = next + keyword_bytes.len();
        if after_keyword < bytes.len() && is_identifier_part(bytes[after_keyword]) {
            return None;
        }
        next = after_keyword;
    }
    Some(next)
}

/// Mirrors `trimRightSpaces`.
fn trim_right_spaces(value: &mut Vec<u8>) {
    while matches!(value.last(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
        value.pop();
    }
}

/// Mirrors `copySpaces`.
fn copy_spaces(out: &mut Vec<u8>, value: &str, index: usize) -> usize {
    let bytes = value.as_bytes();
    let mut index = index;
    while index < bytes.len() && matches!(bytes[index], b' ' | b'\t' | b'\n' | b'\r') {
        out.push(bytes[index]);
        index += 1;
    }
    index
}

/// Mirrors `copyQuotedString` (scan-only variant; same termination rules).
fn skip_quoted_string(value: &str, start: usize) -> usize {
    let bytes = value.as_bytes();
    let mut i = start;
    while i < bytes.len() {
        if bytes[i] != b'\'' {
            i += 1;
            continue;
        }
        if i > start && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
            i += 2;
            continue;
        }
        if i > start {
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

/// Mirrors `copyQuotedString`.
fn copy_quoted_string(out: &mut Vec<u8>, value: &str, start: usize) -> usize {
    let next = skip_quoted_string(value, start);
    out.extend_from_slice(&value.as_bytes()[start..next]);
    next
}

/// Mirrors `copyQuotedIdentifier` (scan-only variant).
fn skip_quoted_identifier(value: &str, start: usize) -> usize {
    let bytes = value.as_bytes();
    let mut i = start;
    while i < bytes.len() {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        if i > start && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
            i += 2;
            continue;
        }
        if i > start {
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

/// Mirrors `copyQuotedIdentifier`.
fn copy_quoted_identifier(out: &mut Vec<u8>, value: &str, start: usize) -> usize {
    let next = skip_quoted_identifier(value, start);
    out.extend_from_slice(&value.as_bytes()[start..next]);
    next
}

/// Mirrors `skipSpaces`.
fn skip_spaces(value: &str, index: usize) -> usize {
    let bytes = value.as_bytes();
    let mut index = index;
    while index < bytes.len() && matches!(bytes[index], b' ' | b'\t' | b'\n' | b'\r') {
        index += 1;
    }
    index
}

/// Mirrors `isIdentifierStart`.
fn is_identifier_start(ch: u8) -> bool {
    ch.is_ascii_alphabetic() || ch == b'_'
}

/// Mirrors `isIdentifierPart`.
fn is_identifier_part(ch: u8) -> bool {
    is_identifier_start(ch) || ch.is_ascii_digit() || ch == b'$'
}

/// ASCII case-insensitive prefix check (legacy: `strings.HasPrefix(strings.ToLower(s), prefix)`).
fn has_prefix_fold(value: &str, prefix: &str) -> bool {
    let bytes = value.as_bytes();
    let prefix_bytes = prefix.as_bytes();
    bytes.len() >= prefix_bytes.len()
        && bytes[..prefix_bytes.len()].eq_ignore_ascii_case(prefix_bytes)
}
