//! Execution of persisted BigQuery SQL routines:
//! - `SCALAR_FUNCTION` (SQL UDFs) in SELECT / WHERE / nested UDF calls
//! - `TABLE_VALUED_FUNCTION` (SQL TVFs) in FROM
//! - `PROCEDURE` (SQL stored procedures) via `CALL` for supported SELECT bodies
//!
//! Non-SQL routines (such as JAVASCRIPT) and procedural scripting statements
//! fail closed with BigQuery-style `invalidQuery` errors.

use std::collections::HashMap;

use crate::model::RoutineResource;
use crate::server::Server;
use crate::sql_eval::QueryExecutionResult;
use crate::sql_expr::{
    eval_expr, parse_expr, standard_sql_to_legacy_type, validate_and_infer_expr_type, SqlExpr,
    SqlValue, MAX_ROUTINE_DEPTH,
};
use crate::validation::validate_resource_id;

pub fn resolve_routine_parts(
    name_parts: &[String],
    default_project_id: &str,
) -> Result<(String, String, String), String> {
    let (project, dataset, routine) = match name_parts {
        [dataset, routine] => {
            let project = if default_project_id.is_empty() {
                "devcloud"
            } else {
                default_project_id
            };
            (project.to_string(), dataset.clone(), routine.clone())
        }
        [project, dataset, routine] => (project.clone(), dataset.clone(), routine.clone()),
        _ => {
            return Err(
                "routine reference must be dataset.routine or project.dataset.routine".to_string(),
            )
        }
    };
    validate_resource_id(&project, "project")?;
    validate_resource_id(&dataset, "dataset")?;
    validate_resource_id(&routine, "routine")?;
    Ok((project, dataset, routine))
}

fn ensure_sql_language(routine: &RoutineResource) -> Result<(), String> {
    let lang = routine.language.trim();
    if lang.is_empty() || lang.eq_ignore_ascii_case("SQL") {
        Ok(())
    } else {
        Err("only SQL routines are supported for execution".to_string())
    }
}

impl Server {
    fn load_executable_routine(
        &self,
        name_parts: &[String],
        default_project_id: &str,
        expected_type: &str,
        depth: i32,
    ) -> Result<RoutineResource, String> {
        if depth >= MAX_ROUTINE_DEPTH {
            return Err("routine call depth exceeded".to_string());
        }
        let (project_id, dataset_id, routine_id) =
            resolve_routine_parts(name_parts, default_project_id)?;
        let routine = self
            .read_routine(&project_id, &dataset_id, &routine_id)
            .map_err(|err| err.to_string())?;
        let Some(routine) = routine else {
            return Err(format!(
                "not found: routine {project_id}:{dataset_id}.{routine_id}"
            ));
        };
        if !routine.routine_type.eq_ignore_ascii_case(expected_type) {
            return Err(format!(
                "routine {project_id}:{dataset_id}.{routine_id} is not a {expected_type}"
            ));
        }
        ensure_sql_language(&routine)?;
        if routine.definition_body.trim().is_empty() {
            return Err("routine definitionBody is required".to_string());
        }
        Ok(routine)
    }

    pub fn eval_scalar_routine(
        &self,
        name_parts: &[String],
        default_project_id: &str,
        args: &[SqlValue],
        depth: i32,
    ) -> Result<SqlValue, String> {
        let routine =
            self.load_executable_routine(name_parts, default_project_id, "SCALAR_FUNCTION", depth)?;
        if args.len() != routine.arguments.len() {
            return Err(format!(
                "routine {} expects {} argument(s), got {}",
                routine.routine_reference.routine_id,
                routine.arguments.len(),
                args.len()
            ));
        }
        let mut arg_map: HashMap<String, SqlValue> = HashMap::with_capacity(args.len());
        for (decl, val) in routine.arguments.iter().zip(args.iter()) {
            let coerced = match decl.data_type.as_ref().map(|dt| dt.type_kind.as_str()) {
                Some(kind) if !kind.trim().is_empty() => val.coerce_to_type(kind)?,
                _ => val.clone(),
            };
            arg_map.insert(decl.name.to_ascii_lowercase(), coerced);
        }
        let body_expr = parse_expr(&routine.definition_body)?;
        let project = &routine.routine_reference.project_id;
        let result = eval_expr(
            self,
            project,
            &body_expr,
            &|name| arg_map.get(&name.to_ascii_lowercase()).cloned(),
            depth + 1,
        )?;
        match routine.return_type.as_ref().map(|dt| dt.type_kind.as_str()) {
            Some(kind) if !kind.trim().is_empty() => result.coerce_to_type(kind),
            _ => Ok(result),
        }
    }

    pub fn validate_scalar_routine(
        &self,
        name_parts: &[String],
        default_project_id: &str,
        arg_count: usize,
        depth: i32,
    ) -> Result<String, String> {
        let routine =
            self.load_executable_routine(name_parts, default_project_id, "SCALAR_FUNCTION", depth)?;
        if arg_count != routine.arguments.len() {
            return Err(format!(
                "routine {} expects {} argument(s), got {}",
                routine.routine_reference.routine_id,
                routine.arguments.len(),
                arg_count
            ));
        }
        let mut arg_types: HashMap<String, String> =
            HashMap::with_capacity(routine.arguments.len());
        for decl in &routine.arguments {
            let legacy_type = decl
                .data_type
                .as_ref()
                .map(|dt| standard_sql_to_legacy_type(&dt.type_kind))
                .unwrap_or_else(|| "STRING".to_string());
            arg_types.insert(decl.name.to_ascii_lowercase(), legacy_type);
        }
        let body_expr = parse_expr(&routine.definition_body)?;
        let inferred = validate_and_infer_expr_type(
            self,
            &routine.routine_reference.project_id,
            &body_expr,
            &|name| arg_types.get(&name.to_ascii_lowercase()).cloned(),
            depth + 1,
        )?;
        match routine.return_type.as_ref().map(|dt| dt.type_kind.as_str()) {
            Some(kind) if !kind.trim().is_empty() => Ok(standard_sql_to_legacy_type(kind)),
            _ => Ok(inferred),
        }
    }

    pub(crate) fn execute_tvf(
        &self,
        name_parts: &[String],
        raw_args: &[SqlExpr],
        default_project_id: &str,
        dry_run: bool,
        depth: i32,
    ) -> Result<QueryExecutionResult, String> {
        let routine = self.load_executable_routine(
            name_parts,
            default_project_id,
            "TABLE_VALUED_FUNCTION",
            depth,
        )?;
        if raw_args.len() != routine.arguments.len() {
            return Err(format!(
                "routine {} expects {} argument(s), got {}",
                routine.routine_reference.routine_id,
                routine.arguments.len(),
                raw_args.len()
            ));
        }
        let bindings =
            self.evaluate_routine_literal_bindings(&routine, raw_args, default_project_id, depth)?;
        let bound_sql = substitute_sql_identifiers(&routine.definition_body, &bindings);
        self.execute_query_for_view(
            &routine.routine_reference.project_id,
            &bound_sql,
            dry_run,
            depth + 1,
        )
    }

    pub(crate) fn execute_call_procedure(
        &self,
        raw_query: &str,
        default_project_id: &str,
        dry_run: bool,
        depth: i32,
    ) -> Result<QueryExecutionResult, String> {
        let trimmed = raw_query.strip_suffix(';').unwrap_or(raw_query).trim();
        let after_call = trimmed
            .get("CALL ".len()..)
            .ok_or_else(|| "invalid CALL statement".to_string())?
            .trim();
        let expr = parse_expr(after_call)?;
        let SqlExpr::FunctionCall { name_parts, args } = expr else {
            return Err("CALL requires a routine invocation".to_string());
        };
        let routine =
            self.load_executable_routine(&name_parts, default_project_id, "PROCEDURE", depth)?;
        if args.len() != routine.arguments.len() {
            return Err(format!(
                "routine {} expects {} argument(s), got {}",
                routine.routine_reference.routine_id,
                routine.arguments.len(),
                args.len()
            ));
        }
        let bindings =
            self.evaluate_routine_literal_bindings(&routine, &args, default_project_id, depth)?;
        let inner = unwrap_begin_end(&routine.definition_body)?;
        let bound_sql = substitute_sql_identifiers(inner, &bindings);
        if !bound_sql.trim().to_ascii_uppercase().starts_with("SELECT ") {
            return Err("only SQL procedures with a single SELECT body are supported".to_string());
        }
        self.execute_query_for_view(
            &routine.routine_reference.project_id,
            &bound_sql,
            dry_run,
            depth + 1,
        )
    }

    fn evaluate_routine_literal_bindings(
        &self,
        routine: &RoutineResource,
        raw_args: &[SqlExpr],
        default_project_id: &str,
        depth: i32,
    ) -> Result<HashMap<String, String>, String> {
        let mut bindings = HashMap::with_capacity(raw_args.len());
        for (decl, arg_expr) in routine.arguments.iter().zip(raw_args.iter()) {
            let val = eval_expr(self, default_project_id, arg_expr, &|_| None, depth + 1)?;
            let coerced = match decl.data_type.as_ref().map(|dt| dt.type_kind.as_str()) {
                Some(kind) if !kind.trim().is_empty() => val.coerce_to_type(kind)?,
                _ => val,
            };
            bindings.insert(decl.name.to_ascii_lowercase(), coerced.to_sql_literal());
        }
        Ok(bindings)
    }
}

fn unwrap_begin_end(body: &str) -> Result<&str, String> {
    let mut inner = body.trim();
    if let Some(stripped) = inner.strip_suffix(';') {
        inner = stripped.trim();
    }
    let upper = inner.to_ascii_uppercase();
    if upper.starts_with("BEGIN") && upper.ends_with("END") {
        let after_begin = inner["BEGIN".len()..inner.len() - "END".len()].trim();
        let after_begin = after_begin.strip_suffix(';').unwrap_or(after_begin).trim();
        if after_begin.is_empty() {
            return Err("procedure body is empty".to_string());
        }
        // Reject multi-statement scripts cleanly.
        if has_top_level_semicolon(after_begin) {
            return Err("multi-statement procedures are not supported".to_string());
        }
        return Ok(after_begin);
    }
    if has_top_level_semicolon(inner) {
        return Err("multi-statement procedures are not supported".to_string());
    }
    Ok(inner)
}

fn has_top_level_semicolon(sql: &str) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_single {
            if b == b'\\' {
                i += 2;
                continue;
            }
            if b == b'\'' {
                in_single = false;
            }
        } else if in_double {
            if b == b'\\' {
                i += 2;
                continue;
            }
            if b == b'"' {
                in_double = false;
            }
        } else if in_backtick {
            if b == b'`' {
                in_backtick = false;
            }
        } else {
            match b {
                b'\'' => in_single = true,
                b'"' => in_double = true,
                b'`' => in_backtick = true,
                b';' => return true,
                _ => {}
            }
        }
        i += 1;
    }
    false
}

/// Replaces bare SQL identifiers matching routine argument names (outside
/// string literals and backticks, and not preceded by `.`) with their bound
/// SQL literal text.
fn substitute_sql_identifiers(sql: &str, bindings: &HashMap<String, String>) -> String {
    if bindings.is_empty() {
        return sql.to_string();
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_single {
            if b == b'\\' && i + 1 < bytes.len() {
                out.push(b as char);
                out.push(bytes[i + 1] as char);
                i += 2;
                continue;
            }
            if b == b'\'' {
                in_single = false;
            }
            let ch = sql[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        if in_double {
            if b == b'\\' && i + 1 < bytes.len() {
                out.push(b as char);
                out.push(bytes[i + 1] as char);
                i += 2;
                continue;
            }
            if b == b'"' {
                in_double = false;
            }
            let ch = sql[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        if in_backtick {
            if b == b'`' {
                in_backtick = false;
            }
            let ch = sql[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        match b {
            b'\'' => {
                in_single = true;
                out.push('\'');
                i += 1;
            }
            b'"' => {
                in_double = true;
                out.push('"');
                i += 1;
            }
            b'`' => {
                in_backtick = true;
                out.push('`');
                i += 1;
            }
            _ if b.is_ascii_alphabetic() || b == b'_' => {
                let preceded_by_dot = i > 0 && bytes[i - 1] == b'.';
                let start = i;
                i += 1;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = &sql[start..i];
                let followed_by_dot = i < bytes.len() && bytes[i] == b'.';
                if !preceded_by_dot && !followed_by_dot {
                    if let Some(replacement) = bindings.get(&word.to_ascii_lowercase()) {
                        out.push_str(replacement);
                        continue;
                    }
                }
                out.push_str(word);
            }
            _ => {
                let ch = sql[i..].chars().next().unwrap();
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    out
}
