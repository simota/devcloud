//! Local IAM resource-policy and condition evaluator for DynamoDB.
//!
//! Evaluates IAM policy JSON documents attached to a table via
//! `PutResourcePolicy`. When no resource policy is attached (or its
//! `Statement` list is empty), all requests remain allowed so relaxed local
//! workflows and existing tests continue working unchanged.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::errors::ApiError;
use crate::model::{Item, TableDescription};
use crate::number::{compare_number_strings, is_valid_number};

const ACCOUNT_ID: &str = "000000000000";

/// Caller identity resolved for a request (from SigV4 Credential header when
/// present, or the server's configured default access key).
#[derive(Debug, Clone)]
pub struct CallerIdentity {
    pub access_key_id: String,
    pub account_id: String,
    pub principal_arn: String,
    pub region: String,
}

impl CallerIdentity {
    pub fn from_access_key(access_key: &str, region: &str) -> Self {
        let key = if access_key.is_empty() {
            "dev"
        } else {
            access_key
        };
        CallerIdentity {
            access_key_id: key.to_string(),
            account_id: ACCOUNT_ID.to_string(),
            principal_arn: format!("arn:aws:iam::{ACCOUNT_ID}:user/{key}"),
            region: region.to_string(),
        }
    }
}

/// Request-scoped IAM evaluation context for a DynamoDB operation against one
/// table.
#[derive(Debug, Clone)]
pub struct EvalContext {
    pub action: String,
    pub resource_arn: String,
    pub index_arn: Option<String>,
    pub caller: CallerIdentity,
    /// Multi-valued condition keys (lowercase key -> values).
    pub keys: BTreeMap<String, Vec<String>>,
    /// Keys that are intentionally unbounded (e.g. `dynamodb:leadingkeys` on a
    /// `Scan`, or `dynamodb:attributes` on an unprojected read). Any non-IfExists
    /// positive/ForAllValues check on an unbounded key evaluates to false.
    pub unbounded_keys: BTreeSet<String>,
    /// True for policy management actions (`Put/Get/DeleteResourcePolicy`),
    /// which are only blocked by an explicit `Deny`.
    pub is_policy_management: bool,
}

impl EvalContext {
    pub fn new(action: &str, resource_arn: &str, caller: CallerIdentity) -> Self {
        let mut keys = BTreeMap::new();
        keys.insert(
            "aws:requestedregion".to_string(),
            vec![caller.region.clone()],
        );
        keys.insert(
            "aws:principalarn".to_string(),
            vec![caller.principal_arn.clone()],
        );
        keys.insert(
            "aws:principalaccount".to_string(),
            vec![caller.account_id.clone()],
        );
        keys.insert(
            "aws:username".to_string(),
            vec![caller.access_key_id.clone()],
        );
        keys.insert("aws:userid".to_string(), vec![caller.access_key_id.clone()]);
        EvalContext {
            action: action.to_string(),
            resource_arn: resource_arn.to_string(),
            index_arn: None,
            caller,
            keys,
            unbounded_keys: BTreeSet::new(),
            is_policy_management: false,
        }
    }

    pub fn with_resource_tags(mut self, tags: &BTreeMap<String, String>) -> Self {
        for (k, v) in tags {
            self.keys.insert(
                format!("aws:resourcetag/{}", k.to_ascii_lowercase()),
                vec![v.clone()],
            );
        }
        self
    }

    pub fn with_request_tags(mut self, tags: &[(String, String)]) -> Self {
        let mut tag_keys = Vec::with_capacity(tags.len());
        for (k, v) in tags {
            tag_keys.push(k.clone());
            self.keys.insert(
                format!("aws:requesttag/{}", k.to_ascii_lowercase()),
                vec![v.clone()],
            );
        }
        if !tag_keys.is_empty() {
            self.keys.insert("aws:tagkeys".to_string(), tag_keys);
        }
        self
    }

    pub fn with_tag_keys(mut self, tag_keys: Vec<String>) -> Self {
        if !tag_keys.is_empty() {
            self.keys.insert("aws:tagkeys".to_string(), tag_keys);
        }
        self
    }

    pub fn set_key(&mut self, key: &str, values: Vec<String>) {
        self.keys.insert(key.to_ascii_lowercase(), values);
    }

    pub fn mark_unbounded(&mut self, key: &str) {
        self.unbounded_keys.insert(key.to_ascii_lowercase());
    }
}

/// Evaluates a table's attached resource policy JSON string against `ctx`.
/// Returns `Ok(())` when allowed or `Err(ApiError::access_denied(..))` when
/// denied.
pub fn evaluate_resource_policy(policy_json: &str, ctx: &EvalContext) -> Result<(), ApiError> {
    if policy_json.trim().is_empty() {
        return Ok(());
    }
    let doc: Value = serde_json::from_str(policy_json)
        .map_err(|_| ApiError::access_denied("invalid resource policy document"))?;
    let Some(stmt_val) = doc.get("Statement") else {
        return Ok(());
    };
    let statements: Vec<&Value> = match stmt_val {
        Value::Array(arr) => arr.iter().collect(),
        Value::Object(_) => vec![stmt_val],
        _ => return Ok(()),
    };
    if statements.is_empty() {
        return Ok(());
    }

    let mut has_allow_statement = false;
    let mut explicitly_allowed = false;

    for stmt in statements {
        let Some(obj) = stmt.as_object() else {
            continue;
        };
        let effect = obj.get("Effect").and_then(Value::as_str).unwrap_or("");
        let is_allow = effect.eq_ignore_ascii_case("Allow");
        let is_deny = effect.eq_ignore_ascii_case("Deny");
        if !is_allow && !is_deny {
            continue;
        }
        if is_allow {
            has_allow_statement = true;
        }
        if !matches_principal(obj, &ctx.caller) {
            continue;
        }
        if !matches_action(obj, &ctx.action) {
            continue;
        }
        if !matches_resource(obj, &ctx.resource_arn, ctx.index_arn.as_deref()) {
            continue;
        }
        if !matches_condition_block(obj.get("Condition"), ctx) {
            continue;
        }
        if is_deny {
            return Err(ApiError::access_denied(format!(
                "explicit deny in resource policy for {}",
                ctx.action
            )));
        }
        if is_allow {
            explicitly_allowed = true;
        }
    }

    if ctx.is_policy_management {
        return Ok(());
    }
    if has_allow_statement && !explicitly_allowed {
        return Err(ApiError::access_denied(format!(
            "not authorized to perform {} on {}",
            ctx.action, ctx.resource_arn
        )));
    }
    Ok(())
}

// --- Statement matching -----------------------------------------------------

fn matches_principal(stmt: &serde_json::Map<String, Value>, caller: &CallerIdentity) -> bool {
    if let Some(not_principal) = stmt.get("NotPrincipal") {
        return !principal_value_matches(not_principal, caller);
    }
    match stmt.get("Principal") {
        None => true,
        Some(p) => principal_value_matches(p, caller),
    }
}

fn principal_value_matches(val: &Value, caller: &CallerIdentity) -> bool {
    match val {
        Value::String(s) => principal_entry_matches(s, caller),
        Value::Array(arr) => arr.iter().any(|v| principal_value_matches(v, caller)),
        Value::Object(map) => {
            if let Some(aws) = map.get("AWS") {
                return principal_value_matches(aws, caller);
            }
            false
        }
        _ => false,
    }
}

fn principal_entry_matches(entry: &str, caller: &CallerIdentity) -> bool {
    if entry == "*" || entry == caller.account_id || entry == caller.principal_arn {
        return true;
    }
    if entry == format!("arn:aws:iam::{}:root", caller.account_id) {
        return true;
    }
    wildcard_match(entry, &caller.principal_arn, false)
}

fn matches_action(stmt: &serde_json::Map<String, Value>, action: &str) -> bool {
    if let Some(not_action) = stmt.get("NotAction") {
        let patterns = value_to_strings(not_action);
        return !patterns.iter().any(|p| wildcard_match(p, action, true));
    }
    let Some(action_val) = stmt.get("Action") else {
        return false;
    };
    let patterns = value_to_strings(action_val);
    patterns.iter().any(|p| wildcard_match(p, action, true))
}

fn matches_resource(
    stmt: &serde_json::Map<String, Value>,
    resource_arn: &str,
    index_arn: Option<&str>,
) -> bool {
    let targets: Vec<&str> = match index_arn {
        Some(idx) => vec![resource_arn, idx],
        None => vec![resource_arn],
    };
    if let Some(not_resource) = stmt.get("NotResource") {
        let patterns = value_to_strings(not_resource);
        return !patterns
            .iter()
            .any(|p| targets.iter().any(|t| wildcard_match(p, t, false)));
    }
    let Some(res_val) = stmt.get("Resource") else {
        return false;
    };
    let patterns = value_to_strings(res_val);
    patterns
        .iter()
        .any(|p| targets.iter().any(|t| wildcard_match(p, t, false)))
}

// --- Condition evaluation ---------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetQuantifier {
    None,
    ForAllValues,
    ForAnyValue,
}

fn matches_condition_block(condition: Option<&Value>, ctx: &EvalContext) -> bool {
    let Some(cond_val) = condition else {
        return true;
    };
    let Some(operators) = cond_val.as_object() else {
        return false;
    };
    // Every operator entry in Condition is ANDed.
    for (raw_op, key_map_val) in operators {
        let Some(key_map) = key_map_val.as_object() else {
            return false;
        };
        for (raw_key, expected_val) in key_map {
            if !eval_condition_entry(raw_op, raw_key, expected_val, ctx) {
                return false;
            }
        }
    }
    true
}

fn eval_condition_entry(
    raw_op: &str,
    raw_key: &str,
    expected_val: &Value,
    ctx: &EvalContext,
) -> bool {
    let (quantifier, op_after_quant) = if let Some(rest) = raw_op.strip_prefix("ForAllValues:") {
        (SetQuantifier::ForAllValues, rest)
    } else if let Some(rest) = raw_op.strip_prefix("ForAnyValue:") {
        (SetQuantifier::ForAnyValue, rest)
    } else {
        (SetQuantifier::None, raw_op)
    };

    let (base_op, if_exists) = if op_after_quant != "Null" && op_after_quant.ends_with("IfExists") {
        (
            &op_after_quant[..op_after_quant.len() - "IfExists".len()],
            true,
        )
    } else {
        (op_after_quant, false)
    };

    let key = raw_key.to_ascii_lowercase();
    let is_unbounded = ctx.unbounded_keys.contains(&key);
    let actual_opt = ctx.keys.get(&key);

    if base_op == "Null" {
        let expect_null = value_to_bool(expected_val);
        let is_null = actual_opt.map(|v| v.is_empty()).unwrap_or(true) && !is_unbounded;
        return match expect_null {
            Some(want) => is_null == want,
            None => false,
        };
    }

    if is_unbounded {
        return if_exists
            && matches!(
                base_op,
                "StringNotEquals"
                    | "StringNotEqualsIgnoreCase"
                    | "StringNotLike"
                    | "NumericNotEquals"
            );
    }

    let Some(actual_values) = actual_opt else {
        return if_exists || quantifier == SetQuantifier::ForAllValues;
    };
    if actual_values.is_empty() {
        return if_exists || quantifier == SetQuantifier::ForAllValues;
    }

    let expected_values: Vec<String> = value_to_strings(expected_val)
        .into_iter()
        .map(|v| expand_policy_vars(&v, ctx))
        .collect();
    if expected_values.is_empty() {
        return false;
    }

    let is_negated = matches!(
        base_op,
        "StringNotEquals" | "StringNotEqualsIgnoreCase" | "StringNotLike" | "NumericNotEquals"
    );

    let single_matches_set = |actual: &str| -> bool {
        if is_negated {
            // For negated operators (e.g. StringNotEquals: [a, b]), actual must
            // satisfy the negated comparison against EVERY listed value.
            expected_values
                .iter()
                .all(|exp| compare_single(base_op, actual, exp))
        } else {
            expected_values
                .iter()
                .any(|exp| compare_single(base_op, actual, exp))
        }
    };

    match quantifier {
        SetQuantifier::ForAllValues => actual_values.iter().all(|a| single_matches_set(a)),
        SetQuantifier::ForAnyValue => actual_values.iter().any(|a| single_matches_set(a)),
        SetQuantifier::None => actual_values.iter().all(|a| single_matches_set(a)),
    }
}

fn compare_single(op: &str, actual: &str, expected: &str) -> bool {
    match op {
        "StringEquals" => actual == expected,
        "StringNotEquals" => actual != expected,
        "StringEqualsIgnoreCase" => actual.eq_ignore_ascii_case(expected),
        "StringNotEqualsIgnoreCase" => !actual.eq_ignore_ascii_case(expected),
        "StringLike" => wildcard_match(expected, actual, false),
        "StringNotLike" => !wildcard_match(expected, actual, false),
        "Bool" => actual.eq_ignore_ascii_case(expected),
        "NumericEquals" => numeric_cmp(actual, expected) == Some(std::cmp::Ordering::Equal),
        "NumericNotEquals" => numeric_cmp(actual, expected) != Some(std::cmp::Ordering::Equal),
        "NumericLessThan" => numeric_cmp(actual, expected) == Some(std::cmp::Ordering::Less),
        "NumericLessThanEquals" => matches!(
            numeric_cmp(actual, expected),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        "NumericGreaterThan" => numeric_cmp(actual, expected) == Some(std::cmp::Ordering::Greater),
        "NumericGreaterThanEquals" => matches!(
            numeric_cmp(actual, expected),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
        _ => false,
    }
}

fn numeric_cmp(a: &str, b: &str) -> Option<std::cmp::Ordering> {
    if is_valid_number(a) && is_valid_number(b) {
        Some(compare_number_strings(a, b))
    } else {
        None
    }
}

fn expand_policy_vars(input: &str, ctx: &EvalContext) -> String {
    let mut out = input.to_string();
    for (var, key) in [
        ("${aws:username}", "aws:username"),
        ("${aws:userid}", "aws:userid"),
        ("${aws:PrincipalArn}", "aws:principalarn"),
        ("${aws:PrincipalAccount}", "aws:principalaccount"),
        ("${aws:RequestedRegion}", "aws:requestedregion"),
    ] {
        if out.contains(var) {
            if let Some(val) = ctx.keys.get(key).and_then(|v| v.first()) {
                out = out.replace(var, val);
            }
        }
    }
    out
}

fn value_to_strings(val: &Value) -> Vec<String> {
    match val {
        Value::String(s) => vec![s.clone()],
        Value::Number(n) => vec![n.to_string()],
        Value::Bool(b) => vec![b.to_string()],
        Value::Array(arr) => arr.iter().flat_map(value_to_strings).collect(),
        _ => Vec::new(),
    }
}

fn value_to_bool(val: &Value) -> Option<bool> {
    match val {
        Value::Bool(b) => Some(*b),
        Value::String(s) if s.eq_ignore_ascii_case("true") => Some(true),
        Value::String(s) if s.eq_ignore_ascii_case("false") => Some(false),
        Value::Array(arr) if arr.len() == 1 => value_to_bool(&arr[0]),
        _ => None,
    }
}

/// Glob matcher supporting `*` and `?`.
pub fn wildcard_match(pattern: &str, text: &str, case_insensitive: bool) -> bool {
    let p: Vec<char> = if case_insensitive {
        pattern.to_ascii_lowercase().chars().collect()
    } else {
        pattern.chars().collect()
    };
    let t: Vec<char> = if case_insensitive {
        text.to_ascii_lowercase().chars().collect()
    } else {
        text.chars().collect()
    };
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star_pi = Some(pi);
            star_ti = ti;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

// --- DynamoDB request metadata extractors -----------------------------------

/// Extracts the scalar string representation of an AttributeValue (`S`/`N`/`B`)
/// for `dynamodb:LeadingKeys`.
pub fn scalar_key_string(value: &Value) -> Option<String> {
    let obj = value.as_object()?;
    for ty in ["S", "N", "B"] {
        if let Some(s) = obj.get(ty).and_then(Value::as_str) {
            return Some(s.to_string());
        }
    }
    None
}

/// Extracts the table's partition (HASH) key scalar value from an item or key.
pub fn leading_key_from_item(desc: &TableDescription, item: &Item) -> Option<String> {
    let hash_attr = crate::validation::table_hash_key(&desc.key_schema);
    if hash_attr.is_empty() {
        return None;
    }
    item.get(hash_attr).and_then(scalar_key_string)
}

/// Extracts the partition key equality value from a `Query`
/// `KeyConditionExpression`, if constrained by `hash_attr = :val`.
pub fn leading_key_from_key_condition(
    desc: &TableDescription,
    index_name: &str,
    key_condition: &str,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, Value>,
) -> Option<String> {
    let hash_attr = if index_name.is_empty() {
        crate::validation::table_hash_key(&desc.key_schema)
    } else if let Some(gsi) = desc
        .global_secondary_indexes
        .iter()
        .find(|i| i.index_name == index_name)
    {
        crate::validation::table_hash_key(&gsi.key_schema)
    } else if let Some(lsi) = desc
        .local_secondary_indexes
        .iter()
        .find(|i| i.index_name == index_name)
    {
        crate::validation::table_hash_key(&lsi.key_schema)
    } else {
        ""
    };
    if hash_attr.is_empty() {
        return None;
    }

    // Split top-level `AND` clauses case-insensitively and inspect `lhs = rhs`.
    for clause in split_top_level_and(key_condition) {
        let trimmed = clause.trim().trim_matches(|c| c == '(' || c == ')').trim();
        let Some((lhs_raw, rhs_raw)) = split_single_equals(trimmed) else {
            continue;
        };
        let lhs = resolve_attr_name(lhs_raw.trim(), names);
        let rhs = rhs_raw.trim();
        if lhs == hash_attr {
            if let Some(val) = values.get(rhs) {
                return scalar_key_string(val);
            }
        }
        let rhs_name = resolve_attr_name(rhs, names);
        if rhs_name == hash_attr {
            if let Some(val) = values.get(lhs_raw.trim()) {
                return scalar_key_string(val);
            }
        }
    }
    None
}

/// Resolves attribute names referenced in a `ProjectionExpression`.
pub fn projection_attributes(projection: &str, names: &BTreeMap<String, String>) -> Vec<String> {
    let projection = projection.trim();
    if projection.is_empty() {
        return Vec::new();
    }
    let mut set = BTreeSet::new();
    for token in projection.split(',') {
        let tok = token.trim();
        if tok.is_empty() {
            continue;
        }
        let root = tok.split(['.', '[']).next().unwrap_or(tok).trim();
        let resolved = resolve_attr_name(root, names);
        if !resolved.is_empty() {
            set.insert(resolved);
        }
    }
    set.into_iter().collect()
}

/// Extracts top-level attribute names touched by an `UpdateExpression`.
pub fn update_expression_attributes(
    update_expr: &str,
    names: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut set = BTreeSet::new();
    let trimmed = update_expr.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    // Extract tokens on the LHS of `=` in SET clauses and targets in REMOVE/ADD/DELETE.
    for token in trimmed.split(|c: char| {
        matches!(
            c,
            ' ' | '\t' | '\n' | '\r' | ',' | '(' | ')' | '+' | '-' | '='
        )
    }) {
        let tok = token.trim();
        if tok.is_empty() || tok.starts_with(':') {
            continue;
        }
        let upper = tok.to_ascii_uppercase();
        if matches!(
            upper.as_str(),
            "SET" | "REMOVE" | "ADD" | "DELETE" | "IF_NOT_EXISTS" | "LIST_APPEND"
        ) {
            continue;
        }
        let root = tok.split(['.', '[']).next().unwrap_or(tok).trim();
        if root.is_empty() || root.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let resolved = resolve_attr_name(root, names);
        if !resolved.is_empty() {
            set.insert(resolved);
        }
    }
    set.into_iter().collect()
}

fn resolve_attr_name(token: &str, names: &BTreeMap<String, String>) -> String {
    if token.starts_with('#') {
        names
            .get(token)
            .cloned()
            .unwrap_or_else(|| token.to_string())
    } else {
        token.to_string()
    }
}

fn split_top_level_and(expr: &str) -> Vec<&str> {
    let bytes = expr.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = (depth - 1).max(0),
            _ if depth == 0 && i + 5 <= bytes.len() => {
                let slice = &expr[i..i + 5];
                if slice.eq_ignore_ascii_case(" AND ") {
                    parts.push(&expr[start..i]);
                    i += 5;
                    start = i;
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&expr[start..]);
    parts
}

fn split_single_equals(clause: &str) -> Option<(&str, &str)> {
    let bytes = clause.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'=' {
            let prev = if i > 0 { bytes[i - 1] } else { 0 };
            let next = if i + 1 < bytes.len() { bytes[i + 1] } else { 0 };
            if !matches!(prev, b'<' | b'>' | b'!' | b'=') && next != b'=' {
                return Some((&clause[..i], &clause[i + 1..]));
            }
        }
    }
    None
}

/// Extracts the SigV4 access key id from an `Authorization` header if present.
pub fn access_key_from_auth_header(auth: &str) -> Option<String> {
    let rest = auth.trim().strip_prefix("AWS4-HMAC-SHA256 ")?;
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(cred) = part.strip_prefix("Credential=") {
            if let Some((access_key, _)) = cred.split_once('/') {
                if !access_key.is_empty() {
                    return Some(access_key.to_string());
                }
            }
        }
    }
    None
}
