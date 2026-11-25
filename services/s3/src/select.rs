//! Deterministic local SQL evaluator for S3 `SelectObjectContent`.
//!
//! Supports
//! `SELECT <projections> FROM S3Object [AS] [<alias>] [[INNER|CROSS] JOIN S3Object <alias> [ON <expr>] | , S3Object <alias>]
//! [WHERE <expr>] [GROUP BY <expr>, ...] [ORDER BY <expr>|<n>|<aggregate> [ASC|DESC], ...] [LIMIT <n>]`
//! over CSV (`FileHeaderInfo` = `USE`/`IGNORE`/`NONE`) and JSON (`LINES`/`DOCUMENT`)
//! objects, emitting CSV or JSON Lines records.
//!
//! A `JOIN` is a self-join of the object's records (the only table S3 Select
//! has); qualify columns with the table aliases. `GROUP BY` groups in order of
//! first appearance; non-aggregate projections take the group's first record.
//! `ORDER BY` is a stable sort; NULLs sort last ascending and first descending;
//! CSV fields are strings, so `CAST` them for numeric order.

use serde_json::{Map, Number, Value};

#[derive(Debug, Default)]
pub struct SelectObjectContent {
    pub expression: String,
    pub expression_type: String,
    pub input_csv: Option<SelectCsvSerialization>,
    pub input_json: Option<SelectJsonSerialization>,
    pub output_csv: Option<SelectCsvSerialization>,
    pub output_json: Option<SelectJsonSerialization>,
}

#[derive(Debug, Default, Clone)]
pub struct SelectCsvSerialization {
    pub file_header_info: String,
    pub record_delimiter: String,
    pub field_delimiter: String,
    pub quote_character: String,
    pub comments: String,
    pub quote_fields: String,
}

#[derive(Debug, Default, Clone)]
pub struct SelectJsonSerialization {
    pub kind: String,
    pub record_delimiter: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectError {
    Invalid(String),
    Unsupported(String),
}

pub fn evaluate(request: &SelectObjectContent, body: &[u8]) -> Result<Vec<u8>, SelectError> {
    let sql = unescape_xml(&request.expression);
    let query = parse_select_query(&sql)?;

    let (records, is_csv_input) = if let Some(csv) = &request.input_csv {
        (parse_csv_records(csv, body)?, true)
    } else if let Some(json) = &request.input_json {
        (parse_json_records(json, body)?, false)
    } else {
        return Err(SelectError::Unsupported(
            "input serialization is not supported".to_string(),
        ));
    };

    if request.output_csv.is_none() && request.output_json.is_none() {
        return Err(SelectError::Unsupported(
            "output serialization is not supported".to_string(),
        ));
    }

    let records = match &query.join {
        Some(join) => join_records(records, join, query.alias.as_deref())?,
        None => records,
    };
    let alias = query.alias.as_deref();
    let grouped = query.is_aggregate() || !query.group_by.is_empty();
    // Without sorting or grouping, LIMIT can stop the scan early.
    let early_limit = if grouped || !query.order_by.is_empty() {
        None
    } else {
        query.limit
    };

    let mut filtered = Vec::new();
    for rec in records {
        if let Some(where_expr) = &query.where_clause {
            if !eval_expr(where_expr, &rec, alias).is_truthy() {
                continue;
            }
        }
        filtered.push(rec);
        if early_limit.is_some_and(|limit| filtered.len() >= limit) {
            break;
        }
    }

    // Each output row with its ORDER BY key values.
    let mut rows: Vec<(OutputRow, Vec<SqlVal>)> = Vec::new();
    if grouped {
        let groups = if query.group_by.is_empty() {
            vec![filtered]
        } else {
            group_records(filtered, &query.group_by, alias)
        };
        for group in &groups {
            let mut row = Vec::with_capacity(query.projections.len());
            for (idx, item) in query.projections.iter().enumerate() {
                let val = match &item.kind {
                    ProjectionKind::Aggregate { .. } => eval_aggregate(item, group, alias)?,
                    ProjectionKind::Expr(expr) => group
                        .first()
                        .map(|rec| eval_expr(expr, rec, alias))
                        .unwrap_or(SqlVal::Null),
                    ProjectionKind::Wildcard(_) => {
                        return Err(SelectError::Invalid(
                            "'*' cannot be projected in an aggregate or GROUP BY query".to_string(),
                        ));
                    }
                };
                let col_name = item
                    .alias
                    .clone()
                    .or_else(|| match &item.kind {
                        ProjectionKind::Expr(expr) => default_column_name(expr, is_csv_input),
                        _ => None,
                    })
                    .unwrap_or_else(|| format!("_{}", idx + 1));
                row.push((col_name, val));
            }
            let row = OutputRow::Named(row);
            let mut keys = Vec::with_capacity(query.order_by.len());
            for item in &query.order_by {
                keys.push(order_key(item, &row, group.first(), Some(group), alias)?);
            }
            rows.push((row, keys));
        }
    } else {
        for rec in &filtered {
            let row = project_record(&query.projections, rec, alias, is_csv_input);
            let mut keys = Vec::with_capacity(query.order_by.len());
            for item in &query.order_by {
                keys.push(order_key(item, &row, Some(rec), None, alias)?);
            }
            rows.push((row, keys));
        }
    }
    if !query.order_by.is_empty() {
        rows.sort_by(|(_, a), (_, b)| {
            for ((x, y), item) in a.iter().zip(b).zip(&query.order_by) {
                let ord = order_cmp(x, y);
                let ord = if item.descending { ord.reverse() } else { ord };
                if ord.is_ne() {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }
    let projected_rows: Vec<OutputRow> = rows.into_iter().map(|(row, _)| row).collect();

    if let Some(out_csv) = &request.output_csv {
        format_csv_output(out_csv, &projected_rows)
    } else if let Some(out_json) = &request.output_json {
        format_json_output(out_json, &projected_rows)
    } else {
        Err(SelectError::Unsupported(
            "output serialization is not supported".to_string(),
        ))
    }
}

pub fn unescape_xml(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        rest = &rest[amp..];
        if let Some(semi) = rest.find(';') {
            let entity = &rest[1..semi];
            let decoded = match entity {
                "lt" => Some('<'),
                "gt" => Some('>'),
                "amp" => Some('&'),
                "quot" => Some('"'),
                "apos" => Some('\''),
                _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                    u32::from_str_radix(&entity[2..], 16)
                        .ok()
                        .and_then(char::from_u32)
                }
                _ if entity.starts_with('#') => {
                    entity[1..].parse::<u32>().ok().and_then(char::from_u32)
                }
                _ => None,
            };
            if let Some(ch) = decoded {
                out.push(ch);
                rest = &rest[semi + 1..];
                continue;
            }
        }
        out.push('&');
        rest = &rest[1..];
    }
    out.push_str(rest);
    out
}

// --- Record model -----------------------------------------------------------

#[derive(Debug, Clone)]
enum Record {
    Csv {
        headers: Option<Vec<String>>,
        fields: Vec<String>,
    },
    Json(Value),
    /// A self-join row: each side's record under its table alias.
    Joined(Vec<(String, Record)>),
}

#[derive(Debug, Clone)]
enum OutputRow {
    CsvRaw(Vec<String>),
    JsonRaw(Value),
    Named(Vec<(String, SqlVal)>),
}

#[derive(Debug, Clone)]
enum SqlVal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Json(Value),
}

impl SqlVal {
    fn is_null(&self) -> bool {
        matches!(self, SqlVal::Null)
    }

    fn is_truthy(&self) -> bool {
        matches!(self, SqlVal::Bool(true))
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            SqlVal::Int(i) => Some(*i as f64),
            SqlVal::Float(f) => Some(*f),
            SqlVal::Str(s) => s.trim().parse::<f64>().ok(),
            _ => None,
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            SqlVal::Int(i) => Some(*i),
            SqlVal::Float(f) => Some(*f as i64),
            SqlVal::Str(s) => s.trim().parse::<i64>().ok(),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<String> {
        match self {
            SqlVal::Null => None,
            SqlVal::Bool(b) => Some(b.to_string()),
            SqlVal::Int(i) => Some(i.to_string()),
            SqlVal::Float(f) => Some(format_float(*f)),
            SqlVal::Str(s) => Some(s.clone()),
            SqlVal::Json(v) => Some(v.to_string()),
        }
    }

    fn to_csv_cell(&self) -> String {
        match self {
            SqlVal::Null => String::new(),
            SqlVal::Bool(b) => b.to_string(),
            SqlVal::Int(i) => i.to_string(),
            SqlVal::Float(f) => format_float(*f),
            SqlVal::Str(s) => s.clone(),
            SqlVal::Json(v) => match v {
                Value::Null => String::new(),
                Value::String(s) => s.clone(),
                other => other.to_string(),
            },
        }
    }

    fn to_json_value(&self) -> Value {
        match self {
            SqlVal::Null => Value::Null,
            SqlVal::Bool(b) => Value::Bool(*b),
            SqlVal::Int(i) => Value::Number((*i).into()),
            SqlVal::Float(f) => Number::from_f64(*f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            SqlVal::Str(s) => Value::String(s.clone()),
            SqlVal::Json(v) => v.clone(),
        }
    }

    fn from_json(v: &Value) -> Self {
        match v {
            Value::Null => SqlVal::Null,
            Value::Bool(b) => SqlVal::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    SqlVal::Int(i)
                } else if let Some(f) = n.as_f64() {
                    SqlVal::Float(f)
                } else {
                    SqlVal::Str(n.to_string())
                }
            }
            Value::String(s) => SqlVal::Str(s.clone()),
            other => SqlVal::Json(other.clone()),
        }
    }
}

fn format_float(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{:.0}", f)
    } else {
        f.to_string()
    }
}

// --- Input parsing ----------------------------------------------------------

fn parse_csv_records(
    csv: &SelectCsvSerialization,
    body: &[u8],
) -> Result<Vec<Record>, SelectError> {
    let field_delim = if csv.field_delimiter.is_empty() {
        ","
    } else {
        csv.field_delimiter.as_str()
    };
    if field_delim.len() != 1 {
        return Err(SelectError::Unsupported(
            "only single-byte CSV field delimiters are supported".to_string(),
        ));
    }
    let delim = field_delim.as_bytes()[0] as char;
    let quote = if csv.quote_character.is_empty() {
        '"'
    } else if csv.quote_character.len() == 1 {
        csv.quote_character.as_bytes()[0] as char
    } else {
        return Err(SelectError::Unsupported(
            "only single-byte CSV quote characters are supported".to_string(),
        ));
    };
    let comment_char = csv.comments.chars().next();
    let rec_delim = if csv.record_delimiter.is_empty() {
        "\n"
    } else {
        csv.record_delimiter.as_str()
    };

    let text = String::from_utf8_lossy(body);
    let raw_lines = split_csv_lines(&text, rec_delim, quote);
    let mut rows: Vec<Vec<String>> = Vec::new();
    for line in raw_lines {
        if line.is_empty() {
            continue;
        }
        if let Some(c) = comment_char {
            if line.starts_with(c) {
                continue;
            }
        }
        rows.push(parse_csv_row(&line, delim, quote));
    }

    let header_mode = csv.file_header_info.trim();
    let headers = if header_mode.eq_ignore_ascii_case("USE") {
        if rows.is_empty() {
            None
        } else {
            Some(rows.remove(0))
        }
    } else if header_mode.eq_ignore_ascii_case("IGNORE") {
        if !rows.is_empty() {
            rows.remove(0);
        }
        None
    } else {
        None
    };

    Ok(rows
        .into_iter()
        .map(|fields| Record::Csv {
            headers: headers.clone(),
            fields,
        })
        .collect())
}

fn split_csv_lines(text: &str, rec_delim: &str, quote: char) -> Vec<String> {
    if rec_delim == "\n" {
        let mut out = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = text.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == quote {
                in_quotes = !in_quotes;
                current.push(ch);
            } else if ch == '\r' && !in_quotes {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                out.push(std::mem::take(&mut current));
            } else if ch == '\n' && !in_quotes {
                out.push(std::mem::take(&mut current));
            } else {
                current.push(ch);
            }
        }
        if !current.is_empty() {
            out.push(current);
        }
        out
    } else {
        text.split(rec_delim).map(str::to_string).collect()
    }
}

fn parse_csv_row(line: &str, delim: char, quote: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == quote {
                if chars.peek() == Some(&quote) {
                    chars.next();
                    current.push(quote);
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == quote && current.is_empty() {
            in_quotes = true;
        } else if ch == delim {
            fields.push(std::mem::take(&mut current));
        } else {
            current.push(ch);
        }
    }
    fields.push(current);
    fields
}

fn parse_json_records(
    json: &SelectJsonSerialization,
    body: &[u8],
) -> Result<Vec<Record>, SelectError> {
    let kind = json.kind.trim();
    if kind.eq_ignore_ascii_case("DOCUMENT") {
        let value: Value = serde_json::from_slice(body)
            .map_err(|_| SelectError::Invalid("JSON input is malformed".to_string()))?;
        return match value {
            Value::Array(items) => Ok(items.into_iter().map(Record::Json).collect()),
            other => Ok(vec![Record::Json(other)]),
        };
    }
    if !kind.is_empty() && !kind.eq_ignore_ascii_case("LINES") {
        return Err(SelectError::Unsupported(
            "only JSON LINES and DOCUMENT input are supported".to_string(),
        ));
    }
    let mut records = Vec::new();
    for line in String::from_utf8_lossy(body)
        .lines()
        .filter(|l| !l.trim().is_empty())
    {
        let value: Value = serde_json::from_str(line)
            .map_err(|_| SelectError::Invalid("JSON input is malformed".to_string()))?;
        records.push(Record::Json(value));
    }
    Ok(records)
}

// --- Output formatting ------------------------------------------------------

fn format_csv_output(
    csv: &SelectCsvSerialization,
    rows: &[OutputRow],
) -> Result<Vec<u8>, SelectError> {
    let field_delim = if csv.field_delimiter.is_empty() {
        ","
    } else {
        csv.field_delimiter.as_str()
    };
    if field_delim.len() != 1 {
        return Err(SelectError::Unsupported(
            "only single-byte CSV field delimiters are supported".to_string(),
        ));
    }
    let delim = field_delim.as_bytes()[0] as char;
    let quote = if csv.quote_character.is_empty() {
        '"'
    } else if csv.quote_character.len() == 1 {
        csv.quote_character.as_bytes()[0] as char
    } else {
        return Err(SelectError::Unsupported(
            "only single-byte CSV quote characters are supported".to_string(),
        ));
    };
    let always_quote = csv.quote_fields.eq_ignore_ascii_case("ALWAYS");
    let rec_delim = if csv.record_delimiter.is_empty() {
        "\n"
    } else {
        csv.record_delimiter.as_str()
    };

    let mut out = String::new();
    for row in rows {
        let cells: Vec<String> = match row {
            OutputRow::CsvRaw(fields) => fields.clone(),
            OutputRow::JsonRaw(v) => match v {
                Value::Object(map) => map
                    .values()
                    .map(|val| SqlVal::from_json(val).to_csv_cell())
                    .collect(),
                other => vec![SqlVal::from_json(other).to_csv_cell()],
            },
            OutputRow::Named(pairs) => pairs.iter().map(|(_, v)| v.to_csv_cell()).collect(),
        };
        for (i, cell) in cells.iter().enumerate() {
            if i > 0 {
                out.push(delim);
            }
            let needs_quote = always_quote
                || cell.contains(delim)
                || cell.contains(quote)
                || cell.contains('\n')
                || cell.contains('\r');
            if needs_quote {
                out.push(quote);
                for ch in cell.chars() {
                    if ch == quote {
                        out.push(quote);
                    }
                    out.push(ch);
                }
                out.push(quote);
            } else {
                out.push_str(cell);
            }
        }
        out.push_str(rec_delim);
    }
    Ok(out.into_bytes())
}

fn format_json_output(
    json: &SelectJsonSerialization,
    rows: &[OutputRow],
) -> Result<Vec<u8>, SelectError> {
    let rec_delim = if json.record_delimiter.is_empty() {
        "\n"
    } else {
        json.record_delimiter.as_str()
    };
    let mut out = Vec::new();
    for row in rows {
        let value = match row {
            OutputRow::JsonRaw(v) => v.clone(),
            OutputRow::CsvRaw(fields) => {
                let mut map = Map::new();
                for (idx, field) in fields.iter().enumerate() {
                    map.insert(format!("_{}", idx + 1), Value::String(field.clone()));
                }
                Value::Object(map)
            }
            OutputRow::Named(pairs) => {
                let mut map = Map::new();
                for (name, val) in pairs {
                    map.insert(name.clone(), val.to_json_value());
                }
                Value::Object(map)
            }
        };
        let encoded = serde_json::to_vec(&value)
            .map_err(|_| SelectError::Invalid("failed to encode JSON output".to_string()))?;
        out.extend_from_slice(&encoded);
        out.extend_from_slice(rec_delim.as_bytes());
    }
    Ok(out)
}

// --- SQL AST & Parser -------------------------------------------------------

#[derive(Debug)]
struct SelectQuery {
    projections: Vec<ProjectionItem>,
    alias: Option<String>,
    join: Option<JoinSpec>,
    where_clause: Option<Expr>,
    group_by: Vec<Expr>,
    order_by: Vec<OrderItem>,
    limit: Option<usize>,
}

/// `S3Object <left> JOIN S3Object <right> [ON <expr>]` over the same records.
#[derive(Debug)]
struct JoinSpec {
    right_alias: String,
    on: Option<Expr>,
}

#[derive(Debug)]
struct OrderItem {
    key: OrderKey,
    descending: bool,
}

#[derive(Debug)]
enum OrderKey {
    /// 1-based output column position.
    Position(usize),
    Expr(Expr),
    Aggregate {
        func: AggFunc,
        arg: Option<Expr>,
    },
}

impl SelectQuery {
    fn is_aggregate(&self) -> bool {
        self.projections
            .iter()
            .any(|p| matches!(p.kind, ProjectionKind::Aggregate { .. }))
    }
}

#[derive(Debug)]
struct ProjectionItem {
    kind: ProjectionKind,
    alias: Option<String>,
}

#[derive(Debug)]
enum ProjectionKind {
    /// `*`, or `<alias>.*` (which keeps only that table's columns in a join).
    Wildcard(Option<String>),
    Expr(Expr),
    Aggregate {
        func: AggFunc,
        arg: Option<Expr>, // None for COUNT(*)
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
enum PathStep {
    Field(String),
    Index(usize),
}

#[derive(Debug, Clone)]
enum Expr {
    Literal(SqlVal),
    Path(Vec<PathStep>),
    UnaryNot(Box<Expr>),
    Binary {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        items: Vec<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Cast {
        expr: Box<Expr>,
        target: CastType,
    },
    Func {
        name: String,
        args: Vec<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinOp {
    Or,
    And,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CastType {
    Int,
    Float,
    String,
    Bool,
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    QuotedIdent(String),
    Str(String),
    Int(i64),
    Float(f64),
    Star,
    Comma,
    Dot,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Semicolon,
}

fn tokenize(input: &str) -> Result<Vec<Token>, SelectError> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }
        match ch {
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            ';' => {
                tokens.push(Token::Semicolon);
                i += 1;
            }
            '=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            '!' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Ne);
                    i += 2;
                } else {
                    return Err(SelectError::Invalid("unexpected '!'".to_string()));
                }
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Le);
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == '>' {
                    tokens.push(Token::Ne);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Ge);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            '\'' => {
                i += 1;
                let mut s = String::new();
                let mut closed = false;
                while i < chars.len() {
                    if chars[i] == '\'' {
                        if i + 1 < chars.len() && chars[i + 1] == '\'' {
                            s.push('\'');
                            i += 2;
                        } else {
                            i += 1;
                            closed = true;
                            break;
                        }
                    } else {
                        s.push(chars[i]);
                        i += 1;
                    }
                }
                if !closed {
                    return Err(SelectError::Invalid(
                        "unterminated string literal".to_string(),
                    ));
                }
                tokens.push(Token::Str(s));
            }
            '"' => {
                i += 1;
                let mut s = String::new();
                let mut closed = false;
                while i < chars.len() {
                    if chars[i] == '"' {
                        if i + 1 < chars.len() && chars[i + 1] == '"' {
                            s.push('"');
                            i += 2;
                        } else {
                            i += 1;
                            closed = true;
                            break;
                        }
                    } else {
                        s.push(chars[i]);
                        i += 1;
                    }
                }
                if !closed {
                    return Err(SelectError::Invalid(
                        "unterminated quoted identifier".to_string(),
                    ));
                }
                tokens.push(Token::QuotedIdent(s));
            }
            c if c.is_ascii_digit()
                || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit()) =>
            {
                let start = i;
                if c == '-' {
                    i += 1;
                }
                let mut is_float = false;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    if chars[i] == '.' {
                        if is_float {
                            break;
                        }
                        is_float = true;
                    }
                    i += 1;
                }
                let raw: String = chars[start..i].iter().collect();
                if is_float {
                    let v = raw
                        .parse::<f64>()
                        .map_err(|_| SelectError::Invalid("invalid number literal".to_string()))?;
                    tokens.push(Token::Float(v));
                } else {
                    let v = raw
                        .parse::<i64>()
                        .map_err(|_| SelectError::Invalid("invalid integer literal".to_string()))?;
                    tokens.push(Token::Int(v));
                }
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let ident: String = chars[start..i].iter().collect();
                tokens.push(Token::Ident(ident));
            }
            other => {
                return Err(SelectError::Unsupported(format!(
                    "unsupported token '{other}' in SQL expression"
                )));
            }
        }
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset)
    }

    fn next(&mut self) -> Option<Token> {
        let tok = self.tokens.get(self.pos).cloned();
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn is_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    fn consume_kw(&mut self, kw: &str) -> bool {
        if self.is_kw(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn parse_query(&mut self) -> Result<SelectQuery, SelectError> {
        if !self.consume_kw("SELECT") {
            return Err(SelectError::Invalid(
                "query must start with SELECT".to_string(),
            ));
        }
        if self.is_kw("DISTINCT") {
            return Err(SelectError::Unsupported(
                "DISTINCT is not supported".to_string(),
            ));
        }

        let mut projections = Vec::new();
        loop {
            projections.push(self.parse_projection_item()?);
            if matches!(self.peek(), Some(Token::Comma)) {
                self.next();
            } else {
                break;
            }
        }
        if projections.is_empty() {
            return Err(SelectError::Invalid("empty SELECT projection".to_string()));
        }

        if !self.consume_kw("FROM") {
            return Err(SelectError::Invalid("expected FROM clause".to_string()));
        }
        match self.next() {
            Some(Token::Ident(name)) if name.eq_ignore_ascii_case("S3Object") => {}
            _ => {
                return Err(SelectError::Invalid(
                    "FROM target must be S3Object".to_string(),
                ));
            }
        }

        let alias = self.parse_table_alias()?;
        let join = self.parse_join(alias.as_deref())?;

        let where_clause = if self.consume_kw("WHERE") {
            Some(self.parse_or_expr()?)
        } else {
            None
        };

        let mut group_by = Vec::new();
        if self.is_kw("GROUP") {
            self.next();
            if !self.consume_kw("BY") {
                return Err(SelectError::Invalid("expected BY after GROUP".to_string()));
            }
            loop {
                group_by.push(self.parse_or_expr()?);
                if matches!(self.peek(), Some(Token::Comma)) {
                    self.next();
                } else {
                    break;
                }
            }
        }
        let has_agg = projections
            .iter()
            .any(|p| matches!(p.kind, ProjectionKind::Aggregate { .. }));
        if has_agg
            && group_by.is_empty()
            && !projections
                .iter()
                .all(|p| matches!(p.kind, ProjectionKind::Aggregate { .. }))
        {
            return Err(SelectError::Invalid(
                "non-aggregate projections next to aggregates need GROUP BY".to_string(),
            ));
        }

        let mut order_by = Vec::new();
        if self.is_kw("ORDER") {
            self.next();
            if !self.consume_kw("BY") {
                return Err(SelectError::Invalid("expected BY after ORDER".to_string()));
            }
            loop {
                order_by.push(self.parse_order_item()?);
                if matches!(self.peek(), Some(Token::Comma)) {
                    self.next();
                } else {
                    break;
                }
            }
        }

        let limit = if self.consume_kw("LIMIT") {
            match self.next() {
                Some(Token::Int(n)) if n >= 0 => Some(n as usize),
                _ => {
                    return Err(SelectError::Invalid(
                        "LIMIT must be a non-negative integer".to_string(),
                    ));
                }
            }
        } else {
            None
        };

        while matches!(self.peek(), Some(Token::Semicolon)) {
            self.next();
        }

        if let Some(tok) = self.peek() {
            return match tok {
                Token::Ident(kw)
                    if kw.eq_ignore_ascii_case("HAVING")
                        || kw.eq_ignore_ascii_case("JOIN")
                        || kw.eq_ignore_ascii_case("LEFT")
                        || kw.eq_ignore_ascii_case("RIGHT")
                        || kw.eq_ignore_ascii_case("FULL")
                        || kw.eq_ignore_ascii_case("UNION") =>
                {
                    Err(SelectError::Unsupported(format!(
                        "{kw} clause is not supported"
                    )))
                }
                _ => Err(SelectError::Invalid(
                    "unexpected trailing tokens in SQL expression".to_string(),
                )),
            };
        }

        Ok(SelectQuery {
            projections,
            alias,
            join,
            where_clause,
            group_by,
            order_by,
            limit,
        })
    }

    fn parse_table_alias(&mut self) -> Result<Option<String>, SelectError> {
        if self.consume_kw("AS") {
            return match self.next() {
                Some(Token::Ident(a)) | Some(Token::QuotedIdent(a)) => Ok(Some(a)),
                _ => Err(SelectError::Invalid("expected alias after AS".to_string())),
            };
        }
        match self.peek() {
            Some(Token::Ident(a)) if !is_clause_keyword(a) => {
                let a = a.clone();
                self.pos += 1;
                Ok(Some(a))
            }
            Some(Token::QuotedIdent(a)) => {
                let a = a.clone();
                self.pos += 1;
                Ok(Some(a))
            }
            _ => Ok(None),
        }
    }

    /// `[INNER] JOIN S3Object <alias> ON <expr>`, `CROSS JOIN S3Object <alias>`,
    /// or `, S3Object <alias>`. Both sides need distinct aliases.
    fn parse_join(&mut self, left_alias: Option<&str>) -> Result<Option<JoinSpec>, SelectError> {
        let needs_on = if matches!(self.peek(), Some(Token::Comma)) {
            self.next();
            false
        } else if self.is_kw("CROSS") {
            self.next();
            if !self.consume_kw("JOIN") {
                return Err(SelectError::Invalid(
                    "expected JOIN after CROSS".to_string(),
                ));
            }
            false
        } else if self.is_kw("INNER") || self.is_kw("JOIN") {
            if self.consume_kw("INNER") && !self.is_kw("JOIN") {
                return Err(SelectError::Invalid(
                    "expected JOIN after INNER".to_string(),
                ));
            }
            self.next();
            true
        } else {
            return Ok(None);
        };
        match self.next() {
            Some(Token::Ident(name)) if name.eq_ignore_ascii_case("S3Object") => {}
            _ => {
                return Err(SelectError::Invalid(
                    "JOIN target must be S3Object".to_string(),
                ));
            }
        }
        let right_alias = self.parse_table_alias()?;
        let (Some(left), Some(right)) = (left_alias, right_alias) else {
            return Err(SelectError::Invalid(
                "both sides of a JOIN need a table alias".to_string(),
            ));
        };
        if left.eq_ignore_ascii_case(&right) {
            return Err(SelectError::Invalid(
                "JOIN table aliases must differ".to_string(),
            ));
        }
        let on = if needs_on {
            if !self.consume_kw("ON") {
                return Err(SelectError::Invalid("expected ON after JOIN".to_string()));
            }
            Some(self.parse_or_expr()?)
        } else {
            None
        };
        if matches!(self.peek(), Some(Token::Comma)) || self.is_kw("JOIN") {
            return Err(SelectError::Unsupported(
                "joining more than two tables is not supported".to_string(),
            ));
        }
        Ok(Some(JoinSpec {
            right_alias: right,
            on,
        }))
    }

    fn parse_order_item(&mut self) -> Result<OrderItem, SelectError> {
        let key = match (self.peek(), self.peek_at(1)) {
            (Some(Token::Int(n)), next)
                if matches!(next, None | Some(Token::Comma) | Some(Token::Semicolon))
                    || matches!(next, Some(Token::Ident(w)) if is_clause_keyword(w)
                        || w.eq_ignore_ascii_case("ASC")
                        || w.eq_ignore_ascii_case("DESC")) =>
            {
                let n = *n;
                self.next();
                if n < 1 {
                    return Err(SelectError::Invalid(
                        "ORDER BY position must be at least 1".to_string(),
                    ));
                }
                OrderKey::Position(n as usize)
            }
            _ => match self.parse_aggregate_call()? {
                Some(ProjectionKind::Aggregate { func, arg }) => OrderKey::Aggregate { func, arg },
                _ => OrderKey::Expr(self.parse_or_expr()?),
            },
        };
        let descending = if self.consume_kw("DESC") {
            true
        } else {
            self.consume_kw("ASC");
            false
        };
        Ok(OrderItem { key, descending })
    }

    /// `COUNT(*)` / `COUNT|SUM|AVG|MIN|MAX(<expr>)` at the cursor, if present.
    fn parse_aggregate_call(&mut self) -> Result<Option<ProjectionKind>, SelectError> {
        let Some(Token::Ident(name)) = self.peek() else {
            return Ok(None);
        };
        if !matches!(self.peek_at(1), Some(Token::LParen)) {
            return Ok(None);
        }
        let Some(func) = parse_agg_func(name) else {
            return Ok(None);
        };
        self.pos += 2; // consume name and '('
        let arg = if func == AggFunc::Count && matches!(self.peek(), Some(Token::Star)) {
            self.next();
            None
        } else {
            Some(self.parse_primary_or_func()?)
        };
        if !matches!(self.next(), Some(Token::RParen)) {
            return Err(SelectError::Invalid(
                "expected ')' after aggregate argument".to_string(),
            ));
        }
        Ok(Some(ProjectionKind::Aggregate { func, arg }))
    }

    fn parse_projection_item(&mut self) -> Result<ProjectionItem, SelectError> {
        if matches!(self.peek(), Some(Token::Star)) {
            self.next();
            return Ok(ProjectionItem {
                kind: ProjectionKind::Wildcard(None),
                alias: None,
            });
        }
        // alias.*
        if let (Some(Token::Ident(qualifier)), Some(Token::Dot), Some(Token::Star)) =
            (self.peek(), self.peek_at(1), self.peek_at(2))
        {
            let qualifier = qualifier.clone();
            self.pos += 3;
            return Ok(ProjectionItem {
                kind: ProjectionKind::Wildcard(Some(qualifier)),
                alias: None,
            });
        }

        // Check aggregate function: COUNT/SUM/AVG/MIN/MAX(...)
        let kind = match self.parse_aggregate_call()? {
            Some(kind) => kind,
            None => ProjectionKind::Expr(self.parse_or_expr()?),
        };

        let alias = if self.consume_kw("AS") {
            match self.next() {
                Some(Token::Ident(a)) | Some(Token::QuotedIdent(a)) => Some(a),
                _ => return Err(SelectError::Invalid("expected column alias".to_string())),
            }
        } else if let Some(Token::Ident(a)) = self.peek() {
            if !is_clause_keyword(a) {
                let a = a.clone();
                self.pos += 1;
                Some(a)
            } else {
                None
            }
        } else {
            None
        };

        Ok(ProjectionItem { kind, alias })
    }

    fn parse_or_expr(&mut self) -> Result<Expr, SelectError> {
        let mut left = self.parse_and_expr()?;
        while self.consume_kw("OR") {
            let right = self.parse_and_expr()?;
            left = Expr::Binary {
                op: BinOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, SelectError> {
        let mut left = self.parse_not_expr()?;
        while self.consume_kw("AND") {
            let right = self.parse_not_expr()?;
            left = Expr::Binary {
                op: BinOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr, SelectError> {
        if self.consume_kw("NOT") {
            let inner = self.parse_not_expr()?;
            return Ok(Expr::UnaryNot(Box::new(inner)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr, SelectError> {
        let left = self.parse_primary_or_func()?;

        if self.consume_kw("IS") {
            let negated = self.consume_kw("NOT");
            if !self.consume_kw("NULL") {
                return Err(SelectError::Invalid("expected NULL after IS".to_string()));
            }
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }

        let negated = self.consume_kw("NOT");
        if self.consume_kw("LIKE") {
            let pattern = self.parse_primary_or_func()?;
            return Ok(Expr::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                negated,
            });
        }
        if self.consume_kw("IN") {
            if !matches!(self.next(), Some(Token::LParen)) {
                return Err(SelectError::Invalid("expected '(' after IN".to_string()));
            }
            let mut items = Vec::new();
            if !matches!(self.peek(), Some(Token::RParen)) {
                loop {
                    items.push(self.parse_primary_or_func()?);
                    if matches!(self.peek(), Some(Token::Comma)) {
                        self.next();
                    } else {
                        break;
                    }
                }
            }
            if !matches!(self.next(), Some(Token::RParen)) {
                return Err(SelectError::Invalid(
                    "expected ')' after IN list".to_string(),
                ));
            }
            return Ok(Expr::InList {
                expr: Box::new(left),
                items,
                negated,
            });
        }
        if self.consume_kw("BETWEEN") {
            let low = self.parse_primary_or_func()?;
            if !self.consume_kw("AND") {
                return Err(SelectError::Invalid(
                    "expected AND in BETWEEN expression".to_string(),
                ));
            }
            let high = self.parse_primary_or_func()?;
            return Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated,
            });
        }
        if negated {
            return Err(SelectError::Invalid(
                "unexpected NOT in comparison".to_string(),
            ));
        }

        let op = match self.peek() {
            Some(Token::Eq) => Some(BinOp::Eq),
            Some(Token::Ne) => Some(BinOp::Ne),
            Some(Token::Lt) => Some(BinOp::Lt),
            Some(Token::Le) => Some(BinOp::Le),
            Some(Token::Gt) => Some(BinOp::Gt),
            Some(Token::Ge) => Some(BinOp::Ge),
            _ => None,
        };
        if let Some(op) = op {
            self.next();
            let right = self.parse_primary_or_func()?;
            return Ok(Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            });
        }

        Ok(left)
    }

    fn parse_primary_or_func(&mut self) -> Result<Expr, SelectError> {
        match self.peek().cloned() {
            Some(Token::LParen) => {
                self.next();
                let inner = self.parse_or_expr()?;
                if !matches!(self.next(), Some(Token::RParen)) {
                    return Err(SelectError::Invalid("expected ')'".to_string()));
                }
                Ok(inner)
            }
            Some(Token::Str(s)) => {
                self.next();
                Ok(Expr::Literal(SqlVal::Str(s)))
            }
            Some(Token::Int(n)) => {
                self.next();
                Ok(Expr::Literal(SqlVal::Int(n)))
            }
            Some(Token::Float(f)) => {
                self.next();
                Ok(Expr::Literal(SqlVal::Float(f)))
            }
            Some(Token::Ident(ref s)) if s.eq_ignore_ascii_case("TRUE") => {
                self.next();
                Ok(Expr::Literal(SqlVal::Bool(true)))
            }
            Some(Token::Ident(ref s)) if s.eq_ignore_ascii_case("FALSE") => {
                self.next();
                Ok(Expr::Literal(SqlVal::Bool(false)))
            }
            Some(Token::Ident(ref s)) if s.eq_ignore_ascii_case("NULL") => {
                self.next();
                Ok(Expr::Literal(SqlVal::Null))
            }
            Some(Token::Ident(ref s))
                if s.eq_ignore_ascii_case("CAST")
                    && matches!(self.peek_at(1), Some(Token::LParen)) =>
            {
                self.pos += 2;
                let expr = self.parse_primary_or_func()?;
                if !self.consume_kw("AS") {
                    return Err(SelectError::Invalid("expected AS in CAST".to_string()));
                }
                let target = match self.next() {
                    Some(Token::Ident(ty)) => match ty.to_ascii_uppercase().as_str() {
                        "INT" | "INTEGER" => CastType::Int,
                        "FLOAT" | "DECIMAL" | "NUMERIC" => CastType::Float,
                        "STRING" | "VARCHAR" | "CHAR" => CastType::String,
                        "BOOL" | "BOOLEAN" => CastType::Bool,
                        _ => {
                            return Err(SelectError::Unsupported(format!(
                                "CAST to {ty} is not supported"
                            )));
                        }
                    },
                    _ => return Err(SelectError::Invalid("expected type in CAST".to_string())),
                };
                if !matches!(self.next(), Some(Token::RParen)) {
                    return Err(SelectError::Invalid("expected ')' after CAST".to_string()));
                }
                Ok(Expr::Cast {
                    expr: Box::new(expr),
                    target,
                })
            }
            Some(Token::Ident(name)) if matches!(self.peek_at(1), Some(Token::LParen)) => {
                let upper = name.to_ascii_uppercase();
                if !matches!(
                    upper.as_str(),
                    "LOWER" | "UPPER" | "TRIM" | "COALESCE" | "CHAR_LENGTH" | "CHARACTER_LENGTH"
                ) {
                    return Err(SelectError::Unsupported(format!(
                        "function {name} is not supported"
                    )));
                }
                self.pos += 2;
                let mut args = Vec::new();
                if !matches!(self.peek(), Some(Token::RParen)) {
                    loop {
                        args.push(self.parse_or_expr()?);
                        if matches!(self.peek(), Some(Token::Comma)) {
                            self.next();
                        } else {
                            break;
                        }
                    }
                }
                if !matches!(self.next(), Some(Token::RParen)) {
                    return Err(SelectError::Invalid(
                        "expected ')' after function arguments".to_string(),
                    ));
                }
                Ok(Expr::Func { name: upper, args })
            }
            Some(Token::Ident(_)) | Some(Token::QuotedIdent(_)) => self.parse_path(),
            _ => Err(SelectError::Invalid("expected expression".to_string())),
        }
    }

    fn parse_path(&mut self) -> Result<Expr, SelectError> {
        let first = match self.next() {
            Some(Token::Ident(s)) | Some(Token::QuotedIdent(s)) => s,
            _ => return Err(SelectError::Invalid("expected identifier".to_string())),
        };
        let mut steps = vec![PathStep::Field(first)];
        loop {
            if matches!(self.peek(), Some(Token::Dot)) {
                self.next();
                match self.next() {
                    Some(Token::Ident(s)) | Some(Token::QuotedIdent(s)) => {
                        steps.push(PathStep::Field(s));
                    }
                    _ => {
                        return Err(SelectError::Invalid(
                            "expected field name after '.'".to_string(),
                        ));
                    }
                }
            } else if matches!(self.peek(), Some(Token::LBracket)) {
                self.next();
                let idx = match self.next() {
                    Some(Token::Int(n)) if n >= 0 => n as usize,
                    _ => {
                        return Err(SelectError::Invalid(
                            "expected non-negative index inside '[]'".to_string(),
                        ));
                    }
                };
                if !matches!(self.next(), Some(Token::RBracket)) {
                    return Err(SelectError::Invalid("expected ']'".to_string()));
                }
                steps.push(PathStep::Index(idx));
            } else {
                break;
            }
        }
        Ok(Expr::Path(steps))
    }
}

fn parse_select_query(sql: &str) -> Result<SelectQuery, SelectError> {
    let tokens = tokenize(sql)?;
    let mut parser = Parser::new(tokens);
    parser.parse_query()
}

fn parse_agg_func(name: &str) -> Option<AggFunc> {
    match name.to_ascii_uppercase().as_str() {
        "COUNT" => Some(AggFunc::Count),
        "SUM" => Some(AggFunc::Sum),
        "AVG" => Some(AggFunc::Avg),
        "MIN" => Some(AggFunc::Min),
        "MAX" => Some(AggFunc::Max),
        _ => None,
    }
}

fn is_clause_keyword(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "FROM"
            | "WHERE"
            | "LIMIT"
            | "GROUP"
            | "ORDER"
            | "HAVING"
            | "JOIN"
            | "INNER"
            | "CROSS"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "ON"
            | "UNION"
            | "AS"
    )
}

// --- Evaluation -------------------------------------------------------------

fn project_record(
    projections: &[ProjectionItem],
    rec: &Record,
    alias: Option<&str>,
    is_csv_input: bool,
) -> OutputRow {
    if let [ProjectionItem {
        kind: ProjectionKind::Wildcard(_),
        ..
    }] = projections
    {
        match rec {
            Record::Csv { fields, .. } => return OutputRow::CsvRaw(fields.clone()),
            Record::Json(v) => return OutputRow::JsonRaw(v.clone()),
            Record::Joined(_) => {}
        }
    }
    let mut pairs = Vec::new();
    for (idx, item) in projections.iter().enumerate() {
        match &item.kind {
            ProjectionKind::Wildcard(qualifier) => match rec {
                Record::Joined(sides) => {
                    for (side_alias, side) in sides {
                        if qualifier
                            .as_deref()
                            .is_none_or(|q| q.eq_ignore_ascii_case(side_alias))
                        {
                            wildcard_pairs(side, idx, &mut pairs);
                        }
                    }
                }
                other => wildcard_pairs(other, idx, &mut pairs),
            },
            ProjectionKind::Expr(expr) => {
                let val = eval_expr(expr, rec, alias);
                let col_name = item
                    .alias
                    .clone()
                    .or_else(|| default_column_name(expr, is_csv_input))
                    .unwrap_or_else(|| format!("_{}", idx + 1));
                pairs.push((col_name, val));
            }
            ProjectionKind::Aggregate { .. } => {}
        }
    }
    OutputRow::Named(pairs)
}

fn wildcard_pairs(rec: &Record, idx: usize, pairs: &mut Vec<(String, SqlVal)>) {
    match rec {
        Record::Csv { headers, fields } => {
            for (fi, field) in fields.iter().enumerate() {
                let name = headers
                    .as_ref()
                    .and_then(|h| h.get(fi).cloned())
                    .unwrap_or_else(|| format!("_{}", fi + 1));
                pairs.push((name, SqlVal::Str(field.clone())));
            }
        }
        Record::Json(Value::Object(map)) => {
            for (k, v) in map {
                pairs.push((k.clone(), SqlVal::from_json(v)));
            }
        }
        Record::Json(other) => {
            pairs.push((format!("_{}", idx + 1), SqlVal::from_json(other)));
        }
        Record::Joined(sides) => {
            for (_, side) in sides {
                wildcard_pairs(side, idx, pairs);
            }
        }
    }
}

/// Upper bound on joined rows, so a self-join of a large object fails fast.
const MAX_JOIN_ROWS: usize = 25_000_000;

fn join_records(
    records: Vec<Record>,
    join: &JoinSpec,
    left_alias: Option<&str>,
) -> Result<Vec<Record>, SelectError> {
    if records.len().saturating_mul(records.len()) > MAX_JOIN_ROWS {
        return Err(SelectError::Unsupported(
            "JOIN input is too large to evaluate locally".to_string(),
        ));
    }
    let left_alias = left_alias.unwrap_or_default().to_string();
    let mut joined = Vec::new();
    for left in &records {
        for right in &records {
            let row = Record::Joined(vec![
                (left_alias.clone(), left.clone()),
                (join.right_alias.clone(), right.clone()),
            ]);
            if join
                .on
                .as_ref()
                .is_none_or(|on| eval_expr(on, &row, None).is_truthy())
            {
                joined.push(row);
            }
        }
    }
    Ok(joined)
}

/// Groups records by the `GROUP BY` values, in order of first appearance.
fn group_records(records: Vec<Record>, keys: &[Expr], alias: Option<&str>) -> Vec<Vec<Record>> {
    let mut index: std::collections::HashMap<Vec<String>, usize> = std::collections::HashMap::new();
    let mut groups: Vec<Vec<Record>> = Vec::new();
    for rec in records {
        let key: Vec<String> = keys
            .iter()
            .map(|expr| group_key(&eval_expr(expr, &rec, alias)))
            .collect();
        match index.get(&key) {
            Some(&i) => groups[i].push(rec),
            None => {
                index.insert(key, groups.len());
                groups.push(vec![rec]);
            }
        }
    }
    groups
}

/// A type-tagged string so `1`, `'1'`, and NULL land in different groups while
/// `1` and `1.0` share one.
fn group_key(val: &SqlVal) -> String {
    match val {
        SqlVal::Null => "null".to_string(),
        SqlVal::Bool(b) => format!("b:{b}"),
        SqlVal::Int(i) => format!("n:{i}"),
        SqlVal::Float(f) if f.fract() == 0.0 && f.abs() < 9.0e15 => format!("n:{}", *f as i64),
        SqlVal::Float(f) => format!("n:{f}"),
        SqlVal::Str(s) => format!("s:{s}"),
        SqlVal::Json(v) => format!("j:{v}"),
    }
}

/// The value an `ORDER BY` item sorts `row` by: an output column (by position
/// or by name), an aggregate over the group, or an expression over the record.
fn order_key(
    item: &OrderItem,
    row: &OutputRow,
    rec: Option<&Record>,
    group: Option<&[Record]>,
    alias: Option<&str>,
) -> Result<SqlVal, SelectError> {
    match &item.key {
        OrderKey::Position(n) => Ok(match row {
            OutputRow::Named(pairs) => pairs.get(n - 1).map(|(_, v)| v.clone()),
            OutputRow::CsvRaw(fields) => fields.get(n - 1).cloned().map(SqlVal::Str),
            OutputRow::JsonRaw(Value::Object(map)) => {
                map.values().nth(n - 1).map(SqlVal::from_json)
            }
            OutputRow::JsonRaw(_) => None,
        }
        .unwrap_or(SqlVal::Null)),
        OrderKey::Aggregate { func, arg } => {
            let Some(group) = group else {
                return Err(SelectError::Invalid(
                    "ORDER BY an aggregate needs an aggregate or GROUP BY query".to_string(),
                ));
            };
            let item = ProjectionItem {
                kind: ProjectionKind::Aggregate {
                    func: *func,
                    arg: arg.clone(),
                },
                alias: None,
            };
            eval_aggregate(&item, group, alias)
        }
        OrderKey::Expr(expr) => {
            if let (Expr::Path(steps), OutputRow::Named(pairs)) = (expr, row) {
                if let [PathStep::Field(name)] = steps.as_slice() {
                    if let Some((_, v)) = pairs.iter().find(|(col, _)| col == name) {
                        return Ok(v.clone());
                    }
                }
            }
            Ok(rec
                .map(|rec| eval_expr(expr, rec, alias))
                .unwrap_or(SqlVal::Null))
        }
    }
}

/// Ascending order: NULLs last, then values by `compare_vals`, falling back to
/// their string forms for values of different types.
fn order_cmp(a: &SqlVal, b: &SqlVal) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => compare_vals(a, b).unwrap_or_else(|| a.as_str().cmp(&b.as_str())),
    }
}

fn default_column_name(expr: &Expr, _is_csv_input: bool) -> Option<String> {
    if let Expr::Path(steps) = expr {
        if let Some(PathStep::Field(last)) = steps.last() {
            return Some(last.clone());
        }
    }
    None
}

fn eval_aggregate(
    item: &ProjectionItem,
    records: &[Record],
    alias: Option<&str>,
) -> Result<SqlVal, SelectError> {
    let ProjectionKind::Aggregate { func, arg } = &item.kind else {
        return Err(SelectError::Invalid("expected aggregate".to_string()));
    };
    match func {
        AggFunc::Count => {
            let count = match arg {
                None => records.len() as i64,
                Some(expr) => records
                    .iter()
                    .filter(|r| !eval_expr(expr, r, alias).is_null())
                    .count() as i64,
            };
            Ok(SqlVal::Int(count))
        }
        AggFunc::Sum | AggFunc::Avg => {
            let Some(expr) = arg else {
                return Err(SelectError::Invalid(
                    "aggregate requires argument".to_string(),
                ));
            };
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for r in records {
                let v = eval_expr(expr, r, alias);
                if let Some(n) = v.as_f64() {
                    sum += n;
                    count += 1;
                }
            }
            if count == 0 {
                return Ok(SqlVal::Null);
            }
            let result = if *func == AggFunc::Avg {
                sum / (count as f64)
            } else {
                sum
            };
            if result.fract() == 0.0 && result.abs() < (i64::MAX as f64) {
                Ok(SqlVal::Int(result as i64))
            } else {
                Ok(SqlVal::Float(result))
            }
        }
        AggFunc::Min | AggFunc::Max => {
            let Some(expr) = arg else {
                return Err(SelectError::Invalid(
                    "aggregate requires argument".to_string(),
                ));
            };
            let mut best: Option<SqlVal> = None;
            for r in records {
                let v = eval_expr(expr, r, alias);
                if v.is_null() {
                    continue;
                }
                let replace = match &best {
                    None => true,
                    Some(cur) => match compare_vals(&v, cur) {
                        Some(ord) => {
                            if *func == AggFunc::Min {
                                ord.is_lt()
                            } else {
                                ord.is_gt()
                            }
                        }
                        None => false,
                    },
                };
                if replace {
                    best = Some(v);
                }
            }
            Ok(best.unwrap_or(SqlVal::Null))
        }
    }
}

fn eval_expr(expr: &Expr, rec: &Record, alias: Option<&str>) -> SqlVal {
    match expr {
        Expr::Literal(v) => v.clone(),
        Expr::Path(steps) => resolve_path(steps, rec, alias),
        Expr::UnaryNot(inner) => match eval_expr(inner, rec, alias) {
            SqlVal::Bool(b) => SqlVal::Bool(!b),
            SqlVal::Null => SqlVal::Null,
            _ => SqlVal::Bool(false),
        },
        Expr::IsNull { expr, negated } => {
            let is_null = eval_expr(expr, rec, alias).is_null();
            SqlVal::Bool(if *negated { !is_null } else { is_null })
        }
        Expr::Binary { op, left, right } => match op {
            BinOp::And => {
                let l = eval_expr(left, rec, alias);
                if matches!(l, SqlVal::Bool(false)) {
                    return SqlVal::Bool(false);
                }
                let r = eval_expr(right, rec, alias);
                if matches!(r, SqlVal::Bool(false)) {
                    return SqlVal::Bool(false);
                }
                if l.is_truthy() && r.is_truthy() {
                    SqlVal::Bool(true)
                } else {
                    SqlVal::Null
                }
            }
            BinOp::Or => {
                let l = eval_expr(left, rec, alias);
                if l.is_truthy() {
                    return SqlVal::Bool(true);
                }
                let r = eval_expr(right, rec, alias);
                if r.is_truthy() {
                    return SqlVal::Bool(true);
                }
                if matches!(l, SqlVal::Bool(false)) && matches!(r, SqlVal::Bool(false)) {
                    SqlVal::Bool(false)
                } else {
                    SqlVal::Null
                }
            }
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                let l = eval_expr(left, rec, alias);
                let r = eval_expr(right, rec, alias);
                if l.is_null() || r.is_null() {
                    return SqlVal::Null;
                }
                let Some(ord) = compare_vals(&l, &r) else {
                    return SqlVal::Bool(matches!(op, BinOp::Ne));
                };
                let res = match op {
                    BinOp::Eq => ord.is_eq(),
                    BinOp::Ne => ord.is_ne(),
                    BinOp::Lt => ord.is_lt(),
                    BinOp::Le => ord.is_le(),
                    BinOp::Gt => ord.is_gt(),
                    BinOp::Ge => ord.is_ge(),
                    _ => false,
                };
                SqlVal::Bool(res)
            }
        },
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let Some(text) = eval_expr(expr, rec, alias).as_str() else {
                return SqlVal::Null;
            };
            let Some(pat) = eval_expr(pattern, rec, alias).as_str() else {
                return SqlVal::Null;
            };
            let matched = sql_like(&text, &pat);
            SqlVal::Bool(if *negated { !matched } else { matched })
        }
        Expr::InList {
            expr,
            items,
            negated,
        } => {
            let val = eval_expr(expr, rec, alias);
            if val.is_null() {
                return SqlVal::Null;
            }
            let matched = items.iter().any(|item| {
                compare_vals(&val, &eval_expr(item, rec, alias)) == Some(std::cmp::Ordering::Equal)
            });
            SqlVal::Bool(if *negated { !matched } else { matched })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let val = eval_expr(expr, rec, alias);
            let lo = eval_expr(low, rec, alias);
            let hi = eval_expr(high, rec, alias);
            if val.is_null() || lo.is_null() || hi.is_null() {
                return SqlVal::Null;
            }
            let in_range = compare_vals(&val, &lo).is_some_and(|o| o.is_ge())
                && compare_vals(&val, &hi).is_some_and(|o| o.is_le());
            SqlVal::Bool(if *negated { !in_range } else { in_range })
        }
        Expr::Cast { expr, target } => {
            let val = eval_expr(expr, rec, alias);
            if val.is_null() {
                return SqlVal::Null;
            }
            match target {
                CastType::Int => val.as_i64().map(SqlVal::Int).unwrap_or(SqlVal::Null),
                CastType::Float => val.as_f64().map(SqlVal::Float).unwrap_or(SqlVal::Null),
                CastType::String => val.as_str().map(SqlVal::Str).unwrap_or(SqlVal::Null),
                CastType::Bool => match val {
                    SqlVal::Bool(b) => SqlVal::Bool(b),
                    SqlVal::Str(ref s) if s.eq_ignore_ascii_case("true") => SqlVal::Bool(true),
                    SqlVal::Str(ref s) if s.eq_ignore_ascii_case("false") => SqlVal::Bool(false),
                    _ => SqlVal::Null,
                },
            }
        }
        Expr::Func { name, args } => match name.as_str() {
            "LOWER" => args
                .first()
                .and_then(|a| eval_expr(a, rec, alias).as_str())
                .map(|s| SqlVal::Str(s.to_lowercase()))
                .unwrap_or(SqlVal::Null),
            "UPPER" => args
                .first()
                .and_then(|a| eval_expr(a, rec, alias).as_str())
                .map(|s| SqlVal::Str(s.to_uppercase()))
                .unwrap_or(SqlVal::Null),
            "TRIM" => args
                .first()
                .and_then(|a| eval_expr(a, rec, alias).as_str())
                .map(|s| SqlVal::Str(s.trim().to_string()))
                .unwrap_or(SqlVal::Null),
            "CHAR_LENGTH" | "CHARACTER_LENGTH" => args
                .first()
                .and_then(|a| eval_expr(a, rec, alias).as_str())
                .map(|s| SqlVal::Int(s.chars().count() as i64))
                .unwrap_or(SqlVal::Null),
            "COALESCE" => {
                for a in args {
                    let v = eval_expr(a, rec, alias);
                    if !v.is_null() {
                        return v;
                    }
                }
                SqlVal::Null
            }
            _ => SqlVal::Null,
        },
    }
}

fn resolve_path(steps: &[PathStep], rec: &Record, alias: Option<&str>) -> SqlVal {
    // Strip leading S3Object or table alias qualifier when followed by more steps.
    let effective = if steps.len() > 1 {
        match &steps[0] {
            PathStep::Field(first)
                if first.eq_ignore_ascii_case("S3Object")
                    || alias.is_some_and(|a| a.eq_ignore_ascii_case(first)) =>
            {
                &steps[1..]
            }
            _ => steps,
        }
    } else {
        steps
    };

    match rec {
        Record::Csv { headers, fields } => {
            let [PathStep::Field(col)] = effective else {
                return SqlVal::Null;
            };
            if let Some(rest) = col.strip_prefix('_') {
                if let Ok(idx) = rest.parse::<usize>() {
                    if idx >= 1 {
                        return fields
                            .get(idx - 1)
                            .cloned()
                            .map(SqlVal::Str)
                            .unwrap_or(SqlVal::Null);
                    }
                }
            }
            if let Some(headers) = headers {
                if let Some(pos) = headers.iter().position(|h| h == col) {
                    return fields
                        .get(pos)
                        .cloned()
                        .map(SqlVal::Str)
                        .unwrap_or(SqlVal::Null);
                }
                if let Some(pos) = headers.iter().position(|h| h.eq_ignore_ascii_case(col)) {
                    return fields
                        .get(pos)
                        .cloned()
                        .map(SqlVal::Str)
                        .unwrap_or(SqlVal::Null);
                }
            }
            SqlVal::Null
        }
        Record::Json(root) => {
            let mut cur = root;
            for step in effective {
                match step {
                    PathStep::Field(name) => {
                        let Some(map) = cur.as_object() else {
                            return SqlVal::Null;
                        };
                        if let Some(next) = map.get(name) {
                            cur = next;
                        } else if let Some((_, next)) =
                            map.iter().find(|(k, _)| k.eq_ignore_ascii_case(name))
                        {
                            cur = next;
                        } else {
                            return SqlVal::Null;
                        }
                    }
                    PathStep::Index(idx) => {
                        let Some(arr) = cur.as_array() else {
                            return SqlVal::Null;
                        };
                        match arr.get(*idx) {
                            Some(next) => cur = next,
                            None => return SqlVal::Null,
                        }
                    }
                }
            }
            SqlVal::from_json(cur)
        }
        Record::Joined(sides) => resolve_joined_path(steps, sides),
    }
}

/// `<alias>.<path>` resolves in that table; an unqualified path resolves in
/// the first table that has it.
fn resolve_joined_path(steps: &[PathStep], sides: &[(String, Record)]) -> SqlVal {
    if let [PathStep::Field(first), rest @ ..] = steps {
        if !rest.is_empty() {
            if let Some((_, side)) = sides.iter().find(|(a, _)| a.eq_ignore_ascii_case(first)) {
                return resolve_path(rest, side, None);
            }
        }
    }
    sides
        .iter()
        .map(|(_, side)| resolve_path(steps, side, None))
        .find(|v| !v.is_null())
        .unwrap_or(SqlVal::Null)
}

fn compare_vals(a: &SqlVal, b: &SqlVal) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (SqlVal::Int(x), SqlVal::Int(y)) => Some(x.cmp(y)),
        (SqlVal::Bool(x), SqlVal::Bool(y)) => Some(x.cmp(y)),
        // Numeric comparison when either side is numeric and both parse as f64.
        (SqlVal::Int(_) | SqlVal::Float(_), _) | (_, SqlVal::Int(_) | SqlVal::Float(_)) => {
            let x = a.as_f64()?;
            let y = b.as_f64()?;
            x.partial_cmp(&y)
        }
        (SqlVal::Str(x), SqlVal::Str(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn sql_like(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((star_pi, star_ti)) = star {
            pi = star_pi + 1;
            ti = star_ti + 1;
            star = Some((star_pi, ti));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}
