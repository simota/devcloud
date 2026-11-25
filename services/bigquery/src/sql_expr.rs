//! Deterministic GoogleSQL expression parser and evaluator for SQL UDF bodies,
//! computed SELECT projections, and WHERE predicates.
//!
//! Error messages intentionally avoid echoing raw literals or query text so
//! error responses never leak sensitive query payloads.

use serde_json::Value;

use crate::model::{TableFieldSchema, TableSchema};
use crate::responses::default_string;
use crate::server::Server;
use crate::sql_eval::{is_json_null, raw_float, raw_int, raw_value_for_response};
use crate::wire_json;

pub const MAX_ROUTINE_DEPTH: i32 = 8;

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl SqlValue {
    pub fn from_raw_json(raw: &str, field: Option<&TableFieldSchema>) -> SqlValue {
        if raw.trim().is_empty() || is_json_null(raw) {
            return SqlValue::Null;
        }
        if let Some(field) = field {
            let field_type = default_string(field.field_type.clone(), "STRING").to_uppercase();
            match field_type.as_str() {
                "INTEGER" | "INT64" => {
                    if let Some(v) = raw_int(raw) {
                        return SqlValue::Int(v);
                    }
                }
                "FLOAT" | "FLOAT64" | "NUMERIC" | "BIGNUMERIC" => {
                    if let Some(v) = raw_float(raw) {
                        return SqlValue::Float(v);
                    }
                }
                "BOOLEAN" | "BOOL" => {
                    if let Ok(v) = serde_json::from_str::<bool>(raw) {
                        return SqlValue::Bool(v);
                    }
                    if let Ok(s) = serde_json::from_str::<String>(raw) {
                        if let Some(b) = parse_bool_str(&s) {
                            return SqlValue::Bool(b);
                        }
                    }
                }
                "STRING" | "BYTES" | "DATE" | "DATETIME" | "TIME" | "TIMESTAMP" | "GEOGRAPHY" => {
                    if let Ok(s) = serde_json::from_str::<String>(raw) {
                        return SqlValue::String(s);
                    }
                }
                _ => {}
            }
        }
        match raw_value_for_response(raw) {
            Value::Null => SqlValue::Null,
            Value::Bool(b) => SqlValue::Bool(b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    SqlValue::Int(i)
                } else if let Some(f) = n.as_f64() {
                    SqlValue::Float(f)
                } else {
                    SqlValue::String(n.to_string())
                }
            }
            Value::String(s) => SqlValue::String(s),
            other => SqlValue::String(other.to_string()),
        }
    }

    pub fn to_cell_value(&self, field_type: &str) -> Value {
        if matches!(self, SqlValue::Null) {
            return Value::Null;
        }
        let coerced = self
            .coerce_to_type(field_type)
            .unwrap_or_else(|_| self.clone());
        match (field_type.to_ascii_uppercase().as_str(), coerced) {
            (_, SqlValue::Null) => Value::Null,
            ("INTEGER" | "INT64", SqlValue::Int(i)) => Value::String(i.to_string()),
            ("BOOLEAN" | "BOOL", SqlValue::Bool(b)) => Value::String(b.to_string()),
            ("NUMERIC" | "BIGNUMERIC", SqlValue::Float(f)) => Value::String(format_float(f)),
            ("NUMERIC" | "BIGNUMERIC", SqlValue::Int(i)) => Value::String(i.to_string()),
            ("FLOAT" | "FLOAT64", SqlValue::Float(f)) => serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or_else(|| Value::String(format_float(f))),
            ("FLOAT" | "FLOAT64", SqlValue::Int(i)) => Value::Number(
                (i as f64)
                    .try_into()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .unwrap_or_else(|| serde_json::Number::from(i)),
            ),
            (_, SqlValue::String(s)) => Value::String(s),
            (_, SqlValue::Int(i)) => Value::String(i.to_string()),
            (_, SqlValue::Bool(b)) => Value::String(b.to_string()),
            (_, SqlValue::Float(f)) => serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or_else(|| Value::String(format_float(f))),
        }
    }

    pub fn to_raw_json(&self) -> String {
        match self {
            SqlValue::Null => "null".to_string(),
            SqlValue::Bool(b) => b.to_string(),
            SqlValue::Int(i) => i.to_string(),
            SqlValue::Float(f) => {
                if f.is_finite() {
                    format_float(*f)
                } else {
                    String::from_utf8(wire_json::marshal(&format_float(*f)))
                        .expect("json string is utf-8")
                }
            }
            SqlValue::String(s) => {
                String::from_utf8(wire_json::marshal(s)).expect("json string is utf-8")
            }
        }
    }

    pub fn to_sql_literal(&self) -> String {
        match self {
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Bool(true) => "TRUE".to_string(),
            SqlValue::Bool(false) => "FALSE".to_string(),
            SqlValue::Int(i) => i.to_string(),
            SqlValue::Float(f) => format_float(*f),
            SqlValue::String(s) => {
                String::from_utf8(wire_json::marshal(s)).expect("json string is utf-8")
            }
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            SqlValue::Int(i) => Some(*i as f64),
            SqlValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn coerce_to_type(&self, target_type: &str) -> Result<SqlValue, String> {
        if matches!(self, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        match target_type.trim().to_ascii_uppercase().as_str() {
            "" => Ok(self.clone()),
            "STRING" | "BYTES" | "DATE" | "DATETIME" | "TIME" | "TIMESTAMP" | "GEOGRAPHY"
            | "JSON" => Ok(SqlValue::String(self.to_display_string())),
            "INT64" | "INTEGER" => match self {
                SqlValue::Int(i) => Ok(SqlValue::Int(*i)),
                SqlValue::Float(f) if f.is_finite() => Ok(SqlValue::Int(f.round() as i64)),
                SqlValue::Bool(b) => Ok(SqlValue::Int(if *b { 1 } else { 0 })),
                SqlValue::String(s) => s
                    .trim()
                    .parse::<i64>()
                    .map(SqlValue::Int)
                    .map_err(|_| "cannot cast value to INT64".to_string()),
                _ => Err("cannot cast value to INT64".to_string()),
            },
            "FLOAT64" | "FLOAT" | "NUMERIC" | "BIGNUMERIC" => match self {
                SqlValue::Float(f) => Ok(SqlValue::Float(*f)),
                SqlValue::Int(i) => Ok(SqlValue::Float(*i as f64)),
                SqlValue::String(s) => s
                    .trim()
                    .parse::<f64>()
                    .map(SqlValue::Float)
                    .map_err(|_| "cannot cast value to FLOAT64".to_string()),
                _ => Err("cannot cast value to FLOAT64".to_string()),
            },
            "BOOL" | "BOOLEAN" => match self {
                SqlValue::Bool(b) => Ok(SqlValue::Bool(*b)),
                SqlValue::Int(i) => Ok(SqlValue::Bool(*i != 0)),
                SqlValue::String(s) => parse_bool_str(s)
                    .map(SqlValue::Bool)
                    .ok_or_else(|| "cannot cast value to BOOL".to_string()),
                _ => Err("cannot cast value to BOOL".to_string()),
            },
            _ => Err("unsupported cast target type".to_string()),
        }
    }

    pub fn to_display_string(&self) -> String {
        match self {
            SqlValue::Null => String::new(),
            SqlValue::Bool(b) => b.to_string(),
            SqlValue::Int(i) => i.to_string(),
            SqlValue::Float(f) => format_float(*f),
            SqlValue::String(s) => s.clone(),
        }
    }
}

pub fn standard_sql_to_legacy_type(type_kind: &str) -> String {
    match type_kind.trim().to_ascii_uppercase().as_str() {
        "INT64" | "INTEGER" => "INTEGER".to_string(),
        "FLOAT64" | "FLOAT" => "FLOAT".to_string(),
        "BOOL" | "BOOLEAN" => "BOOLEAN".to_string(),
        "STRING" => "STRING".to_string(),
        "BYTES" => "BYTES".to_string(),
        "NUMERIC" => "NUMERIC".to_string(),
        "BIGNUMERIC" => "BIGNUMERIC".to_string(),
        "DATE" => "DATE".to_string(),
        "DATETIME" => "DATETIME".to_string(),
        "TIME" => "TIME".to_string(),
        "TIMESTAMP" => "TIMESTAMP".to_string(),
        "GEOGRAPHY" => "GEOGRAPHY".to_string(),
        "JSON" => "JSON".to_string(),
        other if !other.is_empty() => other.to_string(),
        _ => "STRING".to_string(),
    }
}

fn parse_bool_str(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "1" => Some(true),
        "false" | "f" | "0" => Some(false),
        _ => None,
    }
}

fn format_float(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    if value == 0.0 {
        return "0".to_string();
    }
    format!("{value}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Pos,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

#[derive(Debug, Clone)]
pub enum SqlExpr {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Ident(String),
    UnaryOp {
        op: UnaryOp,
        expr: Box<SqlExpr>,
    },
    BinaryOp {
        op: BinaryOp,
        left: Box<SqlExpr>,
        right: Box<SqlExpr>,
    },
    IsNull {
        expr: Box<SqlExpr>,
        negated: bool,
    },
    IsBool {
        expr: Box<SqlExpr>,
        expected: bool,
        negated: bool,
    },
    Cast {
        expr: Box<SqlExpr>,
        target_type: String,
        safe: bool,
    },
    Case {
        operand: Option<Box<SqlExpr>>,
        when_clauses: Vec<(SqlExpr, SqlExpr)>,
        else_expr: Option<Box<SqlExpr>>,
    },
    FunctionCall {
        name_parts: Vec<String>,
        args: Vec<SqlExpr>,
    },
}

// --- Tokenizer --------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    BacktickIdent(String),
    Int(i64),
    Float(f64),
    StringLit(String),
    LParen,
    RParen,
    Comma,
    Dot,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Concat,
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        match b {
            b'(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            b')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            b'.' => {
                if i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
                    let (tok, next) = read_number(input, i)?;
                    tokens.push(tok);
                    i = next;
                } else {
                    tokens.push(Token::Dot);
                    i += 1;
                }
            }
            b'+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            b'-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            b'*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            b'/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            b'%' => {
                tokens.push(Token::Percent);
                i += 1;
            }
            b'|' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'|' {
                    tokens.push(Token::Concat);
                    i += 2;
                } else {
                    return Err("unsupported SQL operator".to_string());
                }
            }
            b'=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            b'!' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    tokens.push(Token::NotEq);
                    i += 2;
                } else {
                    return Err("unsupported SQL operator".to_string());
                }
            }
            b'<' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    tokens.push(Token::Le);
                    i += 2;
                } else if i + 1 < bytes.len() && bytes[i + 1] == b'>' {
                    tokens.push(Token::NotEq);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            b'>' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    tokens.push(Token::Ge);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            b'\'' | b'"' => {
                let (s, next) = read_string_literal(input, i, b)?;
                tokens.push(Token::StringLit(s));
                i = next;
            }
            b'`' => {
                let start = i + 1;
                let Some(end_rel) = input[start..].find('`') else {
                    return Err("unterminated quoted identifier".to_string());
                };
                let ident = input[start..start + end_rel].to_string();
                if ident.is_empty() {
                    return Err("empty quoted identifier".to_string());
                }
                tokens.push(Token::BacktickIdent(ident));
                i = start + end_rel + 1;
            }
            b'0'..=b'9' => {
                let (tok, next) = read_number(input, i)?;
                tokens.push(tok);
                i = next;
            }
            _ if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                i += 1;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                tokens.push(Token::Ident(input[start..i].to_string()));
            }
            _ => return Err("unsupported syntax in SQL expression".to_string()),
        }
    }
    Ok(tokens)
}

fn read_string_literal(input: &str, start: usize, quote: u8) -> Result<(String, usize), String> {
    let bytes = input.as_bytes();
    let mut out = String::new();
    let mut i = start + 1;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\\' {
            if i + 1 >= bytes.len() {
                return Err("unterminated string literal".to_string());
            }
            let esc = bytes[i + 1];
            match esc {
                b'n' => out.push('\n'),
                b'r' => out.push('\r'),
                b't' => out.push('\t'),
                b'\\' => out.push('\\'),
                b'\'' => out.push('\''),
                b'"' => out.push('"'),
                other => out.push(other as char),
            }
            i += 2;
            continue;
        }
        if b == quote {
            if i + 1 < bytes.len() && bytes[i + 1] == quote {
                out.push(quote as char);
                i += 2;
                continue;
            }
            return Ok((out, i + 1));
        }
        let ch = input[i..]
            .chars()
            .next()
            .ok_or_else(|| "unterminated string literal".to_string())?;
        out.push(ch);
        i += ch.len_utf8();
    }
    Err("unterminated string literal".to_string())
}

fn read_number(input: &str, start: usize) -> Result<(Token, usize), String> {
    let bytes = input.as_bytes();
    let mut i = start;
    let mut has_dot = false;
    let mut has_exp = false;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_digit() {
            i += 1;
        } else if b == b'.' && !has_dot && !has_exp {
            has_dot = true;
            i += 1;
        } else if (b == b'e' || b == b'E') && !has_exp {
            has_exp = true;
            i += 1;
            if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
                i += 1;
            }
        } else {
            break;
        }
    }
    let slice = &input[start..i];
    if has_dot || has_exp {
        let f = slice
            .parse::<f64>()
            .map_err(|_| "invalid numeric literal".to_string())?;
        Ok((Token::Float(f), i))
    } else {
        let n = slice
            .parse::<i64>()
            .map_err(|_| "invalid numeric literal".to_string())?;
        Ok((Token::Int(n), i))
    }
}

// --- Parser -----------------------------------------------------------------

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

pub fn parse_expr(input: &str) -> Result<SqlExpr, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("SQL expression is required".to_string());
    }
    let tokens = tokenize(trimmed)?;
    if tokens.is_empty() {
        return Err("SQL expression is required".to_string());
    }
    let mut parser = Parser { tokens, pos: 0 };
    let expr = parser.parse_or()?;
    if parser.pos < parser.tokens.len() {
        return Err("unexpected token in SQL expression".to_string());
    }
    Ok(expr)
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset)
    }

    fn next(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let tok = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(tok)
        } else {
            None
        }
    }

    fn peek_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    fn consume_keyword(&mut self, kw: &str) -> bool {
        if self.peek_keyword(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_token(&mut self, expected: &Token) -> Result<(), String> {
        match self.next() {
            Some(ref tok) if tok == expected => Ok(()),
            _ => Err("unexpected token in SQL expression".to_string()),
        }
    }

    fn parse_or(&mut self) -> Result<SqlExpr, String> {
        let mut left = self.parse_and()?;
        while self.consume_keyword("OR") {
            let right = self.parse_and()?;
            left = SqlExpr::BinaryOp {
                op: BinaryOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<SqlExpr, String> {
        let mut left = self.parse_not()?;
        while self.consume_keyword("AND") {
            let right = self.parse_not()?;
            left = SqlExpr::BinaryOp {
                op: BinaryOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<SqlExpr, String> {
        if self.consume_keyword("NOT") {
            let expr = self.parse_not()?;
            return Ok(SqlExpr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<SqlExpr, String> {
        let mut expr = self.parse_additive()?;
        loop {
            if self.consume_keyword("IS") {
                let negated = self.consume_keyword("NOT");
                if self.consume_keyword("NULL") {
                    expr = SqlExpr::IsNull {
                        expr: Box::new(expr),
                        negated,
                    };
                } else if self.consume_keyword("TRUE") {
                    expr = SqlExpr::IsBool {
                        expr: Box::new(expr),
                        expected: true,
                        negated,
                    };
                } else if self.consume_keyword("FALSE") {
                    expr = SqlExpr::IsBool {
                        expr: Box::new(expr),
                        expected: false,
                        negated,
                    };
                } else {
                    return Err("unsupported IS predicate in SQL expression".to_string());
                }
                continue;
            }
            let op = match self.peek() {
                Some(Token::Eq) => Some(BinaryOp::Eq),
                Some(Token::NotEq) => Some(BinaryOp::NotEq),
                Some(Token::Lt) => Some(BinaryOp::Lt),
                Some(Token::Le) => Some(BinaryOp::Le),
                Some(Token::Gt) => Some(BinaryOp::Gt),
                Some(Token::Ge) => Some(BinaryOp::Ge),
                _ => None,
            };
            if let Some(op) = op {
                self.next();
                let right = self.parse_additive()?;
                expr = SqlExpr::BinaryOp {
                    op,
                    left: Box::new(expr),
                    right: Box::new(right),
                };
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_additive(&mut self) -> Result<SqlExpr, String> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Some(Token::Plus) => Some(BinaryOp::Add),
                Some(Token::Minus) => Some(BinaryOp::Sub),
                Some(Token::Concat) => Some(BinaryOp::Concat),
                _ => None,
            };
            if let Some(op) = op {
                self.next();
                let right = self.parse_multiplicative()?;
                left = SqlExpr::BinaryOp {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<SqlExpr, String> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Some(Token::Star) => Some(BinaryOp::Mul),
                Some(Token::Slash) => Some(BinaryOp::Div),
                Some(Token::Percent) => Some(BinaryOp::Mod),
                _ => None,
            };
            if let Some(op) = op {
                self.next();
                let right = self.parse_unary()?;
                left = SqlExpr::BinaryOp {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<SqlExpr, String> {
        match self.peek() {
            Some(Token::Minus) => {
                self.next();
                let expr = self.parse_unary()?;
                Ok(SqlExpr::UnaryOp {
                    op: UnaryOp::Neg,
                    expr: Box::new(expr),
                })
            }
            Some(Token::Plus) => {
                self.next();
                let expr = self.parse_unary()?;
                Ok(SqlExpr::UnaryOp {
                    op: UnaryOp::Pos,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<SqlExpr, String> {
        let Some(tok) = self.peek().cloned() else {
            return Err("unexpected end of SQL expression".to_string());
        };
        match tok {
            Token::Int(n) => {
                self.next();
                Ok(SqlExpr::Int(n))
            }
            Token::Float(f) => {
                self.next();
                Ok(SqlExpr::Float(f))
            }
            Token::StringLit(s) => {
                self.next();
                Ok(SqlExpr::String(s))
            }
            Token::LParen => {
                self.next();
                let inner = self.parse_or()?;
                self.expect_token(&Token::RParen)?;
                Ok(inner)
            }
            Token::Ident(ref s) if s.eq_ignore_ascii_case("NULL") => {
                self.next();
                Ok(SqlExpr::Null)
            }
            Token::Ident(ref s) if s.eq_ignore_ascii_case("TRUE") => {
                self.next();
                Ok(SqlExpr::Bool(true))
            }
            Token::Ident(ref s) if s.eq_ignore_ascii_case("FALSE") => {
                self.next();
                Ok(SqlExpr::Bool(false))
            }
            Token::Ident(ref s) if s.eq_ignore_ascii_case("CASE") => self.parse_case(),
            Token::Ident(ref s)
                if (s.eq_ignore_ascii_case("CAST") || s.eq_ignore_ascii_case("SAFE_CAST"))
                    && matches!(self.peek_at(1), Some(Token::LParen)) =>
            {
                let safe = s.eq_ignore_ascii_case("SAFE_CAST");
                self.next();
                self.expect_token(&Token::LParen)?;
                let expr = self.parse_or()?;
                if !self.consume_keyword("AS") {
                    return Err("CAST requires AS <type>".to_string());
                }
                let target_type = match self.next() {
                    Some(Token::Ident(t)) | Some(Token::BacktickIdent(t)) => t,
                    _ => return Err("CAST requires a target type".to_string()),
                };
                self.expect_token(&Token::RParen)?;
                Ok(SqlExpr::Cast {
                    expr: Box::new(expr),
                    target_type,
                    safe,
                })
            }
            Token::Ident(_) | Token::BacktickIdent(_) => self.parse_ident_or_call(),
            _ => Err("unexpected token in SQL expression".to_string()),
        }
    }

    fn parse_case(&mut self) -> Result<SqlExpr, String> {
        self.next(); // consume CASE
        let operand = if self.peek_keyword("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_or()?))
        };
        let mut when_clauses = Vec::new();
        while self.consume_keyword("WHEN") {
            let cond = self.parse_or()?;
            if !self.consume_keyword("THEN") {
                return Err("CASE WHEN requires THEN".to_string());
            }
            let result = self.parse_or()?;
            when_clauses.push((cond, result));
        }
        if when_clauses.is_empty() {
            return Err("CASE requires at least one WHEN clause".to_string());
        }
        let else_expr = if self.consume_keyword("ELSE") {
            Some(Box::new(self.parse_or()?))
        } else {
            None
        };
        if !self.consume_keyword("END") {
            return Err("CASE requires END".to_string());
        }
        Ok(SqlExpr::Case {
            operand,
            when_clauses,
            else_expr,
        })
    }

    fn parse_ident_or_call(&mut self) -> Result<SqlExpr, String> {
        let mut parts = Vec::new();
        let first_was_backtick;
        match self.next() {
            Some(Token::Ident(s)) => {
                first_was_backtick = false;
                parts.push(s);
            }
            Some(Token::BacktickIdent(s)) => {
                first_was_backtick = true;
                parts.push(s);
            }
            _ => return Err("expected identifier".to_string()),
        }
        while matches!(self.peek(), Some(Token::Dot)) {
            self.next();
            match self.next() {
                Some(Token::Ident(s)) | Some(Token::BacktickIdent(s)) => parts.push(s),
                _ => return Err("expected identifier after '.'".to_string()),
            }
        }
        if matches!(self.peek(), Some(Token::LParen)) {
            self.next(); // consume '('
            let mut args = Vec::new();
            if !matches!(self.peek(), Some(Token::RParen)) {
                loop {
                    args.push(self.parse_or()?);
                    if matches!(self.peek(), Some(Token::Comma)) {
                        self.next();
                    } else {
                        break;
                    }
                }
            }
            self.expect_token(&Token::RParen)?;
            let name_parts = if parts.len() == 1 && first_was_backtick && parts[0].contains('.') {
                parts[0].split('.').map(str::to_string).collect()
            } else {
                parts
            };
            return Ok(SqlExpr::FunctionCall { name_parts, args });
        }
        if parts.len() == 1 {
            Ok(SqlExpr::Ident(parts.into_iter().next().unwrap()))
        } else {
            Err("qualified column references are not supported in expressions".to_string())
        }
    }
}

// --- Evaluation -------------------------------------------------------------

pub fn eval_expr(
    server: &Server,
    default_project_id: &str,
    expr: &SqlExpr,
    resolve_ident: &dyn Fn(&str) -> Option<SqlValue>,
    depth: i32,
) -> Result<SqlValue, String> {
    match expr {
        SqlExpr::Null => Ok(SqlValue::Null),
        SqlExpr::Bool(b) => Ok(SqlValue::Bool(*b)),
        SqlExpr::Int(n) => Ok(SqlValue::Int(*n)),
        SqlExpr::Float(f) => Ok(SqlValue::Float(*f)),
        SqlExpr::String(s) => Ok(SqlValue::String(s.clone())),
        SqlExpr::Ident(name) => {
            resolve_ident(name).ok_or_else(|| format!("unknown identifier {name:?}"))
        }
        SqlExpr::UnaryOp { op, expr } => {
            let val = eval_expr(server, default_project_id, expr, resolve_ident, depth)?;
            match op {
                UnaryOp::Pos => match val {
                    SqlValue::Null => Ok(SqlValue::Null),
                    SqlValue::Int(i) => Ok(SqlValue::Int(i)),
                    SqlValue::Float(f) => Ok(SqlValue::Float(f)),
                    _ => Err("unary '+' requires a numeric operand".to_string()),
                },
                UnaryOp::Neg => match val {
                    SqlValue::Null => Ok(SqlValue::Null),
                    SqlValue::Int(i) => Ok(SqlValue::Int(i.wrapping_neg())),
                    SqlValue::Float(f) => Ok(SqlValue::Float(-f)),
                    _ => Err("unary '-' requires a numeric operand".to_string()),
                },
                UnaryOp::Not => match val {
                    SqlValue::Null => Ok(SqlValue::Null),
                    SqlValue::Bool(b) => Ok(SqlValue::Bool(!b)),
                    _ => Err("NOT requires a boolean operand".to_string()),
                },
            }
        }
        SqlExpr::BinaryOp { op, left, right } => eval_binary(
            server,
            default_project_id,
            *op,
            left,
            right,
            resolve_ident,
            depth,
        ),
        SqlExpr::IsNull { expr, negated } => {
            let val = eval_expr(server, default_project_id, expr, resolve_ident, depth)?;
            let is_null = matches!(val, SqlValue::Null);
            Ok(SqlValue::Bool(if *negated { !is_null } else { is_null }))
        }
        SqlExpr::IsBool {
            expr,
            expected,
            negated,
        } => {
            let val = eval_expr(server, default_project_id, expr, resolve_ident, depth)?;
            let matches_expected = matches!(val, SqlValue::Bool(b) if b == *expected);
            Ok(SqlValue::Bool(if *negated {
                !matches_expected
            } else {
                matches_expected
            }))
        }
        SqlExpr::Cast {
            expr,
            target_type,
            safe,
        } => {
            let val = eval_expr(server, default_project_id, expr, resolve_ident, depth)?;
            match val.coerce_to_type(target_type) {
                Ok(v) => Ok(v),
                Err(_) if *safe => Ok(SqlValue::Null),
                Err(e) => Err(e),
            }
        }
        SqlExpr::Case {
            operand,
            when_clauses,
            else_expr,
        } => {
            let target = match operand {
                Some(op_expr) => Some(eval_expr(
                    server,
                    default_project_id,
                    op_expr,
                    resolve_ident,
                    depth,
                )?),
                None => None,
            };
            for (when_expr, then_expr) in when_clauses {
                let matched = match &target {
                    Some(t) => {
                        let w =
                            eval_expr(server, default_project_id, when_expr, resolve_ident, depth)?;
                        compare_values(t, &w) == Some(std::cmp::Ordering::Equal)
                    }
                    None => {
                        let c =
                            eval_expr(server, default_project_id, when_expr, resolve_ident, depth)?;
                        matches!(c, SqlValue::Bool(true))
                    }
                };
                if matched {
                    return eval_expr(server, default_project_id, then_expr, resolve_ident, depth);
                }
            }
            match else_expr {
                Some(el) => eval_expr(server, default_project_id, el, resolve_ident, depth),
                None => Ok(SqlValue::Null),
            }
        }
        SqlExpr::FunctionCall { name_parts, args } => eval_function_call(
            server,
            default_project_id,
            name_parts,
            args,
            resolve_ident,
            depth,
        ),
    }
}

fn eval_binary(
    server: &Server,
    default_project_id: &str,
    op: BinaryOp,
    left: &SqlExpr,
    right: &SqlExpr,
    resolve_ident: &dyn Fn(&str) -> Option<SqlValue>,
    depth: i32,
) -> Result<SqlValue, String> {
    if op == BinaryOp::And {
        let lv = eval_expr(server, default_project_id, left, resolve_ident, depth)?;
        if matches!(lv, SqlValue::Bool(false)) {
            return Ok(SqlValue::Bool(false));
        }
        let rv = eval_expr(server, default_project_id, right, resolve_ident, depth)?;
        return match (lv, rv) {
            (SqlValue::Bool(true), SqlValue::Bool(b)) => Ok(SqlValue::Bool(b)),
            (SqlValue::Null, SqlValue::Bool(false)) => Ok(SqlValue::Bool(false)),
            (SqlValue::Bool(true), SqlValue::Null)
            | (SqlValue::Null, SqlValue::Bool(true))
            | (SqlValue::Null, SqlValue::Null) => Ok(SqlValue::Null),
            _ => Err("AND requires boolean operands".to_string()),
        };
    }
    if op == BinaryOp::Or {
        let lv = eval_expr(server, default_project_id, left, resolve_ident, depth)?;
        if matches!(lv, SqlValue::Bool(true)) {
            return Ok(SqlValue::Bool(true));
        }
        let rv = eval_expr(server, default_project_id, right, resolve_ident, depth)?;
        return match (lv, rv) {
            (SqlValue::Bool(false), SqlValue::Bool(b)) => Ok(SqlValue::Bool(b)),
            (SqlValue::Null, SqlValue::Bool(true)) => Ok(SqlValue::Bool(true)),
            (SqlValue::Bool(false), SqlValue::Null)
            | (SqlValue::Null, SqlValue::Bool(false))
            | (SqlValue::Null, SqlValue::Null) => Ok(SqlValue::Null),
            _ => Err("OR requires boolean operands".to_string()),
        };
    }

    let lv = eval_expr(server, default_project_id, left, resolve_ident, depth)?;
    let rv = eval_expr(server, default_project_id, right, resolve_ident, depth)?;
    if matches!(lv, SqlValue::Null) || matches!(rv, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    match op {
        BinaryOp::Concat => match (lv, rv) {
            (SqlValue::String(a), SqlValue::String(b)) => Ok(SqlValue::String(format!("{a}{b}"))),
            _ => Err("|| requires string operands".to_string()),
        },
        BinaryOp::Add => match (lv, rv) {
            (SqlValue::Int(a), SqlValue::Int(b)) => Ok(SqlValue::Int(a.wrapping_add(b))),
            (a, b) => match (a.as_f64(), b.as_f64()) {
                (Some(x), Some(y)) => Ok(SqlValue::Float(x + y)),
                _ => Err("+ requires numeric operands".to_string()),
            },
        },
        BinaryOp::Sub => match (lv, rv) {
            (SqlValue::Int(a), SqlValue::Int(b)) => Ok(SqlValue::Int(a.wrapping_sub(b))),
            (a, b) => match (a.as_f64(), b.as_f64()) {
                (Some(x), Some(y)) => Ok(SqlValue::Float(x - y)),
                _ => Err("- requires numeric operands".to_string()),
            },
        },
        BinaryOp::Mul => match (lv, rv) {
            (SqlValue::Int(a), SqlValue::Int(b)) => Ok(SqlValue::Int(a.wrapping_mul(b))),
            (a, b) => match (a.as_f64(), b.as_f64()) {
                (Some(x), Some(y)) => Ok(SqlValue::Float(x * y)),
                _ => Err("* requires numeric operands".to_string()),
            },
        },
        BinaryOp::Div => match (lv.as_f64(), rv.as_f64()) {
            (Some(_), Some(y)) if y == 0.0 => Err("division by zero".to_string()),
            (Some(x), Some(y)) => Ok(SqlValue::Float(x / y)),
            _ => Err("/ requires numeric operands".to_string()),
        },
        BinaryOp::Mod => match (lv, rv) {
            (SqlValue::Int(_), SqlValue::Int(0)) => Err("division by zero".to_string()),
            (SqlValue::Int(a), SqlValue::Int(b)) => Ok(SqlValue::Int(a % b)),
            _ => Err("% requires integer operands".to_string()),
        },
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::Le
        | BinaryOp::Gt
        | BinaryOp::Ge => {
            let Some(ord) = compare_values(&lv, &rv) else {
                return Err("incompatible operand types in comparison".to_string());
            };
            let result = match op {
                BinaryOp::Eq => ord == std::cmp::Ordering::Equal,
                BinaryOp::NotEq => ord != std::cmp::Ordering::Equal,
                BinaryOp::Lt => ord == std::cmp::Ordering::Less,
                BinaryOp::Le => ord != std::cmp::Ordering::Greater,
                BinaryOp::Gt => ord == std::cmp::Ordering::Greater,
                BinaryOp::Ge => ord != std::cmp::Ordering::Less,
                _ => unreachable!(),
            };
            Ok(SqlValue::Bool(result))
        }
        BinaryOp::And | BinaryOp::Or => unreachable!(),
    }
}

pub fn compare_values(left: &SqlValue, right: &SqlValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => None,
        (SqlValue::Int(a), SqlValue::Int(b)) => Some(a.cmp(b)),
        (SqlValue::Float(a), SqlValue::Float(b)) => {
            Some(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        }
        (SqlValue::Int(a), SqlValue::Float(b)) => Some(
            (*a as f64)
                .partial_cmp(b)
                .unwrap_or(std::cmp::Ordering::Equal),
        ),
        (SqlValue::Float(a), SqlValue::Int(b)) => Some(
            a.partial_cmp(&(*b as f64))
                .unwrap_or(std::cmp::Ordering::Equal),
        ),
        (SqlValue::String(a), SqlValue::String(b)) => Some(a.cmp(b)),
        (SqlValue::Bool(a), SqlValue::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn eval_function_call(
    server: &Server,
    default_project_id: &str,
    name_parts: &[String],
    args: &[SqlExpr],
    resolve_ident: &dyn Fn(&str) -> Option<SqlValue>,
    depth: i32,
) -> Result<SqlValue, String> {
    if name_parts.len() == 2 || name_parts.len() == 3 {
        let mut eval_args = Vec::with_capacity(args.len());
        for arg in args {
            eval_args.push(eval_expr(
                server,
                default_project_id,
                arg,
                resolve_ident,
                depth,
            )?);
        }
        return server.eval_scalar_routine(name_parts, default_project_id, &eval_args, depth);
    }
    if name_parts.len() != 1 {
        return Err("unsupported function reference".to_string());
    }
    let func = name_parts[0].to_ascii_uppercase();

    // Short-circuiting built-ins.
    match func.as_str() {
        "IF" => {
            if args.len() != 3 {
                return Err("IF requires 3 arguments".to_string());
            }
            let cond = eval_expr(server, default_project_id, &args[0], resolve_ident, depth)?;
            return if matches!(cond, SqlValue::Bool(true)) {
                eval_expr(server, default_project_id, &args[1], resolve_ident, depth)
            } else {
                eval_expr(server, default_project_id, &args[2], resolve_ident, depth)
            };
        }
        "COALESCE" => {
            if args.is_empty() {
                return Err("COALESCE requires at least 1 argument".to_string());
            }
            for arg in args {
                let val = eval_expr(server, default_project_id, arg, resolve_ident, depth)?;
                if !matches!(val, SqlValue::Null) {
                    return Ok(val);
                }
            }
            return Ok(SqlValue::Null);
        }
        "IFNULL" => {
            if args.len() != 2 {
                return Err("IFNULL requires 2 arguments".to_string());
            }
            let first = eval_expr(server, default_project_id, &args[0], resolve_ident, depth)?;
            return if matches!(first, SqlValue::Null) {
                eval_expr(server, default_project_id, &args[1], resolve_ident, depth)
            } else {
                Ok(first)
            };
        }
        _ => {}
    }

    let mut vals = Vec::with_capacity(args.len());
    for arg in args {
        vals.push(eval_expr(
            server,
            default_project_id,
            arg,
            resolve_ident,
            depth,
        )?);
    }

    eval_builtin_function(&func, &vals)
}

fn eval_builtin_function(func: &str, vals: &[SqlValue]) -> Result<SqlValue, String> {
    match func {
        "NULLIF" => {
            expect_arity(func, vals, 2)?;
            if matches!(vals[0], SqlValue::Null) {
                return Ok(SqlValue::Null);
            }
            if compare_values(&vals[0], &vals[1]) == Some(std::cmp::Ordering::Equal) {
                Ok(SqlValue::Null)
            } else {
                Ok(vals[0].clone())
            }
        }
        "LOWER" => {
            expect_arity(func, vals, 1)?;
            match &vals[0] {
                SqlValue::Null => Ok(SqlValue::Null),
                SqlValue::String(s) => Ok(SqlValue::String(s.to_lowercase())),
                _ => Err("LOWER requires a STRING argument".to_string()),
            }
        }
        "UPPER" => {
            expect_arity(func, vals, 1)?;
            match &vals[0] {
                SqlValue::Null => Ok(SqlValue::Null),
                SqlValue::String(s) => Ok(SqlValue::String(s.to_uppercase())),
                _ => Err("UPPER requires a STRING argument".to_string()),
            }
        }
        "TRIM" | "LTRIM" | "RTRIM" => {
            if vals.is_empty() || vals.len() > 2 {
                return Err(format!("{func} requires 1 or 2 arguments"));
            }
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            let SqlValue::String(s) = &vals[0] else {
                return Err(format!("{func} requires a STRING argument"));
            };
            let result = if vals.len() == 2 {
                let SqlValue::String(chars) = &vals[1] else {
                    return Err(format!("{func} characters argument must be STRING"));
                };
                let set: Vec<char> = chars.chars().collect();
                match func {
                    "TRIM" => s.trim_matches(|c| set.contains(&c)).to_string(),
                    "LTRIM" => s.trim_start_matches(|c| set.contains(&c)).to_string(),
                    _ => s.trim_end_matches(|c| set.contains(&c)).to_string(),
                }
            } else {
                match func {
                    "TRIM" => s.trim().to_string(),
                    "LTRIM" => s.trim_start().to_string(),
                    _ => s.trim_end().to_string(),
                }
            };
            Ok(SqlValue::String(result))
        }
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            expect_arity(func, vals, 1)?;
            match &vals[0] {
                SqlValue::Null => Ok(SqlValue::Null),
                SqlValue::String(s) => Ok(SqlValue::Int(s.chars().count() as i64)),
                _ => Err(format!("{func} requires a STRING argument")),
            }
        }
        "CONCAT" => {
            if vals.is_empty() {
                return Err("CONCAT requires at least 1 argument".to_string());
            }
            let mut out = String::new();
            for v in vals {
                match v {
                    SqlValue::Null => return Ok(SqlValue::Null),
                    SqlValue::String(s) => out.push_str(s),
                    _ => return Err("CONCAT requires STRING arguments".to_string()),
                }
            }
            Ok(SqlValue::String(out))
        }
        "SUBSTR" | "SUBSTRING" => {
            if vals.len() < 2 || vals.len() > 3 {
                return Err(format!("{func} requires 2 or 3 arguments"));
            }
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            let SqlValue::String(s) = &vals[0] else {
                return Err(format!("{func} requires a STRING first argument"));
            };
            let SqlValue::Int(pos) = vals[1] else {
                return Err(format!("{func} position must be INT64"));
            };
            let chars: Vec<char> = s.chars().collect();
            let len = chars.len() as i64;
            let start = if pos > 0 {
                (pos - 1).min(len) as usize
            } else if pos < 0 {
                (len + pos).max(0) as usize
            } else {
                0
            };
            let sub: String = if vals.len() == 3 {
                let SqlValue::Int(take) = vals[2] else {
                    return Err(format!("{func} length must be INT64"));
                };
                if take < 0 {
                    return Err(format!("{func} length must be non-negative"));
                }
                chars.into_iter().skip(start).take(take as usize).collect()
            } else {
                chars.into_iter().skip(start).collect()
            };
            Ok(SqlValue::String(sub))
        }
        "REPLACE" => {
            expect_arity(func, vals, 3)?;
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            match (&vals[0], &vals[1], &vals[2]) {
                (SqlValue::String(s), SqlValue::String(from), SqlValue::String(to)) => {
                    Ok(SqlValue::String(s.replace(from.as_str(), to.as_str())))
                }
                _ => Err("REPLACE requires STRING arguments".to_string()),
            }
        }
        "STARTS_WITH" => {
            expect_arity(func, vals, 2)?;
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            match (&vals[0], &vals[1]) {
                (SqlValue::String(s), SqlValue::String(prefix)) => {
                    Ok(SqlValue::Bool(s.starts_with(prefix.as_str())))
                }
                _ => Err("STARTS_WITH requires STRING arguments".to_string()),
            }
        }
        "ENDS_WITH" => {
            expect_arity(func, vals, 2)?;
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            match (&vals[0], &vals[1]) {
                (SqlValue::String(s), SqlValue::String(suffix)) => {
                    Ok(SqlValue::Bool(s.ends_with(suffix.as_str())))
                }
                _ => Err("ENDS_WITH requires STRING arguments".to_string()),
            }
        }
        "CONTAINS_SUBSTR" => {
            expect_arity(func, vals, 2)?;
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            match (&vals[0], &vals[1]) {
                (SqlValue::String(s), SqlValue::String(needle)) => Ok(SqlValue::Bool(
                    s.to_lowercase().contains(&needle.to_lowercase()),
                )),
                _ => Err("CONTAINS_SUBSTR requires STRING arguments".to_string()),
            }
        }
        "REVERSE" => {
            expect_arity(func, vals, 1)?;
            match &vals[0] {
                SqlValue::Null => Ok(SqlValue::Null),
                SqlValue::String(s) => Ok(SqlValue::String(s.chars().rev().collect())),
                _ => Err("REVERSE requires a STRING argument".to_string()),
            }
        }
        "LEFT" | "RIGHT" => {
            expect_arity(func, vals, 2)?;
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            let SqlValue::String(s) = &vals[0] else {
                return Err(format!("{func} requires a STRING first argument"));
            };
            let SqlValue::Int(n) = vals[1] else {
                return Err(format!("{func} length must be INT64"));
            };
            if n < 0 {
                return Err(format!("{func} length must be non-negative"));
            }
            let chars: Vec<char> = s.chars().collect();
            let total = chars.len();
            let count = (n as usize).min(total);
            let out: String = if func == "LEFT" {
                chars.into_iter().take(count).collect()
            } else {
                chars.into_iter().skip(total - count).collect()
            };
            Ok(SqlValue::String(out))
        }
        "LPAD" | "RPAD" => {
            if vals.len() < 2 || vals.len() > 3 {
                return Err(format!("{func} requires 2 or 3 arguments"));
            }
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            let SqlValue::String(s) = &vals[0] else {
                return Err(format!("{func} requires a STRING first argument"));
            };
            let SqlValue::Int(target) = vals[1] else {
                return Err(format!("{func} length must be INT64"));
            };
            if target < 0 {
                return Err(format!("{func} length must be non-negative"));
            }
            let pad = if vals.len() == 3 {
                let SqlValue::String(p) = &vals[2] else {
                    return Err(format!("{func} pattern must be STRING"));
                };
                if p.is_empty() {
                    return Err(format!("{func} pattern cannot be empty"));
                }
                p.as_str()
            } else {
                " "
            };
            let target = target as usize;
            let chars: Vec<char> = s.chars().collect();
            if chars.len() >= target {
                return Ok(SqlValue::String(chars.into_iter().take(target).collect()));
            }
            let needed = target - chars.len();
            let pad_chars: Vec<char> = pad.chars().collect();
            let padding: String = (0..needed)
                .map(|idx| pad_chars[idx % pad_chars.len()])
                .collect();
            let result = if func == "LPAD" {
                format!("{padding}{s}")
            } else {
                format!("{s}{padding}")
            };
            Ok(SqlValue::String(result))
        }
        "ABS" => {
            expect_arity(func, vals, 1)?;
            match vals[0] {
                SqlValue::Null => Ok(SqlValue::Null),
                SqlValue::Int(i) => Ok(SqlValue::Int(i.wrapping_abs())),
                SqlValue::Float(f) => Ok(SqlValue::Float(f.abs())),
                _ => Err("ABS requires a numeric argument".to_string()),
            }
        }
        "ROUND" => {
            if vals.is_empty() || vals.len() > 2 {
                return Err("ROUND requires 1 or 2 arguments".to_string());
            }
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            let Some(x) = vals[0].as_f64() else {
                return Err("ROUND requires a numeric first argument".to_string());
            };
            let digits = if vals.len() == 2 {
                let SqlValue::Int(d) = vals[1] else {
                    return Err("ROUND digits must be INT64".to_string());
                };
                d
            } else {
                0
            };
            let factor = 10f64.powi(digits as i32);
            Ok(SqlValue::Float((x * factor).round() / factor))
        }
        "FLOOR" => {
            expect_arity(func, vals, 1)?;
            match vals[0] {
                SqlValue::Null => Ok(SqlValue::Null),
                SqlValue::Int(i) => Ok(SqlValue::Float(i as f64)),
                SqlValue::Float(f) => Ok(SqlValue::Float(f.floor())),
                _ => Err("FLOOR requires a numeric argument".to_string()),
            }
        }
        "CEIL" | "CEILING" => {
            expect_arity(func, vals, 1)?;
            match vals[0] {
                SqlValue::Null => Ok(SqlValue::Null),
                SqlValue::Int(i) => Ok(SqlValue::Float(i as f64)),
                SqlValue::Float(f) => Ok(SqlValue::Float(f.ceil())),
                _ => Err(format!("{func} requires a numeric argument")),
            }
        }
        "MOD" => {
            expect_arity(func, vals, 2)?;
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            match (&vals[0], &vals[1]) {
                (SqlValue::Int(_), SqlValue::Int(0)) => Err("division by zero".to_string()),
                (SqlValue::Int(a), SqlValue::Int(b)) => Ok(SqlValue::Int(a % b)),
                _ => Err("MOD requires INT64 arguments".to_string()),
            }
        }
        "POW" | "POWER" => {
            expect_arity(func, vals, 2)?;
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            match (vals[0].as_f64(), vals[1].as_f64()) {
                (Some(a), Some(b)) => Ok(SqlValue::Float(a.powf(b))),
                _ => Err(format!("{func} requires numeric arguments")),
            }
        }
        "SQRT" => {
            expect_arity(func, vals, 1)?;
            match vals[0] {
                SqlValue::Null => Ok(SqlValue::Null),
                _ => match vals[0].as_f64() {
                    Some(x) if x < 0.0 => Err("SQRT argument must be non-negative".to_string()),
                    Some(x) => Ok(SqlValue::Float(x.sqrt())),
                    None => Err("SQRT requires a numeric argument".to_string()),
                },
            }
        }
        "SAFE_DIVIDE" => {
            expect_arity(func, vals, 2)?;
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            match (vals[0].as_f64(), vals[1].as_f64()) {
                (Some(_), Some(y)) if y == 0.0 => Ok(SqlValue::Null),
                (Some(x), Some(y)) => Ok(SqlValue::Float(x / y)),
                _ => Err("SAFE_DIVIDE requires numeric arguments".to_string()),
            }
        }
        "GREATEST" | "LEAST" => {
            if vals.is_empty() {
                return Err(format!("{func} requires at least 1 argument"));
            }
            if vals.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(SqlValue::Null);
            }
            let mut best = vals[0].clone();
            for v in &vals[1..] {
                let Some(ord) = compare_values(v, &best) else {
                    return Err(format!("{func} arguments must have comparable types"));
                };
                if (func == "GREATEST" && ord == std::cmp::Ordering::Greater)
                    || (func == "LEAST" && ord == std::cmp::Ordering::Less)
                {
                    best = v.clone();
                }
            }
            Ok(best)
        }
        _ => Err("unsupported SQL function".to_string()),
    }
}

fn expect_arity(func: &str, vals: &[SqlValue], expected: usize) -> Result<(), String> {
    if vals.len() != expected {
        return Err(format!("{func} requires {expected} argument(s)"));
    }
    Ok(())
}

// --- Static Validation & Type Inference (for dryRun) ------------------------

pub fn validate_and_infer_expr_type(
    server: &Server,
    default_project_id: &str,
    expr: &SqlExpr,
    resolve_ident_type: &dyn Fn(&str) -> Option<String>,
    depth: i32,
) -> Result<String, String> {
    match expr {
        SqlExpr::Null => Ok("STRING".to_string()),
        SqlExpr::Bool(_) => Ok("BOOLEAN".to_string()),
        SqlExpr::Int(_) => Ok("INTEGER".to_string()),
        SqlExpr::Float(_) => Ok("FLOAT".to_string()),
        SqlExpr::String(_) => Ok("STRING".to_string()),
        SqlExpr::Ident(name) => resolve_ident_type(name)
            .ok_or_else(|| format!("selected field {name:?} does not exist")),
        SqlExpr::UnaryOp { op, expr } => {
            let t = validate_and_infer_expr_type(
                server,
                default_project_id,
                expr,
                resolve_ident_type,
                depth,
            )?;
            match op {
                UnaryOp::Not => Ok("BOOLEAN".to_string()),
                UnaryOp::Neg | UnaryOp::Pos => Ok(t),
            }
        }
        SqlExpr::BinaryOp { op, left, right } => {
            let lt = validate_and_infer_expr_type(
                server,
                default_project_id,
                left,
                resolve_ident_type,
                depth,
            )?;
            let rt = validate_and_infer_expr_type(
                server,
                default_project_id,
                right,
                resolve_ident_type,
                depth,
            )?;
            match op {
                BinaryOp::Concat => Ok("STRING".to_string()),
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge
                | BinaryOp::And
                | BinaryOp::Or => Ok("BOOLEAN".to_string()),
                BinaryOp::Div => Ok("FLOAT".to_string()),
                BinaryOp::Mod => Ok("INTEGER".to_string()),
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul => {
                    if lt == "FLOAT" || rt == "FLOAT" {
                        Ok("FLOAT".to_string())
                    } else {
                        Ok("INTEGER".to_string())
                    }
                }
            }
        }
        SqlExpr::IsNull { expr, .. } | SqlExpr::IsBool { expr, .. } => {
            validate_and_infer_expr_type(
                server,
                default_project_id,
                expr,
                resolve_ident_type,
                depth,
            )?;
            Ok("BOOLEAN".to_string())
        }
        SqlExpr::Cast {
            expr, target_type, ..
        } => {
            validate_and_infer_expr_type(
                server,
                default_project_id,
                expr,
                resolve_ident_type,
                depth,
            )?;
            Ok(standard_sql_to_legacy_type(target_type))
        }
        SqlExpr::Case {
            operand,
            when_clauses,
            else_expr,
        } => {
            if let Some(op) = operand {
                validate_and_infer_expr_type(
                    server,
                    default_project_id,
                    op,
                    resolve_ident_type,
                    depth,
                )?;
            }
            let mut inferred = None;
            for (w, t) in when_clauses {
                validate_and_infer_expr_type(
                    server,
                    default_project_id,
                    w,
                    resolve_ident_type,
                    depth,
                )?;
                let tt = validate_and_infer_expr_type(
                    server,
                    default_project_id,
                    t,
                    resolve_ident_type,
                    depth,
                )?;
                if inferred.is_none() {
                    inferred = Some(tt);
                }
            }
            if let Some(el) = else_expr {
                let et = validate_and_infer_expr_type(
                    server,
                    default_project_id,
                    el,
                    resolve_ident_type,
                    depth,
                )?;
                if inferred.is_none() {
                    inferred = Some(et);
                }
            }
            Ok(inferred.unwrap_or_else(|| "STRING".to_string()))
        }
        SqlExpr::FunctionCall { name_parts, args } => {
            let mut arg_types = Vec::with_capacity(args.len());
            for arg in args {
                arg_types.push(validate_and_infer_expr_type(
                    server,
                    default_project_id,
                    arg,
                    resolve_ident_type,
                    depth,
                )?);
            }
            if name_parts.len() == 2 || name_parts.len() == 3 {
                return server.validate_scalar_routine(
                    name_parts,
                    default_project_id,
                    args.len(),
                    depth,
                );
            }
            if name_parts.len() != 1 {
                return Err("unsupported function reference".to_string());
            }
            infer_builtin_type(&name_parts[0].to_ascii_uppercase(), &arg_types)
        }
    }
}

fn infer_builtin_type(func: &str, arg_types: &[String]) -> Result<String, String> {
    match func {
        "IF" => {
            if arg_types.len() != 3 {
                return Err("IF requires 3 arguments".to_string());
            }
            Ok(arg_types[1].clone())
        }
        "COALESCE" => {
            if arg_types.is_empty() {
                return Err("COALESCE requires at least 1 argument".to_string());
            }
            Ok(arg_types[0].clone())
        }
        "IFNULL" | "NULLIF" => {
            if arg_types.len() != 2 {
                return Err(format!("{func} requires 2 arguments"));
            }
            Ok(arg_types[0].clone())
        }
        "LOWER" | "UPPER" | "REVERSE" => {
            if arg_types.len() != 1 {
                return Err(format!("{func} requires 1 argument"));
            }
            Ok("STRING".to_string())
        }
        "TRIM" | "LTRIM" | "RTRIM" => {
            if arg_types.is_empty() || arg_types.len() > 2 {
                return Err(format!("{func} requires 1 or 2 arguments"));
            }
            Ok("STRING".to_string())
        }
        "CONCAT" => {
            if arg_types.is_empty() {
                return Err("CONCAT requires at least 1 argument".to_string());
            }
            Ok("STRING".to_string())
        }
        "SUBSTR" | "SUBSTRING" | "LPAD" | "RPAD" => {
            if arg_types.len() < 2 || arg_types.len() > 3 {
                return Err(format!("{func} requires 2 or 3 arguments"));
            }
            Ok("STRING".to_string())
        }
        "REPLACE" => {
            if arg_types.len() != 3 {
                return Err("REPLACE requires 3 arguments".to_string());
            }
            Ok("STRING".to_string())
        }
        "LEFT" | "RIGHT" => {
            if arg_types.len() != 2 {
                return Err(format!("{func} requires 2 arguments"));
            }
            Ok("STRING".to_string())
        }
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            if arg_types.len() != 1 {
                return Err(format!("{func} requires 1 argument"));
            }
            Ok("INTEGER".to_string())
        }
        "STARTS_WITH" | "ENDS_WITH" | "CONTAINS_SUBSTR" => {
            if arg_types.len() != 2 {
                return Err(format!("{func} requires 2 arguments"));
            }
            Ok("BOOLEAN".to_string())
        }
        "ABS" => {
            if arg_types.len() != 1 {
                return Err("ABS requires 1 argument".to_string());
            }
            Ok(arg_types[0].clone())
        }
        "ROUND" => {
            if arg_types.is_empty() || arg_types.len() > 2 {
                return Err("ROUND requires 1 or 2 arguments".to_string());
            }
            Ok("FLOAT".to_string())
        }
        "FLOOR" | "CEIL" | "CEILING" | "SQRT" => {
            if arg_types.len() != 1 {
                return Err(format!("{func} requires 1 argument"));
            }
            Ok("FLOAT".to_string())
        }
        "MOD" => {
            if arg_types.len() != 2 {
                return Err("MOD requires 2 arguments".to_string());
            }
            Ok("INTEGER".to_string())
        }
        "POW" | "POWER" | "SAFE_DIVIDE" => {
            if arg_types.len() != 2 {
                return Err(format!("{func} requires 2 arguments"));
            }
            Ok("FLOAT".to_string())
        }
        "GREATEST" | "LEAST" => {
            if arg_types.is_empty() {
                return Err(format!("{func} requires at least 1 argument"));
            }
            Ok(arg_types[0].clone())
        }
        _ => Err("unsupported SQL function".to_string()),
    }
}

/// Resolves a schema column type by case-insensitive lookup.
pub fn schema_field_type(schema: &TableSchema, name: &str) -> Option<String> {
    schema
        .fields
        .iter()
        .rev()
        .find(|f| f.name.eq_ignore_ascii_case(name))
        .map(|f| standard_sql_to_legacy_type(&default_string(f.field_type.clone(), "STRING")))
}

/// Finds a schema field by case-insensitive lookup.
pub fn find_schema_field<'a>(schema: &'a TableSchema, name: &str) -> Option<&'a TableFieldSchema> {
    schema
        .fields
        .iter()
        .rev()
        .find(|f| f.name.eq_ignore_ascii_case(name))
}

/// Splits a comma-separated SQL list at top level (ignoring commas inside
/// parentheses, single quotes, double quotes, and backticks).
pub fn split_top_level_commas(input: &str) -> Result<Vec<String>, String> {
    let bytes = input.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut i = 0usize;
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
            i += 1;
            continue;
        }
        if in_double {
            if b == b'\\' {
                i += 2;
                continue;
            }
            if b == b'"' {
                in_double = false;
            }
            i += 1;
            continue;
        }
        if in_backtick {
            if b == b'`' {
                in_backtick = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => in_single = true,
            b'"' => in_double = true,
            b'`' => in_backtick = true,
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth < 0 {
                    return Err("unbalanced parentheses in SQL query".to_string());
                }
            }
            b',' if depth == 0 => {
                parts.push(input[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    if depth != 0 || in_single || in_double || in_backtick {
        return Err("unterminated quote or parenthesis in SQL query".to_string());
    }
    parts.push(input[start..].trim().to_string());
    Ok(parts)
}

/// Finds a top-level keyword (case-insensitive, bounded by whitespace/parens)
/// outside quotes and parentheses.
pub fn find_top_level_keyword(input: &str, keyword: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let kw_len = keyword.len();
    let mut depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut i = 0usize;
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
            i += 1;
            continue;
        }
        if in_double {
            if b == b'\\' {
                i += 2;
                continue;
            }
            if b == b'"' {
                in_double = false;
            }
            i += 1;
            continue;
        }
        if in_backtick {
            if b == b'`' {
                in_backtick = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => in_single = true,
            b'"' => in_double = true,
            b'`' => in_backtick = true,
            b'(' => depth += 1,
            b')' => depth = (depth - 1).max(0),
            _ if depth == 0 && i + kw_len <= bytes.len() => {
                if input[i..i + kw_len].eq_ignore_ascii_case(keyword) {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}
