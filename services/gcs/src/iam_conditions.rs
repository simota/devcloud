//! Deterministic evaluation of a Cloud Storage IAM Conditions (CEL) subset for
//! `strict` auth mode.
//!
//! Supported attributes: `resource.name` (`projects/_/buckets/{b}` or
//! `projects/_/buckets/{b}/objects/{o}`), `resource.type`
//! (`storage.googleapis.com/Bucket` / `storage.googleapis.com/Object`),
//! `resource.service` (`storage.googleapis.com`), and `request.time`.
//!
//! Supported syntax: string literals (single or double quoted), `true` /
//! `false`, `timestamp("RFC3339")`, the string methods `startsWith`,
//! `endsWith`, and `contains`, comparison operators (`==`, `!=` on strings,
//! booleans and timestamps; `<`, `<=`, `>`, `>=` on timestamps and strings),
//! `!`, `&&`, `||`, and parentheses. Anything else is an evaluation error, and a
//! binding whose condition errors never grants access (fail closed).

use devcloud_s3::time_fmt::parse_rfc3339;

/// Request attributes a condition is evaluated against.
#[derive(Debug, Clone)]
pub struct ConditionContext {
    pub resource_name: String,
    pub resource_type: &'static str,
    /// `request.time` as Unix seconds.
    pub request_time: i64,
}

impl ConditionContext {
    pub fn new(bucket: &str, object: Option<&str>, request_time: i64) -> Self {
        match object {
            Some(object) => Self {
                resource_name: format!("projects/_/buckets/{bucket}/objects/{object}"),
                resource_type: "storage.googleapis.com/Object",
                request_time,
            },
            None => Self {
                resource_name: format!("projects/_/buckets/{bucket}"),
                resource_type: "storage.googleapis.com/Bucket",
                request_time,
            },
        }
    }
}

/// True only when `expression` parses and evaluates to `true`.
pub fn condition_holds(expression: &str, ctx: &ConditionContext) -> bool {
    let Some(tokens) = tokenize(expression) else {
        return false;
    };
    let mut parser = Parser { tokens, pos: 0 };
    let Some(ast) = parser.expr() else {
        return false;
    };
    if parser.pos != parser.tokens.len() {
        return false;
    }
    matches!(eval(&ast, ctx), Some(Value::Bool(true)))
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Str(String),
    Op(&'static str),
}

fn tokenize(input: &str) -> Option<Vec<Token>> {
    let chars: Vec<char> = input.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '"' || c == '\'' {
            let mut s = String::new();
            i += 1;
            loop {
                let ch = *chars.get(i)?;
                i += 1;
                if ch == c {
                    break;
                }
                if ch == '\\' {
                    let escaped = *chars.get(i)?;
                    i += 1;
                    s.push(match escaped {
                        'n' => '\n',
                        't' => '\t',
                        other => other,
                    });
                } else {
                    s.push(ch);
                }
            }
            out.push(Token::Str(s));
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            out.push(Token::Ident(chars[start..i].iter().collect()));
            continue;
        }
        let two: String = chars[i..chars.len().min(i + 2)].iter().collect();
        let op = match two.as_str() {
            "&&" => Some("&&"),
            "||" => Some("||"),
            "==" => Some("=="),
            "!=" => Some("!="),
            "<=" => Some("<="),
            ">=" => Some(">="),
            _ => None,
        };
        if let Some(op) = op {
            out.push(Token::Op(op));
            i += 2;
            continue;
        }
        let op = match c {
            '!' => "!",
            '<' => "<",
            '>' => ">",
            '(' => "(",
            ')' => ")",
            '.' => ".",
            ',' => ",",
            _ => return None,
        };
        out.push(Token::Op(op));
        i += 1;
    }
    Some(out)
}

#[derive(Debug, Clone)]
enum Ast {
    Str(String),
    Bool(bool),
    Ident(String),
    Member(Box<Ast>, String),
    Call(Option<Box<Ast>>, String, Vec<Ast>),
    Not(Box<Ast>),
    And(Box<Ast>, Box<Ast>),
    Or(Box<Ast>, Box<Ast>),
    Cmp(&'static str, Box<Ast>, Box<Ast>),
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek_op(&self, op: &str) -> bool {
        matches!(self.tokens.get(self.pos), Some(Token::Op(o)) if *o == op)
    }

    fn eat_op(&mut self, op: &str) -> bool {
        let hit = self.peek_op(op);
        if hit {
            self.pos += 1;
        }
        hit
    }

    fn expr(&mut self) -> Option<Ast> {
        let mut lhs = self.and()?;
        while self.eat_op("||") {
            lhs = Ast::Or(Box::new(lhs), Box::new(self.and()?));
        }
        Some(lhs)
    }

    fn and(&mut self) -> Option<Ast> {
        let mut lhs = self.unary()?;
        while self.eat_op("&&") {
            lhs = Ast::And(Box::new(lhs), Box::new(self.unary()?));
        }
        Some(lhs)
    }

    fn unary(&mut self) -> Option<Ast> {
        if self.eat_op("!") {
            return Some(Ast::Not(Box::new(self.unary()?)));
        }
        let lhs = self.member()?;
        for op in ["==", "!=", "<=", ">=", "<", ">"] {
            if self.eat_op(op) {
                return Some(Ast::Cmp(op, Box::new(lhs), Box::new(self.member()?)));
            }
        }
        Some(lhs)
    }

    fn args(&mut self) -> Option<Vec<Ast>> {
        let mut args = Vec::new();
        if self.eat_op(")") {
            return Some(args);
        }
        loop {
            args.push(self.expr()?);
            if self.eat_op(")") {
                return Some(args);
            }
            if !self.eat_op(",") {
                return None;
            }
        }
    }

    fn member(&mut self) -> Option<Ast> {
        let mut node = self.primary()?;
        while self.eat_op(".") {
            let Some(Token::Ident(name)) = self.tokens.get(self.pos).cloned() else {
                return None;
            };
            self.pos += 1;
            node = if self.eat_op("(") {
                Ast::Call(Some(Box::new(node)), name, self.args()?)
            } else {
                Ast::Member(Box::new(node), name)
            };
        }
        Some(node)
    }

    fn primary(&mut self) -> Option<Ast> {
        let token = self.tokens.get(self.pos).cloned()?;
        self.pos += 1;
        match token {
            Token::Str(s) => Some(Ast::Str(s)),
            Token::Ident(name) if name == "true" => Some(Ast::Bool(true)),
            Token::Ident(name) if name == "false" => Some(Ast::Bool(false)),
            Token::Ident(name) => {
                if self.eat_op("(") {
                    Some(Ast::Call(None, name, self.args()?))
                } else {
                    Some(Ast::Ident(name))
                }
            }
            Token::Op("(") => {
                let inner = self.expr()?;
                self.eat_op(")").then_some(inner)
            }
            Token::Op(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Str(String),
    Bool(bool),
    Time(i64),
    Namespace(&'static str),
}

fn eval(ast: &Ast, ctx: &ConditionContext) -> Option<Value> {
    match ast {
        Ast::Str(s) => Some(Value::Str(s.clone())),
        Ast::Bool(b) => Some(Value::Bool(*b)),
        Ast::Ident(name) => match name.as_str() {
            "resource" => Some(Value::Namespace("resource")),
            "request" => Some(Value::Namespace("request")),
            _ => None,
        },
        Ast::Member(base, field) => match (eval(base, ctx)?, field.as_str()) {
            (Value::Namespace("resource"), "name") => Some(Value::Str(ctx.resource_name.clone())),
            (Value::Namespace("resource"), "type") => {
                Some(Value::Str(ctx.resource_type.to_string()))
            }
            (Value::Namespace("resource"), "service") => {
                Some(Value::Str("storage.googleapis.com".to_string()))
            }
            (Value::Namespace("request"), "time") => Some(Value::Time(ctx.request_time)),
            _ => None,
        },
        Ast::Call(None, name, args) if name == "timestamp" && args.len() == 1 => {
            match eval(&args[0], ctx)? {
                Value::Str(s) => parse_rfc3339(&s).map(|(secs, _)| Value::Time(secs)),
                _ => None,
            }
        }
        Ast::Call(Some(target), name, args) if args.len() == 1 => {
            let (Value::Str(target), Value::Str(arg)) = (eval(target, ctx)?, eval(&args[0], ctx)?)
            else {
                return None;
            };
            match name.as_str() {
                "startsWith" => Some(Value::Bool(target.starts_with(&arg))),
                "endsWith" => Some(Value::Bool(target.ends_with(&arg))),
                "contains" => Some(Value::Bool(target.contains(&arg))),
                _ => None,
            }
        }
        Ast::Call(..) => None,
        Ast::Not(inner) => match eval(inner, ctx)? {
            Value::Bool(b) => Some(Value::Bool(!b)),
            _ => None,
        },
        // CEL's logical operators absorb an error on the other side when one
        // side already decides the result.
        Ast::And(lhs, rhs) => match (eval(lhs, ctx), eval(rhs, ctx)) {
            (Some(Value::Bool(false)), _) | (_, Some(Value::Bool(false))) => {
                Some(Value::Bool(false))
            }
            (Some(Value::Bool(true)), Some(Value::Bool(true))) => Some(Value::Bool(true)),
            _ => None,
        },
        Ast::Or(lhs, rhs) => match (eval(lhs, ctx), eval(rhs, ctx)) {
            (Some(Value::Bool(true)), _) | (_, Some(Value::Bool(true))) => Some(Value::Bool(true)),
            (Some(Value::Bool(false)), Some(Value::Bool(false))) => Some(Value::Bool(false)),
            _ => None,
        },
        Ast::Cmp(op, lhs, rhs) => {
            let ordering = match (eval(lhs, ctx)?, eval(rhs, ctx)?) {
                (Value::Str(a), Value::Str(b)) => a.cmp(&b),
                (Value::Time(a), Value::Time(b)) => a.cmp(&b),
                (Value::Bool(a), Value::Bool(b)) if matches!(*op, "==" | "!=") => a.cmp(&b),
                _ => return None,
            };
            Some(Value::Bool(match *op {
                "==" => ordering.is_eq(),
                "!=" => ordering.is_ne(),
                "<" => ordering.is_lt(),
                "<=" => ordering.is_le(),
                ">" => ordering.is_gt(),
                ">=" => ordering.is_ge(),
                _ => return None,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object_ctx(object: &str) -> ConditionContext {
        // 2026-06-01T12:00:00Z
        ConditionContext::new("data", Some(object), 1_780_315_200)
    }

    #[test]
    fn evaluates_resource_name_prefix_and_type() {
        let ctx = object_ctx("reports/q1.csv");
        assert!(condition_holds(
            r#"resource.name.startsWith("projects/_/buckets/data/objects/reports/")"#,
            &ctx
        ));
        assert!(!condition_holds(
            r#"resource.name.startsWith('projects/_/buckets/data/objects/private/')"#,
            &ctx
        ));
        assert!(condition_holds(
            r#"resource.type == "storage.googleapis.com/Object" && resource.name.endsWith(".csv")"#,
            &ctx
        ));
        let bucket = ConditionContext::new("data", None, 0);
        assert!(condition_holds(
            r#"resource.type == "storage.googleapis.com/Bucket" || resource.name.startsWith("x")"#,
            &bucket
        ));
        assert!(condition_holds(
            r#"resource.service == "storage.googleapis.com""#,
            &bucket
        ));
    }

    #[test]
    fn evaluates_request_time_against_timestamps() {
        let ctx = object_ctx("a");
        assert!(condition_holds(
            r#"request.time < timestamp("2026-07-01T00:00:00Z")"#,
            &ctx
        ));
        assert!(!condition_holds(
            r#"request.time >= timestamp("2026-07-01T00:00:00Z")"#,
            &ctx
        ));
        assert!(condition_holds(
            r#"!(request.time > timestamp("2026-07-01T00:00:00Z"))"#,
            &ctx
        ));
    }

    #[test]
    fn unsupported_or_malformed_expressions_fail_closed() {
        let ctx = object_ctx("a");
        for expr in [
            "",
            "false",
            "resource.labels.env == 'prod'",
            "resource.name.matches('.*')",
            "request.time.getHours('UTC') > 3",
            "resource.name ==",
            "(true",
            "'a' < timestamp('2026-01-01T00:00:00Z')",
            "true true",
        ] {
            assert!(!condition_holds(expr, &ctx), "{expr:?} must not grant");
        }
        // A decided logical operator absorbs an unsupported side.
        assert!(condition_holds("true || resource.labels.env == 'x'", &ctx));
        assert!(!condition_holds(
            "false && resource.labels.env == 'x'",
            &ctx
        ));
    }
}
