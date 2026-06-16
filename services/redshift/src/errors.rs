//! SQL-layer error type.
//!
//! The legacy implementation uses ad-hoc `errors.New` / `fmt.Errorf` strings for
//! every SQL parsing/execution failure; parity code (pgwire error responses,
//! Data API error fields, tests) compares the messages. `SqlError` is a plain
//! message wrapper that keeps those strings byte-identical.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError(pub String);

impl SqlError {
    pub fn new(message: impl Into<String>) -> Self {
        SqlError(message.into())
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SqlError {}
