//! Rust reimplementation of the devcloud DynamoDB service (strangler-fig
//! increment #4). Foundation + table management: the Go-compatible JSON encoder,
//! the attribute-value / table-description model, `state.json` persistence, and
//! the table lifecycle operations (Create/Describe/Delete/Update/List). Item
//! operations, expressions, the SigV4 path, and the HTTP server land in later
//! parts.

pub mod attribute;
pub mod errors;
pub mod expression;
pub mod go_json;
pub mod model;
pub mod number;
pub mod persistence;
pub mod requests;
pub mod responses;
pub mod server;
pub mod time_util;
pub mod update_expression;
pub mod validation;
