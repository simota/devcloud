//! Rust reimplementation of the devcloud DynamoDB service (strangler-fig
//! increment #4). A behavior-preserving port of `internal/services/dynamodb`:
//! the Go-compatible JSON encoder, the attribute-value / table model,
//! `state.json` persistence, all 39 operations (tables, items, expressions,
//! queries, batch/transact, PartiQL, streams, TTL/backups, tags/policy), the
//! SigV4 verifier, and the AWS JSON 1.0 HTTP server.

pub mod attribute;
pub mod errors;
pub mod expression;
pub mod go_json;
pub mod http;
pub mod model;
pub mod number;
pub mod partiql;
pub mod persistence;
pub mod query;
pub mod requests;
pub mod responses;
pub mod server;
pub mod sigv4;
pub mod streams;
pub mod time_util;
pub mod update_expression;
pub mod validation;
