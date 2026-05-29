//! Rust reimplementation of the devcloud DynamoDB service (strangler-fig
//! increment #4). This foundation crate provides the Go-compatible JSON encoder,
//! the attribute-value / table-description model, and `state.json` persistence;
//! handlers, expressions, the SigV4 path, and the HTTP server land in later
//! parts.

pub mod go_json;
pub mod model;
pub mod persistence;
