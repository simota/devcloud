//! Rust reimplementation of the devcloud Pub/Sub **REST** service (strangler-fig
//! increment #5). The gRPC protocol stays on the Go engine; this crate ports the
//! REST protocol and the shared in-memory resource state + `resources.json` /
//! `pubsub.json` persistence. This foundation part lands the Go-compatible JSON
//! encoder, the resource model, and persistence; handlers and the HTTP server
//! land in later parts.

pub mod go_json;
pub mod model;
pub mod persistence;
