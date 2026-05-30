//! Rust reimplementation of the devcloud Pub/Sub **REST** service (strangler-fig
//! increment #5). The gRPC protocol stays on the Go engine; this crate ports the
//! REST protocol — topics, subscriptions, snapshots, schemas, messages
//! (publish/pull/ack), IAM, seek — plus the shared in-memory resource state and
//! byte-compatible `resources.json` / `pubsub.json` persistence, served over an
//! AWS-style hand-rolled HTTP/1.1 server.

pub mod delivery;
pub mod duration;
pub mod errors;
pub mod go_json;
pub mod http;
pub mod model;
pub mod patch;
pub mod paths;
pub mod persistence;
pub mod responses;
pub mod server;
pub mod time_fmt;
pub mod validation;
