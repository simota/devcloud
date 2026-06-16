//! Rust reimplementation of the devcloud Application Auto Scaling service
//! (strangler-fig increment #2 of the legacy-to-Rust transmute).
//!
//! Behavior-preserving port of `internal/services/applicationautoscaling`: the
//! AWS JSON 1.1 protocol (13 operations over a single `POST /`), SigV4
//! verification, and `state.json` persistence — verified by a 1:1 port of the
//! legacy test suite plus byte-exact golden-oracle checks.

#![allow(non_snake_case)] // AWS API field names are PascalCase on the wire.

pub mod http;
pub mod server;
pub mod sigv4;
pub mod time_fmt;
pub mod types;

pub use server::{Config, Server};
