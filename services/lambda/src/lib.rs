//! Local AWS Lambda emulator for devcloud.
//!
//! Serves the Lambda REST API (`/2015-03-31/functions`, invoke, tags, account
//! settings) and executes `python3.x` / `nodejs*` handlers from zip deployment
//! packages as local child processes. Function state persists to `state.json`
//! under the configured storage path; packages may come inline (`ZipFile`) or
//! from the shared local S3 store (`S3Bucket`/`S3Key`).

#![allow(non_snake_case)]

use std::sync::OnceLock;
use tokio::sync::mpsc::UnboundedSender;

static EVENT_SINK: OnceLock<UnboundedSender<String>> = OnceLock::new();

/// Installs the process-wide dashboard event sink (called once by the
/// orchestrator). Events are `{"type":..,"service":"lambda","payload":..}`.
pub fn set_event_sink(tx: UnboundedSender<String>) {
    let _ = EVENT_SINK.set(tx);
}

pub(crate) fn event_sink() -> Option<&'static UnboundedSender<String>> {
    EVENT_SINK.get()
}

pub mod body;
pub mod code_store;
pub mod container;
pub mod http;
pub mod runtime;
pub mod server;
pub mod sigv4;
pub mod time_fmt;
pub mod zip;

pub use server::{Config, Server};
