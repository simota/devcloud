//! `devcloud-pubsub` binary entry point.
//!
//! The REST HTTP server is wired in a later part of increment #5; this
//! foundation build only exposes the library (Go-compatible JSON, model,
//! persistence). The Go engine still serves Pub/Sub.
fn main() {
    eprintln!(
        "devcloud-pubsub: REST server not yet implemented (foundation build of \
         transmute increment #5); the Go engine still serves Pub/Sub"
    );
    std::process::exit(1);
}
