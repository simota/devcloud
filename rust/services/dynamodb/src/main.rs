//! `devcloud-dynamodb` binary entry point.
//!
//! The HTTP server is wired in a later part of increment #4; this foundation
//! build only exposes the library (Go-compatible JSON, model, persistence).
fn main() {
    eprintln!(
        "devcloud-dynamodb: HTTP server not yet implemented (foundation build of \
         transmute increment #4); the Go engine still serves DynamoDB"
    );
    std::process::exit(1);
}
