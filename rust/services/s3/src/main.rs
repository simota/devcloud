//! Entry point for the Rust devcloud S3 server.
//!
//! The HTTP server, routing, SigV4 auth, and daemon seam are added in later parts
//! of increment #7. Until then this binary is a placeholder so the crate's library
//! (foundation: model, persistence, helpers) can build and be tested in isolation.

fn main() {
    eprintln!("devcloud-s3: HTTP server not yet implemented (increment #7 in progress)");
    std::process::exit(1);
}
