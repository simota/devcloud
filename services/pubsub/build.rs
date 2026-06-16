//! Compiles the trimmed local Pub/Sub protos into Rust via tonic-build.
//!
//! The protos under `proto/google/pubsub/v1/` are hand-authored copies of the
//! canonical googleapis definitions with all `google.api.*` annotations and IAM
//! methods stripped — only the package, message names, field numbers, types, and
//! service + method names are preserved (gRPC wire compatibility depends only on
//! those). `compile_well_known_types(true)` lets prost map the imported
//! `google.protobuf.*` types to `prost_types`.

fn main() {
    let proto_root = "proto";
    let protos = [
        "proto/google/pubsub/v1/schema.proto",
        "proto/google/pubsub/v1/pubsub.proto",
    ];

    tonic_build::configure()
        // Clients are emitted for the in-process gRPC conformance test
        // (`tests/grpc_conformance.rs`), which drives the tonic servers via the
        // generated `*Client` types over a loopback channel. The legacy daemon seam
        // never uses them, so they sit unused in non-test builds — harmless.
        .build_client(true)
        .build_server(true)
        .compile_protos(&protos, &[proto_root])
        .expect("compile pubsub protos");

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
}
