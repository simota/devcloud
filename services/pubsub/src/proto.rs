//! Generated Pub/Sub protobuf + tonic gRPC types.
//!
//! `tonic_build` emits one Rust module per proto package; both `pubsub.proto`
//! and `schema.proto` declare `package google.pubsub.v1;`, so they land in a
//! single `google.pubsub.v1` file included here as `pubsub`.

pub mod pubsub {
    tonic::include_proto!("google.pubsub.v1");
}
