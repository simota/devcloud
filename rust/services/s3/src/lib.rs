//! Rust reimplementation of the devcloud **S3** service (strangler-fig increment
//! #7 — the hub). S3 owns the `BucketStore` boundary that GCS, BigQuery, and
//! Redshift share, so it is migrated last among the storage services.
//!
//! The crate now covers the repo's S3 full acceptance gate: Go-compatible JSON
//! persistence (`json.MarshalIndent` byte-for-byte), bucket/object/multipart
//! models, on-disk store behavior, XML responses, selected S3 subresources,
//! SigV4, HTTP routing, daemon integration, and dashboard event bridging.

pub mod base64;
pub mod csv;
pub mod go_json;
pub mod hashes;
pub mod http;
pub mod model;
pub mod objops;
pub mod percent;
pub mod responses;
pub mod store;
pub mod store_config;
pub mod store_inventory;
pub mod store_multipart;
pub mod store_objectlock;
pub mod store_objects;
pub mod time_fmt;
pub mod validation;
pub mod xml;
