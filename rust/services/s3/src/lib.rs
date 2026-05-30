//! Rust reimplementation of the devcloud **S3** service (strangler-fig increment
//! #7 — the hub). S3 owns the `BucketStore` boundary that GCS, BigQuery, and
//! Redshift share, so it is migrated last among the storage services.
//!
//! Part 1 (this commit) is the foundation: Go-compatible JSON persistence
//! (`json.MarshalIndent` byte-for-byte), the bucket/object/multipart model
//! structs with byte-exact field ordering and `omitempty` semantics, name/key
//! validation, and the deterministic ETag / CRC32C / base64 helpers. Later parts
//! add the on-disk `FileBucketStore`, the object and multipart data planes, the
//! XML response layer, SigV4, routing, and the daemon seam.

pub mod base64;
pub mod go_json;
pub mod hashes;
pub mod model;
pub mod objops;
pub mod store;
pub mod store_config;
pub mod store_multipart;
pub mod store_objectlock;
pub mod store_objects;
pub mod time_fmt;
pub mod validation;
