//! Rust reimplementation of the devcloud GCS JSON API surface.
//!
//! This crate intentionally reuses `devcloud_s3::store::FileBucketStore` so GCS
//! and S3 continue to share the same object-core layout under `.devcloud/s3`.

pub mod http;
