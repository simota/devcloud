//! Mirrors `internal/services/sqs/hashing.rs`.
//!
//! The interesting one is [`md5_of_message_attributes`], which reproduces AWS's
//! exact attribute-digest wire format: attributes sorted by name, each encoded
//! as length-prefixed (4-byte big-endian) name, length-prefixed data type, a
//! 1-byte transport type (1 = string, 2 = binary), then the length-prefixed
//! value. The MD5 of that buffer is `MD5OfMessageAttributes`.

use std::collections::BTreeMap;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use md5::{Digest, Md5};

pub use crate::model::MessageAttributeValue;

/// Lowercase hex MD5 of a UTF-8 string. Mirrors `md5Hex`.
pub fn md5_hex(value: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(value.as_bytes());
    hex_lower(&hasher.finalize())
}

/// Mirrors `md5OfMessageAttributes`: returns "" for an empty map, otherwise the
/// MD5 over the AWS attribute-digest encoding (attributes sorted by name).
pub fn md5_of_message_attributes(attrs: &BTreeMap<String, MessageAttributeValue>) -> String {
    if attrs.is_empty() {
        return String::new();
    }
    // BTreeMap already iterates in sorted key order, matching legacy sort.Strings.
    let mut payload: Vec<u8> = Vec::new();
    for (name, attr) in attrs {
        write_attr_string(&mut payload, name);
        write_attr_string(&mut payload, &attr.data_type);
        if attr.data_type.to_ascii_lowercase().starts_with("binary") {
            payload.push(2);
            write_attr_bytes(&mut payload, &decode_binary_attribute(&attr.binary_value));
        } else {
            payload.push(1);
            write_attr_string(&mut payload, &attr.string_value);
        }
    }
    let mut hasher = Md5::new();
    hasher.update(&payload);
    hex_lower(&hasher.finalize())
}

fn write_attr_string(buf: &mut Vec<u8>, value: &str) {
    write_attr_bytes(buf, value.as_bytes());
}

fn write_attr_bytes(buf: &mut Vec<u8>, value: &[u8]) {
    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buf.extend_from_slice(value);
}

/// Mirrors `decodeBinaryAttribute`: base64-decode, falling back to the raw bytes
/// when the value is not valid base64.
fn decode_binary_attribute(value: &str) -> Vec<u8> {
    match BASE64.decode(value.as_bytes()) {
        Ok(decoded) => decoded,
        Err(_) => value.as_bytes().to_vec(),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
