//! DynamoDB Streams: record generation and the shard-iterator codec.
//!
//! Mirrors the non-handler logic of `internal/services/dynamodb/streams.rs`.
//! Records are appended to a table's `stream_records` on each mutating write
//! when a stream is enabled; the iterator is a base64url-`RawURLEncoding` JSON
//! blob `{"streamArn","shardId","position"}`. The handlers themselves live on
//! `Server` (server.rs) since they touch table state.

use serde::{Deserialize, Serialize};

use crate::attribute::extract_key;
use crate::model::{Item, StreamRecord, StreamRecordImage, TableDescription};

/// The opaque shard-iterator payload. JSON field names are lowercase, matching
/// legacy `streamIterator` (`streamArn`, `shardId`, `position`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamIterator {
    #[serde(rename = "streamArn")]
    pub stream_arn: String,
    #[serde(rename = "shardId")]
    pub shard_id: String,
    #[serde(rename = "position")]
    pub position: i64,
}

/// The event name for a write, mirroring `streamEventName`.
pub fn stream_event_name(existed: bool, delete: bool) -> &'static str {
    if delete {
        "REMOVE"
    } else if existed {
        "MODIFY"
    } else {
        "INSERT"
    }
}

/// Builds the next stream record for a write (or `None` when the stream is
/// disabled, or for a REMOVE of a non-existent item). Mirrors
/// `appendStreamRecordLocked` minus the push. `now_unix` supplies
/// `ApproximateCreationDateTime` (injectable for tests).
pub fn build_stream_record(
    description: &TableDescription,
    region: &str,
    existing_count: usize,
    event_name: &str,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    now_unix: i64,
) -> Option<StreamRecord> {
    let spec = description.stream_specification.as_ref()?;
    if !spec.stream_enabled {
        return None;
    }
    if event_name == "REMOVE" && old_item.is_none() {
        return None;
    }
    let source = if event_name == "REMOVE" {
        old_item
    } else {
        new_item
    };
    let keys = extract_key(description, source?).ok()?;
    let sequence = (existing_count + 1).to_string();
    let view_type = spec.stream_view_type.clone();
    let mut image = StreamRecordImage {
        approximate_creation_date_time: now_unix,
        keys,
        new_image: Item::new(),
        old_image: Item::new(),
        sequence_number: sequence.clone(),
        size_bytes: 0,
        stream_view_type: view_type.clone(),
    };
    match view_type.as_str() {
        "NEW_IMAGE" => {
            if event_name != "REMOVE" {
                if let Some(new) = new_item {
                    image.new_image = new.clone();
                }
            }
        }
        "OLD_IMAGE" => {
            if let Some(old) = old_item {
                image.old_image = old.clone();
            }
        }
        "NEW_AND_OLD_IMAGES" => {
            if event_name != "REMOVE" {
                if let Some(new) = new_item {
                    image.new_image = new.clone();
                }
            }
            if let Some(old) = old_item {
                image.old_image = old.clone();
            }
        }
        _ => {}
    }
    // SizeBytes = byte length of the image marshaled while SizeBytes is still 0
    // (legacy computes it before setting the field). Uses legacy-compatible marshaling.
    image.size_bytes = crate::wire_json::marshal(&image).len() as i64;
    Some(StreamRecord {
        event_id: format!("{}:{sequence}", description.table_name),
        event_name: event_name.to_string(),
        event_source: "aws:dynamodb".to_string(),
        event_version: "1.1".to_string(),
        aws_region: region.to_string(),
        dynamodb: image,
    })
}

/// Encodes an iterator to base64url without padding (legacy `RawURLEncoding`).
pub fn encode_iterator(iterator: &StreamIterator) -> String {
    let payload = crate::wire_json::marshal(iterator);
    base64url_encode(&payload)
}

/// Decodes a base64url-`RawURLEncoding` iterator, rejecting blobs that lack a
/// stream ARN or shard id (mirroring `decodeStreamIterator`).
pub fn decode_iterator(value: &str) -> Result<StreamIterator, String> {
    let payload = base64url_decode(value).ok_or_else(|| "invalid base64".to_string())?;
    let iterator: StreamIterator = serde_json::from_slice(&payload).map_err(|e| e.to_string())?;
    if iterator.stream_arn.is_empty() || iterator.shard_id.is_empty() {
        return Err("invalid stream iterator".to_string());
    }
    Ok(iterator)
}

// --- base64url (RawURLEncoding: '-'/'_' alphabet, no padding) ---------------

fn base64url_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(n >> 6) as usize & 63] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[n as usize & 63] as char);
        }
    }
    out
}

fn base64url_decode(input: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        if chunk.len() == 1 {
            return None;
        }
        let mut n = 0u32;
        for &c in chunk {
            n = (n << 6) | val(c)?;
        }
        // Left-align the accumulated bits for the partial final group.
        n <<= 6 * (4 - chunk.len());
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterator_round_trips() {
        let it = StreamIterator {
            stream_arn: "arn:aws:dynamodb:us-east-1:000000000000:table/T/stream/2026".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            position: 3,
        };
        let encoded = encode_iterator(&it);
        // No padding characters.
        assert!(!encoded.contains('='));
        let decoded = decode_iterator(&encoded).unwrap();
        assert_eq!(decoded.stream_arn, it.stream_arn);
        assert_eq!(decoded.position, 3);
    }

    #[test]
    fn decode_rejects_empty_fields() {
        let bad = encode_iterator(&StreamIterator {
            stream_arn: String::new(),
            shard_id: "s".to_string(),
            position: 0,
        });
        assert!(decode_iterator(&bad).is_err());
    }

    #[test]
    fn base64url_matches_known_vector() {
        // "abc" -> "YWJj"; "ab" -> "YWI"; "a" -> "YQ" (no padding).
        assert_eq!(base64url_encode(b"abc"), "YWJj");
        assert_eq!(base64url_encode(b"ab"), "YWI");
        assert_eq!(base64url_encode(b"a"), "YQ");
        assert_eq!(base64url_decode("YWJj").unwrap(), b"abc");
        assert_eq!(base64url_decode("YWI").unwrap(), b"ab");
    }

    #[test]
    fn event_names() {
        assert_eq!(stream_event_name(false, false), "INSERT");
        assert_eq!(stream_event_name(true, false), "MODIFY");
        assert_eq!(stream_event_name(false, true), "REMOVE");
    }
}
