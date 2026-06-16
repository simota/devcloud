//! Mirrors `internal/services/sqs/validation.rs`.
//!
//! Pure validators with the same messages the legacy service produces (the
//! responses layer maps message substrings to AWS error codes, so the exact
//! wording matters and is preserved verbatim).

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;

use crate::hashing::MessageAttributeValue;

/// Mirrors `validateMessageAttributeName`.
pub fn validate_message_attribute_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("invalid attribute name: message attribute name is required".into());
    }
    if name.len() > 256 {
        return Err(
            "invalid attribute name: message attribute name must be no longer than 256 characters"
                .into(),
        );
    }
    let lower = name.to_ascii_lowercase();
    if lower.starts_with("aws.") || lower.starts_with("amazon.") {
        return Err(
            "invalid attribute name: message attribute name must not start with AWS. or Amazon."
                .into(),
        );
    }
    if name.starts_with('.') || name.ends_with('.') || name.contains("..") {
        return Err("invalid attribute name: message attribute name must not start or end with a period or contain consecutive periods".into());
    }
    for r in name.chars() {
        if r.is_ascii_alphanumeric() || r == '_' || r == '-' || r == '.' {
            continue;
        }
        return Err(
            "invalid attribute name: message attribute name contains unsupported characters".into(),
        );
    }
    Ok(())
}

/// Mirrors `validateMessageAttributeValue`.
pub fn validate_message_attribute_value(
    name: &str,
    attr: &MessageAttributeValue,
) -> Result<(), String> {
    if attr.data_type.trim().is_empty() {
        return Err(format!(
            "invalid attribute value for {name}: DataType is required"
        ));
    }
    let data_type = attr.data_type.to_ascii_lowercase();
    if is_unsupported_list_type(&data_type) {
        return Err(format!(
            "invalid attribute value for {name}: list DataType is not supported"
        ));
    }
    if data_type.starts_with("string") {
        Ok(())
    } else if data_type.starts_with("number") {
        if attr.string_value.parse::<f64>().is_err() {
            return Err(format!(
                "invalid attribute value for {name}: Number attributes must be numeric"
            ));
        }
        Ok(())
    } else if data_type.starts_with("binary") {
        if attr.binary_value.is_empty() {
            return Err(format!(
                "invalid attribute value for {name}: BinaryValue is required"
            ));
        }
        if BASE64.decode(attr.binary_value.as_bytes()).is_err() {
            return Err(format!(
                "invalid attribute value for {name}: BinaryValue must be base64"
            ));
        }
        Ok(())
    } else {
        Err(format!(
            "invalid attribute value for {name}: unsupported DataType"
        ))
    }
}

/// Mirrors `validateMessageSystemAttributes`: only `AWSTraceHeader` is allowed.
pub fn validate_message_system_attribute(
    name: &str,
    attr: &MessageAttributeValue,
) -> Result<(), String> {
    if name != "AWSTraceHeader" {
        return Err(format!(
            "invalid attribute value: unsupported message system attribute {name}"
        ));
    }
    validate_message_attribute_value(name, attr)
}

fn is_unsupported_list_type(data_type: &str) -> bool {
    data_type == "string.list"
        || data_type.starts_with("string.list.")
        || data_type == "binary.list"
        || data_type.starts_with("binary.list.")
}

/// Mirrors `validMessageBody`: rejects the Unicode replacement char and code
/// points outside the SQS-allowed set (#x9 | #xA | #xD | #x20-#xD7FF |
/// #xE000-#xFFFD | #x10000-#x10FFFF).
pub fn valid_message_body(body: &str) -> bool {
    for r in body.chars() {
        let c = r as u32;
        if r == '\u{FFFD}' {
            return false;
        }
        if r == '\t' || r == '\n' || r == '\r' {
            continue;
        }
        if (0x20..=0xD7FF).contains(&c)
            || (0xE000..=0xFFFD).contains(&c)
            || (0x10000..=0x10FFFF).contains(&c)
        {
            continue;
        }
        return false;
    }
    true
}

/// Mirrors `validBatchEntryID`: `^[A-Za-z0-9_-]{1,80}$`.
pub fn valid_batch_entry_id(id: &str) -> bool {
    let len = id.chars().count();
    if len == 0 || len > 80 {
        return false;
    }
    id.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Mirrors `queueNamePattern`: `^[A-Za-z0-9_-]{1,80}(\.fifo)?$`.
pub fn valid_queue_name(name: &str) -> bool {
    let base = name.strip_suffix(".fifo").unwrap_or(name);
    let len = base.chars().count();
    if len == 0 || len > 80 {
        return false;
    }
    base.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
