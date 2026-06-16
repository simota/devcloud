//! Subscription metadata / policy validation.
//!
//! Mirrors the subscription-side validators in
//! `internal/services/pubsub/validation.rs` (filter, retry policy, push config,
//! dead-letter policy, expiration policy, message-retention duration). Error
//! wording is verbatim. Filter matching uses the same two patterns legacy compiles:
//! an attribute comparison and a `hasPrefix(...)` form.

use serde_json::Value;

use crate::duration::valid_google_duration;
use crate::errors::ApiError;
use crate::paths::valid_full_topic_name;

/// True for `""`/`TYPE_UNSPECIFIED`/`PROTOCOL_BUFFER`/`AVRO`, mirroring
/// `validSchemaType`.
pub fn valid_schema_type(schema_type: &str) -> bool {
    matches!(
        schema_type,
        "" | "TYPE_UNSPECIFIED" | "PROTOCOL_BUFFER" | "AVRO"
    )
}

/// True for `""`/`ENCODING_UNSPECIFIED`/`JSON`/`BINARY`, mirroring
/// `validSchemaEncoding`.
pub fn valid_schema_encoding(encoding: &str) -> bool {
    matches!(encoding, "" | "ENCODING_UNSPECIFIED" | "JSON" | "BINARY")
}

/// Validates a schema definition, mirroring `validateSchemaDefinition` (AVRO
/// must be a JSON object; others are unchecked). An empty/whitespace definition
/// is always allowed.
pub fn validate_schema_definition(schema_type: &str, definition: &str) -> Result<(), ApiError> {
    if definition.trim().is_empty() {
        return Ok(());
    }
    if schema_type == "AVRO" {
        match serde_json::from_str::<Value>(definition) {
            Ok(Value::Object(_)) => {}
            Ok(_) => {
                return Err(ApiError::invalid_argument(
                    "avro schema definition must be a json object",
                ))
            }
            Err(_) => {
                return Err(ApiError::invalid_argument(
                    "avro schema definition must be valid json",
                ))
            }
        }
    }
    Ok(())
}

/// Validates message bytes for a schema encoding, mirroring
/// `validSchemaMessageData` (only `JSON` is checked: it must be valid JSON).
pub fn valid_schema_message_data(message: &[u8], encoding: &str) -> bool {
    if message.is_empty() {
        return true;
    }
    if encoding == "JSON" {
        return serde_json::from_slice::<Value>(message).is_ok();
    }
    true
}

/// Decodes base64 trying StdEncoding then RawStdEncoding, mirroring
/// `decodeBase64Bytes`.
pub fn decode_base64_bytes(value: &str) -> Option<Vec<u8>> {
    base64_decode(value, true).or_else(|| base64_decode(value, false))
}

/// Minimal base64 decoder. `padded` selects StdEncoding (requires `=` padding to
/// a multiple of 4) vs RawStdEncoding (no padding).
fn base64_decode(input: &str, padded: bool) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes = input.as_bytes();
    if padded {
        if !bytes.len().is_multiple_of(4) {
            return None;
        }
        let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
        for chunk in bytes.chunks(4) {
            let pads = chunk.iter().rev().take_while(|&&b| b == b'=').count();
            if pads > 2 {
                return None;
            }
            let mut acc = 0u32;
            for (j, &c) in chunk.iter().enumerate() {
                let v = if c == b'=' {
                    if j < 4 - pads {
                        return None;
                    }
                    0
                } else {
                    val(c)?
                };
                acc = (acc << 6) | v;
            }
            out.push((acc >> 16) as u8);
            if pads < 2 {
                out.push((acc >> 8) as u8);
            }
            if pads < 1 {
                out.push(acc as u8);
            }
        }
        Some(out)
    } else {
        // RawStdEncoding: no padding allowed.
        if bytes.contains(&b'=') {
            return None;
        }
        let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
        for chunk in bytes.chunks(4) {
            if chunk.len() == 1 {
                return None;
            }
            let mut acc = 0u32;
            for &c in chunk {
                acc = (acc << 6) | val(c)?;
            }
            acc <<= 6 * (4 - chunk.len());
            out.push((acc >> 16) as u8);
            if chunk.len() > 2 {
                out.push((acc >> 8) as u8);
            }
            if chunk.len() > 3 {
                out.push(acc as u8);
            }
        }
        Some(out)
    }
}

/// Validates a subscription filter, mirroring `validateSubscriptionFilter`.
pub fn validate_subscription_filter(filter: &str) -> Result<(), ApiError> {
    let filter = filter.trim();
    if filter.is_empty() {
        return Ok(());
    }
    if parse_comparison_filter(filter).is_none() && parse_prefix_filter(filter).is_none() {
        return Err(ApiError::invalid_argument(
            "unsupported subscription filter",
        ));
    }
    Ok(())
}

/// Parses `attributes.<key> [!=|=] "<value>"`, returning `(key, op, value)`.
/// Mirrors `attributeComparisonFilterPattern`:
/// `^attributes\.([A-Za-z0-9_.-]+)\s*(!=|=)\s*"([^"]*)"$`.
pub fn parse_comparison_filter(filter: &str) -> Option<(String, String, String)> {
    let rest = filter.strip_prefix("attributes.")?;
    // key = [A-Za-z0-9_.-]+
    let key_end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')))
        .unwrap_or(rest.len());
    if key_end == 0 {
        return None;
    }
    let key = &rest[..key_end];
    let after_key = rest[key_end..].trim_start();
    let (op, after_op) = if let Some(r) = after_key.strip_prefix("!=") {
        ("!=", r)
    } else if let Some(r) = after_key.strip_prefix('=') {
        ("=", r)
    } else {
        return None;
    };
    let after_op = after_op.trim_start();
    let value = after_op.strip_prefix('"')?;
    let value = value.strip_suffix('"')?;
    // `[^"]*` — the value must contain no embedded quote.
    if value.contains('"') {
        return None;
    }
    Some((key.to_string(), op.to_string(), value.to_string()))
}

/// Parses `hasPrefix( attributes.<key> , "<prefix>" )`, returning `(key, prefix)`.
/// Mirrors `attributePrefixFilterPattern`.
pub fn parse_prefix_filter(filter: &str) -> Option<(String, String)> {
    let inner = filter.strip_prefix("hasPrefix(")?.strip_suffix(')')?;
    let inner = inner.trim();
    let inner = inner.strip_prefix("attributes.")?;
    let (key_part, value_part) = inner.split_once(',')?;
    let key = key_part.trim();
    if key.is_empty()
        || !key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    {
        return None;
    }
    let value_part = value_part.trim();
    let value = value_part.strip_prefix('"')?.strip_suffix('"')?;
    if value.contains('"') {
        return None;
    }
    Some((key.to_string(), value.to_string()))
}

/// Validates `messageRetentionDuration` + `expirationPolicy`, mirroring
/// `validateSubscriptionMetadata`.
pub fn validate_subscription_metadata(
    message_retention_duration: &str,
    expiration_policy: Option<&Value>,
) -> Result<(), ApiError> {
    if !message_retention_duration.trim().is_empty()
        && !valid_google_duration(message_retention_duration)
    {
        return Err(ApiError::invalid_argument(
            "messageRetentionDuration must be a non-negative duration",
        ));
    }
    if let Some(policy) = expiration_policy.and_then(Value::as_object) {
        if policy.is_empty() {
            return Ok(());
        }
        let ttl = policy
            .get("ttl")
            .ok_or_else(|| ApiError::invalid_argument("expirationPolicy.ttl is required"))?;
        let ttl = match ttl.as_str() {
            Some(s) if !s.trim().is_empty() => s,
            _ => {
                return Err(ApiError::invalid_argument(
                    "expirationPolicy.ttl must be a duration string",
                ))
            }
        };
        if !valid_google_duration(ttl) {
            return Err(ApiError::invalid_argument(
                "expirationPolicy.ttl must be a non-negative duration",
            ));
        }
    }
    Ok(())
}

/// Validates a dead-letter policy, mirroring `validateDeadLetterPolicy`.
pub fn validate_dead_letter_policy(policy: Option<&Value>) -> Result<(), ApiError> {
    let Some(policy) = policy.and_then(Value::as_object) else {
        return Ok(());
    };
    if policy.is_empty() {
        return Ok(());
    }
    let topic = policy.get("deadLetterTopic").ok_or_else(|| {
        ApiError::invalid_argument("deadLetterPolicy.deadLetterTopic is required")
    })?;
    match topic.as_str() {
        Some(t) if valid_full_topic_name(t) => {}
        _ => {
            return Err(ApiError::invalid_argument(
                "invalid deadLetterPolicy.deadLetterTopic",
            ))
        }
    }
    let max_attempts = dead_letter_max_delivery_attempts(policy).ok_or_else(|| {
        ApiError::invalid_argument("deadLetterPolicy.maxDeliveryAttempts is required")
    })?;
    if !(5..=100).contains(&max_attempts) {
        return Err(ApiError::invalid_argument(
            "deadLetterPolicy.maxDeliveryAttempts must be between 5 and 100",
        ));
    }
    Ok(())
}

/// The dead-letter topic name from a policy (empty when absent), mirroring
/// `deadLetterTopic`.
pub fn dead_letter_topic(policy: Option<&Value>) -> String {
    policy
        .and_then(Value::as_object)
        .and_then(|o| o.get("deadLetterTopic"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn dead_letter_max_delivery_attempts(policy: &serde_json::Map<String, Value>) -> Option<i64> {
    let raw = policy.get("maxDeliveryAttempts")?;
    match raw {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i)
            } else {
                // float that is integral
                let f = n.as_f64()?;
                if f == f.trunc() {
                    Some(f as i64)
                } else {
                    None
                }
            }
        }
        _ => None,
    }
}

/// Validates a retry policy, mirroring `validateRetryPolicy`.
pub fn validate_retry_policy(policy: Option<&Value>) -> Result<(), ApiError> {
    let Some(policy) = policy.and_then(Value::as_object) else {
        return Ok(());
    };
    if policy.is_empty() {
        return Ok(());
    }
    let min = retry_policy_duration(policy, "minimumBackoff")?;
    let max = retry_policy_duration(policy, "maximumBackoff")?;
    if let (Some(min), Some(max)) = (min, max) {
        if min > max {
            return Err(ApiError::invalid_argument(
                "retryPolicy.minimumBackoff must be less than or equal to retryPolicy.maximumBackoff",
            ));
        }
    }
    Ok(())
}

fn retry_policy_duration(
    policy: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<i128>, ApiError> {
    let Some(raw) = policy.get(field) else {
        return Ok(None);
    };
    let value = match raw.as_str() {
        Some(s) if !s.trim().is_empty() => s,
        _ => {
            return Err(ApiError::invalid_argument(format!(
                "retryPolicy.{field} must be a duration string"
            )))
        }
    };
    match crate::duration::parse_go_duration(value) {
        Some(n) if n >= 0 => Ok(Some(n)),
        _ => Err(ApiError::invalid_argument(format!(
            "retryPolicy.{field} must be a non-negative duration"
        ))),
    }
}

/// Validates a push config, mirroring `validatePushConfig`.
pub fn validate_push_config(config: Option<&Value>) -> Result<(), ApiError> {
    let Some(config) = config.and_then(Value::as_object) else {
        return Ok(());
    };
    if config.is_empty() {
        return Ok(());
    }
    let Some(raw_endpoint) = config.get("pushEndpoint") else {
        return Ok(());
    };
    let endpoint = match raw_endpoint.as_str() {
        Some(s) if !s.trim().is_empty() => s,
        _ => {
            return Err(ApiError::invalid_argument(
                "pushConfig.pushEndpoint must be an http or https URL",
            ))
        }
    };
    validate_push_endpoint(endpoint)
}

/// Minimal URL check matching legacy `url.Parse` usage: scheme http/https, a
/// non-empty host, and no user info.
fn validate_push_endpoint(endpoint: &str) -> Result<(), ApiError> {
    let bad = || ApiError::invalid_argument("pushConfig.pushEndpoint must be an http or https URL");
    let user_info_err =
        || ApiError::invalid_argument("pushConfig.pushEndpoint must not include user info");
    let (scheme, rest) = endpoint.split_once("://").ok_or_else(bad)?;
    if scheme != "http" && scheme != "https" {
        return Err(bad());
    }
    // Authority is up to the first '/', '?' or '#'.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let host = match authority.rsplit_once('@') {
        Some((user, _host)) if !user.is_empty() => return Err(user_info_err()),
        Some((_, host)) => host,
        None => authority,
    };
    if host.is_empty() {
        return Err(bad());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn filter_patterns() {
        assert!(validate_subscription_filter("attributes.region = \"us\"").is_ok());
        assert!(validate_subscription_filter("attributes.x != \"y\"").is_ok());
        assert!(validate_subscription_filter("hasPrefix(attributes.k, \"pre\")").is_ok());
        assert!(validate_subscription_filter("bogus").is_err());
        assert_eq!(
            parse_comparison_filter("attributes.region = \"us\""),
            Some(("region".to_string(), "=".to_string(), "us".to_string()))
        );
    }

    #[test]
    fn dead_letter_policy_bounds() {
        assert!(validate_dead_letter_policy(Some(&json!({
            "deadLetterTopic": "projects/p/topics/dlq", "maxDeliveryAttempts": 5
        })))
        .is_ok());
        assert_eq!(
            validate_dead_letter_policy(Some(&json!({
                "deadLetterTopic": "projects/p/topics/dlq", "maxDeliveryAttempts": 2
            })))
            .unwrap_err()
            .message,
            "deadLetterPolicy.maxDeliveryAttempts must be between 5 and 100"
        );
    }

    #[test]
    fn retry_policy_order() {
        assert!(validate_retry_policy(Some(
            &json!({"minimumBackoff": "10s", "maximumBackoff": "600s"})
        ))
        .is_ok());
        assert!(validate_retry_policy(Some(
            &json!({"minimumBackoff": "600s", "maximumBackoff": "10s"})
        ))
        .is_err());
    }

    #[test]
    fn push_endpoint_checks() {
        assert!(
            validate_push_config(Some(&json!({"pushEndpoint": "https://example.com/push"})))
                .is_ok()
        );
        assert_eq!(
            validate_push_config(Some(&json!({"pushEndpoint": "ftp://bad"})))
                .unwrap_err()
                .message,
            "pushConfig.pushEndpoint must be an http or https URL"
        );
        assert_eq!(
            validate_push_config(Some(&json!({"pushEndpoint": "https://user@example.com/"})))
                .unwrap_err()
                .message,
            "pushConfig.pushEndpoint must not include user info"
        );
    }
}
