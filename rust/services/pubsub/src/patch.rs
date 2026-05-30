//! Topic PATCH decoding: unwrap a `{"topic":{...}}` body, resolve the update
//! mask (from the `updateMask` query param, a body `updateMask` string/`{paths}`,
//! or the present body fields), and normalize field names.
//!
//! Mirrors `decodeTopicPatch` / `topicUpdateMaskFields` / `normalizeTopicPatchField`
//! in `internal/services/pubsub/patch_masks.go`. Returns the parsed `Topic`
//! patch + the normalized field set, or an error (rendered by Go as
//! `"invalid json request"`).

use serde_json::{Map, Value};

use crate::model::Topic;

/// The decoded patch: the `Topic` (best-effort) and the resolved field set.
pub struct TopicPatch {
    pub topic: Topic,
    pub fields: Vec<String>,
}

/// Decodes a topic PATCH from the raw body bytes and the `updateMask` query
/// param (empty when absent). Returns `None` on any decode error (the handler
/// maps that to a 400 "invalid json request").
pub fn decode_topic_patch(body: &[u8], update_mask_query: &str) -> Option<TopicPatch> {
    // Empty body decodes to `{}`.
    let bytes: &[u8] = if body.is_empty() { b"{}" } else { body };
    let root: Map<String, Value> = serde_json::from_slice(bytes).ok()?;
    // Unwrap `{"topic": {...}}` if present.
    let topic_body: Map<String, Value> = match root.get("topic") {
        Some(Value::Object(inner)) => inner.clone(),
        Some(_) => return None,
        None => root.clone(),
    };
    let topic: Topic = serde_json::from_value(Value::Object(topic_body.clone())).ok()?;

    let fields = resolve_fields(&root, &topic_body, update_mask_query)?;
    Some(TopicPatch { topic, fields })
}

fn resolve_fields(
    root: &Map<String, Value>,
    topic_body: &Map<String, Value>,
    update_mask_query: &str,
) -> Option<Vec<String>> {
    if !update_mask_query.is_empty() {
        return parse_update_mask(update_mask_query);
    }
    if let Some(raw) = root.get("updateMask") {
        if let Some(mask) = raw.as_str() {
            return parse_update_mask(mask);
        }
        // `{"paths": [...]}` form.
        let paths = raw.get("paths").and_then(Value::as_array)?;
        let joined: Vec<String> = paths
            .iter()
            .filter_map(|p| p.as_str().map(str::to_string))
            .collect();
        return parse_update_mask(&joined.join(","));
    }
    // Default: the present body fields (those that normalize).
    let mut fields = Vec::new();
    for key in topic_body.keys() {
        if let Some(normalized) = normalize_field(key) {
            if !fields.contains(&normalized) {
                fields.push(normalized);
            }
        }
    }
    Some(fields)
}

fn parse_update_mask(mask: &str) -> Option<Vec<String>> {
    let mut fields = Vec::new();
    for raw in mask.split(',') {
        let field = raw.trim();
        if field.is_empty() {
            continue;
        }
        let normalized = normalize_field(field)?;
        if !fields.contains(&normalized) {
            fields.push(normalized);
        }
    }
    Some(fields)
}

/// Normalizes a topic patch field (stripping a `topic.` prefix, accepting both
/// camelCase and snake_case), mirroring `normalizeTopicPatchField`. Returns
/// `None` for an unsupported field.
fn normalize_field(field: &str) -> Option<String> {
    let field = field.strip_prefix("topic.").unwrap_or(field);
    let normalized = match field {
        "name" => "name",
        "labels" => "labels",
        "messageRetentionDuration" | "message_retention_duration" => "messageRetentionDuration",
        "schemaSettings" | "schema_settings" => "schemaSettings",
        "kmsKeyName" | "kms_key_name" => "kmsKeyName",
        _ => return None,
    };
    Some(normalized.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_plain_patch_with_query_mask() {
        let p = decode_topic_patch(br#"{"labels":{"a":"b"}}"#, "labels").unwrap();
        assert_eq!(p.fields, vec!["labels"]);
        assert_eq!(p.topic.labels.get("a"), Some(&"b".to_string()));
    }

    #[test]
    fn unwraps_topic_body_and_body_mask() {
        let p = decode_topic_patch(
            br#"{"topic":{"kmsKeyName":"k"},"updateMask":"kmsKeyName"}"#,
            "",
        )
        .unwrap();
        assert_eq!(p.fields, vec!["kmsKeyName"]);
        assert_eq!(p.topic.kms_key_name, "k");
    }

    #[test]
    fn default_fields_from_body() {
        let p = decode_topic_patch(br#"{"labels":{},"kmsKeyName":"k"}"#, "").unwrap();
        assert!(p.fields.contains(&"labels".to_string()));
        assert!(p.fields.contains(&"kmsKeyName".to_string()));
    }

    #[test]
    fn unsupported_mask_field_errors() {
        assert!(decode_topic_patch(b"{}", "bogus").is_none());
    }
}
