//! Table-request validation — error wording verbatim from
//! `internal/services/dynamodb/{table_handlers,streams}.go`.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::{AttributeDefinition, KeySchemaElement, StreamSpecification};
use crate::requests::CreateTableRequest;

/// Validates a `StreamSpecification`. Returns `Ok` when the stream is disabled.
/// Mirrors `validateStreamSpecification`.
pub fn validate_stream_specification(spec: &StreamSpecification) -> Result<(), String> {
    if !spec.stream_enabled {
        return Ok(());
    }
    match spec.stream_view_type.as_str() {
        "KEYS_ONLY" | "NEW_IMAGE" | "OLD_IMAGE" | "NEW_AND_OLD_IMAGES" => Ok(()),
        "" => Err("stream view type is required when stream is enabled".to_string()),
        _ => Err(
            "stream view type must be KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, or NEW_AND_OLD_IMAGES"
                .to_string(),
        ),
    }
}

/// Returns the table HASH key attribute name (empty if none). Mirrors
/// `tableHashKey`.
pub fn table_hash_key(schema: &[KeySchemaElement]) -> &str {
    for element in schema {
        if element.key_type == "HASH" {
            return &element.attribute_name;
        }
    }
    ""
}

/// Validates a `CreateTable` request, mirroring `validateCreateTableRequest`
/// branch-for-branch (including error message text and ordering).
pub fn validate_create_table_request(request: &CreateTableRequest) -> Result<(), String> {
    if request.table_name.is_empty() {
        return Err("table name is required".to_string());
    }
    if request.key_schema.is_empty() {
        return Err("key schema is required".to_string());
    }
    let mut hash_keys = 0;
    let mut range_keys = 0;
    let mut attributes: BTreeSet<&str> = BTreeSet::new();
    for definition in &request.attribute_definitions {
        if definition.attribute_name.is_empty() {
            return Err("attribute name is required".to_string());
        }
        if !matches!(definition.attribute_type.as_str(), "S" | "N" | "B") {
            return Err("attribute type must be S, N, or B".to_string());
        }
        attributes.insert(definition.attribute_name.as_str());
    }
    for element in &request.key_schema {
        if element.attribute_name.is_empty() {
            return Err("key attribute name is required".to_string());
        }
        if !attributes.contains(element.attribute_name.as_str()) {
            return Err("key schema attributes must be defined".to_string());
        }
        match element.key_type.as_str() {
            "HASH" => hash_keys += 1,
            "RANGE" => range_keys += 1,
            _ => return Err("key type must be HASH or RANGE".to_string()),
        }
    }
    if hash_keys != 1 || range_keys > 1 || request.key_schema.len() > 2 {
        return Err("key schema must include one HASH key and at most one RANGE key".to_string());
    }
    if !request.billing_mode.is_empty()
        && request.billing_mode != "PAY_PER_REQUEST"
        && request.billing_mode != "PROVISIONED"
    {
        return Err("billing mode must be PAY_PER_REQUEST or PROVISIONED".to_string());
    }
    if request.stream_specification.stream_enabled {
        validate_stream_specification(&request.stream_specification)?;
    }
    let mut index_names: BTreeSet<&str> = BTreeSet::new();
    for index in &request.global_secondary_indexes {
        if index.index_name.is_empty() {
            return Err("global secondary index name is required".to_string());
        }
        if index_names.contains(index.index_name.as_str()) {
            return Err("global secondary index names must be unique".to_string());
        }
        index_names.insert(index.index_name.as_str());
        if index.key_schema.is_empty() {
            return Err("global secondary index key schema is required".to_string());
        }
        let mut index_hash_keys = 0;
        let mut index_range_keys = 0;
        for element in &index.key_schema {
            if !attributes.contains(element.attribute_name.as_str()) {
                return Err(
                    "global secondary index key schema attributes must be defined".to_string(),
                );
            }
            match element.key_type.as_str() {
                "HASH" => index_hash_keys += 1,
                "RANGE" => index_range_keys += 1,
                _ => {
                    return Err("global secondary index key type must be HASH or RANGE".to_string())
                }
            }
        }
        if index_hash_keys != 1 || index_range_keys > 1 || index.key_schema.len() > 2 {
            return Err(
                "global secondary index key schema must include one HASH key and at most one RANGE key"
                    .to_string(),
            );
        }
        if !projection_type_ok(&index.projection.projection_type) {
            return Err(
                "global secondary index projection type must be ALL, KEYS_ONLY, or INCLUDE"
                    .to_string(),
            );
        }
    }
    for index in &request.local_secondary_indexes {
        if index.index_name.is_empty() {
            return Err("local secondary index name is required".to_string());
        }
        if index_names.contains(index.index_name.as_str()) {
            return Err("secondary index names must be unique".to_string());
        }
        index_names.insert(index.index_name.as_str());
        if index.key_schema.len() != 2 {
            return Err(
                "local secondary index key schema must include table HASH key and one RANGE key"
                    .to_string(),
            );
        }
        if index.key_schema[0].key_type != "HASH"
            || index.key_schema[0].attribute_name != table_hash_key(&request.key_schema)
        {
            return Err("local secondary index HASH key must match table HASH key".to_string());
        }
        let mut range_keys = 0;
        for element in &index.key_schema {
            if !attributes.contains(element.attribute_name.as_str()) {
                return Err(
                    "local secondary index key schema attributes must be defined".to_string(),
                );
            }
            match element.key_type.as_str() {
                "HASH" => {}
                "RANGE" => range_keys += 1,
                _ => return Err("local secondary index key type must be HASH or RANGE".to_string()),
            }
        }
        if range_keys != 1 {
            return Err("local secondary index key schema must include one RANGE key".to_string());
        }
        if !projection_type_ok(&index.projection.projection_type) {
            return Err(
                "local secondary index projection type must be ALL, KEYS_ONLY, or INCLUDE"
                    .to_string(),
            );
        }
    }
    Ok(())
}

/// `""` (default) is allowed; otherwise must be one of the three named types.
fn projection_type_ok(projection_type: &str) -> bool {
    matches!(projection_type, "" | "ALL" | "KEYS_ONLY" | "INCLUDE")
}

/// Validates attribute-definition updates for `UpdateTable`, mirroring
/// `validateAttributeDefinitionUpdates`.
pub fn validate_attribute_definition_updates(
    existing: &[AttributeDefinition],
    updates: &[AttributeDefinition],
) -> Result<(), String> {
    let mut types: BTreeMap<&str, &str> = BTreeMap::new();
    for definition in existing {
        types.insert(&definition.attribute_name, &definition.attribute_type);
    }
    for definition in updates {
        if definition.attribute_name.is_empty() {
            return Err("attribute name is required".to_string());
        }
        if !matches!(definition.attribute_type.as_str(), "S" | "N" | "B") {
            return Err("attribute type must be S, N, or B".to_string());
        }
        if let Some(existing_type) = types.get(definition.attribute_name.as_str()) {
            if *existing_type != definition.attribute_type {
                return Err(
                    "attribute definitions cannot change existing attribute type".to_string(),
                );
            }
        }
        types.insert(&definition.attribute_name, &definition.attribute_type);
    }
    Ok(())
}
