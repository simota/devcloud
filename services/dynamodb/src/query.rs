//! Query / Scan item collection: sorting, key/filter matching, projection,
//! pagination, and `Select`.
//!
//! Mirrors `internal/services/dynamodb/query_scan.rs`. Items are sorted by the
//! relevant key schema (table or index), then optionally reversed, then walked
//! from an exclusive start key, collecting matches up to `Limit` and emitting a
//! `LastEvaluatedKey` when more remain. The matcher closure differs between Query
//! (key condition) and Scan (filter), and Scan counts unmatched items toward the
//! limit (`limit_counts_unmatched`).

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::attribute::{extract_key, item_key, project_result_item};
use crate::model::{Item, KeySchemaElement, TableDescription};

/// An item paired with its internal key string (the `json.Marshal` of its key
/// attributes), mirroring legacy `keyedItem`.
#[derive(Clone)]
pub struct KeyedItem {
    pub key: String,
    pub value: Item,
}

/// Sorts items by the given key schema, then by key string, mirroring
/// `sortedItemsForQuery`.
pub fn sorted_items_for_query(
    items: &BTreeMap<String, Item>,
    description: &TableDescription,
    index_name: &str,
) -> Vec<KeyedItem> {
    let mut out: Vec<KeyedItem> = items
        .iter()
        .map(|(k, v)| KeyedItem {
            key: k.clone(),
            value: v.clone(),
        })
        .collect();
    let schema = query_key_schema(description, index_name);
    out.sort_by(
        |a, b| match compare_items_by_schema(&a.value, &b.value, schema) {
            Ordering::Equal => a.key.cmp(&b.key),
            ord => ord,
        },
    );
    out
}

/// Like `sorted_items_for_query` but, for an index, drops items missing any of
/// the index key attributes. Mirrors `sortedItemsForScan`.
pub fn sorted_items_for_scan(
    items: &BTreeMap<String, Item>,
    description: &TableDescription,
    index_name: &str,
) -> Vec<KeyedItem> {
    let sorted = sorted_items_for_query(items, description, index_name);
    if index_name.is_empty() {
        return sorted;
    }
    let schema = query_key_schema(description, index_name);
    sorted
        .into_iter()
        .filter(|c| item_has_all_keys(&c.value, schema))
        .collect()
}

fn item_has_all_keys(value: &Item, schema: &[KeySchemaElement]) -> bool {
    schema.iter().all(|e| value.contains_key(&e.attribute_name))
}

/// Returns the key schema for an index (or the table when `index_name` is empty
/// or unknown). Mirrors `queryKeySchema`.
pub fn query_key_schema<'a>(
    description: &'a TableDescription,
    index_name: &str,
) -> &'a [KeySchemaElement] {
    if index_name.is_empty() {
        return &description.key_schema;
    }
    for index in &description.global_secondary_indexes {
        if index.index_name == index_name {
            return &index.key_schema;
        }
    }
    for index in &description.local_secondary_indexes {
        if index.index_name == index_name {
            return &index.key_schema;
        }
    }
    &description.key_schema
}

fn compare_items_by_schema(left: &Item, right: &Item, schema: &[KeySchemaElement]) -> Ordering {
    for element in schema {
        let l = left.get(&element.attribute_name);
        let r = right.get(&element.attribute_name);
        let ord = compare_optional_values(l, r);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Compares two optional attribute values, mirroring legacy nil-aware
/// `compareAttributeValues`: nil == nil, nil < present, present > nil.
fn compare_optional_values(left: Option<&Value>, right: Option<&Value>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(l), Some(r)) => crate::attribute::compare_attribute_values(l, r),
    }
}

/// Reverses the item order in place, mirroring `reverseItems`.
pub fn reverse_items(items: &mut [KeyedItem]) {
    items.reverse();
}

/// Computes the start-key string for pagination, mirroring `startKeyString`
/// (empty key => no start filter).
pub fn start_key_string(description: &TableDescription, start: &Item) -> Result<String, String> {
    if start.is_empty() {
        return Ok(String::new());
    }
    item_key(description, start)
}

/// An `ExclusiveStartKey` resolved for a walk over sorted items: the table
/// item-key string (the sort tie-breaker), the key attributes themselves, and
/// the walk direction.
pub struct StartKey {
    key: String,
    item: Item,
    descending: bool,
}

impl StartKey {
    /// Validates and resolves `start` (an empty item means "from the
    /// beginning"). `descending` is true when the source was reversed.
    pub fn new(
        description: &TableDescription,
        start: &Item,
        descending: bool,
    ) -> Result<Self, String> {
        Ok(StartKey {
            key: start_key_string(description, start)?,
            item: start.clone(),
            descending,
        })
    }

    /// Whether the start key carries every attribute of `schema`, so its
    /// position can be computed without the start item still existing. A key
    /// lacking the index key attributes falls back to an exact item match.
    fn is_positional(&self, schema: &[KeySchemaElement]) -> bool {
        item_has_all_keys(&self.item, schema)
    }

    /// True when `candidate` sorts strictly after the start key in walk
    /// order. The comparison is positional (key schema, then the table key
    /// string — the same order `sorted_items_for_query` uses), so pagination
    /// resumes correctly even when the start item itself was deleted.
    fn is_after(&self, candidate: &KeyedItem, schema: &[KeySchemaElement]) -> bool {
        let ord = compare_items_by_schema(&candidate.value, &self.item, schema)
            .then_with(|| candidate.key.as_str().cmp(self.key.as_str()));
        if self.descending {
            ord == Ordering::Less
        } else {
            ord == Ordering::Greater
        }
    }

    fn is_exact(&self, candidate: &KeyedItem) -> bool {
        candidate.key == self.key
    }
}

/// Walks the (already-sorted) source collecting matches, mirroring
/// `collectItems`. Returns the response object (`Items`/`Count`/`ScannedCount`
/// and optionally `LastEvaluatedKey`).
#[allow(clippy::too_many_arguments)]
pub fn collect_items(
    description: &TableDescription,
    index_name: &str,
    source: &[KeyedItem],
    limit: i64,
    start: &StartKey,
    projection: &str,
    names: &BTreeMap<String, String>,
    limit_counts_unmatched: bool,
    mut matcher: impl FnMut(&Item) -> Result<bool, String>,
) -> Result<Map<String, Value>, String> {
    let mut response_items: Vec<Item> = Vec::new();
    let mut scanned: i64 = 0;
    let schema = query_key_schema(description, index_name);
    let mut started = start.key.is_empty();
    let positional = start.is_positional(schema);
    for candidate in source {
        if !started {
            if !positional {
                started = start.is_exact(candidate);
                continue;
            }
            if !start.is_after(candidate, schema) {
                continue;
            }
            started = true;
        }
        let matched = matcher(&candidate.value)?;
        if matched || limit_counts_unmatched {
            scanned += 1;
        }
        if !matched {
            if limit_counts_unmatched && limit > 0 && scanned == limit {
                return Ok(limited_items_response(
                    description,
                    index_name,
                    candidate,
                    response_items,
                    scanned,
                    has_more_items(source, &candidate.key),
                ));
            }
            continue;
        }
        response_items.push(project_result_item(
            description,
            index_name,
            &candidate.value,
            projection,
            names,
        ));
        if limit > 0 && scanned == limit {
            let has_more = if limit_counts_unmatched {
                has_more_items(source, &candidate.key)
            } else {
                has_more_matches(source, &candidate.key, &mut matcher)?
            };
            return Ok(limited_items_response(
                description,
                index_name,
                candidate,
                response_items,
                scanned,
                has_more,
            ));
        }
    }
    Ok(items_response(response_items, scanned))
}

fn items_response(items: Vec<Item>, scanned: i64) -> Map<String, Value> {
    let mut m = Map::new();
    let count = items.len() as i64;
    m.insert("Items".to_string(), serde_json::to_value(items).unwrap());
    m.insert("Count".to_string(), Value::from(count));
    m.insert("ScannedCount".to_string(), Value::from(scanned));
    m
}

fn limited_items_response(
    description: &TableDescription,
    index_name: &str,
    candidate: &KeyedItem,
    items: Vec<Item>,
    scanned: i64,
    has_more: bool,
) -> Map<String, Value> {
    let mut response = items_response(items, scanned);
    if has_more {
        if let Ok(mut last_key) = extract_key(description, &candidate.value) {
            // An index walk also needs the index key to resume positionally.
            if !index_name.is_empty() {
                for element in query_key_schema(description, index_name) {
                    if let Some(attr) = candidate.value.get(&element.attribute_name) {
                        last_key.insert(element.attribute_name.clone(), attr.clone());
                    }
                }
            }
            response.insert(
                "LastEvaluatedKey".to_string(),
                serde_json::to_value(last_key).unwrap(),
            );
        }
    }
    response
}

fn has_more_items(source: &[KeyedItem], after_key: &str) -> bool {
    let mut found = false;
    for candidate in source {
        if !found {
            found = candidate.key == after_key;
            continue;
        }
        return true;
    }
    false
}

fn has_more_matches(
    source: &[KeyedItem],
    after_key: &str,
    matcher: &mut impl FnMut(&Item) -> Result<bool, String>,
) -> Result<bool, String> {
    let mut found = false;
    for candidate in source {
        if !found {
            found = candidate.key == after_key;
            continue;
        }
        if matcher(&candidate.value)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Validates the `Select` value, mirroring `validateSelect`.
pub fn validate_select(select_value: &str, projection_expression: &str) -> Result<(), String> {
    match select_value.trim().to_uppercase().as_str() {
        "" | "ALL_ATTRIBUTES" | "ALL_PROJECTED_ATTRIBUTES" | "SPECIFIC_ATTRIBUTES" => Ok(()),
        "COUNT" => {
            if !projection_expression.trim().is_empty() {
                Err("select COUNT cannot be used with ProjectionExpression".to_string())
            } else {
                Ok(())
            }
        }
        other => Err(format!("unsupported select value {other}")),
    }
}

/// Drops `Items` when `Select=COUNT`, mirroring `applySelect`.
pub fn apply_select(response: &mut Map<String, Value>, select_value: &str) {
    if select_value.trim().eq_ignore_ascii_case("COUNT") {
        response.remove("Items");
    }
}
