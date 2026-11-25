//! GCS bucket lifecycle configuration validation, deterministic local
//! evaluation (`Delete` and `SetStorageClass`), and XML serialization/parsing.

use devcloud_s3::model::Object;
use devcloud_s3::store::StoreError;
use devcloud_s3::time_fmt::{parse_lifecycle_date, parse_rfc3339};
use devcloud_s3::xml::Element;

use crate::http::{json_error, Response, Server};
use crate::iam_acl::extract_tag_text;
use crate::retention::object_mutation_blocked_reason_with_meta;
use crate::state::{
    delete_object_sidecars, read_bucket_meta, read_object_meta, write_object_meta, GcsBucketMeta,
    GcsLifecycle, GcsLifecycleAction, GcsLifecycleCondition, GcsLifecycleRule, GcsObjectMeta,
};

pub fn validate_lifecycle(lifecycle: &GcsLifecycle) -> Result<(), Response> {
    for rule in &lifecycle.rule {
        let action_type = rule.action.action_type.trim();
        match action_type {
            "Delete" | "AbortIncompleteMultipartUpload" => {}
            "SetStorageClass" => {
                if rule.action.storage_class.trim().is_empty() {
                    return Err(json_error(
                        400,
                        "invalid",
                        "SetStorageClass lifecycle action requires storageClass",
                    ));
                }
            }
            _ => {
                return Err(json_error(
                    400,
                    "invalid",
                    &format!("unsupported lifecycle action type {action_type:?}"),
                ));
            }
        }
        if let Some(age) = rule.condition.age {
            if age < 0 {
                return Err(json_error(
                    400,
                    "invalid",
                    "lifecycle condition age must be non-negative",
                ));
            }
        }
        if !rule.condition.created_before.trim().is_empty()
            && parse_lifecycle_date(rule.condition.created_before.trim()).is_none()
        {
            return Err(json_error(
                400,
                "invalid",
                "lifecycle condition createdBefore must be YYYY-MM-DD or RFC3339",
            ));
        }
    }
    Ok(())
}

/// Applies the bucket's GCS lifecycle rules at the server's current clock.
///
/// - `Delete` removes matching objects that are not protected by retention or
///   holds, emitting `OBJECT_DELETE` notifications.
/// - `SetStorageClass` updates the object's GCS storageClass sidecar metadata
///   when it differs, emitting `OBJECT_METADATA_UPDATE` notifications.
pub fn apply_bucket_lifecycle(server: &mut Server, bucket: &str) -> usize {
    let Ok(bucket_meta) = read_bucket_meta(server.store(), bucket) else {
        return 0;
    };
    let Some(lifecycle) = bucket_meta.lifecycle.as_ref() else {
        return 0;
    };
    if lifecycle.rule.is_empty() {
        return 0;
    }
    let objects = match server.store().list_objects(bucket, "") {
        Ok(Some(objs)) => objs,
        _ => return 0,
    };
    let now = server.now();
    let mut actions_applied = 0;

    for object in objects {
        let mut obj_meta =
            read_object_meta(server.store(), bucket, &object.key).unwrap_or_default();
        let mut deleted = false;
        let mut class_changed = false;
        for rule in &lifecycle.rule {
            if !condition_matches(&rule.condition, &bucket_meta, &object, &obj_meta, &now) {
                continue;
            }
            match rule.action.action_type.as_str() {
                "Delete" => {
                    if object_mutation_blocked_reason_with_meta(
                        &bucket_meta,
                        &object,
                        &obj_meta,
                        &now,
                    )
                    .is_some()
                    {
                        continue;
                    }
                    match server
                        .store()
                        .delete_object_with_result(bucket, &object.key, false)
                    {
                        Ok((_, true)) => {
                            let _ = delete_object_sidecars(server.store(), bucket, &object.key);
                            crate::notifications::emit_object_notification(
                                server,
                                "OBJECT_DELETE",
                                &object,
                            );
                            deleted = true;
                            actions_applied += 1;
                            break;
                        }
                        Err(StoreError::ObjectLocked) => continue,
                        _ => continue,
                    }
                }
                "SetStorageClass" => {
                    let target_class = rule.action.storage_class.trim().to_ascii_uppercase();
                    let current_class = effective_storage_class(&bucket_meta, &obj_meta);
                    if !target_class.is_empty() && current_class != target_class {
                        obj_meta.storage_class = target_class;
                        if write_object_meta(server.store(), bucket, &object.key, &obj_meta).is_ok()
                        {
                            class_changed = true;
                            actions_applied += 1;
                        }
                    }
                }
                _ => {}
            }
        }
        if !deleted && class_changed {
            crate::notifications::emit_object_notification(
                server,
                "OBJECT_METADATA_UPDATE",
                &object,
            );
        }
    }
    actions_applied
}

pub fn effective_storage_class(bucket_meta: &GcsBucketMeta, obj_meta: &GcsObjectMeta) -> String {
    if !obj_meta.storage_class.is_empty() {
        obj_meta.storage_class.clone()
    } else if !bucket_meta.storage_class.is_empty() {
        bucket_meta.storage_class.clone()
    } else {
        "STANDARD".to_string()
    }
}

fn condition_matches(
    cond: &GcsLifecycleCondition,
    bucket_meta: &GcsBucketMeta,
    object: &Object,
    obj_meta: &GcsObjectMeta,
    now: &str,
) -> bool {
    // At least one condition field must be present for a rule to match.
    let has_any = cond.age.is_some()
        || !cond.created_before.is_empty()
        || cond.is_live.is_some()
        || !cond.matches_prefix.is_empty()
        || !cond.matches_suffix.is_empty()
        || !cond.matches_storage_class.is_empty();
    if !has_any {
        return false;
    }

    if let Some(is_live) = cond.is_live {
        if !is_live {
            return false;
        }
    }

    let created = if object.created_at.is_empty() || object.created_at == "0001-01-01T00:00:00Z" {
        &object.last_modified
    } else {
        &object.created_at
    };
    let created_secs = match parse_rfc3339(created) {
        Some((s, _)) => s,
        None => return false,
    };

    if let Some(age_days) = cond.age {
        let Some((now_secs, _)) = parse_rfc3339(now) else {
            return false;
        };
        if now_secs < created_secs.saturating_add(age_days.saturating_mul(86_400)) {
            return false;
        }
    }

    if !cond.created_before.trim().is_empty() {
        let Some(cutoff_secs) = parse_lifecycle_date(cond.created_before.trim()) else {
            return false;
        };
        if created_secs >= cutoff_secs {
            return false;
        }
    }

    if !cond.matches_prefix.is_empty()
        && !cond
            .matches_prefix
            .iter()
            .any(|p| object.key.starts_with(p.as_str()))
    {
        return false;
    }

    if !cond.matches_suffix.is_empty()
        && !cond
            .matches_suffix
            .iter()
            .any(|s| object.key.ends_with(s.as_str()))
    {
        return false;
    }

    if !cond.matches_storage_class.is_empty() {
        let current = effective_storage_class(bucket_meta, obj_meta);
        if !cond
            .matches_storage_class
            .iter()
            .any(|sc| sc.eq_ignore_ascii_case(&current))
        {
            return false;
        }
    }

    true
}

// --- XML Lifecycle serialization & parsing ---------------------------------

pub fn render_lifecycle_xml(lifecycle: &GcsLifecycle) -> Vec<u8> {
    let mut root = Element::new("LifecycleConfiguration");
    for rule in &lifecycle.rule {
        let action_el = match rule.action.action_type.as_str() {
            "SetStorageClass" => Element::new("Action").child(
                Element::new("SetStorageClass").attr("storageClass", &rule.action.storage_class),
            ),
            "AbortIncompleteMultipartUpload" => {
                Element::new("Action").child(Element::new("AbortIncompleteMultipartUpload"))
            }
            _ => Element::new("Action").child(Element::new("Delete")),
        };
        let mut cond_el = Element::new("Condition");
        if let Some(age) = rule.condition.age {
            cond_el = cond_el.text_child("Age", &age.to_string());
        }
        if !rule.condition.created_before.is_empty() {
            cond_el = cond_el.text_child("CreatedBefore", &rule.condition.created_before);
        }
        if let Some(is_live) = rule.condition.is_live {
            cond_el = cond_el.text_child("IsLive", if is_live { "true" } else { "false" });
        }
        if !rule.condition.matches_prefix.is_empty() {
            let mut prefixes = Element::new("MatchesPrefix");
            for p in &rule.condition.matches_prefix {
                prefixes = prefixes.text_child("Prefix", p);
            }
            cond_el = cond_el.child(prefixes);
        }
        if !rule.condition.matches_suffix.is_empty() {
            let mut suffixes = Element::new("MatchesSuffix");
            for s in &rule.condition.matches_suffix {
                suffixes = suffixes.text_child("Suffix", s);
            }
            cond_el = cond_el.child(suffixes);
        }
        for sc in &rule.condition.matches_storage_class {
            cond_el = cond_el.text_child("MatchesStorageClass", sc);
        }
        root = root.child(Element::new("Rule").child(action_el).child(cond_el));
    }
    let mut out = b"<?xml version='1.0' encoding='UTF-8'?>".to_vec();
    out.extend_from_slice(&devcloud_s3::xml::encode(&root));
    out
}

pub fn parse_lifecycle_xml(xml: &str) -> Result<GcsLifecycle, String> {
    let mut rules = Vec::new();
    let mut cursor = xml;
    while let Some(start) = cursor.find("<Rule") {
        let rest = &cursor[start..];
        let Some(end) = rest.find("</Rule>") else {
            return Err("malformed Rule element in lifecycle XML".to_string());
        };
        let block = &rest[..end + "</Rule>".len()];
        cursor = &rest[end + "</Rule>".len()..];

        let action_block = extract_tag_text(block, "Action")
            .ok_or_else(|| "missing Action in lifecycle Rule".to_string())?;
        let action = if action_block.contains("<Delete") {
            GcsLifecycleAction {
                action_type: "Delete".to_string(),
                storage_class: String::new(),
            }
        } else if let Some(pos) = action_block.find("<SetStorageClass") {
            let tag_part = &action_block[pos..];
            let storage_class = extract_attr(tag_part, "storageClass")
                .or_else(|| extract_tag_text(action_block, "SetStorageClass").map(str::to_string))
                .unwrap_or_default();
            if storage_class.trim().is_empty() {
                return Err("SetStorageClass requires storageClass".to_string());
            }
            GcsLifecycleAction {
                action_type: "SetStorageClass".to_string(),
                storage_class: storage_class.trim().to_string(),
            }
        } else if action_block.contains("<AbortIncompleteMultipartUpload") {
            GcsLifecycleAction {
                action_type: "AbortIncompleteMultipartUpload".to_string(),
                storage_class: String::new(),
            }
        } else {
            return Err("unsupported lifecycle Action in XML".to_string());
        };

        let cond_block = extract_tag_text(block, "Condition").unwrap_or("");
        let age = match extract_tag_text(cond_block, "Age") {
            Some(v) => Some(
                v.trim()
                    .parse::<i64>()
                    .map_err(|_| "invalid Age in lifecycle XML".to_string())?,
            ),
            None => None,
        };
        let created_before = extract_tag_text(cond_block, "CreatedBefore")
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let is_live =
            extract_tag_text(cond_block, "IsLive").map(|v| v.trim().eq_ignore_ascii_case("true"));
        let mut matches_prefix = extract_all_tag_texts(cond_block, "Prefix");
        if matches_prefix.is_empty() {
            if let Some(single) = extract_tag_text(cond_block, "MatchesPrefix") {
                if !single.contains('<') && !single.trim().is_empty() {
                    matches_prefix.push(single.trim().to_string());
                }
            }
        }
        let mut matches_suffix = extract_all_tag_texts(cond_block, "Suffix");
        if matches_suffix.is_empty() {
            if let Some(single) = extract_tag_text(cond_block, "MatchesSuffix") {
                if !single.contains('<') && !single.trim().is_empty() {
                    matches_suffix.push(single.trim().to_string());
                }
            }
        }
        let matches_storage_class = extract_all_tag_texts(cond_block, "MatchesStorageClass");

        rules.push(GcsLifecycleRule {
            action,
            condition: GcsLifecycleCondition {
                age,
                created_before,
                is_live,
                matches_prefix,
                matches_suffix,
                matches_storage_class,
            },
        });
    }
    Ok(GcsLifecycle { rule: rules })
}

fn extract_attr(tag: &str, name: &str) -> Option<String> {
    let gt = tag.find('>').unwrap_or(tag.len());
    let head = &tag[..gt];
    let pat = format!("{name}=\"");
    let start = head.find(&pat)? + pat.len();
    let rest = &head[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn extract_all_tag_texts(xml: &str, tag: &str) -> Vec<String> {
    let mut out = Vec::new();
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut rest = xml;
    while let Some(s) = rest.find(&open) {
        let after = &rest[s + open.len()..];
        let Some(e) = after.find(&close) else {
            break;
        };
        out.push(after[..e].trim().to_string());
        rest = &after[e + close.len()..];
    }
    out
}
