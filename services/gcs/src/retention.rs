//! GCS bucket retention policies, object holds (`temporaryHold`,
//! `eventBasedHold`, `defaultEventBasedHold`), per-object retention locks,
//! and delete/overwrite guards.

use devcloud_s3::model::Object;
use devcloud_s3::store::FileBucketStore;
use devcloud_s3::time_fmt::{parse_rfc3339, rfc3339nano_from_unix};

use crate::http::{json_error, method_not_allowed, Request, Response, Server};
use crate::state::{
    read_bucket_meta, read_object_meta, write_bucket_meta, BucketRetentionPolicy, GcsBucketMeta,
    GcsObjectMeta, GcsObjectRetention,
};

/// Validates and updates a bucket's retentionPolicy, enforcing lock semantics.
pub fn apply_bucket_retention_policy_update(
    existing: &Option<BucketRetentionPolicy>,
    incoming: Option<BucketRetentionPolicy>,
    now: &str,
) -> Result<Option<BucketRetentionPolicy>, Response> {
    let existing_locked = existing.as_ref().is_some_and(|p| p.is_locked);
    let existing_period = existing
        .as_ref()
        .and_then(|p| p.retention_period.parse::<i64>().ok())
        .unwrap_or(0);

    match incoming {
        None => {
            if existing_locked {
                return Err(json_error(
                    403,
                    "forbidden",
                    "cannot remove a locked bucket retention policy",
                ));
            }
            Ok(None)
        }
        Some(policy) => {
            let trimmed = policy.retention_period.trim();
            if trimmed.is_empty() || trimmed == "0" {
                if existing_locked {
                    return Err(json_error(
                        403,
                        "forbidden",
                        "cannot remove a locked bucket retention policy",
                    ));
                }
                return Ok(None);
            }
            let Ok(seconds) = trimmed.parse::<i64>() else {
                return Err(json_error(
                    400,
                    "invalid",
                    "retentionPeriod must be a positive integer",
                ));
            };
            if seconds < 0 {
                return Err(json_error(
                    400,
                    "invalid",
                    "retentionPeriod must be non-negative",
                ));
            }
            if existing_locked && seconds < existing_period {
                return Err(json_error(
                    403,
                    "forbidden",
                    "cannot reduce retentionPeriod on a locked bucket retention policy",
                ));
            }
            Ok(Some(BucketRetentionPolicy {
                retention_period: seconds.to_string(),
                effective_time: now.to_string(),
                is_locked: existing_locked,
            }))
        }
    }
}

/// `POST /storage/v1/b/{bucket}/lockRetentionPolicy?ifMetagenerationMatch=<n>`
pub fn handle_lock_retention_policy(server: &mut Server, req: &Request, bucket: &str) -> Response {
    if req.method != "POST" {
        return method_not_allowed("POST");
    }
    let bucket_record = match server.store().get_bucket(bucket) {
        Ok(Some(b)) => b,
        Ok(None) => return json_error(404, "notFound", "bucket not found"),
        Err(_) => return json_error(400, "invalid", "invalid bucket name"),
    };
    let if_meta_raw = req.query_value("ifMetagenerationMatch").trim();
    if if_meta_raw.is_empty() {
        return json_error(
            400,
            "required",
            "ifMetagenerationMatch is required to lock retention policy",
        );
    }
    let Ok(expected_meta) = if_meta_raw.parse::<i64>() else {
        return json_error(400, "invalid", "invalid ifMetagenerationMatch");
    };
    let mut meta = match read_bucket_meta(server.store(), bucket) {
        Ok(m) => m,
        Err(_) => return json_error(500, "backendError", "internal error"),
    };
    let current_meta = meta.metageneration.max(1);
    if expected_meta != current_meta {
        return json_error(412, "conditionNotMet", "precondition failed");
    }
    let Some(policy) = meta.retention_policy.as_mut() else {
        return json_error(
            400,
            "invalid",
            "bucket does not have a retention policy to lock",
        );
    };
    let period = policy.retention_period.parse::<i64>().unwrap_or(0);
    if period <= 0 {
        return json_error(
            400,
            "invalid",
            "bucket does not have a retention policy to lock",
        );
    }
    if !policy.is_locked {
        policy.is_locked = true;
        meta.metageneration = current_meta + 1;
        meta.updated = server.now();
        if write_bucket_meta(server.store(), bucket, &meta).is_err() {
            return json_error(500, "backendError", "internal error");
        }
    }
    Response::json_ok(&crate::http::build_bucket_resource(
        server,
        &bucket_record,
        &meta,
    ))
}

/// Validates and applies a per-object retention update (GCS Object Retention Lock).
pub fn validate_object_retention_update(
    bucket_meta: &GcsBucketMeta,
    existing: &Option<GcsObjectRetention>,
    incoming: Option<GcsObjectRetention>,
    override_unlocked: bool,
    now: &str,
) -> Result<Option<GcsObjectRetention>, Response> {
    if incoming.is_some() && !bucket_meta.object_retention_enabled() {
        return Err(json_error(
            400,
            "invalid",
            "bucket does not have object retention enabled",
        ));
    }
    let now_ts = parse_rfc3339(now).unwrap_or((0, 0));
    let existing_active = existing.as_ref().is_some_and(|r| {
        parse_rfc3339(&r.retain_until_time)
            .map(|ts| ts > now_ts)
            .unwrap_or(true)
    });

    match incoming {
        None => {
            if let Some(cur) = existing {
                if existing_active {
                    if cur.mode.eq_ignore_ascii_case("Locked") {
                        return Err(json_error(
                            403,
                            "forbidden",
                            "cannot remove a Locked object retention configuration",
                        ));
                    }
                    if !override_unlocked {
                        return Err(json_error(
                            403,
                            "forbidden",
                            "overrideUnlockedRetention=true is required to remove Unlocked retention",
                        ));
                    }
                }
            }
            Ok(None)
        }
        Some(raw) => {
            if raw.mode.trim().is_empty() && raw.retain_until_time.trim().is_empty() {
                return validate_object_retention_update(
                    bucket_meta,
                    existing,
                    None,
                    override_unlocked,
                    now,
                );
            }
            let mode = match raw.mode.trim() {
                m if m.eq_ignore_ascii_case("Unlocked") => "Unlocked".to_string(),
                m if m.eq_ignore_ascii_case("Locked") => "Locked".to_string(),
                _ => {
                    return Err(json_error(
                        400,
                        "invalid",
                        "object retention mode must be Unlocked or Locked",
                    ));
                }
            };
            let new_ts = parse_rfc3339(raw.retain_until_time.trim()).ok_or_else(|| {
                json_error(
                    400,
                    "invalid",
                    "object retention retainUntilTime must be RFC3339 UTC",
                )
            })?;
            if let Some(cur) = existing {
                let cur_ts = parse_rfc3339(&cur.retain_until_time).unwrap_or((0, 0));
                if cur.mode.eq_ignore_ascii_case("Locked") {
                    if mode != "Locked" && existing_active {
                        return Err(json_error(
                            403,
                            "forbidden",
                            "cannot downgrade a Locked object retention mode",
                        ));
                    }
                    if new_ts < cur_ts && existing_active {
                        return Err(json_error(
                            403,
                            "forbidden",
                            "cannot shorten retainUntilTime on a Locked object retention",
                        ));
                    }
                } else if new_ts < cur_ts && existing_active && !override_unlocked {
                    return Err(json_error(
                        403,
                        "forbidden",
                        "overrideUnlockedRetention=true is required to shorten Unlocked retention",
                    ));
                }
            }
            Ok(Some(GcsObjectRetention {
                mode,
                retain_until_time: raw.retain_until_time.trim().to_string(),
            }))
        }
    }
}

/// Computes an object's effective bucket-policy `retentionExpirationTime`
/// (empty if the bucket has no active retention policy and no explicit override).
pub fn effective_retention_expiration_time(
    bucket_meta: &GcsBucketMeta,
    object: &Object,
    obj_meta: &GcsObjectMeta,
) -> String {
    if !obj_meta.retention_expiration_time.is_empty() {
        return obj_meta.retention_expiration_time.clone();
    }
    let Some(policy) = &bucket_meta.retention_policy else {
        return String::new();
    };
    let Ok(period) = policy.retention_period.parse::<i64>() else {
        return String::new();
    };
    if period <= 0 {
        return String::new();
    }
    let base = if object.created_at.is_empty() || object.created_at == "0001-01-01T00:00:00Z" {
        &object.last_modified
    } else {
        &object.created_at
    };
    let Some((secs, nanos)) = parse_rfc3339(base) else {
        return String::new();
    };
    rfc3339nano_from_unix(secs.saturating_add(period), nanos)
}

/// Computes a fresh bucket retention expiration time from `now` when an
/// `eventBasedHold` is released.
pub fn recompute_retention_after_event_hold_release(
    bucket_meta: &GcsBucketMeta,
    now: &str,
) -> String {
    let Some(policy) = &bucket_meta.retention_policy else {
        return String::new();
    };
    let Ok(period) = policy.retention_period.parse::<i64>() else {
        return String::new();
    };
    if period <= 0 {
        return String::new();
    }
    let Some((secs, nanos)) = parse_rfc3339(now) else {
        return String::new();
    };
    rfc3339nano_from_unix(secs.saturating_add(period), nanos)
}

/// Checks whether retention policy, per-object retention, or holds forbid
/// deleting or overwriting `object` at `now`. Returns `Some(reason)` if blocked.
pub fn object_mutation_blocked_reason(
    store: &FileBucketStore,
    bucket: &str,
    object: &Object,
    now: &str,
) -> Option<String> {
    let bucket_meta = read_bucket_meta(store, bucket).unwrap_or_default();
    let obj_meta = read_object_meta(store, bucket, &object.key).unwrap_or_default();
    object_mutation_blocked_reason_with_meta(&bucket_meta, object, &obj_meta, now)
}

pub fn object_mutation_blocked_reason_with_meta(
    bucket_meta: &GcsBucketMeta,
    object: &Object,
    obj_meta: &GcsObjectMeta,
    now: &str,
) -> Option<String> {
    if obj_meta.temporary_hold {
        return Some(format!(
            "Object '{}' is under a temporary hold and cannot be deleted or overwritten",
            object.key
        ));
    }
    if obj_meta.event_based_hold {
        return Some(format!(
            "Object '{}' is under an event-based hold and cannot be deleted or overwritten",
            object.key
        ));
    }
    let now_ts = parse_rfc3339(now).unwrap_or((0, 0));
    let bucket_exp = effective_retention_expiration_time(bucket_meta, object, obj_meta);
    if !bucket_exp.is_empty() {
        let locked = parse_rfc3339(&bucket_exp)
            .map(|ts| ts > now_ts)
            .unwrap_or(true);
        if locked {
            return Some(format!(
                "Object '{}' is subject to bucket's retention policy and cannot be deleted or overwritten until {}",
                object.key, bucket_exp
            ));
        }
    }
    if let Some(ret) = &obj_meta.retention {
        if !ret.retain_until_time.is_empty() {
            let locked = parse_rfc3339(&ret.retain_until_time)
                .map(|ts| ts > now_ts)
                .unwrap_or(true);
            if locked {
                return Some(format!(
                    "Object '{}' is subject to object retention ({}) until {}",
                    object.key, ret.mode, ret.retain_until_time
                ));
            }
        }
    }
    None
}
