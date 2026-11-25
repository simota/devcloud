use serde_json::{json, Value};

use crate::server::{invalid, Reply};

const LATEST: &str = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST";
const REVISION: &str = "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION";
const UNSPECIFIED: &str = "TRAFFIC_TARGET_ALLOCATION_TYPE_UNSPECIFIED";

pub(crate) fn default_targets() -> Value {
    json!([{ "type": LATEST, "percent": 100 }])
}

/// Validates the desired allocations and resolves them to concrete revision IDs.
pub(crate) fn statuses(
    traffic: &Value,
    latest: &str,
    exists: impl Fn(&str) -> bool,
) -> Result<Vec<Value>, Reply> {
    let Some(targets) = traffic.as_array() else {
        return Err(invalid("traffic must be a list of traffic targets"));
    };
    if targets.is_empty() {
        return statuses(&default_targets(), latest, exists);
    }
    let mut total = 0;
    let mut statuses = Vec::with_capacity(targets.len());
    for (i, target) in targets.iter().enumerate() {
        if !target.is_object() {
            return Err(invalid(&format!("traffic[{i}] must be an object")));
        }
        let percent = match target.get("percent") {
            None => 0,
            Some(p) => p.as_u64().filter(|p| *p <= 100).ok_or_else(|| {
                invalid(&format!(
                    "traffic[{i}].percent must be an integer between 0 and 100"
                ))
            })?,
        };
        let revision = match target.get("revision") {
            None => "",
            Some(r) => r
                .as_str()
                .ok_or_else(|| invalid(&format!("traffic[{i}].revision must be a revision ID")))?,
        };
        let kind = match target.get("type") {
            None => UNSPECIFIED,
            Some(t) => t
                .as_str()
                .ok_or_else(|| invalid(&format!("traffic[{i}].type must be an allocation type")))?,
        };
        let (kind, revision) = match (kind, revision) {
            (LATEST | UNSPECIFIED, "") => (LATEST, latest),
            (REVISION | UNSPECIFIED, id) if exists(id) => (REVISION, id),
            _ => {
                return Err(invalid(&format!(
                    "traffic[{i}] must target latest or an existing revision of this service"
                )))
            }
        };
        let mut status = json!({ "type": kind, "revision": revision, "percent": percent });
        if let Some(tag) = target.get("tag") {
            let tag = tag
                .as_str()
                .ok_or_else(|| invalid(&format!("traffic[{i}].tag must be a string")))?;
            if !tag.is_empty() {
                crate::server::validate_id(tag, "traffic tag", 63)?;
                if statuses.iter().any(|s: &Value| s["tag"] == tag) {
                    return Err(invalid("traffic tags must be unique"));
                }
                status["tag"] = json!(tag);
            }
        }
        total += percent;
        statuses.push(status);
    }
    if total != 100 {
        return Err(invalid(&format!(
            "traffic percentages must sum to 100, got {total}"
        )));
    }
    Ok(statuses)
}

pub(crate) fn active(service: &Value) -> impl Iterator<Item = (&str, u64)> {
    service["trafficStatuses"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|target| {
            let percent = target["percent"].as_u64()?;
            (percent > 0).then_some((target["revision"].as_str()?, percent))
        })
}
