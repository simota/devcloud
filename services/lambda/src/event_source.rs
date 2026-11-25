//! Event source mapping control plane records and validation.
//!
//! Stores and validates AWS Lambda event source mapping configurations
//! (`/2015-03-31/event-source-mappings`). Polling of the local SQS and
//! DynamoDB Streams services lives in `event_poller`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::server::{parse_json, parse_pagination, str_field, Reply, Server};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct EventSourceMappingRecord {
    pub uuid: String,
    pub event_source_arn: String,
    pub function_arn: String,
    pub function_name: String,
    pub enabled: bool,
    pub state: String,
    pub state_transition_reason: String,
    pub last_modified: f64,
    pub batch_size: i64,
    #[serde(default)]
    pub maximum_batching_window_in_seconds: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starting_position: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starting_position_timestamp: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallelization_factor: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_record_age_in_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bisect_batch_on_function_error: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_retry_attempts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tumbling_window_in_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queues: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub function_response_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_criteria: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaling_config: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_config: Option<Value>,
}

impl EventSourceMappingRecord {
    pub(crate) fn to_json(&self, region: &str, account_id: &str) -> Value {
        let mut v = json!({
            "UUID": self.uuid,
            "EventSourceMappingArn": format!(
                "arn:aws:lambda:{region}:{account_id}:event-source-mapping:{}",
                self.uuid
            ),
            "EventSourceArn": self.event_source_arn,
            "FunctionArn": self.function_arn,
            "State": self.state,
            "StateTransitionReason": self.state_transition_reason,
            "LastModified": self.last_modified,
            "BatchSize": self.batch_size,
            "MaximumBatchingWindowInSeconds": self.maximum_batching_window_in_seconds,
        });
        if let Some(sp) = &self.starting_position {
            v["StartingPosition"] = json!(sp);
        }
        if let Some(ts) = &self.starting_position_timestamp {
            v["StartingPositionTimestamp"] = ts.clone();
        }
        if let Some(pf) = self.parallelization_factor {
            v["ParallelizationFactor"] = json!(pf);
        }
        if let Some(age) = self.maximum_record_age_in_seconds {
            v["MaximumRecordAgeInSeconds"] = json!(age);
        }
        if let Some(bisect) = self.bisect_batch_on_function_error {
            v["BisectBatchOnFunctionError"] = json!(bisect);
        }
        if let Some(retries) = self.maximum_retry_attempts {
            v["MaximumRetryAttempts"] = json!(retries);
        }
        if let Some(tw) = self.tumbling_window_in_seconds {
            v["TumblingWindowInSeconds"] = json!(tw);
        }
        if !self.topics.is_empty() {
            v["Topics"] = json!(self.topics);
        }
        if !self.queues.is_empty() {
            v["Queues"] = json!(self.queues);
        }
        if !self.function_response_types.is_empty() {
            v["FunctionResponseTypes"] = json!(self.function_response_types);
        }
        if let Some(fc) = &self.filter_criteria {
            v["FilterCriteria"] = fc.clone();
        }
        if let Some(sc) = &self.scaling_config {
            v["ScalingConfig"] = sc.clone();
        }
        if let Some(dc) = &self.destination_config {
            v["DestinationConfig"] = dc.clone();
        }
        v
    }
}

/// Validated fields shared between CreateEventSourceMapping and
/// UpdateEventSourceMapping; `None` means omitted in the request.
pub(crate) struct MappingPatch {
    pub enabled: Option<bool>,
    pub batch_size: Option<i64>,
    pub maximum_batching_window_in_seconds: Option<i64>,
    pub starting_position: Option<String>,
    pub starting_position_timestamp: Option<Value>,
    pub parallelization_factor: Option<i64>,
    pub maximum_record_age_in_seconds: Option<i64>,
    pub bisect_batch_on_function_error: Option<bool>,
    pub maximum_retry_attempts: Option<i64>,
    pub tumbling_window_in_seconds: Option<i64>,
    pub topics: Option<Vec<String>>,
    pub queues: Option<Vec<String>>,
    pub function_response_types: Option<Vec<String>>,
    pub filter_criteria: Option<Option<Value>>,
    pub scaling_config: Option<Option<Value>>,
    pub destination_config: Option<Option<Value>>,
}

fn validation(message: &str) -> Reply {
    Reply::error(400, "ValidationException", message)
}

fn invalid_param(message: &str) -> Reply {
    Reply::error(400, "InvalidParameterValueException", message)
}

pub(crate) fn now_epoch_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

pub(crate) fn default_batch_size(event_source_arn: &str) -> i64 {
    if event_source_arn.starts_with("arn:") && event_source_arn.contains(":sqs:") {
        10
    } else {
        100
    }
}

pub(crate) fn parse_patch(req: &Value) -> Result<MappingPatch, Reply> {
    let opt_bool = |key: &str| -> Result<Option<bool>, Reply> {
        match req.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(_) => Err(validation(&format!("{key} must be a boolean"))),
        }
    };
    let opt_int = |key: &str, min: i64, max: i64| -> Result<Option<i64>, Reply> {
        match req.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => match v.as_i64() {
                Some(n) if (min..=max).contains(&n) => Ok(Some(n)),
                _ => Err(validation(&format!(
                    "{key} must be an integer between {min} and {max}"
                ))),
            },
        }
    };
    let opt_str_list = |key: &str| -> Result<Option<Vec<String>>, Reply> {
        match req.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Array(arr)) => arr
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| validation(&format!("{key} must be a list of strings")))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Some),
            Some(_) => Err(validation(&format!("{key} must be a list of strings"))),
        }
    };

    let enabled = opt_bool("Enabled")?;
    let batch_size = opt_int("BatchSize", 1, 10000)?;
    let maximum_batching_window_in_seconds = opt_int("MaximumBatchingWindowInSeconds", 0, 300)?;
    let starting_position = match req.get("StartingPosition") {
        None | Some(Value::Null) => None,
        Some(Value::String(s))
            if matches!(s.as_str(), "TRIM_HORIZON" | "LATEST" | "AT_TIMESTAMP") =>
        {
            Some(s.clone())
        }
        Some(other) => {
            return Err(validation(&format!(
                "Invalid StartingPosition '{other}'; must be TRIM_HORIZON, LATEST, or AT_TIMESTAMP"
            )));
        }
    };
    let starting_position_timestamp = req
        .get("StartingPositionTimestamp")
        .filter(|v| !v.is_null())
        .cloned();
    let parallelization_factor = opt_int("ParallelizationFactor", 1, 10)?;
    let maximum_record_age_in_seconds = match req.get("MaximumRecordAgeInSeconds") {
        None | Some(Value::Null) => None,
        Some(v) => match v.as_i64() {
            Some(-1) => Some(-1),
            Some(n) if (60..=604800).contains(&n) => Some(n),
            _ => {
                return Err(validation(
                    "MaximumRecordAgeInSeconds must be -1 or between 60 and 604800",
                ));
            }
        },
    };
    let bisect_batch_on_function_error = opt_bool("BisectBatchOnFunctionError")?;
    let maximum_retry_attempts = opt_int("MaximumRetryAttempts", -1, 10000)?;
    let tumbling_window_in_seconds = opt_int("TumblingWindowInSeconds", 0, 900)?;
    let topics = opt_str_list("Topics")?;
    let queues = opt_str_list("Queues")?;
    let function_response_types = opt_str_list("FunctionResponseTypes")?;
    if let Some(types) = &function_response_types {
        for t in types {
            if t != "ReportBatchItemFailures" {
                return Err(validation(&format!(
                    "Invalid FunctionResponseTypes value '{t}'; must be ReportBatchItemFailures"
                )));
            }
        }
    }
    let filter_criteria = match req.get("FilterCriteria") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(v) => {
            validate_filter_criteria(v)?;
            Some(Some(v.clone()))
        }
    };
    let scaling_config = match req.get("ScalingConfig") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(v) => {
            validate_scaling_config(v)?;
            Some(Some(v.clone()))
        }
    };
    let destination_config = match req.get("DestinationConfig") {
        None => None,
        Some(Value::Null) => Some(None),
        Some(v) if v.is_object() => Some(Some(v.clone())),
        Some(_) => return Err(validation("DestinationConfig must be an object")),
    };

    Ok(MappingPatch {
        enabled,
        batch_size,
        maximum_batching_window_in_seconds,
        starting_position,
        starting_position_timestamp,
        parallelization_factor,
        maximum_record_age_in_seconds,
        bisect_batch_on_function_error,
        maximum_retry_attempts,
        tumbling_window_in_seconds,
        topics,
        queues,
        function_response_types,
        filter_criteria,
        scaling_config,
        destination_config,
    })
}

fn validate_filter_criteria(v: &Value) -> Result<(), Reply> {
    let Some(obj) = v.as_object() else {
        return Err(validation("FilterCriteria must be an object"));
    };
    let Some(filters) = obj.get("Filters") else {
        return Ok(());
    };
    let Some(arr) = filters.as_array() else {
        return Err(validation("FilterCriteria.Filters must be an array"));
    };
    if arr.len() > 5 {
        return Err(invalid_param(
            "FilterCriteria.Filters cannot have more than 5 filters",
        ));
    }
    for item in arr {
        let Some(fobj) = item.as_object() else {
            return Err(validation(
                "Each filter in FilterCriteria.Filters must be an object",
            ));
        };
        if let Some(pat) = fobj.get("Pattern") {
            let Some(s) = pat.as_str() else {
                return Err(validation(
                    "FilterCriteria.Filters[].Pattern must be a string",
                ));
            };
            match serde_json::from_str::<Value>(s) {
                Ok(Value::Object(_)) => {}
                _ => {
                    return Err(invalid_param(
                        "Invalid filter pattern: Pattern must be a valid JSON object",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_scaling_config(v: &Value) -> Result<(), Reply> {
    let Some(obj) = v.as_object() else {
        return Err(validation("ScalingConfig must be an object"));
    };
    if let Some(mc) = obj.get("MaximumConcurrency").filter(|v| !v.is_null()) {
        match mc.as_i64() {
            Some(n) if (2..=1000).contains(&n) => {}
            _ => {
                return Err(validation(
                    "ScalingConfig.MaximumConcurrency must be between 2 and 1000",
                ));
            }
        }
    }
    Ok(())
}

impl Server {
    /// The mapping as returned by Get/List, with the poller's
    /// `LastProcessingResult` once it has been polled.
    fn mapping_json(&self, rec: &EventSourceMappingRecord) -> Value {
        let mut v = rec.to_json(self.region(), self.account_id());
        if let Some(result) = self.last_processing_result(&rec.uuid) {
            v["LastProcessingResult"] = json!(result);
        }
        v
    }

    fn esm_not_found(&self, uuid: &str) -> Reply {
        Reply::error(
            404,
            "ResourceNotFoundException",
            &format!("The resource you requested does not exist. (EventSourceMapping {uuid})"),
        )
    }

    /// `POST /2015-03-31/event-source-mappings` (`CreateEventSourceMapping`).
    pub fn create_event_source_mapping(&self, body: &[u8]) -> Reply {
        let req = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let fn_id = str_field(&req, "FunctionName");
        if fn_id.is_empty() {
            return validation("1 validation error detected: Value null at 'functionName' failed to satisfy constraint: Member must not be null");
        }
        let event_source_arn = str_field(&req, "EventSourceArn");
        if event_source_arn.is_empty() {
            return validation("1 validation error detected: Value null at 'eventSourceArn' failed to satisfy constraint: Member must not be null");
        }
        let patch = match parse_patch(&req) {
            Ok(p) => p,
            Err(r) => return r,
        };
        let resolved = match self.lookup(&fn_id, None) {
            Ok(r) => r,
            Err(r) => return r,
        };
        let function_arn =
            self.qualified_function_arn(&resolved.function_name, resolved.arn_qualifier.as_deref());
        let enabled = patch.enabled.unwrap_or(true);
        let uuid = self.new_id();
        let rec = EventSourceMappingRecord {
            uuid: uuid.clone(),
            event_source_arn: event_source_arn.clone(),
            function_arn: function_arn.clone(),
            function_name: resolved.function_name,
            enabled,
            state: if enabled { "Enabled" } else { "Disabled" }.to_string(),
            state_transition_reason: "USER_INITIATED".to_string(),
            last_modified: now_epoch_f64(),
            batch_size: patch
                .batch_size
                .unwrap_or_else(|| default_batch_size(&event_source_arn)),
            maximum_batching_window_in_seconds: patch
                .maximum_batching_window_in_seconds
                .unwrap_or(0),
            starting_position: patch.starting_position,
            starting_position_timestamp: patch.starting_position_timestamp,
            parallelization_factor: patch.parallelization_factor,
            maximum_record_age_in_seconds: patch.maximum_record_age_in_seconds,
            bisect_batch_on_function_error: patch.bisect_batch_on_function_error,
            maximum_retry_attempts: patch.maximum_retry_attempts,
            tumbling_window_in_seconds: patch.tumbling_window_in_seconds,
            topics: patch.topics.unwrap_or_default(),
            queues: patch.queues.unwrap_or_default(),
            function_response_types: patch.function_response_types.unwrap_or_default(),
            filter_criteria: patch.filter_criteria.flatten(),
            scaling_config: patch.scaling_config.flatten(),
            destination_config: patch.destination_config.flatten(),
        };
        let result = self.commit(|st| {
            if st
                .event_source_mappings
                .values()
                .any(|m| m.event_source_arn == event_source_arn && m.function_arn == function_arn)
            {
                return Err(Reply::error(
                    409,
                    "ResourceConflictException",
                    &format!(
                        "An event source mapping with EventSourceArn {event_source_arn} and FunctionArn {function_arn} already exists"
                    ),
                ));
            }
            st.event_source_mappings.insert(uuid.clone(), rec.clone());
            Ok(())
        });
        match result {
            Ok(()) => Reply::json(202, &rec.to_json(self.region(), self.account_id())),
            Err(r) => r,
        }
    }

    /// `GET /2015-03-31/event-source-mappings/<uuid>` (`GetEventSourceMapping`).
    pub fn get_event_source_mapping(&self, uuid: &str) -> Reply {
        let st = self.state.lock().unwrap();
        match st.event_source_mappings.get(uuid) {
            Some(rec) => Reply::json(200, &self.mapping_json(rec)),
            None => self.esm_not_found(uuid),
        }
    }

    /// `GET /2015-03-31/event-source-mappings` (`ListEventSourceMappings`).
    pub fn list_event_source_mappings(&self, query: &BTreeMap<String, String>) -> Reply {
        let (start, max_items) = match parse_pagination(query) {
            Ok(p) => p,
            Err(r) => return r,
        };
        let source_filter = query.get("EventSourceArn").map(String::as_str);
        let fn_filter = query
            .get("FunctionName")
            .map(String::as_str)
            .filter(|s| !s.is_empty());
        let resolved_fn_arn = fn_filter.and_then(|f| {
            self.lookup(f, None)
                .ok()
                .map(|r| self.qualified_function_arn(&r.function_name, r.arn_qualifier.as_deref()))
        });
        let st = self.state.lock().unwrap();
        let matching: Vec<Value> = st
            .event_source_mappings
            .values()
            .filter(|m| {
                if let Some(src) = source_filter.filter(|s| !s.is_empty()) {
                    if m.event_source_arn != src {
                        return false;
                    }
                }
                if let Some(f) = fn_filter {
                    let matches_resolved = resolved_fn_arn
                        .as_deref()
                        .is_some_and(|arn| m.function_arn == arn);
                    let matches_raw = m.function_name == f || m.function_arn == f;
                    if !matches_resolved && !matches_raw {
                        return false;
                    }
                }
                true
            })
            .map(|m| self.mapping_json(m))
            .collect();
        let page: Vec<Value> = matching
            .iter()
            .skip(start)
            .take(max_items)
            .cloned()
            .collect();
        let mut out = json!({ "EventSourceMappings": page });
        let next = start.saturating_add(max_items);
        if next < matching.len() {
            out["NextMarker"] = json!(next.to_string());
        }
        Reply::json(200, &out)
    }

    /// `PUT /2015-03-31/event-source-mappings/<uuid>` (`UpdateEventSourceMapping`).
    pub fn update_event_source_mapping(&self, uuid: &str, body: &[u8]) -> Reply {
        let req = match parse_json(body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let patch = match parse_patch(&req) {
            Ok(p) => p,
            Err(r) => return r,
        };
        let new_fn = match req.get("FunctionName").and_then(Value::as_str) {
            Some(id) if !id.is_empty() => {
                let resolved = match self.lookup(id, None) {
                    Ok(r) => r,
                    Err(r) => return r,
                };
                Some((
                    resolved.function_name.clone(),
                    self.qualified_function_arn(
                        &resolved.function_name,
                        resolved.arn_qualifier.as_deref(),
                    ),
                ))
            }
            _ => None,
        };
        let result = self.commit(|st| {
            let rec = st
                .event_source_mappings
                .get_mut(uuid)
                .ok_or_else(|| self.esm_not_found(uuid))?;
            if let Some((fn_name, fn_arn)) = new_fn.clone() {
                rec.function_name = fn_name;
                rec.function_arn = fn_arn;
            }
            if let Some(enabled) = patch.enabled {
                rec.enabled = enabled;
                rec.state = if enabled { "Enabled" } else { "Disabled" }.to_string();
                rec.state_transition_reason = "USER_INITIATED".to_string();
            }
            if let Some(bs) = patch.batch_size {
                rec.batch_size = bs;
            }
            if let Some(w) = patch.maximum_batching_window_in_seconds {
                rec.maximum_batching_window_in_seconds = w;
            }
            if let Some(pf) = patch.parallelization_factor {
                rec.parallelization_factor = Some(pf);
            }
            if let Some(age) = patch.maximum_record_age_in_seconds {
                rec.maximum_record_age_in_seconds = Some(age);
            }
            if let Some(bisect) = patch.bisect_batch_on_function_error {
                rec.bisect_batch_on_function_error = Some(bisect);
            }
            if let Some(retries) = patch.maximum_retry_attempts {
                rec.maximum_retry_attempts = Some(retries);
            }
            if let Some(tw) = patch.tumbling_window_in_seconds {
                rec.tumbling_window_in_seconds = Some(tw);
            }
            if let Some(rt) = patch.function_response_types.clone() {
                rec.function_response_types = rt;
            }
            if let Some(fc) = patch.filter_criteria.clone() {
                rec.filter_criteria = fc;
            }
            if let Some(sc) = patch.scaling_config.clone() {
                rec.scaling_config = sc;
            }
            if let Some(dc) = patch.destination_config.clone() {
                rec.destination_config = dc;
            }
            rec.last_modified = now_epoch_f64();
            Ok(rec.clone())
        });
        match result {
            Ok(rec) => Reply::json(202, &rec.to_json(self.region(), self.account_id())),
            Err(r) => r,
        }
    }

    /// `DELETE /2015-03-31/event-source-mappings/<uuid>` (`DeleteEventSourceMapping`).
    pub fn delete_event_source_mapping(&self, uuid: &str) -> Reply {
        let result = self.commit(|st| {
            let mut removed = st
                .event_source_mappings
                .remove(uuid)
                .ok_or_else(|| self.esm_not_found(uuid))?;
            removed.state = "Deleting".to_string();
            removed.state_transition_reason = "USER_INITIATED".to_string();
            removed.last_modified = now_epoch_f64();
            Ok(removed)
        });
        match result {
            Ok(rec) => Reply::json(202, &rec.to_json(self.region(), self.account_id())),
            Err(r) => r,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_filter_criteria_and_scaling_config() {
        assert!(parse_patch(&json!({
            "BatchSize": 10,
            "FilterCriteria": { "Filters": [{ "Pattern": "{\"body\":{\"ok\":[true]}}" }] },
            "ScalingConfig": { "MaximumConcurrency": 5 },
            "FunctionResponseTypes": ["ReportBatchItemFailures"],
        }))
        .is_ok());

        assert!(parse_patch(&json!({
            "FilterCriteria": { "Filters": [{ "Pattern": "{bad json" }] }
        }))
        .is_err());

        assert!(parse_patch(&json!({
            "ScalingConfig": { "MaximumConcurrency": 1 }
        }))
        .is_err());

        assert!(parse_patch(&json!({ "BatchSize": 0 })).is_err());
        assert!(parse_patch(&json!({ "StartingPosition": "INVALID" })).is_err());
    }
}
