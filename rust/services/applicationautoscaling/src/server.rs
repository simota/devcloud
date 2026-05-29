//! Mirrors `internal/services/applicationautoscaling/{server,store,persistence,handlers}.go`.
//!
//! In-memory state behind a `Mutex`, persisted to `state.json` (byte-compatible
//! with the Go server). Each operation returns an [`Outcome`] (status + optional
//! AWS error type + JSON body) so the HTTP layer stays thin and the dispatch is
//! directly unit-testable.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

use serde_json::{json, Value};

use crate::time_fmt::now_rfc3339;
use crate::types::*;

const SUPPORTED_NAMESPACE: &str = "dynamodb";

/// Server configuration. Mirrors Go `Config`.
#[derive(Clone, Debug, Default)]
pub struct Config {
    pub addr: String,
    pub region: String,
    pub account_id: String,
    pub auth_mode: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub storage_path: String,
}

/// One operation result: HTTP status, optional `X-Amzn-Errortype`, JSON body.
pub struct Outcome {
    pub status: u16,
    pub error_type: Option<String>,
    pub body: Value,
}

impl Outcome {
    fn ok(body: Value) -> Self {
        Outcome {
            status: 200,
            error_type: None,
            body,
        }
    }
}

#[derive(Default)]
struct State {
    scalable_targets: BTreeMap<String, ScalableTarget>,
    scaling_policies: BTreeMap<String, ScalingPolicy>,
    scheduled_actions: BTreeMap<String, ScheduledAction>,
    tags: BTreeMap<String, BTreeMap<String, String>>,
}

pub struct Server {
    config: Config,
    state: Mutex<State>,
    /// Set when initial load failed, mirroring Go's `loadErr` → 500 on every op.
    load_err: Option<String>,
}

fn default_str<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.is_empty() {
        fallback
    } else {
        value
    }
}

impl Server {
    /// Mirrors `NewServer`: build empty state, then load persisted state if a
    /// storage path is configured.
    pub fn new(config: Config) -> Self {
        let mut server = Server {
            config,
            state: Mutex::new(State::default()),
            load_err: None,
        };
        if !server.config.storage_path.is_empty() {
            if let Err(e) = server.load() {
                server.load_err = Some(e);
            }
        }
        server
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn load_err(&self) -> Option<&str> {
        self.load_err.as_deref()
    }

    fn state_path(&self) -> PathBuf {
        PathBuf::from(&self.config.storage_path).join("state.json")
    }

    /// Mirrors `Server.load`: missing file is not an error.
    fn load(&mut self) -> Result<(), String> {
        let path = self.state_path();
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.to_string()),
        };
        let persisted: PersistedState = serde_json::from_slice(&data).map_err(|e| e.to_string())?;
        let st = self.state.get_mut().unwrap();
        st.scalable_targets = persisted.ScalableTargets;
        st.scaling_policies = persisted.ScalingPolicies;
        st.scheduled_actions = persisted.ScheduledActions;
        st.tags = persisted.Tags;
        Ok(())
    }

    /// Mirrors `Server.persistLocked`: atomic write via `state.json.tmp` rename.
    fn persist(&self, st: &State) -> Result<(), String> {
        if self.config.storage_path.is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.config.storage_path).map_err(|e| e.to_string())?;
        let persisted = PersistedState {
            ScalableTargets: st.scalable_targets.clone(),
            ScalingPolicies: st.scaling_policies.clone(),
            ScheduledActions: st.scheduled_actions.clone(),
            Tags: st.tags.clone(),
        };
        let data = serde_json::to_vec(&persisted).map_err(|e| e.to_string())?;
        let path = self.state_path();
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &data).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())
    }

    fn persist_error() -> Outcome {
        error_outcome(
            500,
            "InternalServerError",
            "failed to persist application-autoscaling state",
        )
    }

    // --- Operation dispatch (mirrors `Server.handle`'s switch) ---

    /// Dispatches a decoded operation name + raw JSON body to the matching
    /// handler. `body` is the request payload bytes.
    pub fn dispatch(&self, operation: &str, body: &[u8]) -> Outcome {
        match operation {
            "RegisterScalableTarget" => self.register_scalable_target(body),
            "DescribeScalableTargets" => self.describe_scalable_targets(body),
            "DeregisterScalableTarget" => self.deregister_scalable_target(body),
            "PutScalingPolicy" => self.put_scaling_policy(body),
            "DescribeScalingPolicies" => self.describe_scaling_policies(body),
            "DeleteScalingPolicy" => self.delete_scaling_policy(body),
            "DescribeScalingActivities" => self.describe_scaling_activities(body),
            "PutScheduledAction" => self.put_scheduled_action(body),
            "DescribeScheduledActions" => self.describe_scheduled_actions(body),
            "DeleteScheduledAction" => self.delete_scheduled_action(body),
            "TagResource" => self.tag_resource(body),
            "UntagResource" => self.untag_resource(body),
            "ListTagsForResource" => self.list_tags_for_resource(body),
            _ => error_outcome(400, "UnknownOperationException", "unknown operation"),
        }
    }

    fn register_scalable_target(&self, body: &[u8]) -> Outcome {
        let req: RegisterScalableTargetRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_namespace(&req.ServiceNamespace) {
            return o;
        }
        if req.ResourceId.is_empty() {
            return validation("ResourceId is required");
        }
        if req.ScalableDimension.is_empty() {
            return validation("ScalableDimension is required");
        }
        let key = scalable_target_key(
            &req.ServiceNamespace,
            &req.ResourceId,
            &req.ScalableDimension,
        );

        let mut st = self.state.lock().unwrap();
        let existing = st.scalable_targets.get(&key).cloned();
        let mut target = ScalableTarget {
            ServiceNamespace: req.ServiceNamespace.clone(),
            ResourceId: req.ResourceId.clone(),
            ScalableDimension: req.ScalableDimension.clone(),
            RoleARN: req.RoleARN.clone(),
            CreationTime: now_rfc3339(),
            ..Default::default()
        };
        if let Some(existing) = existing {
            target.CreationTime = existing.CreationTime;
            target.MinCapacity = existing.MinCapacity;
            target.MaxCapacity = existing.MaxCapacity;
            target.RoleARN = existing.RoleARN;
            target.SuspendedState = existing.SuspendedState;
            if !req.RoleARN.is_empty() {
                target.RoleARN = req.RoleARN.clone();
            }
        }
        if let Some(min) = req.MinCapacity {
            target.MinCapacity = min;
        }
        if let Some(max) = req.MaxCapacity {
            target.MaxCapacity = max;
        }
        if let Some(suspended) = req.SuspendedState {
            target.SuspendedState = suspended;
        }
        st.scalable_targets.insert(key, target);
        if self.persist(&st).is_err() {
            return Self::persist_error();
        }
        Outcome::ok(json!({}))
    }

    fn describe_scalable_targets(&self, body: &[u8]) -> Outcome {
        let req: DescribeScalableTargetsRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_namespace(&req.ServiceNamespace) {
            return o;
        }
        let filter: std::collections::HashSet<&String> = req.ResourceIds.iter().collect();
        let st = self.state.lock().unwrap();
        let mut results: Vec<ScalableTarget> = Vec::new();
        for target in st.scalable_targets.values() {
            if target.ServiceNamespace != req.ServiceNamespace {
                continue;
            }
            if !filter.is_empty() && !filter.contains(&target.ResourceId) {
                continue;
            }
            if !req.ScalableDimension.is_empty()
                && target.ScalableDimension != req.ScalableDimension
            {
                continue;
            }
            results.push(target.clone());
        }
        Outcome::ok(
            serde_json::to_value(DescribeScalableTargetsResponse {
                ScalableTargets: results,
            })
            .unwrap(),
        )
    }

    fn deregister_scalable_target(&self, body: &[u8]) -> Outcome {
        let req: DeregisterScalableTargetRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_namespace(&req.ServiceNamespace) {
            return o;
        }
        if req.ResourceId.is_empty() || req.ScalableDimension.is_empty() {
            return validation("ResourceId and ScalableDimension are required");
        }
        let key = scalable_target_key(
            &req.ServiceNamespace,
            &req.ResourceId,
            &req.ScalableDimension,
        );
        let mut st = self.state.lock().unwrap();
        if !st.scalable_targets.contains_key(&key) {
            return error_outcome(400, "ObjectNotFoundException", "scalable target not found");
        }
        st.scalable_targets.remove(&key);
        let prefix = format!("{key}|");
        st.scaling_policies.retain(|k, _| !k.starts_with(&prefix));
        st.scheduled_actions.retain(|k, _| !k.starts_with(&prefix));
        if self.persist(&st).is_err() {
            return Self::persist_error();
        }
        Outcome::ok(json!({}))
    }

    fn put_scaling_policy(&self, body: &[u8]) -> Outcome {
        let req: PutScalingPolicyRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_namespace(&req.ServiceNamespace) {
            return o;
        }
        if req.PolicyName.is_empty() {
            return validation("PolicyName is required");
        }
        if req.ResourceId.is_empty() || req.ScalableDimension.is_empty() {
            return validation("ResourceId and ScalableDimension are required");
        }
        let mut policy_type = req.PolicyType.clone();
        if policy_type.is_empty() {
            policy_type = "StepScaling".to_string();
        }
        if policy_type != "StepScaling" && policy_type != "TargetTrackingScaling" {
            return validation("PolicyType must be StepScaling or TargetTrackingScaling");
        }
        let key = scaling_policy_key(
            &req.ServiceNamespace,
            &req.ResourceId,
            &req.ScalableDimension,
            &req.PolicyName,
        );
        let region = default_str(&self.config.region, "us-east-1").to_string();
        let account = default_str(&self.config.account_id, "000000000000").to_string();

        let mut st = self.state.lock().unwrap();
        let existing = st.scaling_policies.get(&key).cloned();
        let mut policy = ScalingPolicy {
            PolicyName: req.PolicyName.clone(),
            ServiceNamespace: req.ServiceNamespace.clone(),
            ResourceId: req.ResourceId.clone(),
            ScalableDimension: req.ScalableDimension.clone(),
            PolicyType: policy_type,
            StepScalingPolicyConfiguration: req.StepScalingPolicyConfiguration.clone(),
            TargetTrackingScalingPolicyConfiguration: req
                .TargetTrackingScalingPolicyConfiguration
                .clone(),
            Alarms: Vec::new(),
            CreationTime: now_rfc3339(),
            ..Default::default()
        };
        match existing {
            Some(existing) => {
                policy.PolicyARN = existing.PolicyARN;
                policy.CreationTime = existing.CreationTime;
            }
            None => {
                policy.PolicyARN = build_policy_arn(
                    &region,
                    &account,
                    &req.ServiceNamespace,
                    &req.ResourceId,
                    &req.PolicyName,
                );
            }
        }
        let arn = policy.PolicyARN.clone();
        st.scaling_policies.insert(key, policy);
        if self.persist(&st).is_err() {
            return Self::persist_error();
        }
        Outcome::ok(
            serde_json::to_value(PutScalingPolicyResponse {
                PolicyARN: arn,
                Alarms: Vec::new(),
            })
            .unwrap(),
        )
    }

    fn describe_scaling_policies(&self, body: &[u8]) -> Outcome {
        let req: DescribeScalingPoliciesRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_namespace(&req.ServiceNamespace) {
            return o;
        }
        let filter: std::collections::HashSet<&String> = req.PolicyNames.iter().collect();
        let st = self.state.lock().unwrap();
        let mut results: Vec<ScalingPolicy> = Vec::new();
        for policy in st.scaling_policies.values() {
            if policy.ServiceNamespace != req.ServiceNamespace {
                continue;
            }
            if !req.ResourceId.is_empty() && policy.ResourceId != req.ResourceId {
                continue;
            }
            if !req.ScalableDimension.is_empty()
                && policy.ScalableDimension != req.ScalableDimension
            {
                continue;
            }
            if !filter.is_empty() && !filter.contains(&policy.PolicyName) {
                continue;
            }
            results.push(policy.clone());
        }
        Outcome::ok(
            serde_json::to_value(DescribeScalingPoliciesResponse {
                ScalingPolicies: results,
            })
            .unwrap(),
        )
    }

    fn delete_scaling_policy(&self, body: &[u8]) -> Outcome {
        let req: DeleteScalingPolicyRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_namespace(&req.ServiceNamespace) {
            return o;
        }
        if req.PolicyName.is_empty()
            || req.ResourceId.is_empty()
            || req.ScalableDimension.is_empty()
        {
            return validation("PolicyName, ResourceId, and ScalableDimension are required");
        }
        let key = scaling_policy_key(
            &req.ServiceNamespace,
            &req.ResourceId,
            &req.ScalableDimension,
            &req.PolicyName,
        );
        let mut st = self.state.lock().unwrap();
        if !st.scaling_policies.contains_key(&key) {
            return error_outcome(400, "ObjectNotFoundException", "scaling policy not found");
        }
        st.scaling_policies.remove(&key);
        if self.persist(&st).is_err() {
            return Self::persist_error();
        }
        Outcome::ok(json!({}))
    }

    fn describe_scaling_activities(&self, body: &[u8]) -> Outcome {
        let req: DescribeScalingActivitiesRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_namespace(&req.ServiceNamespace) {
            return o;
        }
        Outcome::ok(
            serde_json::to_value(DescribeScalingActivitiesResponse {
                ScalingActivities: Vec::new(),
            })
            .unwrap(),
        )
    }

    fn put_scheduled_action(&self, body: &[u8]) -> Outcome {
        let req: PutScheduledActionRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_namespace(&req.ServiceNamespace) {
            return o;
        }
        if req.ScheduledActionName.is_empty() {
            return validation("ScheduledActionName is required");
        }
        if req.ResourceId.is_empty() || req.ScalableDimension.is_empty() {
            return validation("ResourceId and ScalableDimension are required");
        }
        let key = scheduled_action_key(
            &req.ServiceNamespace,
            &req.ResourceId,
            &req.ScalableDimension,
            &req.ScheduledActionName,
        );
        let mut st = self.state.lock().unwrap();
        let existing = st.scheduled_actions.get(&key).cloned();
        let mut action = ScheduledAction {
            ScheduledActionName: req.ScheduledActionName.clone(),
            ServiceNamespace: req.ServiceNamespace.clone(),
            ResourceId: req.ResourceId.clone(),
            ScalableDimension: req.ScalableDimension.clone(),
            Schedule: req.Schedule.clone(),
            Timezone: req.Timezone.clone(),
            StartTime: req.StartTime.clone(),
            EndTime: req.EndTime.clone(),
            ScalableTargetAction: req.ScalableTargetAction.clone(),
            CreationTime: now_rfc3339(),
        };
        if let Some(existing) = existing {
            action.CreationTime = existing.CreationTime;
        }
        st.scheduled_actions.insert(key, action);
        if self.persist(&st).is_err() {
            return Self::persist_error();
        }
        Outcome::ok(json!({}))
    }

    fn describe_scheduled_actions(&self, body: &[u8]) -> Outcome {
        let req: DescribeScheduledActionsRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_namespace(&req.ServiceNamespace) {
            return o;
        }
        let filter: std::collections::HashSet<&String> = req.ScheduledActionNames.iter().collect();
        let st = self.state.lock().unwrap();
        let mut results: Vec<ScheduledAction> = Vec::new();
        for action in st.scheduled_actions.values() {
            if action.ServiceNamespace != req.ServiceNamespace {
                continue;
            }
            if !req.ResourceId.is_empty() && action.ResourceId != req.ResourceId {
                continue;
            }
            if !req.ScalableDimension.is_empty()
                && action.ScalableDimension != req.ScalableDimension
            {
                continue;
            }
            if !filter.is_empty() && !filter.contains(&action.ScheduledActionName) {
                continue;
            }
            results.push(action.clone());
        }
        Outcome::ok(
            serde_json::to_value(DescribeScheduledActionsResponse {
                ScheduledActions: results,
            })
            .unwrap(),
        )
    }

    fn delete_scheduled_action(&self, body: &[u8]) -> Outcome {
        let req: DeleteScheduledActionRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if let Err(o) = validate_namespace(&req.ServiceNamespace) {
            return o;
        }
        if req.ScheduledActionName.is_empty()
            || req.ResourceId.is_empty()
            || req.ScalableDimension.is_empty()
        {
            return validation(
                "ScheduledActionName, ResourceId, and ScalableDimension are required",
            );
        }
        let key = scheduled_action_key(
            &req.ServiceNamespace,
            &req.ResourceId,
            &req.ScalableDimension,
            &req.ScheduledActionName,
        );
        let mut st = self.state.lock().unwrap();
        if !st.scheduled_actions.contains_key(&key) {
            return error_outcome(400, "ObjectNotFoundException", "scheduled action not found");
        }
        st.scheduled_actions.remove(&key);
        if self.persist(&st).is_err() {
            return Self::persist_error();
        }
        Outcome::ok(json!({}))
    }

    fn tag_resource(&self, body: &[u8]) -> Outcome {
        let req: TagResourceRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if req.ResourceARN.is_empty() {
            return validation("ResourceARN is required");
        }
        let mut st = self.state.lock().unwrap();
        let bucket = st.tags.entry(req.ResourceARN.clone()).or_default();
        for (k, v) in req.Tags {
            bucket.insert(k, v);
        }
        if self.persist(&st).is_err() {
            return Self::persist_error();
        }
        Outcome::ok(json!({}))
    }

    fn untag_resource(&self, body: &[u8]) -> Outcome {
        let req: UntagResourceRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if req.ResourceARN.is_empty() {
            return validation("ResourceARN is required");
        }
        let mut st = self.state.lock().unwrap();
        if let Some(bucket) = st.tags.get_mut(&req.ResourceARN) {
            for k in &req.TagKeys {
                bucket.remove(k);
            }
            if bucket.is_empty() {
                st.tags.remove(&req.ResourceARN);
            }
        }
        if self.persist(&st).is_err() {
            return Self::persist_error();
        }
        Outcome::ok(json!({}))
    }

    fn list_tags_for_resource(&self, body: &[u8]) -> Outcome {
        let req: ListTagsForResourceRequest = match decode(body) {
            Ok(r) => r,
            Err(o) => return o,
        };
        if req.ResourceARN.is_empty() {
            return validation("ResourceARN is required");
        }
        let st = self.state.lock().unwrap();
        let tags = st.tags.get(&req.ResourceARN).cloned().unwrap_or_default();
        Outcome::ok(serde_json::to_value(ListTagsForResourceResponse { Tags: tags }).unwrap())
    }
}

// --- Free helpers (mirror store.go + handlers.go) ---

fn scalable_target_key(namespace: &str, resource_id: &str, dimension: &str) -> String {
    [namespace, resource_id, dimension].join("|")
}

fn scaling_policy_key(namespace: &str, resource_id: &str, dimension: &str, policy: &str) -> String {
    [namespace, resource_id, dimension, policy].join("|")
}

fn scheduled_action_key(namespace: &str, resource_id: &str, dimension: &str, name: &str) -> String {
    [namespace, resource_id, dimension, name].join("|")
}

fn validate_namespace(ns: &str) -> Result<(), Outcome> {
    if ns.is_empty() {
        return Err(validation("ServiceNamespace is required"));
    }
    if ns != SUPPORTED_NAMESPACE {
        return Err(validation("only dynamodb namespace is supported"));
    }
    Ok(())
}

fn build_policy_arn(
    region: &str,
    account: &str,
    namespace: &str,
    resource_id: &str,
    policy_name: &str,
) -> String {
    format!(
        "arn:aws:autoscaling:{region}:{account}:scalingPolicy:{}:resource/{namespace}/{resource_id}:policyName/{policy_name}",
        new_uuid()
    )
}

/// A v4-shaped UUID. The Go server uses crypto randomness, but the value is not
/// behaviorally observable (only the ARN shape is asserted), so we derive a
/// unique, correctly-formatted id from time + an atomic counter.
fn new_uuid() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&nanos.to_be_bytes());
    b[8..].copy_from_slice(&n.to_be_bytes());
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant
    let h = hex::encode(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

fn decode<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, Outcome> {
    serde_json::from_slice(body)
        .map_err(|_| error_outcome(400, "SerializationException", "invalid json request"))
}

fn validation(message: &str) -> Outcome {
    error_outcome(400, "ValidationException", message)
}

/// Builds an AWS-shaped error body, mirroring Go's `writeError`.
pub fn error_outcome(status: u16, name: &str, message: &str) -> Outcome {
    Outcome {
        status,
        error_type: Some(name.to_string()),
        body: json!({
            "__type": format!("com.amazonaws.application-autoscaling.v20160206#{name}"),
            "message": message,
        }),
    }
}
