//! Mirrors `internal/services/applicationautoscaling/types.go`.
//!
//! Wire serialization is pinned to Go's `encoding/json`: struct fields keep
//! their declaration order, `omitempty` maps to `skip_serializing_if`, and
//! map-valued fields use `BTreeMap` / `serde_json::Value` (both sorted) to
//! match Go's sorted-key map marshaling.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// --- Stored entities ---

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScalableTarget {
    pub ServiceNamespace: String,
    pub ResourceId: String,
    pub ScalableDimension: String,
    pub MinCapacity: i64,
    pub MaxCapacity: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub RoleARN: String,
    // Go tags this `omitempty`, but `encoding/json`'s omitempty has NO effect on
    // struct values (only nil pointers / empty maps/slices / zero scalars), so
    // Go always emits `"SuspendedState":{}`. Match that — do NOT skip it.
    #[serde(default)]
    pub SuspendedState: SuspendedState,
    /// RFC 3339 UTC string (Go `time.Time`).
    pub CreationTime: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SuspendedState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub DynamicScalingInSuspended: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub DynamicScalingOutSuspended: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ScheduledScalingSuspended: Option<bool>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScalingPolicy {
    pub PolicyARN: String,
    pub PolicyName: String,
    pub ServiceNamespace: String,
    pub ResourceId: String,
    pub ScalableDimension: String,
    pub PolicyType: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub StepScalingPolicyConfiguration: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub TargetTrackingScalingPolicyConfiguration: Option<Value>,
    pub Alarms: Vec<Value>,
    pub CreationTime: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScheduledAction {
    pub ScheduledActionName: String,
    pub ServiceNamespace: String,
    pub ResourceId: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ScalableDimension: String,
    pub Schedule: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub Timezone: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub StartTime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub EndTime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ScalableTargetAction: Option<Value>,
    pub CreationTime: String,
}

// --- Request types (all fields optional on the wire; mirror Go pointer/zero
//     semantics with Option/defaults) ---

#[derive(Debug, Default, Deserialize)]
pub struct RegisterScalableTargetRequest {
    #[serde(default)]
    pub ServiceNamespace: String,
    #[serde(default)]
    pub ResourceId: String,
    #[serde(default)]
    pub ScalableDimension: String,
    pub MinCapacity: Option<i64>,
    pub MaxCapacity: Option<i64>,
    #[serde(default)]
    pub RoleARN: String,
    pub SuspendedState: Option<SuspendedState>,
    #[serde(default)]
    pub Tags: BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DescribeScalableTargetsRequest {
    #[serde(default)]
    pub ServiceNamespace: String,
    #[serde(default)]
    pub ResourceIds: Vec<String>,
    #[serde(default)]
    pub ScalableDimension: String,
}

#[derive(Serialize)]
pub struct DescribeScalableTargetsResponse {
    pub ScalableTargets: Vec<ScalableTarget>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DeregisterScalableTargetRequest {
    #[serde(default)]
    pub ServiceNamespace: String,
    #[serde(default)]
    pub ResourceId: String,
    #[serde(default)]
    pub ScalableDimension: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct PutScalingPolicyRequest {
    #[serde(default)]
    pub PolicyName: String,
    #[serde(default)]
    pub ServiceNamespace: String,
    #[serde(default)]
    pub ResourceId: String,
    #[serde(default)]
    pub ScalableDimension: String,
    #[serde(default)]
    pub PolicyType: String,
    pub StepScalingPolicyConfiguration: Option<Value>,
    pub TargetTrackingScalingPolicyConfiguration: Option<Value>,
}

#[derive(Serialize)]
pub struct PutScalingPolicyResponse {
    pub PolicyARN: String,
    pub Alarms: Vec<Value>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DescribeScalingPoliciesRequest {
    #[serde(default)]
    pub PolicyNames: Vec<String>,
    #[serde(default)]
    pub ServiceNamespace: String,
    #[serde(default)]
    pub ResourceId: String,
    #[serde(default)]
    pub ScalableDimension: String,
}

#[derive(Serialize)]
pub struct DescribeScalingPoliciesResponse {
    pub ScalingPolicies: Vec<ScalingPolicy>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DeleteScalingPolicyRequest {
    #[serde(default)]
    pub PolicyName: String,
    #[serde(default)]
    pub ServiceNamespace: String,
    #[serde(default)]
    pub ResourceId: String,
    #[serde(default)]
    pub ScalableDimension: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct DescribeScalingActivitiesRequest {
    #[serde(default)]
    pub ServiceNamespace: String,
    #[serde(default)]
    pub ResourceId: String,
    #[serde(default)]
    pub ScalableDimension: String,
}

#[derive(Serialize)]
pub struct DescribeScalingActivitiesResponse {
    pub ScalingActivities: Vec<Value>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PutScheduledActionRequest {
    #[serde(default)]
    pub ServiceNamespace: String,
    #[serde(default)]
    pub ScheduledActionName: String,
    #[serde(default)]
    pub ResourceId: String,
    #[serde(default)]
    pub ScalableDimension: String,
    #[serde(default)]
    pub Schedule: String,
    #[serde(default)]
    pub Timezone: String,
    pub StartTime: Option<String>,
    pub EndTime: Option<String>,
    pub ScalableTargetAction: Option<Value>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DescribeScheduledActionsRequest {
    #[serde(default)]
    pub ServiceNamespace: String,
    #[serde(default)]
    pub ScheduledActionNames: Vec<String>,
    #[serde(default)]
    pub ResourceId: String,
    #[serde(default)]
    pub ScalableDimension: String,
}

#[derive(Serialize)]
pub struct DescribeScheduledActionsResponse {
    pub ScheduledActions: Vec<ScheduledAction>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DeleteScheduledActionRequest {
    #[serde(default)]
    pub ServiceNamespace: String,
    #[serde(default)]
    pub ScheduledActionName: String,
    #[serde(default)]
    pub ResourceId: String,
    #[serde(default)]
    pub ScalableDimension: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct TagResourceRequest {
    #[serde(default)]
    pub ResourceARN: String,
    #[serde(default)]
    pub Tags: BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct UntagResourceRequest {
    #[serde(default)]
    pub ResourceARN: String,
    #[serde(default)]
    pub TagKeys: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListTagsForResourceRequest {
    #[serde(default)]
    pub ResourceARN: String,
}

#[derive(Serialize)]
pub struct ListTagsForResourceResponse {
    pub Tags: BTreeMap<String, String>,
}

// --- Persistence schema (mirrors Go `persistedState`) ---

#[derive(Default, Serialize, Deserialize)]
pub struct PersistedState {
    #[serde(default)]
    pub ScalableTargets: BTreeMap<String, ScalableTarget>,
    #[serde(default)]
    pub ScalingPolicies: BTreeMap<String, ScalingPolicy>,
    #[serde(default)]
    pub ScheduledActions: BTreeMap<String, ScheduledAction>,
    #[serde(default)]
    pub Tags: BTreeMap<String, BTreeMap<String, String>>,
}
