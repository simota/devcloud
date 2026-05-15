package applicationautoscaling

import "time"

type scalableTarget struct {
	ServiceNamespace  string         `json:"ServiceNamespace"`
	ResourceId        string         `json:"ResourceId"`
	ScalableDimension string         `json:"ScalableDimension"`
	MinCapacity       int            `json:"MinCapacity"`
	MaxCapacity       int            `json:"MaxCapacity"`
	RoleARN           string         `json:"RoleARN,omitempty"`
	SuspendedState    suspendedState `json:"SuspendedState,omitempty"`
	CreationTime      time.Time      `json:"CreationTime"`
}

type suspendedState struct {
	DynamicScalingInSuspended  *bool `json:"DynamicScalingInSuspended,omitempty"`
	DynamicScalingOutSuspended *bool `json:"DynamicScalingOutSuspended,omitempty"`
	ScheduledScalingSuspended  *bool `json:"ScheduledScalingSuspended,omitempty"`
}

type scalingPolicy struct {
	PolicyARN                                string         `json:"PolicyARN"`
	PolicyName                               string         `json:"PolicyName"`
	ServiceNamespace                         string         `json:"ServiceNamespace"`
	ResourceId                               string         `json:"ResourceId"`
	ScalableDimension                        string         `json:"ScalableDimension"`
	PolicyType                               string         `json:"PolicyType"`
	StepScalingPolicyConfiguration           map[string]any `json:"StepScalingPolicyConfiguration,omitempty"`
	TargetTrackingScalingPolicyConfiguration map[string]any `json:"TargetTrackingScalingPolicyConfiguration,omitempty"`
	Alarms                                   []any          `json:"Alarms"`
	CreationTime                             time.Time      `json:"CreationTime"`
}

type scheduledAction struct {
	ScheduledActionName  string         `json:"ScheduledActionName"`
	ServiceNamespace     string         `json:"ServiceNamespace"`
	ResourceId           string         `json:"ResourceId"`
	ScalableDimension    string         `json:"ScalableDimension,omitempty"`
	Schedule             string         `json:"Schedule"`
	Timezone             string         `json:"Timezone,omitempty"`
	StartTime            *time.Time     `json:"StartTime,omitempty"`
	EndTime              *time.Time     `json:"EndTime,omitempty"`
	ScalableTargetAction map[string]any `json:"ScalableTargetAction,omitempty"`
	CreationTime         time.Time      `json:"CreationTime"`
}

// Request types.

type registerScalableTargetRequest struct {
	ServiceNamespace  string            `json:"ServiceNamespace"`
	ResourceId        string            `json:"ResourceId"`
	ScalableDimension string            `json:"ScalableDimension"`
	MinCapacity       *int              `json:"MinCapacity"`
	MaxCapacity       *int              `json:"MaxCapacity"`
	RoleARN           string            `json:"RoleARN"`
	SuspendedState    *suspendedState   `json:"SuspendedState"`
	Tags              map[string]string `json:"Tags"`
}

type describeScalableTargetsRequest struct {
	ServiceNamespace  string   `json:"ServiceNamespace"`
	ResourceIds       []string `json:"ResourceIds"`
	ScalableDimension string   `json:"ScalableDimension"`
	MaxResults        int      `json:"MaxResults"`
	NextToken         string   `json:"NextToken"`
}

type describeScalableTargetsResponse struct {
	ScalableTargets []scalableTarget `json:"ScalableTargets"`
}

type deregisterScalableTargetRequest struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceId        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
}

type putScalingPolicyRequest struct {
	PolicyName                               string         `json:"PolicyName"`
	ServiceNamespace                         string         `json:"ServiceNamespace"`
	ResourceId                               string         `json:"ResourceId"`
	ScalableDimension                        string         `json:"ScalableDimension"`
	PolicyType                               string         `json:"PolicyType"`
	StepScalingPolicyConfiguration           map[string]any `json:"StepScalingPolicyConfiguration"`
	TargetTrackingScalingPolicyConfiguration map[string]any `json:"TargetTrackingScalingPolicyConfiguration"`
}

type putScalingPolicyResponse struct {
	PolicyARN string `json:"PolicyARN"`
	Alarms    []any  `json:"Alarms"`
}

type describeScalingPoliciesRequest struct {
	PolicyNames       []string `json:"PolicyNames"`
	ServiceNamespace  string   `json:"ServiceNamespace"`
	ResourceId        string   `json:"ResourceId"`
	ScalableDimension string   `json:"ScalableDimension"`
	MaxResults        int      `json:"MaxResults"`
	NextToken         string   `json:"NextToken"`
}

type describeScalingPoliciesResponse struct {
	ScalingPolicies []scalingPolicy `json:"ScalingPolicies"`
}

type deleteScalingPolicyRequest struct {
	PolicyName        string `json:"PolicyName"`
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceId        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
}

type describeScalingActivitiesRequest struct {
	ServiceNamespace  string `json:"ServiceNamespace"`
	ResourceId        string `json:"ResourceId"`
	ScalableDimension string `json:"ScalableDimension"`
	MaxResults        int    `json:"MaxResults"`
	NextToken         string `json:"NextToken"`
}

type describeScalingActivitiesResponse struct {
	ScalingActivities []any `json:"ScalingActivities"`
}

type putScheduledActionRequest struct {
	ServiceNamespace     string         `json:"ServiceNamespace"`
	ScheduledActionName  string         `json:"ScheduledActionName"`
	ResourceId           string         `json:"ResourceId"`
	ScalableDimension    string         `json:"ScalableDimension"`
	Schedule             string         `json:"Schedule"`
	Timezone             string         `json:"Timezone"`
	StartTime            *time.Time     `json:"StartTime"`
	EndTime              *time.Time     `json:"EndTime"`
	ScalableTargetAction map[string]any `json:"ScalableTargetAction"`
}

type describeScheduledActionsRequest struct {
	ServiceNamespace     string   `json:"ServiceNamespace"`
	ScheduledActionNames []string `json:"ScheduledActionNames"`
	ResourceId           string   `json:"ResourceId"`
	ScalableDimension    string   `json:"ScalableDimension"`
	MaxResults           int      `json:"MaxResults"`
	NextToken            string   `json:"NextToken"`
}

type describeScheduledActionsResponse struct {
	ScheduledActions []scheduledAction `json:"ScheduledActions"`
}

type deleteScheduledActionRequest struct {
	ServiceNamespace    string `json:"ServiceNamespace"`
	ScheduledActionName string `json:"ScheduledActionName"`
	ResourceId          string `json:"ResourceId"`
	ScalableDimension   string `json:"ScalableDimension"`
}

type tagResourceRequest struct {
	ResourceARN string            `json:"ResourceARN"`
	Tags        map[string]string `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsForResourceRequest struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsForResourceResponse struct {
	Tags map[string]string `json:"Tags"`
}

// Persistence schema.

type persistedState struct {
	ScalableTargets  map[string]scalableTarget    `json:"ScalableTargets"`
	ScalingPolicies  map[string]scalingPolicy     `json:"ScalingPolicies"`
	ScheduledActions map[string]scheduledAction   `json:"ScheduledActions"`
	Tags             map[string]map[string]string `json:"Tags"`
}
