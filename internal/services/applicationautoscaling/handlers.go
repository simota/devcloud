package applicationautoscaling

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const supportedNamespace = "dynamodb"

func (s *Server) handleRegisterScalableTarget(w http.ResponseWriter, r *http.Request) {
	var req registerScalableTargetRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if err := validateNamespace(req.ServiceNamespace); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.ResourceId == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ResourceId is required")
		return
	}
	if req.ScalableDimension == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ScalableDimension is required")
		return
	}

	key := scalableTargetKey(req.ServiceNamespace, req.ResourceId, req.ScalableDimension)

	s.mu.Lock()
	existing, exists := s.scalableTargets[key]
	target := scalableTarget{
		ServiceNamespace:  req.ServiceNamespace,
		ResourceId:        req.ResourceId,
		ScalableDimension: req.ScalableDimension,
		RoleARN:           req.RoleARN,
		CreationTime:      time.Now().UTC(),
	}
	if exists {
		target.CreationTime = existing.CreationTime
		target.MinCapacity = existing.MinCapacity
		target.MaxCapacity = existing.MaxCapacity
		target.RoleARN = existing.RoleARN
		target.SuspendedState = existing.SuspendedState
		if req.RoleARN != "" {
			target.RoleARN = req.RoleARN
		}
	}
	if req.MinCapacity != nil {
		target.MinCapacity = *req.MinCapacity
	}
	if req.MaxCapacity != nil {
		target.MaxCapacity = *req.MaxCapacity
	}
	if req.SuspendedState != nil {
		target.SuspendedState = *req.SuspendedState
	}
	s.scalableTargets[key] = target
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist application-autoscaling state")
		return
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleDescribeScalableTargets(w http.ResponseWriter, r *http.Request) {
	var req describeScalableTargetsRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if err := validateNamespace(req.ServiceNamespace); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	resourceFilter := map[string]struct{}{}
	for _, id := range req.ResourceIds {
		resourceFilter[id] = struct{}{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]scalableTarget, 0, len(s.scalableTargets))
	for _, target := range s.scalableTargets {
		if target.ServiceNamespace != req.ServiceNamespace {
			continue
		}
		if len(resourceFilter) > 0 {
			if _, ok := resourceFilter[target.ResourceId]; !ok {
				continue
			}
		}
		if req.ScalableDimension != "" && target.ScalableDimension != req.ScalableDimension {
			continue
		}
		results = append(results, target)
	}
	writeJSON(w, http.StatusOK, describeScalableTargetsResponse{ScalableTargets: results})
}

func (s *Server) handleDeregisterScalableTarget(w http.ResponseWriter, r *http.Request) {
	var req deregisterScalableTargetRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if err := validateNamespace(req.ServiceNamespace); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.ResourceId == "" || req.ScalableDimension == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ResourceId and ScalableDimension are required")
		return
	}
	key := scalableTargetKey(req.ServiceNamespace, req.ResourceId, req.ScalableDimension)

	s.mu.Lock()
	if _, ok := s.scalableTargets[key]; !ok {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, "ObjectNotFoundException", "scalable target not found")
		return
	}
	delete(s.scalableTargets, key)
	// also delete related policies and scheduled actions for the same triple.
	prefix := key + "|"
	for k := range s.scalingPolicies {
		if strings.HasPrefix(k, prefix) {
			delete(s.scalingPolicies, k)
		}
	}
	for k := range s.scheduledActions {
		if strings.HasPrefix(k, prefix) {
			delete(s.scheduledActions, k)
		}
	}
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist application-autoscaling state")
		return
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handlePutScalingPolicy(w http.ResponseWriter, r *http.Request) {
	var req putScalingPolicyRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if err := validateNamespace(req.ServiceNamespace); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.PolicyName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "PolicyName is required")
		return
	}
	if req.ResourceId == "" || req.ScalableDimension == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ResourceId and ScalableDimension are required")
		return
	}
	policyType := req.PolicyType
	if policyType == "" {
		policyType = "StepScaling"
	}
	if policyType != "StepScaling" && policyType != "TargetTrackingScaling" {
		writeError(w, http.StatusBadRequest, "ValidationException", "PolicyType must be StepScaling or TargetTrackingScaling")
		return
	}

	key := scalingPolicyKey(req.ServiceNamespace, req.ResourceId, req.ScalableDimension, req.PolicyName)
	region := defaultString(s.config.Region, "us-east-1")
	account := defaultString(s.config.AccountID, "000000000000")

	s.mu.Lock()
	existing, exists := s.scalingPolicies[key]
	policy := scalingPolicy{
		PolicyName:                               req.PolicyName,
		ServiceNamespace:                         req.ServiceNamespace,
		ResourceId:                               req.ResourceId,
		ScalableDimension:                        req.ScalableDimension,
		PolicyType:                               policyType,
		StepScalingPolicyConfiguration:           req.StepScalingPolicyConfiguration,
		TargetTrackingScalingPolicyConfiguration: req.TargetTrackingScalingPolicyConfiguration,
		Alarms:                                   []any{},
		CreationTime:                             time.Now().UTC(),
	}
	if exists {
		policy.PolicyARN = existing.PolicyARN
		policy.CreationTime = existing.CreationTime
	} else {
		policy.PolicyARN = buildPolicyARN(region, account, req.ServiceNamespace, req.ResourceId, req.PolicyName)
	}
	s.scalingPolicies[key] = policy
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist application-autoscaling state")
		return
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, putScalingPolicyResponse{PolicyARN: policy.PolicyARN, Alarms: []any{}})
}

func (s *Server) handleDescribeScalingPolicies(w http.ResponseWriter, r *http.Request) {
	var req describeScalingPoliciesRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if err := validateNamespace(req.ServiceNamespace); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	policyFilter := map[string]struct{}{}
	for _, name := range req.PolicyNames {
		policyFilter[name] = struct{}{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]scalingPolicy, 0, len(s.scalingPolicies))
	for _, policy := range s.scalingPolicies {
		if policy.ServiceNamespace != req.ServiceNamespace {
			continue
		}
		if req.ResourceId != "" && policy.ResourceId != req.ResourceId {
			continue
		}
		if req.ScalableDimension != "" && policy.ScalableDimension != req.ScalableDimension {
			continue
		}
		if len(policyFilter) > 0 {
			if _, ok := policyFilter[policy.PolicyName]; !ok {
				continue
			}
		}
		results = append(results, policy)
	}
	writeJSON(w, http.StatusOK, describeScalingPoliciesResponse{ScalingPolicies: results})
}

func (s *Server) handleDeleteScalingPolicy(w http.ResponseWriter, r *http.Request) {
	var req deleteScalingPolicyRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if err := validateNamespace(req.ServiceNamespace); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.PolicyName == "" || req.ResourceId == "" || req.ScalableDimension == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "PolicyName, ResourceId, and ScalableDimension are required")
		return
	}
	key := scalingPolicyKey(req.ServiceNamespace, req.ResourceId, req.ScalableDimension, req.PolicyName)

	s.mu.Lock()
	if _, ok := s.scalingPolicies[key]; !ok {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, "ObjectNotFoundException", "scaling policy not found")
		return
	}
	delete(s.scalingPolicies, key)
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist application-autoscaling state")
		return
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleDescribeScalingActivities(w http.ResponseWriter, r *http.Request) {
	var req describeScalingActivitiesRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if err := validateNamespace(req.ServiceNamespace); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, describeScalingActivitiesResponse{ScalingActivities: []any{}})
}

func (s *Server) handlePutScheduledAction(w http.ResponseWriter, r *http.Request) {
	var req putScheduledActionRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if err := validateNamespace(req.ServiceNamespace); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.ScheduledActionName == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ScheduledActionName is required")
		return
	}
	if req.ResourceId == "" || req.ScalableDimension == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ResourceId and ScalableDimension are required")
		return
	}
	key := scheduledActionKey(req.ServiceNamespace, req.ResourceId, req.ScalableDimension, req.ScheduledActionName)

	s.mu.Lock()
	existing, exists := s.scheduledActions[key]
	action := scheduledAction{
		ScheduledActionName:  req.ScheduledActionName,
		ServiceNamespace:     req.ServiceNamespace,
		ResourceId:           req.ResourceId,
		ScalableDimension:    req.ScalableDimension,
		Schedule:             req.Schedule,
		Timezone:             req.Timezone,
		StartTime:            req.StartTime,
		EndTime:              req.EndTime,
		ScalableTargetAction: req.ScalableTargetAction,
		CreationTime:         time.Now().UTC(),
	}
	if exists {
		action.CreationTime = existing.CreationTime
	}
	s.scheduledActions[key] = action
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist application-autoscaling state")
		return
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleDescribeScheduledActions(w http.ResponseWriter, r *http.Request) {
	var req describeScheduledActionsRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if err := validateNamespace(req.ServiceNamespace); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	nameFilter := map[string]struct{}{}
	for _, n := range req.ScheduledActionNames {
		nameFilter[n] = struct{}{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]scheduledAction, 0, len(s.scheduledActions))
	for _, action := range s.scheduledActions {
		if action.ServiceNamespace != req.ServiceNamespace {
			continue
		}
		if req.ResourceId != "" && action.ResourceId != req.ResourceId {
			continue
		}
		if req.ScalableDimension != "" && action.ScalableDimension != req.ScalableDimension {
			continue
		}
		if len(nameFilter) > 0 {
			if _, ok := nameFilter[action.ScheduledActionName]; !ok {
				continue
			}
		}
		results = append(results, action)
	}
	writeJSON(w, http.StatusOK, describeScheduledActionsResponse{ScheduledActions: results})
}

func (s *Server) handleDeleteScheduledAction(w http.ResponseWriter, r *http.Request) {
	var req deleteScheduledActionRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if err := validateNamespace(req.ServiceNamespace); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if req.ScheduledActionName == "" || req.ResourceId == "" || req.ScalableDimension == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ScheduledActionName, ResourceId, and ScalableDimension are required")
		return
	}
	key := scheduledActionKey(req.ServiceNamespace, req.ResourceId, req.ScalableDimension, req.ScheduledActionName)

	s.mu.Lock()
	if _, ok := s.scheduledActions[key]; !ok {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, "ObjectNotFoundException", "scheduled action not found")
		return
	}
	delete(s.scheduledActions, key)
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist application-autoscaling state")
		return
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleTagResource(w http.ResponseWriter, r *http.Request) {
	var req tagResourceRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.ResourceARN == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ResourceARN is required")
		return
	}

	s.mu.Lock()
	bucket, ok := s.tags[req.ResourceARN]
	if !ok {
		bucket = map[string]string{}
	}
	for k, v := range req.Tags {
		bucket[k] = v
	}
	s.tags[req.ResourceARN] = bucket
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist application-autoscaling state")
		return
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleUntagResource(w http.ResponseWriter, r *http.Request) {
	var req untagResourceRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.ResourceARN == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ResourceARN is required")
		return
	}

	s.mu.Lock()
	if bucket, ok := s.tags[req.ResourceARN]; ok {
		for _, k := range req.TagKeys {
			delete(bucket, k)
		}
		if len(bucket) == 0 {
			delete(s.tags, req.ResourceARN)
		} else {
			s.tags[req.ResourceARN] = bucket
		}
	}
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist application-autoscaling state")
		return
	}
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleListTagsForResource(w http.ResponseWriter, r *http.Request) {
	var req listTagsForResourceRequest
	if !decodeRequest(w, r, &req) {
		return
	}
	if req.ResourceARN == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "ResourceARN is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	tags := map[string]string{}
	if bucket, ok := s.tags[req.ResourceARN]; ok {
		for k, v := range bucket {
			tags[k] = v
		}
	}
	writeJSON(w, http.StatusOK, listTagsForResourceResponse{Tags: tags})
}

func validateNamespace(ns string) error {
	if ns == "" {
		return fmt.Errorf("ServiceNamespace is required")
	}
	if ns != supportedNamespace {
		return fmt.Errorf("only dynamodb namespace is supported")
	}
	return nil
}

func buildPolicyARN(region, account, namespace, resourceID, policyName string) string {
	return fmt.Sprintf("arn:aws:autoscaling:%s:%s:scalingPolicy:%s:resource/%s/%s:policyName/%s",
		region, account, newUUID(), namespace, resourceID, policyName)
}

func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// fall back to deterministic suffix; rand failures are extremely rare.
		return "00000000-0000-0000-0000-000000000000"
	}
	// RFC 4122 v4
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	hexBuf := make([]byte, 36)
	hexEnc := hex.EncodeToString(b[:])
	copy(hexBuf, hexEnc[0:8])
	hexBuf[8] = '-'
	copy(hexBuf[9:], hexEnc[8:12])
	hexBuf[13] = '-'
	copy(hexBuf[14:], hexEnc[12:16])
	hexBuf[18] = '-'
	copy(hexBuf[19:], hexEnc[16:20])
	hexBuf[23] = '-'
	copy(hexBuf[24:], hexEnc[20:32])
	return string(hexBuf)
}
