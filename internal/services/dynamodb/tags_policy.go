package dynamodb

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

func (s *Server) handleTagResource(w http.ResponseWriter, r *http.Request) {
	var request tagResourceRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}
	if len(request.Tags) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "tags are required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	if state.tags == nil {
		state.tags = map[string]string{}
	}
	projected := cloneTags(state.tags)
	for _, tag := range request.Tags {
		if tag.Key == "" {
			writeError(w, http.StatusBadRequest, "ValidationException", "tag key is required")
			return
		}
		projected[tag.Key] = tag.Value
	}
	if len(projected) > 50 {
		writeError(w, http.StatusBadRequest, "LimitExceededException", "tag limit exceeded")
		return
	}
	previous := cloneTags(state.tags)
	state.tags = projected
	if err := s.persistLocked(); err != nil {
		state.tags = previous
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleListTagsOfResource(w http.ResponseWriter, r *http.Request) {
	var request listTagsOfResourceRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}
	if request.NextToken != "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "next token is invalid")
		return
	}

	s.mu.Lock()
	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	tags := []tag{}
	if ok {
		for key, value := range state.tags {
			tags = append(tags, tag{Key: key, Value: value})
		}
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Key < tags[j].Key
	})
	writeJSON(w, http.StatusOK, map[string]any{"Tags": tags})
}

func (s *Server) handleUntagResource(w http.ResponseWriter, r *http.Request) {
	var request untagResourceRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}
	if len(request.TagKeys) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "tag keys are required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	previous := cloneTags(state.tags)
	for _, key := range request.TagKeys {
		delete(state.tags, key)
	}
	if err := s.persistLocked(); err != nil {
		state.tags = previous
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handlePutResourcePolicy(w http.ResponseWriter, r *http.Request) {
	var request putResourcePolicyRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}
	if strings.TrimSpace(request.Policy) == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "policy is required")
		return
	}
	if !json.Valid([]byte(request.Policy)) {
		writeError(w, http.StatusBadRequest, "ValidationException", "policy must be valid JSON")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	previousPolicy := state.resourcePolicy
	previousRevision := state.resourcePolicyRevision
	state.resourcePolicy = request.Policy
	state.resourcePolicyRevision = resourcePolicyRevision(request.Policy)
	if err := s.persistLocked(); err != nil {
		state.resourcePolicy = previousPolicy
		state.resourcePolicyRevision = previousRevision
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"RevisionId": state.resourcePolicyRevision})
}

func (s *Server) handleGetResourcePolicy(w http.ResponseWriter, r *http.Request) {
	var request getResourcePolicyRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}

	s.mu.Lock()
	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	policy := ""
	revision := ""
	if ok {
		policy = state.resourcePolicy
		revision = state.resourcePolicyRevision
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	if policy == "" {
		writeError(w, http.StatusBadRequest, "PolicyNotFoundException", "resource policy not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"Policy": policy, "RevisionId": revision})
}

func (s *Server) handleDeleteResourcePolicy(w http.ResponseWriter, r *http.Request) {
	var request deleteResourcePolicyRequest
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.ResourceArn == "" {
		writeError(w, http.StatusBadRequest, "ValidationException", "resource arn is required")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.tableStateForARNLocked(request.ResourceArn)
	if !ok {
		writeError(w, http.StatusBadRequest, "ResourceNotFoundException", "resource not found")
		return
	}
	previousPolicy := state.resourcePolicy
	previousRevision := state.resourcePolicyRevision
	state.resourcePolicy = ""
	state.resourcePolicyRevision = ""
	if err := s.persistLocked(); err != nil {
		state.resourcePolicy = previousPolicy
		state.resourcePolicyRevision = previousRevision
		writeError(w, http.StatusInternalServerError, "InternalServerError", "failed to persist dynamodb state")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}
func (s *Server) tableStateForARNLocked(resourceARN string) (*tableState, bool) {
	for _, state := range s.tables {
		if state.description.TableArn == resourceARN {
			return state, true
		}
	}
	return nil, false
}
func resourcePolicyRevision(policy string) string {
	sum := sha256.Sum256([]byte(policy))
	return fmt.Sprintf("%x", sum[:])
}
