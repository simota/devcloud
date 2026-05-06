package pubsub

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

func (s *Server) handleTopicIAM(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, topicID, action, ok := topicActionParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(topicID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
		return
	}
	name := topicName(project, topicID)
	s.mu.Lock()
	_, found := s.topics[name]
	s.mu.Unlock()
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
		return
	}
	s.handleIAMAction(w, r, action)
}

func (s *Server) handleSubscriptionIAM(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, action, ok := subscriptionAnyActionParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	name := subscriptionName(project, subscriptionID)
	s.mu.Lock()
	_, found := s.subscriptions[name]
	s.mu.Unlock()
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	s.handleIAMAction(w, r, action)
}

func (s *Server) handleIAMAction(w http.ResponseWriter, r *http.Request, action string) {
	switch action {
	case "getIamPolicy":
		writeJSON(w, http.StatusOK, map[string]any{"version": 1, "bindings": []any{}})
	case "setIamPolicy":
		var request struct {
			Policy map[string]any `json:"policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		if request.Policy == nil {
			request.Policy = map[string]any{"version": 1, "bindings": []any{}}
		}
		writeJSON(w, http.StatusOK, request.Policy)
	case "testIamPermissions":
		var request struct {
			Permissions []string `json:"permissions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"permissions": request.Permissions})
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
	}
}

func isIAMAction(action string) bool {
	switch action {
	case "getIamPolicy", "setIamPolicy", "testIamPermissions":
		return true
	default:
		return false
	}
}
