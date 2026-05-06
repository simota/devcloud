package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"
)

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	parts := pathParts(r.URL.EscapedPath())
	project := parts[2]
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	topics := make([]topicResource, 0, len(s.topics))
	for _, topic := range s.topics {
		if resourceProject(topic.Name) == project {
			topics = append(topics, topic)
		}
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(topics), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"topics": topics[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleTopic(w http.ResponseWriter, r *http.Request) {
	project, topicID, ok := topicNameParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := topicName(project, topicID)
	if !validResourceID(topicID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}

	switch r.Method {
	case http.MethodPut:
		var request topicResource
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		if request.Name != "" && request.Name != name {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic name does not match request path")
			return
		}
		if err := validateTopicMetadata(request); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		now := s.now().UTC().Format(time.RFC3339Nano)
		s.mu.Lock()
		defer s.mu.Unlock()
		topic := topicResource{
			Name:                     name,
			Labels:                   copyStringMap(request.Labels),
			CreatedAt:                now,
			UpdatedAt:                now,
			MessageRetentionDuration: request.MessageRetentionDuration,
			SchemaSettings:           copyAnyMap(request.SchemaSettings),
			KMSKeyName:               request.KMSKeyName,
		}
		if _, exists := s.topics[name]; exists {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "topic already exists")
			return
		}
		s.topics[name] = topic
		if err := s.saveResourcesLocked(); err != nil {
			delete(s.topics, name)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		writeJSON(w, http.StatusOK, topic)
	case http.MethodPatch:
		s.handleTopicPatch(w, r, name)
	case http.MethodGet:
		s.mu.Lock()
		topic, found := s.topics[name]
		s.mu.Unlock()
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
			return
		}
		writeJSON(w, http.StatusOK, topic)
	case http.MethodDelete:
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, found := s.topics[name]; !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
			return
		}
		for _, subscription := range s.subscriptions {
			if subscription.Topic == name && !subscription.Detached {
				writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "topic has attached subscriptions")
				return
			}
		}
		delete(s.topics, name)
		if err := s.saveResourcesLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, PUT, PATCH, DELETE")
	}
}

func (s *Server) handleTopicPatch(w http.ResponseWriter, r *http.Request, name string) {
	patch, presentFields, err := decodeTopicPatch(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if patch.Name != "" && patch.Name != name {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic name does not match request path")
		return
	}
	if err := validateTopicMetadata(patch); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	topic, found := s.topics[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
		return
	}
	if hasPatchField(presentFields, "labels") {
		topic.Labels = copyStringMap(patch.Labels)
	}
	if hasPatchField(presentFields, "messageRetentionDuration") {
		topic.MessageRetentionDuration = patch.MessageRetentionDuration
	}
	if hasPatchField(presentFields, "schemaSettings") {
		topic.SchemaSettings = copyAnyMap(patch.SchemaSettings)
	}
	if hasPatchField(presentFields, "kmsKeyName") {
		topic.KMSKeyName = patch.KMSKeyName
	}
	topic.UpdatedAt = s.now().UTC().Format(time.RFC3339Nano)
	s.topics[name] = topic
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, topic)
}

func (s *Server) handleTopicSubscriptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	project, topicID, ok := topicSubscriptionsParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := topicName(project, topicID)
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(topicID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.topics[name]; !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
		return
	}
	subscriptions := make([]string, 0, len(s.subscriptions))
	for _, subscription := range s.subscriptions {
		if subscription.Topic == name && !subscription.Detached {
			subscriptions = append(subscriptions, subscription.Name)
		}
	}
	sort.Strings(subscriptions)
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(subscriptions), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": subscriptions[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleTopicSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	project, topicID, ok := topicSnapshotsParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := topicName(project, topicID)
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(topicID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.topics[name]; !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
		return
	}
	now := s.now().UTC()
	snapshots := make([]string, 0, len(s.snapshots))
	for _, snapshot := range s.snapshots {
		if snapshot.Topic == name && !snapshotExpired(snapshot, now) {
			snapshots = append(snapshots, snapshot.Name)
		}
	}
	sort.Strings(snapshots)
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(snapshots), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": snapshots[start:end], "nextPageToken": nextPageToken})
}

func topicName(project string, topicID string) string {
	return fmt.Sprintf("projects/%s/topics/%s", project, topicID)
}

func (s *Server) generatedSubscriptionNameLocked(project string) string {
	if !validProjectID(project) {
		project = defaultString(s.config.Project, "devcloud")
	}
	for i := 1; ; i++ {
		name := subscriptionName(project, fmt.Sprintf("devcloud-auto-sub-%d", i))
		if _, exists := s.subscriptions[name]; !exists {
			return name
		}
	}
}

func (s *Server) generatedSnapshotNameLocked(project string) string {
	if !validProjectID(project) {
		project = defaultString(s.config.Project, "devcloud")
	}
	for i := 1; ; i++ {
		name := snapshotName(project, fmt.Sprintf("devcloud-auto-snapshot-%d", i))
		if _, exists := s.snapshots[name]; !exists {
			return name
		}
	}
}
