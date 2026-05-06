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

func (s *Server) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
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

	subscriptions := make([]subscriptionResource, 0, len(s.subscriptions))
	for _, subscription := range s.subscriptions {
		if resourceProject(subscription.Name) == project {
			subscriptions = append(subscriptions, subscription)
		}
	}
	sort.Slice(subscriptions, func(i, j int) bool { return subscriptions[i].Name < subscriptions[j].Name })
	start, pageSize, ok := parseListPagination(w, r)
	if !ok {
		return
	}
	end, nextPageToken := pageBounds(len(subscriptions), start, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": subscriptions[start:end], "nextPageToken": nextPageToken})
}

func (s *Server) handleSubscription(w http.ResponseWriter, r *http.Request) {
	project, subscriptionID, ok := subscriptionNameParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	name := subscriptionName(project, subscriptionID)
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}

	switch r.Method {
	case http.MethodPut:
		var request subscriptionResource
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
			return
		}
		if request.Topic == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "subscription topic is required")
			return
		}
		if !validFullTopicName(request.Topic) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
			return
		}
		if request.AckDeadlineSeconds < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds must be non-negative")
			return
		}
		if request.AckDeadlineSeconds == 0 {
			request.AckDeadlineSeconds = s.config.DefaultAckDeadlineSeconds
		}
		if request.AckDeadlineSeconds > s.config.MaxAckDeadlineSeconds {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds exceeds maxAckDeadlineSeconds")
			return
		}
		if err := validateSubscriptionFilter(request.Filter); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		if err := validateSubscriptionMetadata(request); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		if err := validateDeadLetterPolicy(request.DeadLetterPolicy); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		if err := validateRetryPolicy(request.RetryPolicy); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		if err := validatePushConfig(request.PushConfig); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		now := s.now().UTC().Format(time.RFC3339Nano)
		request.Name = name
		request.CreatedAt = now
		request.UpdatedAt = now

		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.subscriptions[name]; exists {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "subscription already exists")
			return
		}
		if _, found := s.topics[request.Topic]; !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
			return
		}
		if !s.deadLetterTopicExistsLocked(request.DeadLetterPolicy) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "dead-letter topic not found")
			return
		}
		request.Labels = copyStringMap(request.Labels)
		s.subscriptions[name] = request
		if err := s.saveResourcesLocked(); err != nil {
			delete(s.subscriptions, name)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		writeJSON(w, http.StatusOK, request)
	case http.MethodGet:
		s.mu.Lock()
		subscription, found := s.subscriptions[name]
		s.mu.Unlock()
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
			return
		}
		writeJSON(w, http.StatusOK, subscription)
	case http.MethodPatch:
		s.handleSubscriptionPatch(w, r, name)
	case http.MethodDelete:
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, found := s.subscriptions[name]; !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
			return
		}
		delete(s.subscriptions, name)
		delete(s.deliveries, name)
		for snapshotName, snapshot := range s.snapshots {
			if snapshot.Subscription == name {
				delete(s.snapshots, snapshotName)
			}
		}
		s.cleanupUnreferencedMessagesLocked()
		if err := s.saveResourcesLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, "GET, PUT, PATCH, DELETE")
	}
}

func (s *Server) handleSeek(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "seek")
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	var request struct {
		Snapshot string `json:"snapshot"`
		Time     string `json:"time"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if request.Snapshot == "" && request.Time == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "snapshot or time is required")
		return
	}
	if request.Snapshot != "" && request.Time != "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "only one of snapshot or time may be set")
		return
	}
	var seekTime time.Time
	if request.Time != "" {
		parsed, err := time.Parse(time.RFC3339Nano, request.Time)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339, request.Time)
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid seek time")
			return
		}
		seekTime = parsed.UTC()
	}
	if request.Snapshot != "" && !validFullSnapshotName(request.Snapshot) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid snapshot name")
		return
	}
	name := subscriptionName(project, subscriptionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	if request.Time != "" {
		s.deliveries[name] = s.seekDeliveriesByTimeLocked(subscription, seekTime)
		if err := s.saveResourcesLocked(); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	snapshot, found := s.snapshots[request.Snapshot]
	if !found || snapshotExpired(snapshot, s.now().UTC()) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "snapshot not found")
		return
	}
	if snapshot.Subscription != name {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "snapshot belongs to a different subscription")
		return
	}
	s.deliveries[name] = snapshotDeliveries(snapshot.Deliveries)
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) seekDeliveriesByTimeLocked(subscription subscriptionResource, seekTime time.Time) []deliveryRecord {
	records := s.deliveries[subscription.Name]
	replayed := make([]deliveryRecord, 0, len(records))
	for _, delivery := range records {
		message, found := s.messages[delivery.MessageID]
		if !found {
			continue
		}
		publishedAt, err := time.Parse(time.RFC3339Nano, message.PublishTime)
		if err != nil || publishedAt.Before(seekTime) {
			continue
		}
		replayed = append(replayed, deliveryRecord{MessageID: delivery.MessageID})
	}
	return replayed
}

func (s *Server) handleSubscriptionPatch(w http.ResponseWriter, r *http.Request, name string) {
	patch, presentFields, err := decodeSubscriptionPatch(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if patch.Name != "" && patch.Name != name {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "subscription name does not match request path")
		return
	}
	if patch.AckDeadlineSeconds < 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds must be non-negative")
		return
	}
	if patch.AckDeadlineSeconds > s.config.MaxAckDeadlineSeconds {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds exceeds maxAckDeadlineSeconds")
		return
	}
	if hasPatchField(presentFields, "filter") {
		if err := validateSubscriptionFilter(patch.Filter); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	if hasPatchField(presentFields, "messageRetentionDuration") || hasPatchField(presentFields, "expirationPolicy") {
		if err := validateSubscriptionMetadata(patch); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	if hasPatchField(presentFields, "deadLetterPolicy") {
		if err := validateDeadLetterPolicy(patch.DeadLetterPolicy); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	if hasPatchField(presentFields, "retryPolicy") {
		if err := validateRetryPolicy(patch.RetryPolicy); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	if hasPatchField(presentFields, "pushConfig") {
		if err := validatePushConfig(patch.PushConfig); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	if hasPatchField(presentFields, "deadLetterPolicy") && !s.deadLetterTopicExistsLocked(patch.DeadLetterPolicy) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "dead-letter topic not found")
		return
	}
	if hasPatchField(presentFields, "topic") && patch.Topic != "" && patch.Topic != subscription.Topic {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "subscription topic cannot be changed")
		return
	}
	if hasPatchField(presentFields, "labels") {
		subscription.Labels = copyStringMap(patch.Labels)
	}
	if hasPatchField(presentFields, "ackDeadlineSeconds") {
		if patch.AckDeadlineSeconds == 0 {
			subscription.AckDeadlineSeconds = s.config.DefaultAckDeadlineSeconds
		} else {
			subscription.AckDeadlineSeconds = patch.AckDeadlineSeconds
		}
	}
	if hasPatchField(presentFields, "enableMessageOrdering") {
		subscription.EnableMessageOrdering = patch.EnableMessageOrdering
	}
	if hasPatchField(presentFields, "enableExactlyOnceDelivery") {
		subscription.EnableExactlyOnceDelivery = patch.EnableExactlyOnceDelivery
	}
	if hasPatchField(presentFields, "retainAckedMessages") {
		subscription.RetainAckedMessages = patch.RetainAckedMessages
	}
	if hasPatchField(presentFields, "messageRetentionDuration") {
		subscription.MessageRetentionDuration = patch.MessageRetentionDuration
	}
	if hasPatchField(presentFields, "expirationPolicy") {
		subscription.ExpirationPolicy = copyAnyMap(patch.ExpirationPolicy)
	}
	if hasPatchField(presentFields, "filter") {
		subscription.Filter = patch.Filter
	}
	if hasPatchField(presentFields, "deadLetterPolicy") {
		subscription.DeadLetterPolicy = copyAnyMap(patch.DeadLetterPolicy)
	}
	if hasPatchField(presentFields, "retryPolicy") {
		subscription.RetryPolicy = copyAnyMap(patch.RetryPolicy)
	}
	if hasPatchField(presentFields, "pushConfig") {
		subscription.PushConfig = copyAnyMap(patch.PushConfig)
	}
	subscription.UpdatedAt = s.now().UTC().Format(time.RFC3339Nano)
	s.subscriptions[name] = subscription
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, subscription)
}

func (s *Server) handleModifyPushConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "modifyPushConfig")
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
	var request struct {
		PushConfig map[string]any `json:"pushConfig"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if err := validatePushConfig(request.PushConfig); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	name := subscriptionName(project, subscriptionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	subscription.PushConfig = copyAnyMap(request.PushConfig)
	subscription.UpdatedAt = s.now().UTC().Format(time.RFC3339Nano)
	s.subscriptions[name] = subscription
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *Server) handleDetachSubscription(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "detach")
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
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	subscription.Detached = true
	subscription.UpdatedAt = s.now().UTC().Format(time.RFC3339Nano)
	s.subscriptions[name] = subscription
	delete(s.deliveries, name)
	for snapshotName, snapshot := range s.snapshots {
		if snapshot.Subscription == name {
			delete(s.snapshots, snapshotName)
		}
	}
	s.cleanupUnreferencedMessagesLocked()
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func subscriptionName(project string, subscriptionID string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", project, subscriptionID)
}
