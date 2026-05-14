package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"devcloud/internal/events"
)

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, topicID, ok := topicPublishParts(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	if !validResourceID(topicID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid topic name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	name := topicName(project, topicID)
	var request struct {
		Messages []struct {
			Data        string            `json:"data"`
			Attributes  map[string]string `json:"attributes"`
			OrderingKey string            `json:"orderingKey"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if len(request.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "messages are required")
		return
	}
	for _, message := range request.Messages {
		if err := validatePublishMessage(message.Data, message.Attributes); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	topic, found := s.topics[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "topic not found")
		return
	}
	for _, message := range request.Messages {
		if err := validateMessageAgainstTopicSchemaSettings(message.Data, topic.SchemaSettings); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	now := s.now().UTC().Format(time.RFC3339Nano)
	messageIDs := make([]string, 0, len(request.Messages))
	for _, incoming := range request.Messages {
		s.nextMessageID++
		messageID := strconv.FormatUint(s.nextMessageID, 10)
		message := pubsubMessage{
			Data:        incoming.Data,
			Attributes:  copyStringMap(incoming.Attributes),
			MessageID:   messageID,
			PublishTime: now,
			OrderingKey: incoming.OrderingKey,
		}
		s.messages[messageID] = message
		for _, subscription := range s.subscriptions {
			if subscription.Topic == name && !subscription.Detached && subscriptionMatchesMessage(subscription, message) {
				s.deliveries[subscription.Name] = append(s.deliveries[subscription.Name], deliveryRecord{MessageID: messageID})
			}
		}
		messageIDs = append(messageIDs, messageID)
	}
	s.cleanupUnreferencedMessagesLocked()
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	events.Emit(s.eventPublisher, events.Event{
		Type:    "pubsub.message.published",
		Service: "pubsub",
		Payload: map[string]any{"topic": name, "count": len(messageIDs)},
	})
	writeJSON(w, http.StatusOK, map[string]any{"messageIds": messageIDs})
}

func (s *Server) handlePull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "pull")
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
		MaxMessages       int   `json:"maxMessages"`
		ReturnImmediately *bool `json:"returnImmediately"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if request.MaxMessages <= 0 {
		request.MaxMessages = 1
	}
	if request.MaxMessages > s.config.MaxPullMessages {
		request.MaxMessages = s.config.MaxPullMessages
	}

	name := subscriptionName(project, subscriptionID)
	if request.ReturnImmediately != nil && !*request.ReturnImmediately {
		s.waitForPullAvailability(r.Context(), name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[name]
	if !found {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "subscription not found")
		return
	}
	if subscription.Detached {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "subscription is detached")
		return
	}
	if subscriptionPushEndpoint(subscription) != "" {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "subscription is configured for push delivery")
		return
	}
	now := s.now().UTC()
	s.cleanupRetainedMessagesLocked(now)
	s.expireLeasesLocked(now)
	ackDeadline := subscription.AckDeadlineSeconds
	if ackDeadline <= 0 {
		ackDeadline = s.config.DefaultAckDeadlineSeconds
	}
	received := make([]map[string]any, 0, request.MaxMessages)
	deliveries := s.deliveries[name]
	blockedOrderingKeys := map[string]struct{}{}
	if subscription.EnableMessageOrdering {
		for _, delivery := range deliveries {
			if delivery.Acked || !delivery.LeaseDeadline.After(now) {
				continue
			}
			message, found := s.messages[delivery.MessageID]
			if !found || message.OrderingKey == "" {
				continue
			}
			blockedOrderingKeys[message.OrderingKey] = struct{}{}
		}
	}
	for i := range deliveries {
		if len(received) >= request.MaxMessages {
			break
		}
		if deliveries[i].Acked || deliveries[i].LeaseDeadline.After(now) {
			continue
		}
		if deliveries[i].NextDeliveryTime.After(now) {
			if subscription.EnableMessageOrdering {
				if message, found := s.messages[deliveries[i].MessageID]; found && message.OrderingKey != "" {
					blockedOrderingKeys[message.OrderingKey] = struct{}{}
				}
			}
			continue
		}
		message, found := s.messages[deliveries[i].MessageID]
		if !found {
			continue
		}
		if s.deadLetterDeliveryLocked(subscription, &deliveries[i], message, now) {
			continue
		}
		if subscription.EnableMessageOrdering && message.OrderingKey != "" {
			if _, blocked := blockedOrderingKeys[message.OrderingKey]; blocked {
				continue
			}
			blockedOrderingKeys[message.OrderingKey] = struct{}{}
		}
		s.nextAckID++
		deliveries[i].AckID = fmt.Sprintf("%s-%d", deliveries[i].MessageID, s.nextAckID)
		deliveries[i].LeaseDeadline = now.Add(time.Duration(ackDeadline) * time.Second)
		deliveries[i].NextDeliveryTime = time.Time{}
		deliveries[i].DeliveryAttempt++
		received = append(received, map[string]any{
			"ackId": deliveries[i].AckID,
			"message": map[string]any{
				"data":        message.Data,
				"attributes":  message.Attributes,
				"messageId":   message.MessageID,
				"publishTime": message.PublishTime,
				"orderingKey": message.OrderingKey,
			},
			"deliveryAttempt": deliveries[i].DeliveryAttempt,
		})
	}
	s.deliveries[name] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	s.cleanupUnreferencedMessagesLocked()
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	if len(received) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	events.Emit(s.eventPublisher, events.Event{
		Type:    "pubsub.message.pulled",
		Service: "pubsub",
		Payload: map[string]any{"subscription": name, "count": len(received)},
	})
	writeJSON(w, http.StatusOK, map[string]any{"receivedMessages": received})
}

func (s *Server) waitForPullAvailability(ctx context.Context, subscriptionName string) {
	deadline := time.NewTimer(s.config.PullWaitTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.pullMayReturn(subscriptionName) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) pullMayReturn(subscriptionName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[subscriptionName]
	if !found || subscription.Detached || subscriptionPushEndpoint(subscription) != "" {
		return true
	}
	now := s.now().UTC()
	s.expireLeasesLocked(now)
	blockedOrderingKeys := map[string]struct{}{}
	if subscription.EnableMessageOrdering {
		for _, delivery := range s.deliveries[subscriptionName] {
			if delivery.Acked || !delivery.LeaseDeadline.After(now) {
				continue
			}
			message, found := s.messages[delivery.MessageID]
			if !found || message.OrderingKey == "" {
				continue
			}
			blockedOrderingKeys[message.OrderingKey] = struct{}{}
		}
	}
	for _, delivery := range s.deliveries[subscriptionName] {
		if delivery.Acked || delivery.LeaseDeadline.After(now) || delivery.NextDeliveryTime.After(now) {
			if subscription.EnableMessageOrdering && delivery.NextDeliveryTime.After(now) {
				if message, found := s.messages[delivery.MessageID]; found && message.OrderingKey != "" {
					blockedOrderingKeys[message.OrderingKey] = struct{}{}
				}
			}
			continue
		}
		message, found := s.messages[delivery.MessageID]
		if !found {
			continue
		}
		if subscription.EnableMessageOrdering && message.OrderingKey != "" {
			if _, blocked := blockedOrderingKeys[message.OrderingKey]; blocked {
				continue
			}
		}
		return true
	}
	return false
}

func (s *Server) handleAcknowledge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "acknowledge")
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	s.updateAckDeadlines(w, r, project, subscriptionID, true)
}

func (s *Server) handleModifyAckDeadline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	project, subscriptionID, ok := subscriptionActionParts(r.URL.EscapedPath(), "modifyAckDeadline")
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	s.updateAckDeadlines(w, r, project, subscriptionID, false)
}

func (s *Server) updateAckDeadlines(w http.ResponseWriter, r *http.Request, project string, subscriptionID string, acknowledge bool) {
	if !validResourceID(subscriptionID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid subscription name")
		return
	}
	if !validProjectID(project) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project name")
		return
	}
	var request struct {
		AckIDs             []string `json:"ackIds"`
		AckDeadlineSeconds int      `json:"ackDeadlineSeconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json request")
		return
	}
	if len(request.AckIDs) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	for _, ackID := range request.AckIDs {
		if strings.TrimSpace(ackID) == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackIds must not contain empty values")
			return
		}
	}
	if !acknowledge && request.AckDeadlineSeconds < 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds must be non-negative")
		return
	}
	if !acknowledge && request.AckDeadlineSeconds > s.config.MaxAckDeadlineSeconds {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds exceeds maxAckDeadlineSeconds")
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
	ackIDs := map[string]struct{}{}
	for _, ackID := range request.AckIDs {
		ackIDs[ackID] = struct{}{}
	}
	now := s.now().UTC()
	s.expireLeasesLocked(now)
	deliveries := s.deliveries[name]
	for i := range deliveries {
		if _, ok := ackIDs[deliveries[i].AckID]; !ok || deliveries[i].Acked {
			continue
		}
		if acknowledge {
			deliveries[i].Acked = true
			deliveries[i].AckID = ""
			deliveries[i].LeaseDeadline = time.Time{}
			deliveries[i].NextDeliveryTime = time.Time{}
			continue
		}
		if request.AckDeadlineSeconds == 0 {
			deliveries[i].AckID = ""
			deliveries[i].LeaseDeadline = time.Time{}
			deliveries[i].NextDeliveryTime = time.Time{}
		} else {
			deliveries[i].LeaseDeadline = now.Add(time.Duration(request.AckDeadlineSeconds) * time.Second)
			deliveries[i].NextDeliveryTime = time.Time{}
		}
	}
	s.deliveries[name] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	s.cleanupUnreferencedMessagesLocked()
	if err := s.saveResourcesLocked(); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "pubsub resource store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}
