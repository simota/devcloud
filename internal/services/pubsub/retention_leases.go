package pubsub

import (
	"strconv"
	"strings"
	"time"
)

func compactAckedDeliveries(deliveries []deliveryRecord, retainAcked bool) []deliveryRecord {
	if retainAcked {
		return deliveries
	}
	if len(deliveries) == 0 {
		return deliveries
	}
	kept := deliveries[:0]
	for _, delivery := range deliveries {
		if !delivery.Acked {
			kept = append(kept, delivery)
		}
	}
	return kept
}

func (s *Server) expireLeasesLocked(now time.Time) {
	for subscription, deliveries := range s.deliveries {
		changed := false
		for i := range deliveries {
			if deliveries[i].Acked || deliveries[i].LeaseDeadline.IsZero() || deliveries[i].LeaseDeadline.After(now) {
				continue
			}
			deliveries[i].AckID = ""
			nextDeliveryTime := deliveries[i].LeaseDeadline
			deliveries[i].LeaseDeadline = time.Time{}
			if backoff := s.subscriptionRetryBackoffLocked(subscription, deliveries[i].DeliveryAttempt); backoff > 0 {
				deliveries[i].NextDeliveryTime = nextDeliveryTime.Add(backoff)
			}
			changed = true
		}
		if changed {
			s.deliveries[subscription] = deliveries
		}
	}
}

func (s *Server) subscriptionRetryBackoffLocked(subscriptionName string, deliveryAttempt int) time.Duration {
	subscription, found := s.subscriptions[subscriptionName]
	if !found {
		return 0
	}
	minimum, ok, err := retryPolicyDuration(subscription.RetryPolicy, "minimumBackoff")
	if err != nil || !ok {
		return 0
	}
	maximum, hasMaximum, err := retryPolicyDuration(subscription.RetryPolicy, "maximumBackoff")
	if err != nil {
		return minimum
	}
	backoff := minimum
	for attempt := 1; attempt < deliveryAttempt; attempt++ {
		if hasMaximum && backoff >= maximum {
			return maximum
		}
		if backoff > time.Duration(1<<62) {
			if hasMaximum {
				return maximum
			}
			return backoff
		}
		backoff *= 2
	}
	if hasMaximum && backoff > maximum {
		return maximum
	}
	return backoff
}

func (s *Server) deadLetterDeliveryLocked(subscription subscriptionResource, delivery *deliveryRecord, message pubsubMessage, now time.Time) bool {
	maxAttempts, ok := deadLetterMaxDeliveryAttempts(subscription.DeadLetterPolicy)
	if !ok || delivery.DeliveryAttempt < maxAttempts {
		return false
	}
	topic := deadLetterTopic(subscription.DeadLetterPolicy)
	if topic == "" {
		return false
	}
	if _, found := s.topics[topic]; !found {
		return false
	}

	s.nextMessageID++
	deadLetterMessageID := strconv.FormatUint(s.nextMessageID, 10)
	deadLetterMessage := pubsubMessage{
		Data:        message.Data,
		Attributes:  copyStringMap(message.Attributes),
		MessageID:   deadLetterMessageID,
		PublishTime: now.UTC().Format(time.RFC3339Nano),
		OrderingKey: message.OrderingKey,
	}
	s.messages[deadLetterMessageID] = deadLetterMessage
	for _, candidate := range s.subscriptions {
		if candidate.Topic == topic && !candidate.Detached {
			s.deliveries[candidate.Name] = append(s.deliveries[candidate.Name], deliveryRecord{MessageID: deadLetterMessageID})
		}
	}
	delivery.Acked = true
	delivery.AckID = ""
	delivery.LeaseDeadline = time.Time{}
	delivery.NextDeliveryTime = time.Time{}
	return true
}

func (s *Server) cleanupRetainedMessagesLocked(now time.Time) {
	s.cleanupExpiredSnapshotsLocked(now)
	if s.config.MessageRetentionSeconds <= 0 || len(s.messages) == 0 {
		return
	}
	for subscription, deliveries := range s.deliveries {
		retention := s.subscriptionMessageRetentionLocked(subscription)
		cutoff := now.Add(-retention)
		kept := deliveries[:0]
		for _, delivery := range deliveries {
			message, found := s.messages[delivery.MessageID]
			if !found {
				continue
			}
			publishedAt, err := time.Parse(time.RFC3339Nano, message.PublishTime)
			if err != nil || publishedAt.Before(cutoff) {
				continue
			}
			kept = append(kept, delivery)
		}
		if len(kept) == 0 {
			delete(s.deliveries, subscription)
		} else {
			s.deliveries[subscription] = kept
		}
	}
	globalCutoff := now.Add(-time.Duration(s.config.MessageRetentionSeconds) * time.Second)
	for name, snapshot := range s.snapshots {
		kept := snapshot.Deliveries[:0]
		for _, delivery := range snapshot.Deliveries {
			message, found := s.messages[delivery.MessageID]
			if !found {
				continue
			}
			publishedAt, err := time.Parse(time.RFC3339Nano, message.PublishTime)
			if err != nil || publishedAt.Before(globalCutoff) {
				continue
			}
			kept = append(kept, delivery)
		}
		snapshot.Deliveries = kept
		s.snapshots[name] = snapshot
	}
	s.cleanupUnreferencedMessagesLocked()
}

func (s *Server) cleanupExpiredSnapshotsLocked(now time.Time) {
	for name, snapshot := range s.snapshots {
		if snapshotExpired(snapshot, now) {
			delete(s.snapshots, name)
		}
	}
}

func (s *Server) subscriptionMessageRetentionLocked(subscriptionName string) time.Duration {
	fallback := time.Duration(s.config.MessageRetentionSeconds) * time.Second
	subscription, found := s.subscriptions[subscriptionName]
	if !found || strings.TrimSpace(subscription.MessageRetentionDuration) == "" {
		if !found {
			return fallback
		}
		topic, topicFound := s.topics[subscription.Topic]
		if !topicFound || strings.TrimSpace(topic.MessageRetentionDuration) == "" {
			return fallback
		}
		retention, err := parseGoogleDuration(topic.MessageRetentionDuration)
		if err != nil || retention <= 0 {
			return fallback
		}
		return retention
	}
	retention, err := parseGoogleDuration(subscription.MessageRetentionDuration)
	if err != nil || retention <= 0 {
		return fallback
	}
	return retention
}

func (s *Server) cleanupUnreferencedMessagesLocked() {
	if len(s.messages) == 0 {
		return
	}
	referenced := map[string]struct{}{}
	for subscriptionName, deliveries := range s.deliveries {
		subscription := s.subscriptions[subscriptionName]
		for _, delivery := range deliveries {
			if delivery.Acked && !subscription.RetainAckedMessages {
				continue
			}
			referenced[delivery.MessageID] = struct{}{}
		}
	}
	for _, snapshot := range s.snapshots {
		for _, delivery := range snapshot.Deliveries {
			referenced[delivery.MessageID] = struct{}{}
		}
	}
	for id := range s.messages {
		if _, ok := referenced[id]; !ok {
			delete(s.messages, id)
		}
	}
}
