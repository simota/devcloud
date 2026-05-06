package pubsub

import (
	"sort"
	"time"
)

type Snapshot struct {
	Status        string                 `json:"status"`
	Running       bool                   `json:"running"`
	Project       string                 `json:"project"`
	Topics        []TopicSnapshot        `json:"topics"`
	Subscriptions []SubscriptionSnapshot `json:"subscriptions"`
}

type TopicSnapshot struct {
	Name              string `json:"name"`
	SubscriptionCount int    `json:"subscriptionCount"`
	CreatedAt         string `json:"createdAt,omitempty"`
	UpdatedAt         string `json:"updatedAt,omitempty"`
}

type SubscriptionSnapshot struct {
	Name                      string             `json:"name"`
	Topic                     string             `json:"topic"`
	Labels                    map[string]string  `json:"labels,omitempty"`
	CreatedAt                 string             `json:"createdAt,omitempty"`
	UpdatedAt                 string             `json:"updatedAt,omitempty"`
	AckDeadlineSeconds        int                `json:"ackDeadlineSeconds"`
	EnableMessageOrdering     bool               `json:"enableMessageOrdering,omitempty"`
	EnableExactlyOnceDelivery bool               `json:"enableExactlyOnceDelivery,omitempty"`
	RetainAckedMessages       bool               `json:"retainAckedMessages,omitempty"`
	MessageRetentionDuration  string             `json:"messageRetentionDuration,omitempty"`
	ExpirationPolicy          map[string]any     `json:"expirationPolicy,omitempty"`
	Filter                    string             `json:"filter,omitempty"`
	DeadLetterPolicy          map[string]any     `json:"deadLetterPolicy,omitempty"`
	RetryPolicy               map[string]any     `json:"retryPolicy,omitempty"`
	PushConfig                map[string]any     `json:"pushConfig,omitempty"`
	BacklogMessages           int                `json:"backlogMessages"`
	InFlightMessages          int                `json:"inFlightMessages"`
	TotalRetainedMessages     int                `json:"totalRetainedMessages"`
	MaxDeliveryAttemptSeen    int                `json:"maxDeliveryAttemptSeen"`
	RecentDeliveries          []DeliverySnapshot `json:"recentDeliveries,omitempty"`
}

type DeliverySnapshot struct {
	MessageID        string `json:"messageId"`
	Subscription     string `json:"subscription,omitempty"`
	PublishTime      string `json:"publishTime,omitempty"`
	OrderingKey      string `json:"orderingKey,omitempty"`
	State            string `json:"state"`
	LeaseDeadline    string `json:"leaseDeadline,omitempty"`
	NextDeliveryTime string `json:"nextDeliveryTime,omitempty"`
	DeliveryAttempt  int    `json:"deliveryAttempt"`
}

type MessageSnapshot struct {
	MessageID     string             `json:"messageId"`
	PublishTime   string             `json:"publishTime,omitempty"`
	OrderingKey   string             `json:"orderingKey,omitempty"`
	Subscriptions []DeliverySnapshot `json:"subscriptions,omitempty"`
}

func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	s.expireLeasesLocked(now)
	s.cleanupRetainedMessagesLocked(now)
	topicNames := make([]string, 0, len(s.topics))
	for name := range s.topics {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames)

	topics := make([]TopicSnapshot, 0, len(topicNames))
	for _, name := range topicNames {
		count := 0
		for _, subscription := range s.subscriptions {
			if subscription.Topic == name {
				count++
			}
		}
		topic := s.topics[name]
		topics = append(topics, TopicSnapshot{
			Name:              name,
			SubscriptionCount: count,
			CreatedAt:         topic.CreatedAt,
			UpdatedAt:         topic.UpdatedAt,
		})
	}

	subscriptionNames := make([]string, 0, len(s.subscriptions))
	for name := range s.subscriptions {
		subscriptionNames = append(subscriptionNames, name)
	}
	sort.Strings(subscriptionNames)

	subscriptions := make([]SubscriptionSnapshot, 0, len(subscriptionNames))
	for _, name := range subscriptionNames {
		subscription := s.subscriptions[name]
		snapshot := SubscriptionSnapshot{
			Name:                      subscription.Name,
			Topic:                     subscription.Topic,
			Labels:                    copyStringMap(subscription.Labels),
			CreatedAt:                 subscription.CreatedAt,
			UpdatedAt:                 subscription.UpdatedAt,
			AckDeadlineSeconds:        subscription.AckDeadlineSeconds,
			EnableMessageOrdering:     subscription.EnableMessageOrdering,
			EnableExactlyOnceDelivery: subscription.EnableExactlyOnceDelivery,
			RetainAckedMessages:       subscription.RetainAckedMessages,
			MessageRetentionDuration:  subscription.MessageRetentionDuration,
			ExpirationPolicy:          copyAnyMap(subscription.ExpirationPolicy),
			Filter:                    subscription.Filter,
			DeadLetterPolicy:          copyAnyMap(subscription.DeadLetterPolicy),
			RetryPolicy:               copyAnyMap(subscription.RetryPolicy),
			PushConfig:                safePushConfigSnapshot(subscription.PushConfig),
		}
		for _, delivery := range s.deliveries[name] {
			if delivery.Acked {
				continue
			}
			snapshot.TotalRetainedMessages++
			if delivery.DeliveryAttempt > snapshot.MaxDeliveryAttemptSeen {
				snapshot.MaxDeliveryAttemptSeen = delivery.DeliveryAttempt
			}
			if delivery.LeaseDeadline.After(now) {
				snapshot.InFlightMessages++
			} else {
				snapshot.BacklogMessages++
			}
			snapshot.RecentDeliveries = append(snapshot.RecentDeliveries, s.deliverySnapshotLocked(delivery, now))
		}
		if len(snapshot.RecentDeliveries) > 20 {
			snapshot.RecentDeliveries = snapshot.RecentDeliveries[len(snapshot.RecentDeliveries)-20:]
		}
		subscriptions = append(subscriptions, snapshot)
	}

	return Snapshot{
		Status:        "running",
		Running:       true,
		Project:       defaultString(s.config.Project, "devcloud"),
		Topics:        topics,
		Subscriptions: subscriptions,
	}
}

func (s *Server) deliverySnapshotLocked(delivery deliveryRecord, now time.Time) DeliverySnapshot {
	state := "backlog"
	leaseDeadline := ""
	nextDeliveryTime := ""
	if delivery.LeaseDeadline.After(now) {
		state = "in-flight"
		leaseDeadline = delivery.LeaseDeadline.UTC().Format(time.RFC3339Nano)
	} else if delivery.NextDeliveryTime.After(now) {
		state = "delayed"
		nextDeliveryTime = delivery.NextDeliveryTime.UTC().Format(time.RFC3339Nano)
	}
	message := s.messages[delivery.MessageID]
	return DeliverySnapshot{
		MessageID:        delivery.MessageID,
		PublishTime:      message.PublishTime,
		OrderingKey:      message.OrderingKey,
		State:            state,
		LeaseDeadline:    leaseDeadline,
		NextDeliveryTime: nextDeliveryTime,
		DeliveryAttempt:  delivery.DeliveryAttempt,
	}
}

func (s *Server) MessageSnapshot(messageID string) (MessageSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	s.cleanupRetainedMessagesLocked(now)
	message, found := s.messages[messageID]
	if !found {
		return MessageSnapshot{}, false
	}
	snapshot := MessageSnapshot{
		MessageID:   message.MessageID,
		PublishTime: message.PublishTime,
		OrderingKey: message.OrderingKey,
	}
	for subscriptionName, deliveries := range s.deliveries {
		for _, delivery := range deliveries {
			if delivery.MessageID != messageID || delivery.Acked {
				continue
			}
			deliverySnapshot := s.deliverySnapshotLocked(delivery, now)
			deliverySnapshot.Subscription = subscriptionName
			snapshot.Subscriptions = append(snapshot.Subscriptions, deliverySnapshot)
		}
	}
	sort.Slice(snapshot.Subscriptions, func(i, j int) bool {
		return snapshot.Subscriptions[i].Subscription < snapshot.Subscriptions[j].Subscription
	})
	return snapshot, true
}
