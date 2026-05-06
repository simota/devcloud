package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

func (s *Server) pushWorker(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: 5 * time.Second}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				delivered, _ := s.deliverPush(ctx, client)
				if !delivered {
					break
				}
			}
		}
	}
}

func (s *Server) deliverPush(ctx context.Context, client *http.Client) (bool, error) {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	delivery, ok, err := s.nextPushDelivery()
	if !ok || err != nil {
		return ok, err
	}
	body, err := json.Marshal(map[string]any{
		"message": map[string]any{
			"data":            delivery.Message.Data,
			"attributes":      delivery.Message.Attributes,
			"messageId":       delivery.Message.MessageID,
			"message_id":      delivery.Message.MessageID,
			"publishTime":     delivery.Message.PublishTime,
			"publish_time":    delivery.Message.PublishTime,
			"orderingKey":     delivery.Message.OrderingKey,
			"ordering_key":    delivery.Message.OrderingKey,
			"deliveryAttempt": delivery.Attempt,
		},
		"subscription":    delivery.SubscriptionName,
		"deliveryAttempt": delivery.Attempt,
	})
	if err != nil {
		s.finishPushDelivery(delivery, false)
		return true, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, delivery.Endpoint, bytes.NewReader(body))
	if err != nil {
		s.finishPushDelivery(delivery, false)
		return true, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		s.finishPushDelivery(delivery, false)
		return true, nil
	}
	defer response.Body.Close()
	s.finishPushDelivery(delivery, response.StatusCode >= 200 && response.StatusCode < 300)
	return true, nil
}

type pushDelivery struct {
	SubscriptionName string
	Endpoint         string
	Message          pubsubMessage
	Attempt          int
}

func (s *Server) nextPushDelivery() (pushDelivery, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	s.cleanupRetainedMessagesLocked(now)
	s.expireLeasesLocked(now)
	subscriptionNames := make([]string, 0, len(s.subscriptions))
	for name := range s.subscriptions {
		subscriptionNames = append(subscriptionNames, name)
	}
	sort.Strings(subscriptionNames)
	for _, subscriptionName := range subscriptionNames {
		subscription := s.subscriptions[subscriptionName]
		endpoint := subscriptionPushEndpoint(subscription)
		if endpoint == "" || subscription.Detached {
			continue
		}
		deliveries := s.deliveries[subscriptionName]
		for i := range deliveries {
			if deliveries[i].Acked || deliveries[i].LeaseDeadline.After(now) || deliveries[i].NextDeliveryTime.After(now) {
				continue
			}
			message, found := s.messages[deliveries[i].MessageID]
			if !found {
				continue
			}
			if s.deadLetterDeliveryLocked(subscription, &deliveries[i], message, now) {
				continue
			}
			ackDeadline := subscription.AckDeadlineSeconds
			if ackDeadline <= 0 {
				ackDeadline = s.config.DefaultAckDeadlineSeconds
			}
			deliveries[i].DeliveryAttempt++
			deliveries[i].LeaseDeadline = now.Add(time.Duration(ackDeadline) * time.Second)
			deliveries[i].NextDeliveryTime = time.Time{}
			s.deliveries[subscriptionName] = deliveries
			if err := s.saveResourcesLocked(); err != nil {
				return pushDelivery{}, false, err
			}
			return pushDelivery{
				SubscriptionName: subscriptionName,
				Endpoint:         endpoint,
				Message:          message,
				Attempt:          deliveries[i].DeliveryAttempt,
			}, true, nil
		}
	}
	return pushDelivery{}, false, nil
}

func (s *Server) finishPushDelivery(delivery pushDelivery, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[delivery.SubscriptionName]
	if !found {
		return
	}
	now := s.now().UTC()
	deliveries := s.deliveries[delivery.SubscriptionName]
	for i := range deliveries {
		if deliveries[i].MessageID != delivery.Message.MessageID || deliveries[i].DeliveryAttempt != delivery.Attempt || deliveries[i].Acked {
			continue
		}
		deliveries[i].LeaseDeadline = time.Time{}
		if success {
			deliveries[i].Acked = true
			deliveries[i].AckID = ""
			deliveries[i].NextDeliveryTime = time.Time{}
		} else {
			deliveries[i].NextDeliveryTime = now.Add(s.subscriptionRetryBackoffLocked(delivery.SubscriptionName, deliveries[i].DeliveryAttempt))
		}
		break
	}
	s.deliveries[delivery.SubscriptionName] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	s.cleanupUnreferencedMessagesLocked()
	_ = s.saveResourcesLocked()
}

func subscriptionPushEndpoint(subscription subscriptionResource) string {
	rawEndpoint, ok := subscription.PushConfig["pushEndpoint"]
	if !ok {
		return ""
	}
	endpoint, _ := rawEndpoint.(string)
	return strings.TrimSpace(endpoint)
}

func safePushConfigSnapshot(config map[string]any) map[string]any {
	if len(config) == 0 {
		return nil
	}
	safe := map[string]any{}
	if endpoint, ok := config["pushEndpoint"].(string); ok && strings.TrimSpace(endpoint) != "" {
		safe["pushEndpoint"] = strings.TrimSpace(endpoint)
	}
	if len(safe) == 0 {
		return nil
	}
	return safe
}
