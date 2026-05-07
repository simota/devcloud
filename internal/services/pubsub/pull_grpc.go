package pubsub

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (a *pubSubGRPCAdapter) Pull(ctx context.Context, request *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	maxMessages := int(request.GetMaxMessages())
	if maxMessages <= 0 {
		maxMessages = 1
	}
	if maxMessages > a.server.config.MaxPullMessages {
		maxMessages = a.server.config.MaxPullMessages
	}
	if !request.GetReturnImmediately() {
		a.server.waitForPullAvailability(ctx, request.GetSubscription())
	}

	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[request.GetSubscription()]
	if !found {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	if subscription.Detached {
		return nil, status.Error(codes.FailedPrecondition, "subscription is detached")
	}
	if subscriptionPushEndpoint(subscription) != "" {
		return nil, status.Error(codes.FailedPrecondition, "subscription is configured for push delivery")
	}
	now := a.server.now().UTC()
	a.server.cleanupRetainedMessagesLocked(now)
	a.server.expireLeasesLocked(now)
	received, deliveries := a.server.pullLocked(subscription, maxMessages, now)
	a.server.deliveries[subscription.Name] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	a.server.cleanupUnreferencedMessagesLocked()
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return &pubsubpb.PullResponse{ReceivedMessages: received}, nil
}

func (a *pubSubGRPCAdapter) Acknowledge(ctx context.Context, request *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	_ = ctx
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	if err := a.server.updateAckDeadlineLocked(request.GetSubscription(), request.GetAckIds(), 0, true); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (a *pubSubGRPCAdapter) ModifyAckDeadline(ctx context.Context, request *pubsubpb.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	_ = ctx
	if request == nil || !validFullSubscriptionName(request.GetSubscription()) {
		return nil, status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	if request.GetAckDeadlineSeconds() < 0 {
		return nil, status.Error(codes.InvalidArgument, "ackDeadlineSeconds must be non-negative")
	}
	if int(request.GetAckDeadlineSeconds()) > a.server.config.MaxAckDeadlineSeconds {
		return nil, status.Error(codes.InvalidArgument, "ackDeadlineSeconds exceeds maxAckDeadlineSeconds")
	}
	if err := a.server.updateAckDeadlineLocked(request.GetSubscription(), request.GetAckIds(), int(request.GetAckDeadlineSeconds()), false); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) pullLocked(subscription subscriptionResource, maxMessages int, now time.Time) ([]*pubsubpb.ReceivedMessage, []deliveryRecord) {
	ackDeadline := subscription.AckDeadlineSeconds
	if ackDeadline <= 0 {
		ackDeadline = s.config.DefaultAckDeadlineSeconds
	}
	return s.pullWithAckDeadlineLocked(subscription, maxMessages, now, ackDeadline)
}

func (s *Server) pullWithAckDeadlineLocked(subscription subscriptionResource, maxMessages int, now time.Time, ackDeadline int) ([]*pubsubpb.ReceivedMessage, []deliveryRecord) {
	received := make([]*pubsubpb.ReceivedMessage, 0, maxMessages)
	deliveries := s.deliveries[subscription.Name]
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
	if ackDeadline <= 0 {
		ackDeadline = s.config.DefaultAckDeadlineSeconds
	}
	for i := range deliveries {
		if len(received) >= maxMessages {
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
		received = append(received, &pubsubpb.ReceivedMessage{
			AckId:           deliveries[i].AckID,
			Message:         message.toProto(),
			DeliveryAttempt: int32(deliveries[i].DeliveryAttempt),
		})
	}
	return received, deliveries
}

func (s *Server) updateAckDeadlineLocked(subscriptionName string, ackIDs []string, ackDeadlineSeconds int, acknowledge bool) error {
	if len(ackIDs) == 0 {
		return nil
	}
	for _, ackID := range ackIDs {
		if strings.TrimSpace(ackID) == "" {
			return status.Error(codes.InvalidArgument, "ackIds must not contain empty values")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	subscription, found := s.subscriptions[subscriptionName]
	if !found {
		return status.Error(codes.NotFound, "subscription not found")
	}
	ackIDSet := map[string]struct{}{}
	for _, ackID := range ackIDs {
		ackIDSet[ackID] = struct{}{}
	}
	now := s.now().UTC()
	s.expireLeasesLocked(now)
	deliveries := s.deliveries[subscriptionName]
	for i := range deliveries {
		if _, ok := ackIDSet[deliveries[i].AckID]; !ok || deliveries[i].Acked {
			continue
		}
		if acknowledge {
			deliveries[i].Acked = true
			deliveries[i].AckID = ""
			deliveries[i].LeaseDeadline = time.Time{}
			deliveries[i].NextDeliveryTime = time.Time{}
			continue
		}
		if ackDeadlineSeconds == 0 {
			deliveries[i].AckID = ""
			deliveries[i].LeaseDeadline = time.Time{}
			deliveries[i].NextDeliveryTime = time.Time{}
		} else {
			deliveries[i].LeaseDeadline = now.Add(time.Duration(ackDeadlineSeconds) * time.Second)
			deliveries[i].NextDeliveryTime = time.Time{}
		}
	}
	s.deliveries[subscriptionName] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	s.cleanupUnreferencedMessagesLocked()
	if err := s.saveResourcesLocked(); err != nil {
		return status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	return nil
}
