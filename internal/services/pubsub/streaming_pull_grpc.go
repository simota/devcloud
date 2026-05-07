package pubsub

import (
	"errors"
	"io"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (a *pubSubGRPCAdapter) StreamingPull(stream pubsubpb.Subscriber_StreamingPullServer) error {
	if a.server.config.StreamingPullDisabled {
		return status.Error(codes.Unimplemented, "streaming pull is disabled")
	}
	initial, err := stream.Recv()
	if errorsIsEOF(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if initial == nil || !validFullSubscriptionName(initial.GetSubscription()) {
		return status.Error(codes.InvalidArgument, "invalid subscription name")
	}
	subscriptionName := initial.GetSubscription()
	streamAckDeadline, err := a.streamingAckDeadline(initial.GetStreamAckDeadlineSeconds())
	if err != nil {
		return err
	}
	if err := a.validateStreamingPullSubscription(subscriptionName); err != nil {
		return err
	}
	maxOutstandingMessages := initial.GetMaxOutstandingMessages()
	maxOutstandingBytes := initial.GetMaxOutstandingBytes()
	outstandingAckIDs := map[string]int{}
	defer a.releaseStreamingOutstandingAckIDs(subscriptionName, outstandingAckIDs)
	if err := a.applyStreamingPullRequest(subscriptionName, initial, true, &streamAckDeadline, outstandingAckIDs); err != nil {
		return err
	}

	requests := make(chan *pubsubpb.StreamingPullRequest, 1)
	requestErrs := make(chan error, 1)
	go func() {
		for {
			request, recvErr := stream.Recv()
			if recvErr != nil {
				requestErrs <- recvErr
				return
			}
			select {
			case requests <- request:
			case <-stream.Context().Done():
				return
			}
		}
	}()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case recvErr := <-requestErrs:
			if errorsIsEOF(recvErr) {
				return nil
			}
			return recvErr
		case request := <-requests:
			if err := a.applyStreamingPullRequest(subscriptionName, request, false, &streamAckDeadline, outstandingAckIDs); err != nil {
				return err
			}
		case <-ticker.C:
			a.pruneStreamingOutstandingAckIDs(subscriptionName, outstandingAckIDs)
			if !streamingPullHasCapacity(outstandingAckIDs, maxOutstandingMessages, maxOutstandingBytes) {
				continue
			}
			remaining := streamingPullRemainingCapacity(outstandingAckIDs, maxOutstandingMessages)
			if remaining <= 0 {
				continue
			}
			response, err := a.streamingPullResponse(subscriptionName, remaining, streamAckDeadline, maxOutstandingBytes, outstandingAckIDs)
			if err != nil {
				return err
			}
			if response == nil {
				continue
			}
			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}

func (a *pubSubGRPCAdapter) pruneStreamingOutstandingAckIDs(subscriptionName string, outstandingAckIDs map[string]int) {
	if len(outstandingAckIDs) == 0 {
		return
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	now := a.server.now().UTC()
	a.server.expireLeasesLocked(now)
	active := map[string]struct{}{}
	for _, delivery := range a.server.deliveries[subscriptionName] {
		if delivery.AckID == "" || delivery.Acked || !delivery.LeaseDeadline.After(now) {
			continue
		}
		active[delivery.AckID] = struct{}{}
	}
	for ackID := range outstandingAckIDs {
		if _, ok := active[ackID]; !ok {
			delete(outstandingAckIDs, ackID)
		}
	}
}

func (a *pubSubGRPCAdapter) releaseStreamingOutstandingAckIDs(subscriptionName string, outstandingAckIDs map[string]int) {
	if len(outstandingAckIDs) == 0 {
		return
	}
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[subscriptionName]
	if !found {
		return
	}
	deliveries := a.server.deliveries[subscriptionName]
	changed := false
	for ackID := range outstandingAckIDs {
		for i := range deliveries {
			if deliveries[i].AckID != ackID || deliveries[i].Acked {
				continue
			}
			deliveries[i].AckID = ""
			deliveries[i].LeaseDeadline = time.Time{}
			deliveries[i].NextDeliveryTime = time.Time{}
			changed = true
			break
		}
	}
	if !changed {
		return
	}
	a.server.deliveries[subscriptionName] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	a.server.cleanupUnreferencedMessagesLocked()
	_ = a.server.saveResourcesLocked()
}

func (a *pubSubGRPCAdapter) validateStreamingPullSubscription(subscriptionName string) error {
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[subscriptionName]
	if !found {
		return status.Error(codes.NotFound, "subscription not found")
	}
	if subscription.Detached {
		return status.Error(codes.FailedPrecondition, "subscription is detached")
	}
	if subscriptionPushEndpoint(subscription) != "" {
		return status.Error(codes.FailedPrecondition, "subscription is configured for push delivery")
	}
	return nil
}

func (a *pubSubGRPCAdapter) streamingAckDeadline(seconds int32) (int, error) {
	if seconds < 0 {
		return 0, status.Error(codes.InvalidArgument, "streamAckDeadlineSeconds must be non-negative")
	}
	if int(seconds) > a.server.config.MaxAckDeadlineSeconds {
		return 0, status.Error(codes.InvalidArgument, "streamAckDeadlineSeconds exceeds maxAckDeadlineSeconds")
	}
	if seconds == 0 {
		return a.server.config.DefaultAckDeadlineSeconds, nil
	}
	return int(seconds), nil
}

func (a *pubSubGRPCAdapter) applyStreamingPullRequest(subscriptionName string, request *pubsubpb.StreamingPullRequest, initial bool, streamAckDeadline *int, outstandingAckIDs map[string]int) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "streaming pull request is required")
	}
	if !initial && request.GetSubscription() != "" && request.GetSubscription() != subscriptionName {
		return status.Error(codes.InvalidArgument, "subscription must not change on a streaming pull stream")
	}
	if !initial && request.GetMaxOutstandingMessages() != 0 {
		return status.Error(codes.InvalidArgument, "maxOutstandingMessages can only be set on the initial request")
	}
	if !initial && request.GetMaxOutstandingBytes() != 0 {
		return status.Error(codes.InvalidArgument, "maxOutstandingBytes can only be set on the initial request")
	}
	if request.GetStreamAckDeadlineSeconds() != 0 {
		deadline, err := a.streamingAckDeadline(request.GetStreamAckDeadlineSeconds())
		if err != nil {
			return err
		}
		*streamAckDeadline = deadline
	}
	if len(request.GetModifyDeadlineAckIds()) != len(request.GetModifyDeadlineSeconds()) {
		return status.Error(codes.InvalidArgument, "modifyDeadlineAckIds and modifyDeadlineSeconds must have the same length")
	}
	if len(request.GetAckIds()) > 0 {
		if err := a.server.updateAckDeadlineLocked(subscriptionName, request.GetAckIds(), 0, true); err != nil {
			return err
		}
		for _, ackID := range request.GetAckIds() {
			delete(outstandingAckIDs, ackID)
		}
	}
	for i, ackID := range request.GetModifyDeadlineAckIds() {
		deadline := request.GetModifyDeadlineSeconds()[i]
		if deadline < 0 {
			return status.Error(codes.InvalidArgument, "modifyDeadlineSeconds must be non-negative")
		}
		if int(deadline) > a.server.config.MaxAckDeadlineSeconds {
			return status.Error(codes.InvalidArgument, "modifyDeadlineSeconds exceeds maxAckDeadlineSeconds")
		}
		if err := a.server.updateAckDeadlineLocked(subscriptionName, []string{ackID}, int(deadline), false); err != nil {
			return err
		}
		if deadline == 0 {
			delete(outstandingAckIDs, ackID)
		}
	}
	return nil
}

func (a *pubSubGRPCAdapter) streamingPullResponse(subscriptionName string, maxMessages int, ackDeadline int, maxOutstandingBytes int64, outstandingAckIDs map[string]int) (*pubsubpb.StreamingPullResponse, error) {
	a.server.mu.Lock()
	defer a.server.mu.Unlock()
	subscription, found := a.server.subscriptions[subscriptionName]
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
	received, deliveries := a.server.pullWithAckDeadlineLocked(subscription, maxMessages, now, ackDeadline)
	if len(received) == 0 {
		return nil, nil
	}
	if maxOutstandingBytes > 0 {
		kept := received[:0]
		remainingBytes := maxOutstandingBytes - int64(streamingOutstandingBytes(outstandingAckIDs))
		for _, message := range received {
			size := streamingReceivedMessageSize(message)
			if int64(size) > remainingBytes && len(kept) > 0 {
				break
			}
			kept = append(kept, message)
			remainingBytes -= int64(size)
		}
		for _, message := range received[len(kept):] {
			releaseUnsentStreamingAckIDLocked(deliveries, message.GetAckId())
		}
		received = kept
	}
	a.server.deliveries[subscription.Name] = compactAckedDeliveries(deliveries, subscription.RetainAckedMessages)
	a.server.cleanupUnreferencedMessagesLocked()
	if err := a.server.saveResourcesLocked(); err != nil {
		return nil, status.Error(codes.Internal, "pubsub resource store unavailable")
	}
	if len(received) == 0 {
		return nil, nil
	}
	for _, message := range received {
		outstandingAckIDs[message.GetAckId()] = streamingReceivedMessageSize(message)
	}
	return &pubsubpb.StreamingPullResponse{ReceivedMessages: received}, nil
}

func releaseAckIDLocked(deliveries []deliveryRecord, ackID string) {
	for i := range deliveries {
		if deliveries[i].AckID != ackID || deliveries[i].Acked {
			continue
		}
		deliveries[i].AckID = ""
		deliveries[i].LeaseDeadline = time.Time{}
		deliveries[i].NextDeliveryTime = time.Time{}
		return
	}
}

func releaseUnsentStreamingAckIDLocked(deliveries []deliveryRecord, ackID string) {
	for i := range deliveries {
		if deliveries[i].AckID != ackID || deliveries[i].Acked {
			continue
		}
		deliveries[i].AckID = ""
		deliveries[i].LeaseDeadline = time.Time{}
		deliveries[i].NextDeliveryTime = time.Time{}
		if deliveries[i].DeliveryAttempt > 0 {
			deliveries[i].DeliveryAttempt--
		}
		return
	}
}

func streamingPullHasCapacity(outstandingAckIDs map[string]int, maxOutstandingMessages int64, maxOutstandingBytes int64) bool {
	if maxOutstandingMessages > 0 && int64(len(outstandingAckIDs)) >= maxOutstandingMessages {
		return false
	}
	if maxOutstandingBytes > 0 && int64(streamingOutstandingBytes(outstandingAckIDs)) >= maxOutstandingBytes {
		return false
	}
	return true
}

func streamingPullRemainingCapacity(outstandingAckIDs map[string]int, maxOutstandingMessages int64) int {
	if maxOutstandingMessages <= 0 {
		return 1
	}
	remaining := int(maxOutstandingMessages) - len(outstandingAckIDs)
	if remaining < 1 {
		return 0
	}
	return remaining
}

func streamingOutstandingBytes(outstandingAckIDs map[string]int) int {
	total := 0
	for _, size := range outstandingAckIDs {
		total += size
	}
	return total
}

func streamingReceivedMessageSize(received *pubsubpb.ReceivedMessage) int {
	if received == nil || received.GetMessage() == nil {
		return 0
	}
	size := len(received.GetMessage().GetData()) + len(received.GetAckId()) + len(received.GetMessage().GetOrderingKey())
	for key, value := range received.GetMessage().GetAttributes() {
		size += len(key) + len(value)
	}
	return size
}

func errorsIsEOF(err error) bool {
	return errors.Is(err, io.EOF)
}
