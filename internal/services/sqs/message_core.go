package sqs

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"
)

func (s *Server) receiveMessages(ctx context.Context, input receiveMessageRequest) ([]receivedMessage, error) {
	if input.QueueURL == "" {
		return nil, errors.New("QueueUrl is required")
	}
	name := queueNameFromURL(input.QueueURL)
	if name == "" {
		return nil, errors.New("queue does not exist")
	}
	maxMessages := 1
	if input.MaxNumberOfMessages != nil {
		maxMessages = *input.MaxNumberOfMessages
	}
	if maxMessages < 1 {
		maxMessages = 1
	}
	if maxMessages > s.maxReceiveBatchSize() {
		maxMessages = s.maxReceiveBatchSize()
	}
	waitSeconds := s.defaultReceiveWaitTimeSeconds()
	if input.WaitTimeSeconds != nil {
		waitSeconds = *input.WaitTimeSeconds
	}
	if waitSeconds < 0 {
		return nil, errors.New("WaitTimeSeconds must be non-negative")
	}
	if waitSeconds > 20 {
		return nil, errors.New("WaitTimeSeconds must be no greater than 20")
	}

	deadline := time.Now().UTC().Add(time.Duration(waitSeconds) * time.Second)
	for {
		messages, err := s.receiveAvailableMessages(name, maxMessages, input.VisibilityTimeout, input.MessageAttributeNames, requestedSystemAttributeNames(input))
		if err != nil || len(messages) > 0 || time.Now().UTC().After(deadline) {
			return messages, err
		}
		waitCh := s.currentWaitChannel()
		sleepFor := time.Until(deadline)
		if sleepFor > 100*time.Millisecond {
			sleepFor = 100 * time.Millisecond
		}
		timer := time.NewTimer(sleepFor)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-waitCh:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (s *Server) currentWaitChannel() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.waitCh
}

func (s *Server) notifyWaitersLocked() {
	close(s.waitCh)
	s.waitCh = make(chan struct{})
}

func (s *Server) receiveAvailableMessages(name string, maxMessages int, visibilityOverride *int, messageAttributeNames []string, systemAttributeNames []string) ([]receivedMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return nil, errors.New("queue does not exist")
	}
	now := time.Now().UTC()
	cleanupExpiredMessagesLocked(queue, now)
	visibilitySeconds := intAttribute(queue.Attributes, "VisibilityTimeout", s.defaultVisibilityTimeoutSeconds())
	if visibilityOverride != nil {
		visibilitySeconds = *visibilityOverride
	}
	if visibilitySeconds < 0 {
		return nil, errors.New("VisibilityTimeout must be non-negative")
	}
	if visibilitySeconds > maxVisibilityTimeoutSeconds {
		return nil, errors.New("VisibilityTimeout must be no greater than 43200")
	}
	messages := make([]receivedMessage, 0, maxMessages)
	blockedFIFOMessageGroups := map[string]struct{}{}
	deliveredFIFOMessageGroups := map[string]struct{}{}
	fifoQueue := isFIFOQueue(queue)
	changed := false
	for _, message := range queue.Messages {
		if len(messages) >= maxMessages {
			break
		}
		if message.Deleted {
			continue
		}
		if fifoQueue && message.MessageGroupID != "" {
			if _, blocked := blockedFIFOMessageGroups[message.MessageGroupID]; blocked {
				continue
			}
			if _, delivered := deliveredFIFOMessageGroups[message.MessageGroupID]; delivered {
				continue
			}
		}
		if now.Before(message.AvailableAt) || now.Before(message.InvisibleUntil) {
			if fifoQueue && message.MessageGroupID != "" {
				blockedFIFOMessageGroups[message.MessageGroupID] = struct{}{}
			}
			continue
		}
		if s.moveToDeadLetterQueueIfNeededLocked(queue, message, now) {
			changed = true
			if fifoQueue && message.MessageGroupID != "" {
				blockedFIFOMessageGroups[message.MessageGroupID] = struct{}{}
			}
			continue
		}
		message.ReceiveCount++
		if message.FirstReceiveAt.IsZero() {
			message.FirstReceiveAt = now
		}
		message.ReceiptHandle = newOpaqueID("rct")
		message.InvisibleUntil = now.Add(time.Duration(visibilitySeconds) * time.Second)
		messages = append(messages, receivedMessageFromState(message, messageAttributeNames, systemAttributeNames))
		changed = true
		if fifoQueue && message.MessageGroupID != "" {
			deliveredFIFOMessageGroups[message.MessageGroupID] = struct{}{}
		}
	}
	if changed {
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func (s *Server) moveToDeadLetterQueueIfNeededLocked(queue *queueState, message *messageState, now time.Time) bool {
	policy, ok := redrivePolicyFromQueue(queue)
	if !ok || message.ReceiveCount < policy.MaxReceiveCount {
		return false
	}
	dlq := s.queueByARNLocked(policy.DeadLetterTargetARN)
	if dlq == nil {
		return false
	}
	moved := cloneMessage(message)
	moved.AvailableAt = now
	moved.InvisibleUntil = time.Time{}
	moved.ReceiptHandle = ""
	moved.ReceiveCount = 0
	moved.FirstReceiveAt = time.Time{}
	moved.Deleted = false
	moved.DeadLetterSourceARN = queue.ARN
	dlq.Messages = append(dlq.Messages, moved)
	message.Deleted = true
	message.ReceiptHandle = ""
	return true
}

func (s *Server) deleteMessage(queueURL string, receiptHandle string) error {
	if queueURL == "" {
		return errors.New("QueueUrl is required")
	}
	if receiptHandle == "" {
		return errors.New("ReceiptHandle is required")
	}
	name := queueNameFromURL(queueURL)
	if name == "" {
		return errors.New("queue does not exist")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return errors.New("queue does not exist")
	}
	now := time.Now().UTC()
	for _, message := range queue.Messages {
		if !message.Deleted && message.ReceiptHandle == receiptHandle {
			if !now.Before(message.InvisibleUntil) {
				message.ReceiptHandle = ""
				if err := s.persistLocked(); err != nil {
					return err
				}
				return errors.New("receipt handle is invalid")
			}
			message.Deleted = true
			message.ReceiptHandle = ""
			return s.persistLocked()
		}
	}
	return errors.New("receipt handle is invalid")
}

func (s *Server) changeMessageVisibility(queueURL string, receiptHandle string, visibilitySeconds int) error {
	if queueURL == "" {
		return errors.New("QueueUrl is required")
	}
	if receiptHandle == "" {
		return errors.New("ReceiptHandle is required")
	}
	if visibilitySeconds < 0 {
		return errors.New("VisibilityTimeout must be non-negative")
	}
	if visibilitySeconds > maxVisibilityTimeoutSeconds {
		return errors.New("VisibilityTimeout must be no greater than 43200")
	}
	name := queueNameFromURL(queueURL)
	if name == "" {
		return errors.New("queue does not exist")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return errors.New("queue does not exist")
	}
	now := time.Now().UTC()
	for _, message := range queue.Messages {
		if !message.Deleted && message.ReceiptHandle == receiptHandle {
			if !now.Before(message.InvisibleUntil) {
				message.ReceiptHandle = ""
				if err := s.persistLocked(); err != nil {
					return err
				}
				return errors.New("receipt handle is invalid")
			}
			message.InvisibleUntil = now.Add(time.Duration(visibilitySeconds) * time.Second)
			if err := s.persistLocked(); err != nil {
				return err
			}
			if visibilitySeconds == 0 {
				s.notifyWaitersLocked()
			}
			return nil
		}
	}
	return errors.New("receipt handle is invalid")
}

func receivedMessageFromState(message *messageState, messageAttributeNames []string, systemAttributeNames []string) receivedMessage {
	attrs := map[string]string{
		"ApproximateReceiveCount": strconv.Itoa(message.ReceiveCount),
		"SentTimestamp":           strconv.FormatInt(message.SentAt.UnixMilli(), 10),
	}
	if !message.FirstReceiveAt.IsZero() {
		attrs["ApproximateFirstReceiveTimestamp"] = strconv.FormatInt(message.FirstReceiveAt.UnixMilli(), 10)
	}
	if wantsAny(systemAttributeNames, "AWSTraceHeader") {
		if traceHeader := message.SystemAttributes["AWSTraceHeader"].StringValue; traceHeader != "" {
			attrs["AWSTraceHeader"] = traceHeader
		}
	}
	response := receivedMessage{
		MessageID:        message.ID,
		ReceiptHandle:    message.ReceiptHandle,
		MD5OfMessageBody: message.BodyMD5,
		Body:             message.Body,
		Attributes:       attrs,
	}
	if wantsAll(messageAttributeNames) {
		response.MessageAttributes = copyMessageAttributes(message.Attributes)
		response.MD5OfMessageAttributes = md5OfMessageAttributes(response.MessageAttributes)
	} else if filtered := filterMessageAttributes(message.Attributes, messageAttributeNames); len(filtered) > 0 {
		response.MessageAttributes = filtered
		response.MD5OfMessageAttributes = md5OfMessageAttributes(filtered)
	}
	if wantsAny(systemAttributeNames, "AWSTraceHeader") {
		response.MD5OfMessageSystemAttributes = md5OfMessageAttributes(message.SystemAttributes)
	}
	return response
}

func wantsAll(names []string) bool {
	for _, name := range names {
		if name == "All" || name == ".*" {
			return true
		}
	}
	return false
}

func wantsAny(names []string, target string) bool {
	if wantsAll(names) {
		return true
	}
	for _, name := range names {
		if name == target {
			return true
		}
	}
	return false
}

func filterMessageAttributes(attrs map[string]messageAttributeValue, names []string) map[string]messageAttributeValue {
	if len(attrs) == 0 || len(names) == 0 {
		return nil
	}
	filtered := map[string]messageAttributeValue{}
	for _, requested := range names {
		if requested == "" {
			continue
		}
		if strings.HasSuffix(requested, ".*") {
			prefix := strings.TrimSuffix(requested, ".*")
			for name, value := range attrs {
				if strings.HasPrefix(name, prefix) {
					filtered[name] = value
				}
			}
			continue
		}
		if value, ok := attrs[requested]; ok {
			filtered[requested] = value
		}
	}
	return filtered
}

func requestedSystemAttributeNames(input receiveMessageRequest) []string {
	if len(input.MessageSystemAttributeNames) > 0 {
		return input.MessageSystemAttributeNames
	}
	return input.AttributeNames
}

func cleanupExpiredMessagesLocked(queue *queueState, now time.Time) {
	retentionSeconds := intAttribute(queue.Attributes, "MessageRetentionPeriod", 345600)
	retained := queue.Messages[:0]
	for _, message := range queue.Messages {
		if message.Deleted {
			continue
		}
		if retentionSeconds > 0 && now.Sub(message.SentAt) > time.Duration(retentionSeconds)*time.Second {
			continue
		}
		retained = append(retained, message)
	}
	queue.Messages = retained
	cleanupExpiredDeduplicationLocked(queue, now)
}

func cleanupExpiredDeduplicationLocked(queue *queueState, now time.Time) {
	for id, state := range queue.Dedup {
		if !now.Before(state.ExpiresAt) {
			delete(queue.Dedup, id)
		}
	}
}

func intAttribute(attrs map[string]string, key string, fallback int) int {
	value, err := strconv.Atoi(attrs[key])
	if err != nil {
		return fallback
	}
	return value
}

func copyMessageAttributes(attrs map[string]messageAttributeValue) map[string]messageAttributeValue {
	copied := make(map[string]messageAttributeValue, len(attrs))
	for key, value := range attrs {
		copied[key] = value
	}
	return copied
}

func cloneMessage(message *messageState) *messageState {
	if message == nil {
		return nil
	}
	cloned := *message
	cloned.Attributes = copyMessageAttributes(message.Attributes)
	cloned.SystemAttributes = copyMessageAttributes(message.SystemAttributes)
	return &cloned
}

func cloneMessages(messages []*messageState) []*messageState {
	cloned := make([]*messageState, 0, len(messages))
	for _, message := range messages {
		if message != nil {
			cloned = append(cloned, cloneMessage(message))
		}
	}
	return cloned
}

func cloneDeduplication(values map[string]deduplicationState) map[string]deduplicationState {
	cloned := make(map[string]deduplicationState, len(values))
	for key, value := range values {
		cloned[key] = deduplicationState{
			ExpiresAt: value.ExpiresAt,
			Message:   cloneMessage(value.Message),
		}
	}
	return cloned
}
