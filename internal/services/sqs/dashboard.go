package sqs

import (
	"sort"
	"time"
)

type Snapshot struct {
	Status  string          `json:"status"`
	Running bool            `json:"running"`
	Region  string          `json:"region"`
	Queues  []QueueSnapshot `json:"queues"`
}

type QueueSnapshot struct {
	Name                  string            `json:"name"`
	URL                   string            `json:"url"`
	ARN                   string            `json:"arn"`
	Attributes            map[string]string `json:"attributes"`
	Tags                  map[string]string `json:"tags,omitempty"`
	CreatedAt             time.Time         `json:"createdAt"`
	VisibleMessages       int               `json:"visibleMessages"`
	NotVisibleMessages    int               `json:"notVisibleMessages"`
	DelayedMessages       int               `json:"delayedMessages"`
	TotalRetainedMessages int               `json:"totalRetainedMessages"`
}

type QueueDetailSnapshot struct {
	Queue    QueueSnapshot     `json:"queue"`
	Messages []MessageSnapshot `json:"messages"`
	Leases   []LeaseSnapshot   `json:"leases"`
}

type DeadLetterSnapshot struct {
	DeadLetterQueue        *QueueSnapshot  `json:"deadLetterQueue,omitempty"`
	DeadLetterSourceQueues []QueueSnapshot `json:"deadLetterSourceQueues"`
}

type MessageSnapshot struct {
	MessageID        string                           `json:"messageId"`
	Body             string                           `json:"body"`
	MD5OfMessageBody string                           `json:"md5OfMessageBody"`
	Attributes       map[string]messageAttributeValue `json:"attributes,omitempty"`
	SystemAttributes map[string]messageAttributeValue `json:"systemAttributes,omitempty"`
	SentAt           time.Time                        `json:"sentAt"`
	AvailableAt      time.Time                        `json:"availableAt"`
	InvisibleUntil   time.Time                        `json:"invisibleUntil,omitempty"`
	ReceiveCount     int                              `json:"receiveCount"`
	FirstReceiveAt   *time.Time                       `json:"firstReceiveAt,omitempty"`
	State            string                           `json:"state"`
	MessageGroupID   string                           `json:"messageGroupId,omitempty"`
	DeduplicationID  string                           `json:"deduplicationId,omitempty"`
	SequenceNumber   string                           `json:"sequenceNumber,omitempty"`
}

type LeaseSnapshot struct {
	MessageID            string    `json:"messageId"`
	VisibleAfter         time.Time `json:"visibleAfter"`
	ReceiveCount         int       `json:"receiveCount"`
	ReceiptHandlePresent bool      `json:"receiptHandlePresent"`
}

func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	names := make([]string, 0, len(s.queues))
	for name := range s.queues {
		names = append(names, name)
	}
	sort.Strings(names)

	queues := make([]QueueSnapshot, 0, len(names))
	for _, name := range names {
		queue := s.queues[name]
		cleanupExpiredMessagesLocked(queue, now)
		queues = append(queues, queueSnapshotLocked(queue, now))
	}

	return Snapshot{
		Status:  "running",
		Running: true,
		Region:  defaultString(s.config.Region, "us-east-1"),
		Queues:  queues,
	}
}

func queueSnapshotLocked(queue *queueState, now time.Time) QueueSnapshot {
	snapshot := QueueSnapshot{
		Name:       queue.Name,
		URL:        queue.URL,
		ARN:        queue.ARN,
		Attributes: copyAttributes(queue.Attributes),
		Tags:       copyAttributes(queue.Tags),
		CreatedAt:  queue.CreatedAt,
	}
	for _, message := range queue.Messages {
		if message.Deleted {
			continue
		}
		snapshot.TotalRetainedMessages++
		switch {
		case now.Before(message.AvailableAt):
			snapshot.DelayedMessages++
		case now.Before(message.InvisibleUntil):
			snapshot.NotVisibleMessages++
		default:
			snapshot.VisibleMessages++
		}
	}
	return snapshot
}

func messageSnapshotLocked(message *messageState, now time.Time) MessageSnapshot {
	var firstReceiveAt *time.Time
	if !message.FirstReceiveAt.IsZero() {
		value := message.FirstReceiveAt
		firstReceiveAt = &value
	}
	return MessageSnapshot{
		MessageID:        message.ID,
		Body:             message.Body,
		MD5OfMessageBody: message.BodyMD5,
		Attributes:       copyMessageAttributes(message.Attributes),
		SystemAttributes: copyMessageAttributes(message.SystemAttributes),
		SentAt:           message.SentAt,
		AvailableAt:      message.AvailableAt,
		InvisibleUntil:   message.InvisibleUntil,
		ReceiveCount:     message.ReceiveCount,
		FirstReceiveAt:   firstReceiveAt,
		State:            messageStateName(message, now),
		MessageGroupID:   message.MessageGroupID,
		DeduplicationID:  message.DeduplicationID,
		SequenceNumber:   message.SequenceNumber,
	}
}

func messageStateName(message *messageState, now time.Time) string {
	switch {
	case message.Deleted:
		return "deleted"
	case now.Before(message.AvailableAt):
		return "delayed"
	case now.Before(message.InvisibleUntil):
		return "in_flight"
	default:
		return "visible"
	}
}

func (s *Server) QueueDetailSnapshot(name string) (QueueDetailSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queue, ok := s.queues[name]
	if !ok {
		return QueueDetailSnapshot{}, false
	}
	now := time.Now().UTC()
	cleanupExpiredMessagesLocked(queue, now)
	detail := QueueDetailSnapshot{
		Queue:    queueSnapshotLocked(queue, now),
		Messages: make([]MessageSnapshot, 0, len(queue.Messages)),
		Leases:   []LeaseSnapshot{},
	}
	for _, message := range queue.Messages {
		if message.Deleted {
			continue
		}
		detail.Messages = append(detail.Messages, messageSnapshotLocked(message, now))
		if message.ReceiptHandle != "" && now.Before(message.InvisibleUntil) {
			detail.Leases = append(detail.Leases, LeaseSnapshot{
				MessageID:            message.ID,
				VisibleAfter:         message.InvisibleUntil,
				ReceiveCount:         message.ReceiveCount,
				ReceiptHandlePresent: true,
			})
		}
	}
	return detail, true
}

func (s *Server) DeadLetterSnapshot(name string) (DeadLetterSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	queue, ok := s.queues[name]
	if !ok {
		return DeadLetterSnapshot{}, false
	}
	now := time.Now().UTC()
	var deadLetterQueue *QueueSnapshot
	if policy, ok := redrivePolicyFromQueue(queue); ok {
		if dlq := s.queueByARNLocked(policy.DeadLetterTargetARN); dlq != nil {
			snapshot := queueSnapshotLocked(dlq, now)
			deadLetterQueue = &snapshot
		}
	}
	sourceNames := make([]string, 0)
	for sourceName, source := range s.queues {
		policy, ok := redrivePolicyFromQueue(source)
		if ok && policy.DeadLetterTargetARN == queue.ARN {
			sourceNames = append(sourceNames, sourceName)
		}
	}
	sort.Strings(sourceNames)
	sources := make([]QueueSnapshot, 0, len(sourceNames))
	for _, sourceName := range sourceNames {
		sources = append(sources, queueSnapshotLocked(s.queues[sourceName], now))
	}
	return DeadLetterSnapshot{
		DeadLetterQueue:        deadLetterQueue,
		DeadLetterSourceQueues: sources,
	}, true
}

func (s *Server) PurgeQueueByName(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue, ok := s.queues[name]
	if !ok {
		return false
	}
	previous := queue.Messages
	queue.Messages = nil
	if err := s.persistLocked(); err != nil {
		queue.Messages = previous
		return false
	}
	return true
}
