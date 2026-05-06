package sqs

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type persistedState struct {
	Queues    map[string]persistedQueue `json:"queues"`
	MoveTasks map[string]moveTaskState  `json:"moveTasks,omitempty"`
}

type persistedQueue struct {
	Name       string                        `json:"name"`
	URL        string                        `json:"url"`
	ARN        string                        `json:"arn"`
	Attributes map[string]string             `json:"attributes"`
	Tags       map[string]string             `json:"tags,omitempty"`
	CreatedAt  time.Time                     `json:"createdAt"`
	ModifiedAt time.Time                     `json:"modifiedAt,omitempty"`
	Messages   []*messageState               `json:"messages,omitempty"`
	Sequence   uint64                        `json:"sequence,omitempty"`
	Dedup      map[string]deduplicationState `json:"dedup,omitempty"`
}

func (s *Server) load() error {
	path := filepath.Join(s.config.StoragePath, "state.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var persisted persistedState
	if err := json.Unmarshal(data, &persisted); err != nil {
		return err
	}
	if persisted.Queues == nil {
		persisted.Queues = map[string]persistedQueue{}
	}
	if persisted.MoveTasks == nil {
		persisted.MoveTasks = map[string]moveTaskState{}
	}
	s.moveTasks = map[string]moveTaskState{}
	for taskHandle, task := range persisted.MoveTasks {
		if taskHandle == "" {
			taskHandle = task.TaskHandle
		}
		if taskHandle == "" {
			continue
		}
		if task.TaskHandle == "" {
			task.TaskHandle = taskHandle
		}
		s.moveTasks[taskHandle] = task
	}
	now := time.Now().UTC()
	for name, persistedQueue := range persisted.Queues {
		if name == "" {
			name = persistedQueue.Name
		}
		if name == "" {
			continue
		}
		queue := &queueState{
			Name:       name,
			URL:        persistedQueue.URL,
			ARN:        persistedQueue.ARN,
			Attributes: copyAttributes(persistedQueue.Attributes),
			Tags:       copyAttributes(persistedQueue.Tags),
			CreatedAt:  persistedQueue.CreatedAt,
			ModifiedAt: persistedQueue.ModifiedAt,
			Messages:   cloneMessages(persistedQueue.Messages),
			Sequence:   persistedQueue.Sequence,
			Dedup:      cloneDeduplication(persistedQueue.Dedup),
		}
		if queue.URL == "" {
			queue.URL = s.queueURL(name)
		}
		if queue.ARN == "" {
			queue.ARN = s.queueARN(name)
		}
		if len(queue.Attributes) == 0 {
			queue.Attributes = s.defaultQueueAttributes()
		}
		if queue.Tags == nil {
			queue.Tags = map[string]string{}
		}
		if queue.CreatedAt.IsZero() {
			queue.CreatedAt = now
		}
		if queue.ModifiedAt.IsZero() {
			queue.ModifiedAt = queue.CreatedAt
		}
		if queue.Dedup == nil {
			queue.Dedup = map[string]deduplicationState{}
		}
		cleanupExpiredMessagesLocked(queue, now)
		s.queues[name] = queue
	}
	return nil
}

func (s *Server) persistLocked() error {
	if strings.TrimSpace(s.config.StoragePath) == "" {
		return nil
	}
	if err := os.MkdirAll(s.config.StoragePath, 0o755); err != nil {
		return err
	}
	persisted := persistedState{
		Queues:    map[string]persistedQueue{},
		MoveTasks: map[string]moveTaskState{},
	}
	for name, queue := range s.queues {
		persisted.Queues[name] = persistedQueue{
			Name:       queue.Name,
			URL:        queue.URL,
			ARN:        queue.ARN,
			Attributes: copyAttributes(queue.Attributes),
			Tags:       copyAttributes(queue.Tags),
			CreatedAt:  queue.CreatedAt,
			ModifiedAt: queueLastModifiedAt(queue),
			Messages:   cloneMessages(queue.Messages),
			Sequence:   queue.Sequence,
			Dedup:      cloneDeduplication(queue.Dedup),
		}
	}
	for taskHandle, task := range s.moveTasks {
		persisted.MoveTasks[taskHandle] = task
	}
	path := filepath.Join(s.config.StoragePath, "state.json")
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(file).Encode(persisted)
	closeErr := file.Close()
	if encodeErr != nil {
		os.Remove(tmpPath)
		return encodeErr
	}
	if closeErr != nil {
		os.Remove(tmpPath)
		return closeErr
	}
	return os.Rename(tmpPath, path)
}

func (s *Server) maxQueues() int {
	if s.config.MaxQueues <= 0 {
		return 256
	}
	return s.config.MaxQueues
}

func (s *Server) maxMessageBytes() int64 {
	if s.config.MaxMessageBytes <= 0 {
		return 1024 * 1024
	}
	return s.config.MaxMessageBytes
}

func (s *Server) maxReceiveBatchSize() int {
	if s.config.MaxReceiveBatchSize <= 0 {
		return 10
	}
	if s.config.MaxReceiveBatchSize > 10 {
		return 10
	}
	return s.config.MaxReceiveBatchSize
}

func (s *Server) defaultVisibilityTimeoutSeconds() int {
	if s.config.DefaultVisibilityTimeoutSeconds < 0 {
		return 30
	}
	if s.config.DefaultVisibilityTimeoutSeconds == 0 {
		return 30
	}
	return s.config.DefaultVisibilityTimeoutSeconds
}

func (s *Server) defaultDelaySeconds() int {
	if s.config.DefaultDelaySeconds < 0 {
		return 0
	}
	return s.config.DefaultDelaySeconds
}

func (s *Server) defaultMessageRetentionSeconds() int {
	if s.config.DefaultMessageRetentionSeconds <= 0 {
		return 345600
	}
	return s.config.DefaultMessageRetentionSeconds
}

func (s *Server) defaultReceiveWaitTimeSeconds() int {
	if s.config.DefaultReceiveWaitTimeSeconds < 0 {
		return 0
	}
	if s.config.DefaultReceiveWaitTimeSeconds > 20 {
		return 20
	}
	return s.config.DefaultReceiveWaitTimeSeconds
}

func cloneQueue(queue *queueState) *queueState {
	return &queueState{
		Name:       queue.Name,
		URL:        queue.URL,
		ARN:        queue.ARN,
		Attributes: copyAttributes(queue.Attributes),
		Tags:       copyAttributes(queue.Tags),
		CreatedAt:  queue.CreatedAt,
		ModifiedAt: queueLastModifiedAt(queue),
	}
}

func queueLastModifiedAt(queue *queueState) time.Time {
	if queue.ModifiedAt.IsZero() {
		return queue.CreatedAt
	}
	return queue.ModifiedAt
}

func copyAttributes(attrs map[string]string) map[string]string {
	copied := make(map[string]string, len(attrs))
	for key, value := range attrs {
		copied[key] = value
	}
	return copied
}
