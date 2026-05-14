package mailstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"devcloud/internal/services/mail"
	"devcloud/internal/storage/blob"
)

type Store interface {
	Append(ctx context.Context, message mail.Message, raw io.Reader) (mail.Message, error)
	List(ctx context.Context, input mail.ListMessagesInput) (mail.ListMessagesResult, error)
	Get(ctx context.Context, id string) (mail.Message, bool, error)
	GetRaw(ctx context.Context, id string) (io.ReadCloser, bool, error)
	Delete(ctx context.Context, id string) error
	DeleteAll(ctx context.Context) error
}

type FileStore struct {
	root  string
	blobs blob.Store
	mu    sync.Mutex
}

func NewFileStore(root string, blobs blob.Store) *FileStore {
	return &FileStore{root: root, blobs: blobs}
}

func (s *FileStore) Append(ctx context.Context, message mail.Message, raw io.Reader) (mail.Message, error) {
	if err := ctx.Err(); err != nil {
		return mail.Message{}, err
	}
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return mail.Message{}, fmt.Errorf("create mail store: %w", err)
	}
	rawID, err := s.blobs.Put(ctx, raw)
	if err != nil {
		return mail.Message{}, err
	}
	message.Raw = rawID
	if message.ReceivedAt.IsZero() {
		message.ReceivedAt = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.OpenFile(s.messagesPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return mail.Message{}, fmt.Errorf("open messages log: %w", err)
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(message); err != nil {
		return mail.Message{}, fmt.Errorf("append message metadata: %w", err)
	}
	return message, nil
}

func (s *FileStore) List(ctx context.Context, input mail.ListMessagesInput) (mail.ListMessagesResult, error) {
	messages, err := s.load(ctx)
	if err != nil {
		return mail.ListMessagesResult{}, err
	}
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].ReceivedAt.After(messages[j].ReceivedAt)
	})
	if input.Limit <= 0 || input.Limit > 100 {
		input.Limit = 100
	}
	if len(messages) > input.Limit {
		messages = messages[:input.Limit]
	}
	return mail.ListMessagesResult{Messages: messages}, nil
}

func (s *FileStore) Get(ctx context.Context, id string) (mail.Message, bool, error) {
	messages, err := s.load(ctx)
	if err != nil {
		return mail.Message{}, false, err
	}
	for _, message := range messages {
		if message.ID == id && message.DeletedAt == nil {
			return message, true, nil
		}
	}
	return mail.Message{}, false, nil
}

func (s *FileStore) GetRaw(ctx context.Context, id string) (io.ReadCloser, bool, error) {
	message, ok, err := s.Get(ctx, id)
	if err != nil || !ok {
		return nil, ok, err
	}
	return s.blobs.Get(ctx, message.Raw)
}

func (s *FileStore) Delete(ctx context.Context, id string) error {
	return s.rewrite(ctx, func(message mail.Message) mail.Message {
		if message.ID == id && message.DeletedAt == nil {
			now := time.Now().UTC()
			message.DeletedAt = &now
		}
		return message
	})
}

func (s *FileStore) DeleteAll(ctx context.Context) error {
	return s.rewrite(ctx, func(message mail.Message) mail.Message {
		if message.DeletedAt == nil {
			now := time.Now().UTC()
			message.DeletedAt = &now
		}
		return message
	})
}

func (s *FileStore) load(ctx context.Context) ([]mail.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	messages, err := s.loadAllLocked(ctx)
	if err != nil {
		return nil, err
	}
	active := messages[:0]
	for _, message := range messages {
		if message.DeletedAt == nil {
			active = append(active, message)
		}
	}
	return active, nil
}

func (s *FileStore) loadAll(ctx context.Context) ([]mail.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadAllLocked(ctx)
}

func (s *FileStore) loadAllLocked(ctx context.Context) ([]mail.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(s.messagesPath())
	if err != nil {
		if os.IsNotExist(err) {
			return []mail.Message{}, nil
		}
		return nil, fmt.Errorf("open messages log: %w", err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	messages := make([]mail.Message, 0)
	for {
		var message mail.Message
		if err := dec.Decode(&message); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decode message metadata: %w", err)
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func (s *FileStore) rewrite(ctx context.Context, mutate func(mail.Message) mail.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	messages, err := s.loadAllLocked(ctx)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return fmt.Errorf("create mail store: %w", err)
	}

	tmp, err := os.CreateTemp(s.root, "messages-*.jsonl")
	if err != nil {
		return fmt.Errorf("create messages temp log: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	enc := json.NewEncoder(tmp)
	for _, message := range messages {
		if err := enc.Encode(mutate(message)); err != nil {
			tmp.Close()
			return fmt.Errorf("write messages temp log: %w", err)
		}
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close messages temp log: %w", err)
	}
	if err := os.Rename(tmpName, s.messagesPath()); err != nil {
		return fmt.Errorf("replace messages log: %w", err)
	}
	return nil
}

func (s *FileStore) messagesPath() string {
	return filepath.Join(s.root, "messages.jsonl")
}
