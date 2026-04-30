package mail

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

type Store interface {
	Append(ctx context.Context, message Message, raw io.Reader) (Message, error)
	List(ctx context.Context, input ListMessagesInput) (ListMessagesResult, error)
	Get(ctx context.Context, id string) (Message, bool, error)
	GetRaw(ctx context.Context, id string) (io.ReadCloser, bool, error)
	Delete(ctx context.Context, id string) error
	DeleteAll(ctx context.Context) error
}

type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

func (s *Service) Receive(ctx context.Context, envelope Envelope, raw io.Reader) (Message, error) {
	data, err := io.ReadAll(raw)
	if err != nil {
		return Message{}, fmt.Errorf("read raw message: %w", err)
	}

	msg := ParseMessage(data, envelope)
	msg.ID = newMessageID()
	msg.ReceivedAt = time.Now().UTC()
	return s.store.Append(ctx, msg, bytes.NewReader(data))
}

func newMessageID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("msg_%d", time.Now().UnixNano())
	}
	return "msg_" + hex.EncodeToString(b[:])
}
