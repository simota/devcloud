package mailstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"devcloud/internal/services/mail"
	"devcloud/internal/storage/blob"
)

func TestFileStoreAppendListGetRawAndDelete(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFileStore(t.TempDir())
	store := NewFileStore(t.TempDir(), blobStore)
	message := mail.Message{
		ID:         "msg_test",
		From:       "a@example.com",
		To:         []string{"b@example.com"},
		Subject:    "Hello",
		ReceivedAt: time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC),
	}

	if _, err := store.Append(ctx, message, strings.NewReader("Subject: Hello\r\n\r\nBody")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	list, err := store.List(ctx, mail.ListMessagesInput{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(list.Messages) != 1 || list.Messages[0].ID != "msg_test" {
		t.Fatalf("List() messages = %#v", list.Messages)
	}

	rc, ok, err := store.GetRaw(ctx, "msg_test")
	if err != nil {
		t.Fatalf("GetRaw() error = %v", err)
	}
	if !ok {
		t.Fatal("GetRaw() ok = false")
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(raw) != "Subject: Hello\r\n\r\nBody" {
		t.Fatalf("raw = %q", string(raw))
	}

	if err := store.Delete(ctx, "msg_test"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	_, ok, err = store.Get(ctx, "msg_test")
	if err != nil {
		t.Fatalf("Get() after delete error = %v", err)
	}
	if ok {
		t.Fatal("Get() after delete ok = true")
	}
}

func TestFileStoreDeleteAllPreservesTombstones(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFileStore(t.TempDir())
	store := NewFileStore(t.TempDir(), blobStore)
	messages := []mail.Message{
		{
			ID:         "msg_one",
			From:       "a@example.com",
			To:         []string{"one@example.com"},
			Subject:    "One",
			ReceivedAt: time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC),
		},
		{
			ID:         "msg_two",
			From:       "a@example.com",
			To:         []string{"two@example.com"},
			Subject:    "Two",
			ReceivedAt: time.Date(2026, 4, 30, 10, 1, 0, 0, time.UTC),
		},
	}
	for _, message := range messages {
		if _, err := store.Append(ctx, message, strings.NewReader("Subject: "+message.Subject+"\r\n\r\nBody")); err != nil {
			t.Fatalf("Append(%s) error = %v", message.ID, err)
		}
	}

	if err := store.Delete(ctx, "msg_one"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := store.DeleteAll(ctx); err != nil {
		t.Fatalf("DeleteAll() error = %v", err)
	}

	list, err := store.List(ctx, mail.ListMessagesInput{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(list.Messages) != 0 {
		t.Fatalf("List() messages = %#v, want empty", list.Messages)
	}

	data, err := os.ReadFile(store.messagesPath())
	if err != nil {
		t.Fatalf("read messages log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("messages log line count = %d, want 2; data = %q", len(lines), string(data))
	}
	for _, line := range lines {
		var message mail.Message
		if err := json.Unmarshal([]byte(line), &message); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		if message.DeletedAt == nil {
			t.Fatalf("message %s DeletedAt = nil, want tombstone", message.ID)
		}
	}
}

func TestFileStoreConcurrentAppendAndListDoesNotReadPartialMetadata(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFileStore(t.TempDir())
	store := NewFileStore(t.TempDir(), blobStore)

	var wg sync.WaitGroup
	errs := make(chan error, 80)
	for i := 0; i < 40; i++ {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			message := mail.Message{
				ID:         fmt.Sprintf("msg_%02d", i),
				From:       "a@example.com",
				To:         []string{"b@example.com"},
				Subject:    fmt.Sprintf("Hello %02d", i),
				ReceivedAt: time.Date(2026, 4, 30, 10, i, 0, 0, time.UTC),
			}
			if _, err := store.Append(ctx, message, strings.NewReader(fmt.Sprintf("Subject: Hello %02d\r\n\r\nBody", i))); err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := store.List(ctx, mail.ListMessagesInput{}); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent store operation error = %v", err)
	}

	list, err := store.List(ctx, mail.ListMessagesInput{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(list.Messages) != 40 {
		t.Fatalf("List() messages = %d, want 40", len(list.Messages))
	}
}
