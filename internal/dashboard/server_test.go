package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"devcloud/internal/services/mail"
)

type dashboardStore struct {
	mu       sync.Mutex
	messages map[string]mail.Message
	raw      map[string]string
	deleted  map[string]bool
}

func newDashboardStore(messages []mail.Message, raw map[string]string) *dashboardStore {
	store := &dashboardStore{
		messages: map[string]mail.Message{},
		raw:      map[string]string{},
		deleted:  map[string]bool{},
	}
	for _, message := range messages {
		store.messages[message.ID] = message
	}
	for id, value := range raw {
		store.raw[id] = value
	}
	return store
}

func (s *dashboardStore) Append(_ context.Context, message mail.Message, raw io.Reader) (mail.Message, error) {
	data, err := io.ReadAll(raw)
	if err != nil {
		return mail.Message{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages[message.ID] = message
	s.raw[message.ID] = string(data)
	return message, nil
}

func (s *dashboardStore) List(context.Context, mail.ListMessagesInput) (mail.ListMessagesResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := mail.ListMessagesResult{}
	for _, message := range s.messages {
		if !s.deleted[message.ID] {
			result.Messages = append(result.Messages, message)
		}
	}
	return result, nil
}

func (s *dashboardStore) Get(_ context.Context, id string) (mail.Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleted[id] {
		return mail.Message{}, false, nil
	}
	message, ok := s.messages[id]
	return message, ok, nil
}

func (s *dashboardStore) GetRaw(_ context.Context, id string) (io.ReadCloser, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleted[id] {
		return nil, false, nil
	}
	value, ok := s.raw[id]
	if !ok {
		return nil, false, nil
	}
	return io.NopCloser(strings.NewReader(value)), true, nil
}

func (s *dashboardStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted[id] = true
	return nil
}

func (s *dashboardStore) DeleteAll(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.messages {
		s.deleted[id] = true
	}
	return nil
}

func TestIndexServesStaticMailDashboard(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Fatalf("Content-Type = %q, want text/html", got)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"devcloud Mail",
		"smtp://localhost:1025",
		`fetch("/api/messages"`,
		`data-tab="raw"`,
		"Storage: .devcloud/data",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard HTML missing %q", want)
		}
	}
	for _, forbidden := range []string{"react", "vite", "tailwind"} {
		if strings.Contains(strings.ToLower(body), forbidden) {
			t.Fatalf("dashboard HTML unexpectedly contains %q", forbidden)
		}
	}
}

func TestMessagesAPIListsDetailsRawAndDeletesMessages(t *testing.T) {
	message := mail.Message{
		ID:         "msg_test",
		From:       "sender@example.com",
		To:         []string{"user@example.com"},
		Subject:    "Hello",
		Headers:    map[string][]string{"Subject": {"Hello"}},
		TextBody:   "body\r\n",
		ReceivedAt: time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC),
	}
	store := newDashboardStore([]mail.Message{message}, map[string]string{
		"msg_test": "Subject: Hello\r\n\r\nbody\r\n",
	})
	routes := NewServer(Config{}, store).routes()

	listRec := performRequest(routes, http.MethodGet, "/api/messages")
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", listRec.Code, http.StatusOK)
	}
	var list mail.ListMessagesResult
	if err := json.NewDecoder(listRec.Body).Decode(&list); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(list.Messages) != 1 || list.Messages[0].ID != "msg_test" {
		t.Fatalf("list messages = %#v", list.Messages)
	}

	detailRec := performRequest(routes, http.MethodGet, "/api/messages/msg_test")
	if detailRec.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d", detailRec.Code, http.StatusOK)
	}
	var detail mail.Message
	if err := json.NewDecoder(detailRec.Body).Decode(&detail); err != nil {
		t.Fatalf("decode detail response: %v", err)
	}
	if detail.Subject != "Hello" || detail.TextBody != "body\r\n" {
		t.Fatalf("detail = %#v", detail)
	}

	rawRec := performRequest(routes, http.MethodGet, "/api/messages/msg_test/raw")
	if rawRec.Code != http.StatusOK {
		t.Fatalf("raw status = %d, want %d", rawRec.Code, http.StatusOK)
	}
	if got := rawRec.Header().Get("Content-Type"); got != "message/rfc822" {
		t.Fatalf("raw Content-Type = %q, want message/rfc822", got)
	}
	if rawRec.Body.String() != "Subject: Hello\r\n\r\nbody\r\n" {
		t.Fatalf("raw body = %q", rawRec.Body.String())
	}

	deleteRec := performRequest(routes, http.MethodDelete, "/api/messages/msg_test")
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d", deleteRec.Code, http.StatusNoContent)
	}
	missingRec := performRequest(routes, http.MethodGet, "/api/messages/msg_test")
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("deleted detail status = %d, want %d", missingRec.Code, http.StatusNotFound)
	}
}

func TestMessagesAPIDeletesAllMessages(t *testing.T) {
	messages := []mail.Message{
		{
			ID:         "msg_one",
			From:       "sender@example.com",
			To:         []string{"one@example.com"},
			Subject:    "One",
			ReceivedAt: time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC),
		},
		{
			ID:         "msg_two",
			From:       "sender@example.com",
			To:         []string{"two@example.com"},
			Subject:    "Two",
			ReceivedAt: time.Date(2026, 4, 30, 10, 1, 0, 0, time.UTC),
		},
	}
	store := newDashboardStore(messages, map[string]string{
		"msg_one": "Subject: One\r\n\r\nbody\r\n",
		"msg_two": "Subject: Two\r\n\r\nbody\r\n",
	})
	routes := NewServer(Config{}, store).routes()

	deleteRec := performRequest(routes, http.MethodDelete, "/api/messages")
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("delete all status = %d, want %d", deleteRec.Code, http.StatusNoContent)
	}

	listRec := performRequest(routes, http.MethodGet, "/api/messages")
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", listRec.Code, http.StatusOK)
	}
	var list mail.ListMessagesResult
	if err := json.NewDecoder(listRec.Body).Decode(&list); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(list.Messages) != 0 {
		t.Fatalf("list messages = %#v, want empty", list.Messages)
	}
	if rec := performRequest(routes, http.MethodGet, "/api/messages/msg_one/raw"); rec.Code != http.StatusNotFound {
		t.Fatalf("deleted raw status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestMessagesAPIHandlesMissingRawAndUnsupportedMethods(t *testing.T) {
	routes := NewServer(Config{}, newDashboardStore(nil, nil)).routes()

	if rec := performRequest(routes, http.MethodGet, "/api/messages/missing/raw"); rec.Code != http.StatusNotFound {
		t.Fatalf("missing raw status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if rec := performRequest(routes, http.MethodGet, "/api/messages/missing/raw/extra"); rec.Code != http.StatusNotFound {
		t.Fatalf("malformed raw status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if rec := performRequest(routes, http.MethodPost, "/api/messages"); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, DELETE" {
		t.Fatalf("POST /api/messages status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if rec := performRequest(routes, http.MethodDelete, "/api/messages/missing/raw"); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET" {
		t.Fatalf("DELETE raw status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if rec := performRequest(routes, http.MethodPatch, "/api/messages/missing"); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, DELETE" {
		t.Fatalf("PATCH detail status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func performRequest(handler http.Handler, method string, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}
