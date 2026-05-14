package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"devcloud/internal/services/mail"
)

func TestMailLegacyPathRedirectsToDashboard(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	req := httptest.NewRequest(http.MethodGet, "/mail", nil)
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusMovedPermanently {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMovedPermanently)
	}
	if got := rec.Header().Get("Location"); got != "/dashboard/mail" {
		t.Fatalf("Location = %q, want /dashboard/mail", got)
	}
}

func TestMailDashboardPathServesReactShell(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	req := httptest.NewRequest(http.MethodGet, "/dashboard/mail", nil)
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Fatalf("Content-Type = %q, want text/html", got)
	}
	if body := rec.Body.String(); !strings.Contains(body, "devcloud Dashboard") {
		t.Fatalf("dashboard React shell missing title: %s", body)
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

func TestMessagesAPIEmptyInboxReturnsArrayNotNull(t *testing.T) {
	routes := NewServer(Config{}, newDashboardStore(nil, nil)).routes()

	rec := performRequest(routes, http.MethodGet, "/api/messages")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := strings.TrimSpace(rec.Body.String())
	if !strings.Contains(body, `"messages":[]`) {
		t.Fatalf("empty inbox response = %s, want messages serialized as []", body)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if string(raw["messages"]) != "[]" {
		t.Fatalf(`messages field = %s, want "[]"`, raw["messages"])
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
