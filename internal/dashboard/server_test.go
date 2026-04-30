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
	s3svc "devcloud/internal/services/s3"
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

func TestIndexServesServiceLinks(t *testing.T) {
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
		"devcloud Services",
		`href="/mail"`,
		`href="/s3"`,
		"Local service dashboards",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("service index HTML missing %q", want)
		}
	}
}

func TestMailPathServesStaticMailDashboard(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	req := httptest.NewRequest(http.MethodGet, "/mail", nil)
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

func TestS3DashboardPageAndAPIExposeObjects(t *testing.T) {
	s3Store := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := s3Store.CreateBucket(context.Background(), "demo-bucket"); err != nil || !created {
		t.Fatalf("create bucket created=%t err=%v", created, err)
	}
	if _, err := s3Store.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:      "demo-bucket",
		Key:         "docs/readme.txt",
		Body:        strings.NewReader("hello from dashboard\n"),
		ContentType: "text/plain",
		Metadata:    map[string]string{"source": "dashboard-test"},
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}
	if _, err := s3Store.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:      "demo-bucket",
		Key:         "docs/read%2Fme.txt",
		Body:        strings.NewReader("literal percent key\n"),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("put object with escaped-looking key: %v", err)
	}
	routes := NewServer(Config{
		S3Endpoint:    "http://127.0.0.1:4566",
		S3Region:      "us-east-1",
		S3AuthMode:    "relaxed",
		S3StoragePath: ".devcloud/data/s3",
	}, newDashboardStore(nil, nil), s3Store).routes()

	page := performRequest(routes, http.MethodGet, "/s3")
	if page.Code != http.StatusOK {
		t.Fatalf("s3 page status = %d, want %d", page.Code, http.StatusOK)
	}
	if body := page.Body.String(); !strings.Contains(body, "devcloud S3") || !strings.Contains(body, "/api/s3/buckets") {
		t.Fatalf("s3 page missing expected shell: %s", body)
	}

	status := performRequest(routes, http.MethodGet, "/api/s3/status")
	if status.Code != http.StatusOK {
		t.Fatalf("s3 status code = %d, want %d", status.Code, http.StatusOK)
	}
	if !strings.Contains(status.Body.String(), `"running"`) {
		t.Fatalf("s3 status missing running state: %s", status.Body.String())
	}

	buckets := performRequest(routes, http.MethodGet, "/api/s3/buckets")
	if buckets.Code != http.StatusOK {
		t.Fatalf("s3 buckets code = %d, want %d", buckets.Code, http.StatusOK)
	}
	if !strings.Contains(buckets.Body.String(), "demo-bucket") {
		t.Fatalf("s3 buckets missing bucket: %s", buckets.Body.String())
	}

	objects := performRequest(routes, http.MethodGet, "/api/s3/buckets/demo-bucket/objects?prefix=docs/")
	if objects.Code != http.StatusOK {
		t.Fatalf("s3 objects code = %d, want %d", objects.Code, http.StatusOK)
	}
	body := objects.Body.String()
	for _, want := range []string{"docs/readme.txt", `"contentType":"text/plain"`, `"source":"dashboard-test"`, "s3://demo-bucket/docs/readme.txt"} {
		if !strings.Contains(body, want) {
			t.Fatalf("s3 objects missing %q: %s", want, body)
		}
	}
	var objectList struct {
		Objects []struct {
			Key         string `json:"key"`
			DownloadURL string `json:"downloadUrl"`
		} `json:"objects"`
	}
	if err := json.Unmarshal(objects.Body.Bytes(), &objectList); err != nil {
		t.Fatalf("decode object list: %v", err)
	}
	var escapedLookingDownloadURL string
	for _, object := range objectList.Objects {
		if object.Key == "docs/read%2Fme.txt" {
			escapedLookingDownloadURL = object.DownloadURL
		}
	}
	if escapedLookingDownloadURL == "" {
		t.Fatalf("object list missing escaped-looking key: %s", objects.Body.String())
	}

	download := performRequest(routes, http.MethodGet, "/api/s3/buckets/demo-bucket/objects/docs/readme.txt/download")
	if download.Code != http.StatusOK {
		t.Fatalf("s3 download code = %d, want %d", download.Code, http.StatusOK)
	}
	if got := download.Body.String(); got != "hello from dashboard\n" {
		t.Fatalf("s3 download body = %q", got)
	}
	if got := download.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("s3 download Content-Type = %q, want text/plain", got)
	}
	if got := download.Header().Get("x-amz-meta-source"); got != "dashboard-test" {
		t.Fatalf("s3 download metadata = %q, want dashboard-test", got)
	}
	if got := download.Header().Get("Content-Disposition"); got != `attachment; filename="readme.txt"` {
		t.Fatalf("s3 download Content-Disposition = %q", got)
	}

	escapedLookingDownload := performRequest(routes, http.MethodGet, escapedLookingDownloadURL)
	if escapedLookingDownload.Code != http.StatusOK {
		t.Fatalf("escaped-looking key download code = %d, want %d; body=%s", escapedLookingDownload.Code, http.StatusOK, escapedLookingDownload.Body.String())
	}
	if got := escapedLookingDownload.Body.String(); got != "literal percent key\n" {
		t.Fatalf("escaped-looking key download body = %q", got)
	}
}

func TestS3DashboardEscapesDynamicObjectValues(t *testing.T) {
	for _, want := range []string{
		"function escapeHTML(value)",
		"escapeHTML(object.key)",
		"escapeHTML(object.s3Uri)",
		"escapeHTML(metadata[key])",
		"data-index",
	} {
		if !strings.Contains(s3IndexHTML, want) {
			t.Fatalf("s3 dashboard HTML missing %q", want)
		}
	}
	for _, forbidden := range []string{
		`data-bucket="' + bucket.name + '"`,
		`<td class="key">' + object.key + '</td>`,
		`<dt>Key</dt><dd>' + object.key + '</dd>`,
		`metadata[key] + '</dd>`,
	} {
		if strings.Contains(s3IndexHTML, forbidden) {
			t.Fatalf("s3 dashboard HTML still contains unsafe interpolation %q", forbidden)
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
