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

	bigquerysvc "devcloud/internal/services/bigquery"
	dynamodbsvc "devcloud/internal/services/dynamodb"
	"devcloud/internal/services/mail"
	pubsubsvc "devcloud/internal/services/pubsub"
	redshiftsvc "devcloud/internal/services/redshift"
	s3svc "devcloud/internal/services/s3"
	sqssvc "devcloud/internal/services/sqs"
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
		`href="/gcs"`,
		`href="/dynamodb"`,
		`href="/bigquery"`,
		`href="/dashboard/sqs"`,
		`href="/dashboard/pubsub"`,
		"Local service dashboards",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("service index HTML missing %q", want)
		}
	}
}

func TestDashboardServicesAPIListsServiceRegistry(t *testing.T) {
	s3Store := s3svc.NewFileBucketStore(t.TempDir())
	server := NewServer(Config{
		MailEndpoint:        "smtp://127.0.0.1:2525",
		MailStoragePath:     ".devcloud/test/mail",
		S3Endpoint:          "http://127.0.0.1:4567",
		S3StoragePath:       ".devcloud/test/s3",
		GCSEndpoint:         "http://127.0.0.1:4444",
		GCSStoragePath:      ".devcloud/test/s3",
		DynamoDBEndpoint:    "http://127.0.0.1:8001",
		DynamoDBStoragePath: ".devcloud/test/dynamodb",
		BigQueryEndpoint:    "http://127.0.0.1:9051",
		BigQueryStoragePath: ".devcloud/test/bigquery",
		RedshiftAPIEndpoint: "http://127.0.0.1:19099",
		RedshiftStoragePath: ".devcloud/test/redshift",
		SQSEndpoint:         "http://127.0.0.1:9325",
		SQSStoragePath:      ".devcloud/test/sqs",
		PubSubRESTEndpoint:  "http://127.0.0.1:18086",
		PubSubStoragePath:   ".devcloud/test/pubsub",
	}, newDashboardStore(nil, nil), s3Store, s3Store)
	server.SetDynamoDB(dynamodbsvc.NewServer(dynamodbsvc.Config{}))
	server.SetBigQuery(bigquerysvc.NewServer(bigquerysvc.Config{Project: "devcloud"}))
	server.SetSQS(sqssvc.NewServer(sqssvc.Config{}))
	server.SetPubSub(pubsubsvc.NewServer(pubsubsvc.Config{Project: "devcloud"}))
	server.SetRedshift(redshiftsvc.NewServer(redshiftsvc.Config{ClusterIdentifier: "devcloud"}))

	rec := performRequest(server.routes(), http.MethodGet, "/api/dashboard/services")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var response dashboardServicesResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode services response: %v", err)
	}
	if len(response.Services) != 8 {
		t.Fatalf("services len = %d, want 8: %#v", len(response.Services), response.Services)
	}
	assertService(t, response.Services[0], DashboardService{
		ID:          "mail",
		Name:        "Mail",
		Path:        "/mail",
		Status:      "running",
		Endpoint:    "smtp://127.0.0.1:2525",
		StoragePath: ".devcloud/test/mail",
	})
	assertService(t, response.Services[1], DashboardService{
		ID:          "s3",
		Name:        "S3",
		Path:        "/s3",
		Status:      "running",
		Endpoint:    "http://127.0.0.1:4567",
		StoragePath: ".devcloud/test/s3",
	})
	assertService(t, response.Services[2], DashboardService{
		ID:          "gcs",
		Name:        "GCS",
		Path:        "/gcs",
		Status:      "running",
		Endpoint:    "http://127.0.0.1:4444",
		StoragePath: ".devcloud/test/s3",
	})
	assertService(t, response.Services[3], DashboardService{
		ID:          "dynamodb",
		Name:        "DynamoDB",
		Path:        "/dynamodb",
		Status:      "running",
		Endpoint:    "http://127.0.0.1:8001",
		StoragePath: ".devcloud/test/dynamodb",
	})
	assertService(t, response.Services[4], DashboardService{
		ID:          "bigquery",
		Name:        "BigQuery",
		Path:        "/bigquery",
		Status:      "running",
		Endpoint:    "http://127.0.0.1:9051",
		StoragePath: ".devcloud/test/bigquery",
	})
	assertService(t, response.Services[5], DashboardService{
		ID:          "redshift",
		Name:        "Redshift",
		Path:        "/dashboard/redshift",
		Status:      "running",
		Endpoint:    "http://127.0.0.1:19099",
		StoragePath: ".devcloud/test/redshift",
	})
	assertService(t, response.Services[6], DashboardService{
		ID:          "sqs",
		Name:        "SQS",
		Path:        "/dashboard/sqs",
		Status:      "running",
		Endpoint:    "http://127.0.0.1:9325",
		StoragePath: ".devcloud/test/sqs",
	})
	assertService(t, response.Services[7], DashboardService{
		ID:          "pubsub",
		Name:        "Pub/Sub",
		Path:        "/dashboard/pubsub",
		Status:      "running",
		Endpoint:    "http://127.0.0.1:18086",
		StoragePath: ".devcloud/test/pubsub",
	})
}

func TestDashboardServicesAPIMarksDisabledServices(t *testing.T) {
	server := NewServer(Config{MailDisabled: true}, newDashboardStore(nil, nil))

	rec := performRequest(server.routes(), http.MethodGet, "/api/dashboard/services")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var response dashboardServicesResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode services response: %v", err)
	}
	if len(response.Services) != 8 {
		t.Fatalf("services len = %d, want 8: %#v", len(response.Services), response.Services)
	}
	if response.Services[0].ID != "mail" || response.Services[0].Status != "disabled" {
		t.Fatalf("mail service = %#v, want disabled mail", response.Services[0])
	}
	if response.Services[1].ID != "s3" || response.Services[1].Status != "disabled" {
		t.Fatalf("s3 service = %#v, want disabled s3", response.Services[1])
	}
	if response.Services[2].ID != "gcs" || response.Services[2].Status != "disabled" {
		t.Fatalf("gcs service = %#v, want disabled gcs", response.Services[2])
	}
	if response.Services[3].ID != "dynamodb" || response.Services[3].Status != "disabled" {
		t.Fatalf("dynamodb service = %#v, want disabled dynamodb", response.Services[3])
	}
	if response.Services[4].ID != "bigquery" || response.Services[4].Status != "disabled" {
		t.Fatalf("bigquery service = %#v, want disabled bigquery", response.Services[4])
	}
	if response.Services[5].ID != "redshift" || response.Services[5].Status != "disabled" {
		t.Fatalf("redshift service = %#v, want disabled redshift", response.Services[5])
	}
	if response.Services[6].ID != "sqs" || response.Services[6].Status != "disabled" {
		t.Fatalf("sqs service = %#v, want disabled sqs", response.Services[6])
	}
	if response.Services[7].ID != "pubsub" || response.Services[7].Status != "disabled" {
		t.Fatalf("pubsub service = %#v, want disabled pubsub", response.Services[7])
	}
}

func TestDashboardServicesAPIRejectsUnsupportedMethods(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))

	rec := performRequest(server.routes(), http.MethodPost, "/api/dashboard/services")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != "GET" {
		t.Fatalf("Allow = %q, want GET", got)
	}
}

func TestReactDashboardAssetsServeWithoutInterceptingCompatibilityRoutes(t *testing.T) {
	routes := NewServer(Config{}, newDashboardStore(nil, nil)).routes()

	index := performRequest(routes, http.MethodGet, "/dashboard/")
	if index.Code != http.StatusOK {
		t.Fatalf("react dashboard status = %d, want %d", index.Code, http.StatusOK)
	}
	if got := index.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Fatalf("react dashboard Content-Type = %q, want text/html", got)
	}
	if body := index.Body.String(); !strings.Contains(body, "devcloud Dashboard") {
		t.Fatalf("react dashboard index missing title: %s", body)
	}

	nestedRoute := performRequest(routes, http.MethodGet, "/dashboard/mail")
	if nestedRoute.Code != http.StatusOK {
		t.Fatalf("react nested route status = %d, want %d", nestedRoute.Code, http.StatusOK)
	}
	if body := nestedRoute.Body.String(); !strings.Contains(body, "devcloud Dashboard") {
		t.Fatalf("react nested route did not fall back to index: %s", body)
	}
	if got := nestedRoute.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("react nested route Cache-Control = %q, want no-cache", got)
	}

	assetPath := reactAssetPath(t, index.Body.String())
	asset := performRequest(routes, http.MethodGet, assetPath)
	if asset.Code != http.StatusOK {
		t.Fatalf("react asset status = %d, want %d for %s", asset.Code, http.StatusOK, assetPath)
	}
	if got := asset.Header().Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("react asset Cache-Control = %q, want immutable cache", got)
	}
	missingAsset := performRequest(routes, http.MethodGet, "/dashboard/assets/missing.js")
	if missingAsset.Code != http.StatusNotFound {
		t.Fatalf("missing react asset status = %d, want %d", missingAsset.Code, http.StatusNotFound)
	}

	compatMail := performRequest(routes, http.MethodGet, "/mail")
	if compatMail.Code != http.StatusOK || !strings.Contains(compatMail.Body.String(), "devcloud Mail") {
		t.Fatalf("compat mail route changed: status=%d body=%s", compatMail.Code, compatMail.Body.String())
	}

	registry := performRequest(routes, http.MethodGet, "/api/dashboard/services")
	if registry.Code != http.StatusOK {
		t.Fatalf("registry status = %d, want %d", registry.Code, http.StatusOK)
	}
	if got := registry.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("registry Content-Type = %q, want application/json", got)
	}
}

func reactAssetPath(t *testing.T, indexHTML string) string {
	t.Helper()
	marker := `src="/dashboard/`
	start := strings.Index(indexHTML, marker)
	if start == -1 {
		marker = `href="/dashboard/`
		start = strings.Index(indexHTML, marker)
	}
	if start == -1 {
		t.Fatalf("react index missing dashboard asset reference: %s", indexHTML)
	}
	start += len(marker) - len("/dashboard/")
	end := strings.Index(indexHTML[start:], `"`)
	if end == -1 {
		t.Fatalf("react index has unterminated asset reference: %s", indexHTML)
	}
	return indexHTML[start : start+end]
}

func performRequest(handler http.Handler, method string, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func performRequestWithBody(handler http.Handler, method string, target string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func performBigQueryRequest(t *testing.T, server *bigquerysvc.Server, method string, target string, body string) {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s %s status = %d, body = %s", method, target, rec.Code, rec.Body.String())
	}
}

func pubsubRequest(t *testing.T, server *pubsubsvc.Server, method string, target string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	return rec
}

func sqsJSONRequest(t *testing.T, server *sqssvc.Server, operation string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+operation)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	return rec
}

func assertService(t *testing.T, got DashboardService, want DashboardService) {
	t.Helper()
	if got.ID != want.ID || got.Name != want.Name || got.Path != want.Path || got.Status != want.Status || got.Endpoint != want.Endpoint || got.StoragePath != want.StoragePath {
		t.Fatalf("service = %#v, want fields %#v", got, want)
	}
	if got.Description == "" {
		t.Fatalf("service %q description is empty", got.ID)
	}
}
