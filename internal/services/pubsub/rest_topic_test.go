package pubsub

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRESTListsEmptyTopics(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/devcloud/topics", nil)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var body struct {
		Topics []any `json:"topics"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Topics == nil || len(body.Topics) != 0 {
		t.Fatalf("topics = %#v, want empty array", body.Topics)
	}
}

func TestRESTRejectsUnsupportedTopicMethod(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/devcloud/topics", nil)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != "GET" {
		t.Fatalf("Allow = %q, want GET", got)
	}
}

func TestRESTAuthModes(t *testing.T) {
	relaxed := NewServer(Config{Project: "devcloud", AuthMode: "relaxed"})
	if rec := performPubSubRequest(relaxed, http.MethodGet, "/v1/projects/devcloud/topics", ""); rec.Code != http.StatusOK {
		t.Fatalf("relaxed status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	oauthRelaxed := NewServer(Config{Project: "devcloud", AuthMode: "oauth-relaxed"})
	if rec := performPubSubRequest(oauthRelaxed, http.MethodGet, "/v1/projects/devcloud/topics", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("oauth-relaxed missing bearer status = %d, want %d: %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/devcloud/topics", nil)
	req.Header.Set("Authorization", "Bearer local-token")
	oauthAuthorized := httptest.NewRecorder()
	oauthRelaxed.ServeHTTP(oauthAuthorized, req)
	if oauthAuthorized.Code != http.StatusOK {
		t.Fatalf("oauth-relaxed bearer status = %d, want %d: %s", oauthAuthorized.Code, http.StatusOK, oauthAuthorized.Body.String())
	}

	strict := NewServer(Config{Project: "devcloud", AuthMode: "strict", BearerToken: "expected-token"})
	wrongReq := httptest.NewRequest(http.MethodGet, "/v1/projects/devcloud/topics", nil)
	wrongReq.Header.Set("Authorization", "Bearer wrong-token")
	wrongToken := httptest.NewRecorder()
	strict.ServeHTTP(wrongToken, wrongReq)
	if wrongToken.Code != http.StatusUnauthorized {
		t.Fatalf("strict wrong token status = %d, want %d: %s", wrongToken.Code, http.StatusUnauthorized, wrongToken.Body.String())
	}
	if strings.Contains(wrongToken.Body.String(), "expected-token") || strings.Contains(wrongToken.Body.String(), "wrong-token") {
		t.Fatalf("auth error leaked token material: %s", wrongToken.Body.String())
	}
	if got := wrongToken.Header().Get("WWW-Authenticate"); got != `Bearer realm="devcloud-pubsub"` {
		t.Fatalf("WWW-Authenticate = %q", got)
	}

	authorizedReq := httptest.NewRequest(http.MethodGet, "/v1/projects/devcloud/topics", nil)
	authorizedReq.Header.Set("Authorization", "Bearer expected-token")
	authorized := httptest.NewRecorder()
	strict.ServeHTTP(authorized, authorizedReq)
	if authorized.Code != http.StatusOK {
		t.Fatalf("strict authorized status = %d, want %d: %s", authorized.Code, http.StatusOK, authorized.Body.String())
	}
}

func TestRESTRejectsInvalidProjectNames(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "list topics", method: http.MethodGet, path: "/v1/projects/bad!/topics"},
		{name: "get topic", method: http.MethodGet, path: "/v1/projects/bad!/topics/orders"},
		{name: "topic subscriptions", method: http.MethodGet, path: "/v1/projects/bad!/topics/orders/subscriptions"},
		{name: "list subscriptions", method: http.MethodGet, path: "/v1/projects/bad!/subscriptions"},
		{name: "get subscription", method: http.MethodGet, path: "/v1/projects/bad!/subscriptions/orders-sub"},
		{name: "pull", method: http.MethodPost, path: "/v1/projects/bad!/subscriptions/orders-sub:pull", body: `{"maxMessages":1}`},
		{name: "acknowledge", method: http.MethodPost, path: "/v1/projects/bad!/subscriptions/orders-sub:acknowledge", body: `{"ackIds":["ack-1"]}`},
		{name: "modify ack deadline", method: http.MethodPost, path: "/v1/projects/bad!/subscriptions/orders-sub:modifyAckDeadline", body: `{"ackIds":["ack-1"],"ackDeadlineSeconds":0}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := performPubSubRequest(server, tt.method, tt.path, tt.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "INVALID_ARGUMENT") {
				t.Fatalf("error body = %s", rec.Body.String())
			}
		})
	}
}

func TestRESTTopicCRUD(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var topic topicResource
	if err := json.NewDecoder(create.Body).Decode(&topic); err != nil {
		t.Fatalf("decode topic: %v", err)
	}
	if topic.Name != "projects/devcloud/topics/orders" {
		t.Fatalf("topic name = %q", topic.Name)
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", get.Code, http.StatusOK)
	}

	list := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", list.Code, http.StatusOK)
	}
	var listed struct {
		Topics []topicResource `json:"topics"`
	}
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode topics: %v", err)
	}
	if len(listed.Topics) != 1 || listed.Topics[0].Name != topic.Name {
		t.Fatalf("topics = %#v", listed.Topics)
	}
}

func TestRESTResourcePathsDecodeEscapedNames(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	createTopic := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders%2Bpriority", "{}")
	if createTopic.Code != http.StatusOK {
		t.Fatalf("create topic status = %d, want %d: %s", createTopic.Code, http.StatusOK, createTopic.Body.String())
	}
	var topic topicResource
	if err := json.NewDecoder(createTopic.Body).Decode(&topic); err != nil {
		t.Fatalf("decode topic: %v", err)
	}
	if topic.Name != "projects/devcloud/topics/orders+priority" {
		t.Fatalf("topic name = %q", topic.Name)
	}

	createSubscription := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders%2Bpriority-sub", `{
		"topic":"projects/devcloud/topics/orders+priority"
	}`)
	if createSubscription.Code != http.StatusOK {
		t.Fatalf("create subscription status = %d, want %d: %s", createSubscription.Code, http.StatusOK, createSubscription.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(createSubscription.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if subscription.Name != "projects/devcloud/subscriptions/orders+priority-sub" || subscription.Topic != topic.Name {
		t.Fatalf("subscription = %#v", subscription)
	}

	getTopic := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders%2Bpriority", "")
	if getTopic.Code != http.StatusOK {
		t.Fatalf("get topic status = %d, want %d: %s", getTopic.Code, http.StatusOK, getTopic.Body.String())
	}
}

func TestRESTTopicMetadataAndPatch(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", `{
		"labels":{"env":"local"},
		"messageRetentionDuration":"600s",
		"schemaSettings":{"schema":"projects/devcloud/schemas/order-event","encoding":"JSON"},
		"kmsKeyName":"projects/devcloud/locations/global/keyRings/local/cryptoKeys/orders"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var created topicResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode created topic: %v", err)
	}
	if created.Labels["env"] != "local" || created.MessageRetentionDuration != "600s" || created.SchemaSettings["encoding"] != "JSON" || created.KMSKeyName == "" {
		t.Fatalf("created topic metadata = %#v", created)
	}

	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/topics/orders?updateMask=labels,message_retention_duration,schema_settings", `{
		"labels":{"env":"test","owner":"pubsub"},
		"messageRetentionDuration":"1200s",
		"schemaSettings":{"schema":"projects/devcloud/schemas/order-event","encoding":"BINARY"}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusOK, patch.Body.String())
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var topic topicResource
	if err := json.NewDecoder(get.Body).Decode(&topic); err != nil {
		t.Fatalf("decode patched topic: %v", err)
	}
	if topic.Labels["env"] != "test" || topic.Labels["owner"] != "pubsub" || topic.MessageRetentionDuration != "1200s" || topic.SchemaSettings["encoding"] != "BINARY" {
		t.Fatalf("patched topic metadata = %#v", topic)
	}
	if topic.KMSKeyName == "" {
		t.Fatalf("patch without kmsKeyName mask should preserve kmsKeyName: %#v", topic)
	}
}

func TestRESTTopicTimestampsPersistAndPatchUpdatesOnlyUpdatedAt(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	server.now = func() time.Time { return now }

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var created topicResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode created topic: %v", err)
	}
	if created.CreatedAt != "2026-05-02T12:00:00Z" || created.UpdatedAt != created.CreatedAt {
		t.Fatalf("created timestamps = createdAt %q updatedAt %q", created.CreatedAt, created.UpdatedAt)
	}

	now = now.Add(5 * time.Minute)
	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/topics/orders?updateMask=labels", `{
		"labels":{"env":"test"}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusOK, patch.Body.String())
	}
	var patched topicResource
	if err := json.NewDecoder(patch.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patched topic: %v", err)
	}
	if patched.CreatedAt != created.CreatedAt || patched.UpdatedAt != "2026-05-02T12:05:00Z" {
		t.Fatalf("patched timestamps = createdAt %q updatedAt %q", patched.CreatedAt, patched.UpdatedAt)
	}

	reloaded := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	get := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/topics/orders", "")
	if get.Code != http.StatusOK {
		t.Fatalf("reloaded get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var persisted topicResource
	if err := json.NewDecoder(get.Body).Decode(&persisted); err != nil {
		t.Fatalf("decode persisted topic: %v", err)
	}
	if persisted.CreatedAt != created.CreatedAt || persisted.UpdatedAt != patched.UpdatedAt {
		t.Fatalf("persisted timestamps = createdAt %q updatedAt %q", persisted.CreatedAt, persisted.UpdatedAt)
	}
	snapshot := reloaded.Snapshot()
	if len(snapshot.Topics) != 1 || snapshot.Topics[0].CreatedAt != created.CreatedAt || snapshot.Topics[0].UpdatedAt != patched.UpdatedAt {
		t.Fatalf("snapshot topics = %#v", snapshot.Topics)
	}
}

func TestRESTRejectsInvalidTopicMetadata(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	invalidRetention := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", `{
		"messageRetentionDuration":"soon"
	}`)
	if invalidRetention.Code != http.StatusBadRequest {
		t.Fatalf("invalid retention status = %d, want %d: %s", invalidRetention.Code, http.StatusBadRequest, invalidRetention.Body.String())
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	invalidSchema := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/topics/orders", `{
		"schemaSettings":{"schema":"projects/devcloud/topics/not-a-schema"}
	}`)
	if invalidSchema.Code != http.StatusBadRequest {
		t.Fatalf("invalid schema status = %d, want %d: %s", invalidSchema.Code, http.StatusBadRequest, invalidSchema.Body.String())
	}

	unsupportedMask := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/topics/orders?updateMask=expirationPolicy", `{
		"labels":{"env":"test"}
	}`)
	if unsupportedMask.Code != http.StatusBadRequest {
		t.Fatalf("unsupported mask status = %d, want %d: %s", unsupportedMask.Code, http.StatusBadRequest, unsupportedMask.Body.String())
	}
}

func TestRESTListTopicsPaginates(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/alpha", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/bravo", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/charlie", "{}")

	firstPage := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics?pageSize=2", "")
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d: %s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first struct {
		Topics        []topicResource `json:"topics"`
		NextPageToken string          `json:"nextPageToken"`
	}
	if err := json.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(first.Topics) != 2 || first.Topics[0].Name != "projects/devcloud/topics/alpha" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}

	secondPage := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics?pageSize=2&pageToken="+first.NextPageToken, "")
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, want %d: %s", secondPage.Code, http.StatusOK, secondPage.Body.String())
	}
	var second struct {
		Topics        []topicResource `json:"topics"`
		NextPageToken string          `json:"nextPageToken"`
	}
	if err := json.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(second.Topics) != 1 || second.Topics[0].Name != "projects/devcloud/topics/charlie" || second.NextPageToken != "" {
		t.Fatalf("second page = %#v", second)
	}
}

func TestRESTListTopicsRejectsInvalidPagination(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	rec := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics?pageToken=not-an-offset", "")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "INVALID_ARGUMENT") {
		t.Fatalf("error body = %s", rec.Body.String())
	}
}

func TestRESTRejectsDuplicateTopicCreate(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	first := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	if first.Code != http.StatusOK {
		t.Fatalf("first create status = %d, want %d: %s", first.Code, http.StatusOK, first.Body.String())
	}
	duplicate := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate create status = %d, want %d: %s", duplicate.Code, http.StatusConflict, duplicate.Body.String())
	}
	if !strings.Contains(duplicate.Body.String(), "ALREADY_EXISTS") {
		t.Fatalf("duplicate error body = %s", duplicate.Body.String())
	}
}
