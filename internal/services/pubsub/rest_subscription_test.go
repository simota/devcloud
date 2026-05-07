package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRESTSubscriptionCRUDAndTopicSubscriptions(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders-dlq", "{}")

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"labels":{"env":"local"},
		"ackDeadlineSeconds":2,
		"enableMessageOrdering":true,
		"enableExactlyOnceDelivery":true,
		"retainAckedMessages":true,
		"messageRetentionDuration":"1200s",
		"expirationPolicy":{"ttl":"86400s"},
		"filter":"attributes.kind=\"test\"",
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":5},
		"retryPolicy":{"minimumBackoff":"1s","maximumBackoff":"10s"},
		"pushConfig":{"pushEndpoint":"http://127.0.0.1:65535/push"}
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(create.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if subscription.Name != "projects/devcloud/subscriptions/orders-sub" || subscription.Topic != "projects/devcloud/topics/orders" {
		t.Fatalf("subscription = %#v", subscription)
	}
	if subscription.Labels["env"] != "local" {
		t.Fatalf("subscription labels = %#v", subscription.Labels)
	}
	if !subscription.EnableMessageOrdering || !subscription.EnableExactlyOnceDelivery || !subscription.RetainAckedMessages || subscription.MessageRetentionDuration != "1200s" || subscription.ExpirationPolicy == nil || subscription.DeadLetterPolicy == nil || subscription.RetryPolicy == nil || subscription.PushConfig == nil {
		t.Fatalf("advanced metadata not preserved: %#v", subscription)
	}
	if subscription.Filter != `attributes.kind="test"` {
		t.Fatalf("filter = %q, want attributes.kind=\"test\"", subscription.Filter)
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", get.Code, http.StatusOK)
	}

	list := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", list.Code, http.StatusOK)
	}
	var listed struct {
		Subscriptions []subscriptionResource `json:"subscriptions"`
	}
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode subscriptions: %v", err)
	}
	if len(listed.Subscriptions) != 1 || listed.Subscriptions[0].Name != subscription.Name {
		t.Fatalf("subscriptions = %#v", listed.Subscriptions)
	}

	topicSubscriptions := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders/subscriptions", "")
	if topicSubscriptions.Code != http.StatusOK {
		t.Fatalf("topic subscriptions status = %d, want %d", topicSubscriptions.Code, http.StatusOK)
	}
	var topicSubs struct {
		Subscriptions []string `json:"subscriptions"`
	}
	if err := json.NewDecoder(topicSubscriptions.Body).Decode(&topicSubs); err != nil {
		t.Fatalf("decode topic subscriptions: %v", err)
	}
	if len(topicSubs.Subscriptions) != 1 || topicSubs.Subscriptions[0] != subscription.Name {
		t.Fatalf("topic subscriptions = %#v", topicSubs.Subscriptions)
	}

	blockedDelete := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/topics/orders", "")
	if blockedDelete.Code != http.StatusBadRequest {
		t.Fatalf("blocked topic delete status = %d, want %d", blockedDelete.Code, http.StatusBadRequest)
	}

	deleteSubscription := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if deleteSubscription.Code != http.StatusNoContent {
		t.Fatalf("delete subscription status = %d, want %d", deleteSubscription.Code, http.StatusNoContent)
	}
	deleteTopic := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/topics/orders", "")
	if deleteTopic.Code != http.StatusNoContent {
		t.Fatalf("delete topic status = %d, want %d", deleteTopic.Code, http.StatusNoContent)
	}
}

func TestRESTSubscriptionTimestampsPersistAndPatchUpdatesOnlyUpdatedAt(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	var created subscriptionResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode created subscription: %v", err)
	}
	if created.CreatedAt != "2026-05-02T12:00:00Z" || created.UpdatedAt != created.CreatedAt {
		t.Fatalf("created timestamps = createdAt %q updatedAt %q", created.CreatedAt, created.UpdatedAt)
	}

	now = now.Add(5 * time.Minute)
	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=labels", `{
		"labels":{"env":"test"}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusOK, patch.Body.String())
	}
	var patched subscriptionResource
	if err := json.NewDecoder(patch.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patched subscription: %v", err)
	}
	if patched.CreatedAt != created.CreatedAt || patched.UpdatedAt != "2026-05-02T12:05:00Z" {
		t.Fatalf("patched timestamps = createdAt %q updatedAt %q", patched.CreatedAt, patched.UpdatedAt)
	}

	reloaded := NewServer(Config{Project: "devcloud", StoragePath: storagePath})
	get := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if get.Code != http.StatusOK {
		t.Fatalf("reloaded get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var persisted subscriptionResource
	if err := json.NewDecoder(get.Body).Decode(&persisted); err != nil {
		t.Fatalf("decode persisted subscription: %v", err)
	}
	if persisted.CreatedAt != created.CreatedAt || persisted.UpdatedAt != patched.UpdatedAt {
		t.Fatalf("persisted timestamps = createdAt %q updatedAt %q", persisted.CreatedAt, persisted.UpdatedAt)
	}
	snapshot := reloaded.Snapshot()
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].CreatedAt != created.CreatedAt || snapshot.Subscriptions[0].UpdatedAt != patched.UpdatedAt {
		t.Fatalf("snapshot subscriptions = %#v", snapshot.Subscriptions)
	}
}

func TestRESTListSubscriptionsPaginates(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-a", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-b", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-c", `{"topic":"projects/devcloud/topics/orders"}`)

	firstPage := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions?pageSize=2", "")
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d: %s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first struct {
		Subscriptions []subscriptionResource `json:"subscriptions"`
		NextPageToken string                 `json:"nextPageToken"`
	}
	if err := json.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(first.Subscriptions) != 2 || first.Subscriptions[0].Name != "projects/devcloud/subscriptions/orders-a" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}

	topicPage := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders/subscriptions?pageSize=1&pageToken=1", "")
	if topicPage.Code != http.StatusOK {
		t.Fatalf("topic page status = %d, want %d: %s", topicPage.Code, http.StatusOK, topicPage.Body.String())
	}
	var topicSubscriptions struct {
		Subscriptions []string `json:"subscriptions"`
		NextPageToken string   `json:"nextPageToken"`
	}
	if err := json.NewDecoder(topicPage.Body).Decode(&topicSubscriptions); err != nil {
		t.Fatalf("decode topic subscriptions: %v", err)
	}
	if len(topicSubscriptions.Subscriptions) != 1 || topicSubscriptions.Subscriptions[0] != "projects/devcloud/subscriptions/orders-b" || topicSubscriptions.NextPageToken == "" {
		t.Fatalf("topic subscriptions page = %#v", topicSubscriptions)
	}
}

func TestRESTRejectsDuplicateSubscriptionCreate(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	body := `{"topic":"projects/devcloud/topics/orders"}`

	first := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", body)
	if first.Code != http.StatusOK {
		t.Fatalf("first create status = %d, want %d: %s", first.Code, http.StatusOK, first.Body.String())
	}
	duplicate := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", body)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate create status = %d, want %d: %s", duplicate.Code, http.StatusConflict, duplicate.Body.String())
	}
	if !strings.Contains(duplicate.Body.String(), "ALREADY_EXISTS") {
		t.Fatalf("duplicate error body = %s", duplicate.Body.String())
	}
}

func TestRESTPatchSubscriptionUpdatesMetadata(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     60,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders-dlq", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2
	}`)

	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=labels,ackDeadlineSeconds,enableMessageOrdering,enableExactlyOnceDelivery,retainAckedMessages,messageRetentionDuration,expirationPolicy,filter,deadLetterPolicy,retryPolicy,pushConfig", `{
		"name":"projects/devcloud/subscriptions/orders-sub",
		"labels":{"env":"test","owner":"pubsub"},
		"ackDeadlineSeconds":30,
		"enableMessageOrdering":true,
		"enableExactlyOnceDelivery":true,
		"retainAckedMessages":true,
		"messageRetentionDuration":"1800s",
		"expirationPolicy":{"ttl":"172800s"},
		"filter":"attributes.kind=\"priority\"",
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":5},
		"retryPolicy":{"minimumBackoff":"2s","maximumBackoff":"20s"},
		"pushConfig":{"pushEndpoint":"http://127.0.0.1:65535/push"}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusOK, patch.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(patch.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if subscription.AckDeadlineSeconds != 30 || !subscription.EnableMessageOrdering || !subscription.EnableExactlyOnceDelivery || !subscription.RetainAckedMessages || subscription.MessageRetentionDuration != "1800s" || subscription.Filter != `attributes.kind="priority"` {
		t.Fatalf("subscription patch did not update core fields: %#v", subscription)
	}
	if subscription.Labels["env"] != "test" || subscription.Labels["owner"] != "pubsub" {
		t.Fatalf("subscription patch did not update labels: %#v", subscription.Labels)
	}
	if subscription.ExpirationPolicy == nil || subscription.DeadLetterPolicy == nil || subscription.RetryPolicy == nil || subscription.PushConfig == nil {
		t.Fatalf("subscription patch did not preserve advanced metadata: %#v", subscription)
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var persisted subscriptionResource
	if err := json.NewDecoder(get.Body).Decode(&persisted); err != nil {
		t.Fatalf("decode persisted subscription: %v", err)
	}
	if persisted.Labels["owner"] != "pubsub" || persisted.AckDeadlineSeconds != 30 || !persisted.EnableMessageOrdering || !persisted.EnableExactlyOnceDelivery || !persisted.RetainAckedMessages || persisted.MessageRetentionDuration != "1800s" || persisted.ExpirationPolicy == nil || persisted.Filter != `attributes.kind="priority"` || persisted.PushConfig == nil {
		t.Fatalf("persisted subscription = %#v", persisted)
	}
}

func TestRESTPatchSubscriptionAcceptsWrappedBodyAndSnakeCaseMask(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     60,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2,
		"enableMessageOrdering":true,
		"enableExactlyOnceDelivery":true,
		"retainAckedMessages":true
	}`)

	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"subscription":{
			"name":"projects/devcloud/subscriptions/orders-sub",
			"ackDeadlineSeconds":10,
			"enableMessageOrdering":false,
			"enableExactlyOnceDelivery":false,
			"retainAckedMessages":false
		},
		"updateMask":{"paths":["ack_deadline_seconds","enable_message_ordering","enable_exactly_once_delivery","retain_acked_messages"]}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusOK, patch.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(patch.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if subscription.AckDeadlineSeconds != 10 || subscription.EnableMessageOrdering || subscription.EnableExactlyOnceDelivery || subscription.RetainAckedMessages {
		t.Fatalf("subscription = %#v", subscription)
	}
}

func TestRESTPatchSubscriptionRejectsUnsafeUpdates(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     5,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/other", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)

	tooLarge := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub", `{"ackDeadlineSeconds":6}`)
	if tooLarge.Code != http.StatusBadRequest {
		t.Fatalf("too large status = %d, want %d: %s", tooLarge.Code, http.StatusBadRequest, tooLarge.Body.String())
	}
	changeTopic := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/other"}`)
	if changeTopic.Code != http.StatusBadRequest {
		t.Fatalf("change topic status = %d, want %d: %s", changeTopic.Code, http.StatusBadRequest, changeTopic.Body.String())
	}
	unsupportedMask := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=expirationPolicy.foo", `{"expirationPolicy":{"ttl":"3600s"}}`)
	if unsupportedMask.Code != http.StatusBadRequest {
		t.Fatalf("unsupported mask status = %d, want %d: %s", unsupportedMask.Code, http.StatusBadRequest, unsupportedMask.Body.String())
	}
	invalidRetention := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=messageRetentionDuration", `{"messageRetentionDuration":"soon"}`)
	if invalidRetention.Code != http.StatusBadRequest {
		t.Fatalf("invalid retention status = %d, want %d: %s", invalidRetention.Code, http.StatusBadRequest, invalidRetention.Body.String())
	}
	invalidExpiration := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=expirationPolicy", `{"expirationPolicy":{"ttl":"later"}}`)
	if invalidExpiration.Code != http.StatusBadRequest {
		t.Fatalf("invalid expiration status = %d, want %d: %s", invalidExpiration.Code, http.StatusBadRequest, invalidExpiration.Body.String())
	}
}

func TestRESTSubscriptionFilterAppliesToPublishedMessages(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", MaxPullMessages: 10})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/priority-orders", `{
		"topic":"projects/devcloud/topics/orders",
		"filter":"attributes.kind=\"priority\""
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/all-orders", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"cHJpb3JpdHk=","attributes":{"kind":"priority"}},
			{"data":"bm9ybWFs","attributes":{"kind":"normal"}}
		]
	}`)

	filteredPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/priority-orders:pull", `{"maxMessages":10}`)
	if filteredPull.Code != http.StatusOK {
		t.Fatalf("filtered pull status = %d, want %d: %s", filteredPull.Code, http.StatusOK, filteredPull.Body.String())
	}
	if !strings.Contains(filteredPull.Body.String(), "cHJpb3JpdHk=") || strings.Contains(filteredPull.Body.String(), "bm9ybWFs") {
		t.Fatalf("filtered subscription received wrong messages: %s", filteredPull.Body.String())
	}

	unfilteredPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/all-orders:pull", `{"maxMessages":10}`)
	if unfilteredPull.Code != http.StatusOK {
		t.Fatalf("unfiltered pull status = %d, want %d: %s", unfilteredPull.Code, http.StatusOK, unfilteredPull.Body.String())
	}
	if !strings.Contains(unfilteredPull.Body.String(), "cHJpb3JpdHk=") || !strings.Contains(unfilteredPull.Body.String(), "bm9ybWFs") {
		t.Fatalf("unfiltered subscription did not receive both messages: %s", unfilteredPull.Body.String())
	}
}

func TestRESTSubscriptionFilterSupportsPrefixAndInequality(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", MaxPullMessages: 10})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/prefix-orders", `{
		"topic":"projects/devcloud/topics/orders",
		"filter":"hasPrefix(attributes.kind, \"priority\")"
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/non-debug-orders", `{
		"topic":"projects/devcloud/topics/orders",
		"filter":"attributes.kind != \"debug\""
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"cHJpb3JpdHktZmFzdA==","attributes":{"kind":"priority-fast"}},
			{"data":"ZGVidWc=","attributes":{"kind":"debug"}},
			{"data":"bm9ybWFs","attributes":{"kind":"normal"}}
		]
	}`)

	prefixPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/prefix-orders:pull", `{"maxMessages":10}`)
	if prefixPull.Code != http.StatusOK {
		t.Fatalf("prefix pull status = %d, want %d: %s", prefixPull.Code, http.StatusOK, prefixPull.Body.String())
	}
	if !strings.Contains(prefixPull.Body.String(), "cHJpb3JpdHktZmFzdA==") || strings.Contains(prefixPull.Body.String(), "ZGVidWc=") || strings.Contains(prefixPull.Body.String(), "bm9ybWFs") {
		t.Fatalf("prefix subscription received wrong messages: %s", prefixPull.Body.String())
	}

	inequalityPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/non-debug-orders:pull", `{"maxMessages":10}`)
	if inequalityPull.Code != http.StatusOK {
		t.Fatalf("inequality pull status = %d, want %d: %s", inequalityPull.Code, http.StatusOK, inequalityPull.Body.String())
	}
	if !strings.Contains(inequalityPull.Body.String(), "cHJpb3JpdHktZmFzdA==") || !strings.Contains(inequalityPull.Body.String(), "bm9ybWFs") || strings.Contains(inequalityPull.Body.String(), "ZGVidWc=") {
		t.Fatalf("inequality subscription received wrong messages: %s", inequalityPull.Body.String())
	}
}

func TestRESTRejectsUnsupportedSubscriptionFilter(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"filter":"attributes.kind:\"priority\""
	}`)
	if create.Code != http.StatusBadRequest {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusBadRequest, create.Body.String())
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=filter", `{
		"filter":"attributes.kind =~ \"priority\""
	}`)
	if patch.Code != http.StatusBadRequest {
		t.Fatalf("patch status = %d, want %d: %s", patch.Code, http.StatusBadRequest, patch.Body.String())
	}
}

func TestRESTModifyPushConfigUpdatesAndClearsMetadata(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"pushConfig":{"pushEndpoint":"http://127.0.0.1:65535/old"}
	}`)

	update := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyPushConfig", `{
		"pushConfig":{"pushEndpoint":"http://127.0.0.1:65535/new","attributes":{"x-goog-version":"v1"}}
	}`)
	if update.Code != http.StatusOK {
		t.Fatalf("update status = %d, want %d: %s", update.Code, http.StatusOK, update.Body.String())
	}
	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(get.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if subscription.PushConfig == nil || subscription.PushConfig["pushEndpoint"] != "http://127.0.0.1:65535/new" {
		t.Fatalf("pushConfig after update = %#v", subscription.PushConfig)
	}

	clear := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyPushConfig", `{}`)
	if clear.Code != http.StatusOK {
		t.Fatalf("clear status = %d, want %d: %s", clear.Code, http.StatusOK, clear.Body.String())
	}
	get = performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	var cleared subscriptionResource
	if err := json.NewDecoder(get.Body).Decode(&cleared); err != nil {
		t.Fatalf("decode subscription after clear: %v", err)
	}
	if cleared.PushConfig != nil {
		t.Fatalf("pushConfig after clear = %#v, want nil", cleared.PushConfig)
	}
}

func TestSnapshotRedactsPushConfigSensitiveFields(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-push", `{
		"topic":"projects/devcloud/topics/orders",
		"pushConfig":{
			"pushEndpoint":"http://127.0.0.1:65535/push",
			"attributes":{"authorization":"Bearer hidden"},
			"oidcToken":{"serviceAccountEmail":"local@example.test","audience":"secret-audience"}
		}
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-push", "")
	if !strings.Contains(get.Body.String(), "secret-audience") {
		t.Fatalf("resource response should preserve push config metadata for REST compatibility: %s", get.Body.String())
	}

	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 {
		t.Fatalf("subscriptions = %#v", snapshot.Subscriptions)
	}
	pushConfig := snapshot.Subscriptions[0].PushConfig
	if pushConfig["pushEndpoint"] != "http://127.0.0.1:65535/push" {
		t.Fatalf("snapshot pushConfig = %#v", pushConfig)
	}
	data, err := json.Marshal(pushConfig)
	if err != nil {
		t.Fatalf("marshal pushConfig: %v", err)
	}
	for _, forbidden := range []string{"authorization", "Bearer hidden", "oidcToken", "secret-audience", "local@example.test"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("snapshot pushConfig leaked %q: %s", forbidden, data)
		}
	}
}

func TestRESTRejectsInvalidPushConfigEndpoint(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"pushConfig":{"pushEndpoint":"ftp://127.0.0.1/push"}
	}`)
	if create.Code != http.StatusBadRequest {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusBadRequest, create.Body.String())
	}
	if !strings.Contains(create.Body.String(), "INVALID_ARGUMENT") {
		t.Fatalf("create error body = %s", create.Body.String())
	}

	createPull := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	if createPull.Code != http.StatusOK {
		t.Fatalf("create pull subscription status = %d, want %d: %s", createPull.Code, http.StatusOK, createPull.Body.String())
	}
	modify := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyPushConfig", `{
		"pushConfig":{"pushEndpoint":"http://user:pass@127.0.0.1:65535/push"}
	}`)
	if modify.Code != http.StatusBadRequest {
		t.Fatalf("modify status = %d, want %d: %s", modify.Code, http.StatusBadRequest, modify.Body.String())
	}
	if !strings.Contains(modify.Body.String(), "INVALID_ARGUMENT") {
		t.Fatalf("modify error body = %s", modify.Body.String())
	}
}

func TestRESTPullRejectsPushSubscriptionWithoutLeakingMessage(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-push", `{
		"topic":"projects/devcloud/topics/orders",
		"pushConfig":{"pushEndpoint":"http://127.0.0.1:65535/push"}
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create push subscription status = %d, want %d: %s", create.Code, http.StatusOK, create.Body.String())
	}
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cHVzaC1vbmx5","attributes":{"token":"hidden"}}]
	}`)

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-push:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusBadRequest {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusBadRequest, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), "FAILED_PRECONDITION") {
		t.Fatalf("pull error body = %s", pull.Body.String())
	}
	for _, forbidden := range []string{"cHVzaC1vbmx5", "hidden", "ackId"} {
		if strings.Contains(pull.Body.String(), forbidden) {
			t.Fatalf("push subscription pull leaked %q: %s", forbidden, pull.Body.String())
		}
	}
}

func TestPushDeliveryAcksSuccessfulHTTPResponse(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	received := make(chan struct {
		Subscription string `json:"subscription"`
		Message      struct {
			Data        string            `json:"data"`
			Attributes  map[string]string `json:"attributes"`
			MessageID   string            `json:"messageId"`
			PublishTime string            `json:"publishTime"`
		} `json:"message"`
	}, 1)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		defer r.Body.Close()
		if r.Method != http.MethodPost {
			t.Errorf("push method = %s, want POST", r.Method)
		}
		var body struct {
			Subscription string `json:"subscription"`
			Message      struct {
				Data        string            `json:"data"`
				Attributes  map[string]string `json:"attributes"`
				MessageID   string            `json:"messageId"`
				PublishTime string            `json:"publishTime"`
			} `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode push body: %v", err)
			return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: r}, nil
		}
		received <- body
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: r}, nil
	})}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-push", `{
		"topic":"projects/devcloud/topics/orders",
		"pushConfig":{"pushEndpoint":"http://127.0.0.1/push"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cHVzaA==","attributes":{"kind":"local"}}]
	}`)

	delivered, err := server.deliverPush(context.Background(), client)
	if err != nil {
		t.Fatalf("deliverPush: %v", err)
	}
	if !delivered {
		t.Fatalf("deliverPush delivered = false")
	}
	select {
	case body := <-received:
		if body.Subscription != "projects/devcloud/subscriptions/orders-push" || body.Message.Data != "cHVzaA==" || body.Message.Attributes["kind"] != "local" || body.Message.MessageID == "" || body.Message.PublishTime == "" {
			t.Fatalf("push body = %#v", body)
		}
	case <-time.After(time.Second):
		t.Fatalf("push endpoint did not receive delivery")
	}
	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].TotalRetainedMessages != 0 {
		t.Fatalf("successful push was not acked: %#v", snapshot.Subscriptions)
	}
}

func TestPushRetrySchedulesFailedHTTPResponse(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", DefaultAckDeadlineSeconds: 2})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	statusCode := http.StatusInternalServerError
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		defer r.Body.Close()
		attempts++
		return &http.Response{StatusCode: statusCode, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: r}, nil
	})}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-push", `{
		"topic":"projects/devcloud/topics/orders",
		"retryPolicy":{"minimumBackoff":"5s","maximumBackoff":"10s"},
		"pushConfig":{"pushEndpoint":"http://127.0.0.1/push"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cmV0cnk="}]
	}`)

	delivered, err := server.deliverPush(context.Background(), client)
	if err != nil {
		t.Fatalf("first deliverPush: %v", err)
	}
	if !delivered || attempts != 1 {
		t.Fatalf("first push delivered=%v attempts=%d", delivered, attempts)
	}
	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || len(snapshot.Subscriptions[0].RecentDeliveries) != 1 {
		t.Fatalf("snapshot after failed push = %#v", snapshot.Subscriptions)
	}
	recent := snapshot.Subscriptions[0].RecentDeliveries[0]
	if recent.State != "delayed" || recent.DeliveryAttempt != 1 || recent.NextDeliveryTime != now.Add(5*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("retry delivery summary = %#v", recent)
	}
	delivered, err = server.deliverPush(context.Background(), client)
	if err != nil {
		t.Fatalf("early retry deliverPush: %v", err)
	}
	if delivered || attempts != 1 {
		t.Fatalf("early retry delivered=%v attempts=%d", delivered, attempts)
	}

	statusCode = http.StatusOK
	now = now.Add(5 * time.Second)
	delivered, err = server.deliverPush(context.Background(), client)
	if err != nil {
		t.Fatalf("retry deliverPush: %v", err)
	}
	if !delivered || attempts != 2 {
		t.Fatalf("retry push delivered=%v attempts=%d", delivered, attempts)
	}
	snapshot = server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].TotalRetainedMessages != 0 {
		t.Fatalf("successful retry was not acked: %#v", snapshot.Subscriptions)
	}
}

func TestRESTDetachSubscriptionDropsBacklogAndBlocksPull(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"ZGV0YWNoZWQ=","attributes":{"secret":"hidden"}}]
	}`)

	detach := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:detach", `{}`)
	if detach.Code != http.StatusOK {
		t.Fatalf("detach status = %d, want %d: %s", detach.Code, http.StatusOK, detach.Body.String())
	}
	for _, forbidden := range []string{"ZGV0YWNoZWQ=", "secret", "hidden"} {
		if strings.Contains(detach.Body.String(), forbidden) {
			t.Fatalf("detach response leaked %q: %s", forbidden, detach.Body.String())
		}
	}

	get := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	var subscription subscriptionResource
	if err := json.NewDecoder(get.Body).Decode(&subscription); err != nil {
		t.Fatalf("decode subscription: %v", err)
	}
	if !subscription.Detached {
		t.Fatalf("subscription detached = false: %#v", subscription)
	}

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusBadRequest {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusBadRequest, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), "FAILED_PRECONDITION") {
		t.Fatalf("pull error body = %s", pull.Body.String())
	}
	if strings.Contains(pull.Body.String(), "ZGV0YWNoZWQ=") || strings.Contains(pull.Body.String(), "hidden") {
		t.Fatalf("detached pull leaked retained message: %s", pull.Body.String())
	}

	topicSubscriptions := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/topics/orders/subscriptions", "")
	if topicSubscriptions.Code != http.StatusOK {
		t.Fatalf("topic subscriptions status = %d, want %d: %s", topicSubscriptions.Code, http.StatusOK, topicSubscriptions.Body.String())
	}
	if strings.Contains(topicSubscriptions.Body.String(), "orders-sub") {
		t.Fatalf("detached subscription should not be listed as attached: %s", topicSubscriptions.Body.String())
	}

	deleteTopic := performPubSubRequest(server, http.MethodDelete, "/v1/projects/devcloud/topics/orders", "")
	if deleteTopic.Code != http.StatusNoContent {
		t.Fatalf("delete topic status = %d, want %d: %s", deleteTopic.Code, http.StatusNoContent, deleteTopic.Body.String())
	}
}

func TestRESTRejectsInvalidDeadLetterPolicy(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	missingTopic := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/missing-dlq-topic", `{
		"topic":"projects/devcloud/topics/orders",
		"deadLetterPolicy":{"maxDeliveryAttempts":5}
	}`)
	if missingTopic.Code != http.StatusBadRequest {
		t.Fatalf("missing deadLetterTopic status = %d, want %d: %s", missingTopic.Code, http.StatusBadRequest, missingTopic.Body.String())
	}

	tooSmall := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/too-small-dlq", `{
		"topic":"projects/devcloud/topics/orders",
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":4}
	}`)
	if tooSmall.Code != http.StatusBadRequest {
		t.Fatalf("too small maxDeliveryAttempts status = %d, want %d: %s", tooSmall.Code, http.StatusBadRequest, tooSmall.Body.String())
	}

	missingTarget := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/missing-dlq-target", `{
		"topic":"projects/devcloud/topics/orders",
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":5}
	}`)
	if missingTarget.Code != http.StatusNotFound {
		t.Fatalf("missing dead-letter topic status = %d, want %d: %s", missingTarget.Code, http.StatusNotFound, missingTarget.Body.String())
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=deadLetterPolicy", `{
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":101}
	}`)
	if patch.Code != http.StatusBadRequest {
		t.Fatalf("patch invalid deadLetterPolicy status = %d, want %d: %s", patch.Code, http.StatusBadRequest, patch.Body.String())
	}

	missingPatchTarget := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=deadLetterPolicy", `{
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":5}
	}`)
	if missingPatchTarget.Code != http.StatusNotFound {
		t.Fatalf("patch missing dead-letter topic status = %d, want %d: %s", missingPatchTarget.Code, http.StatusNotFound, missingPatchTarget.Body.String())
	}
}

func TestRESTRejectsInvalidRetryPolicy(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	badDuration := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/bad-duration", `{
		"topic":"projects/devcloud/topics/orders",
		"retryPolicy":{"minimumBackoff":"soon","maximumBackoff":"10s"}
	}`)
	if badDuration.Code != http.StatusBadRequest {
		t.Fatalf("bad duration status = %d, want %d: %s", badDuration.Code, http.StatusBadRequest, badDuration.Body.String())
	}

	inverted := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/inverted", `{
		"topic":"projects/devcloud/topics/orders",
		"retryPolicy":{"minimumBackoff":"20s","maximumBackoff":"10s"}
	}`)
	if inverted.Code != http.StatusBadRequest {
		t.Fatalf("inverted retryPolicy status = %d, want %d: %s", inverted.Code, http.StatusBadRequest, inverted.Body.String())
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	patch := performPubSubRequest(server, http.MethodPatch, "/v1/projects/devcloud/subscriptions/orders-sub?updateMask=retryPolicy", `{
		"retryPolicy":{"minimumBackoff":"11s","maximumBackoff":"10s"}
	}`)
	if patch.Code != http.StatusBadRequest {
		t.Fatalf("patch invalid retryPolicy status = %d, want %d: %s", patch.Code, http.StatusBadRequest, patch.Body.String())
	}
}

func TestRESTRejectsSubscriptionForMissingTopic(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})

	rec := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/missing"
	}`)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestRESTRejectsAckDeadlinesAboveConfiguredMaximum(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     5,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	create := performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":6
	}`)
	if create.Code != http.StatusBadRequest {
		t.Fatalf("create status = %d, want %d: %s", create.Code, http.StatusBadRequest, create.Body.String())
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"bWF4"}]}`)
	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var pulled struct {
		ReceivedMessages []struct {
			AckID string `json:"ackId"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(pull.Body).Decode(&pulled); err != nil {
		t.Fatalf("decode pull: %v", err)
	}
	if len(pulled.ReceivedMessages) != 1 {
		t.Fatalf("receivedMessages = %#v", pulled.ReceivedMessages)
	}

	modify := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyAckDeadline", `{
		"ackIds":["`+pulled.ReceivedMessages[0].AckID+`"],
		"ackDeadlineSeconds":6
	}`)
	if modify.Code != http.StatusBadRequest {
		t.Fatalf("modify status = %d, want %d: %s", modify.Code, http.StatusBadRequest, modify.Body.String())
	}
}
