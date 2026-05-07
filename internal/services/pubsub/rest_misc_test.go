package pubsub

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRESTTopicAndSubscriptionIAMNoops(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)

	getTopicPolicy := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:getIamPolicy", `{}`)
	if getTopicPolicy.Code != http.StatusOK {
		t.Fatalf("topic getIamPolicy status = %d, want %d: %s", getTopicPolicy.Code, http.StatusOK, getTopicPolicy.Body.String())
	}
	if !strings.Contains(getTopicPolicy.Body.String(), `"bindings":[]`) {
		t.Fatalf("topic policy = %s", getTopicPolicy.Body.String())
	}

	testTopicPermissions := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:testIamPermissions", `{
		"permissions":["pubsub.topics.get","pubsub.topics.publish"]
	}`)
	if testTopicPermissions.Code != http.StatusOK {
		t.Fatalf("topic testIamPermissions status = %d, want %d: %s", testTopicPermissions.Code, http.StatusOK, testTopicPermissions.Body.String())
	}
	if !strings.Contains(testTopicPermissions.Body.String(), "pubsub.topics.publish") {
		t.Fatalf("topic permissions = %s", testTopicPermissions.Body.String())
	}

	setSubscriptionPolicy := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:setIamPolicy", `{
		"policy":{"version":1,"bindings":[{"role":"roles/pubsub.viewer","members":["allUsers"]}]}
	}`)
	if setSubscriptionPolicy.Code != http.StatusOK {
		t.Fatalf("subscription setIamPolicy status = %d, want %d: %s", setSubscriptionPolicy.Code, http.StatusOK, setSubscriptionPolicy.Body.String())
	}
	if !strings.Contains(setSubscriptionPolicy.Body.String(), "roles/pubsub.viewer") {
		t.Fatalf("subscription policy = %s", setSubscriptionPolicy.Body.String())
	}

	missingTopic := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/missing:getIamPolicy", `{}`)
	if missingTopic.Code != http.StatusNotFound {
		t.Fatalf("missing topic IAM status = %d, want %d: %s", missingTopic.Code, http.StatusNotFound, missingTopic.Body.String())
	}
	wrongMethod := performPubSubRequest(server, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub:getIamPolicy", "")
	if wrongMethod.Code != http.StatusMethodNotAllowed {
		t.Fatalf("subscription IAM wrong method status = %d, want %d: %s", wrongMethod.Code, http.StatusMethodNotAllowed, wrongMethod.Body.String())
	}
}

func TestRESTCleansUpUnreferencedMessages(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	publishWithoutSubscribers := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"bm8tc3Vi"}]}`)
	if publishWithoutSubscribers.Code != http.StatusOK {
		t.Fatalf("publish without subscribers status = %d, want %d: %s", publishWithoutSubscribers.Code, http.StatusOK, publishWithoutSubscribers.Body.String())
	}
	if len(server.messages) != 0 {
		t.Fatalf("messages retained without subscriptions = %#v", server.messages)
	}

	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"YWNrZWQ="}]}`)
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
	ack := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":["`+pulled.ReceivedMessages[0].AckID+`"]}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusOK, ack.Body.String())
	}
	if len(server.messages) != 0 {
		t.Fatalf("acked messages retained = %#v", server.messages)
	}
}

func TestRESTRetainAckedMessagesAllowsSeekReplay(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{Project: "devcloud"})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"retainAckedMessages":true
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cmV0YWluLWFja2Vk"}]}`)

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
	ack := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":["`+pulled.ReceivedMessages[0].AckID+`"]}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusOK, ack.Body.String())
	}
	if len(server.messages) != 1 {
		t.Fatalf("acked message was not retained: %#v", server.messages)
	}

	emptyPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(emptyPull.Body.String(), "receivedMessages") {
		t.Fatalf("acked retained message should stay hidden until seek, got %s", emptyPull.Body.String())
	}
	seek := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:seek", `{
		"time":"2026-05-02T11:59:59Z"
	}`)
	if seek.Code != http.StatusOK {
		t.Fatalf("seek status = %d, want %d: %s", seek.Code, http.StatusOK, seek.Body.String())
	}
	replayed := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if replayed.Code != http.StatusOK {
		t.Fatalf("replayed pull status = %d, want %d: %s", replayed.Code, http.StatusOK, replayed.Body.String())
	}
	if !strings.Contains(replayed.Body.String(), "cmV0YWluLWFja2Vk") {
		t.Fatalf("seek did not replay retained acked message: %s", replayed.Body.String())
	}
}

func TestRESTCleansUpMessagesAfterRetention(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                 "devcloud",
		MessageRetentionSeconds: 5,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cmV0YWluZWQ="}]}`)

	now = now.Add(6 * time.Second)
	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	if strings.Contains(pull.Body.String(), "receivedMessages") {
		t.Fatalf("expired message should not be received, got %s", pull.Body.String())
	}
	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || snapshot.Subscriptions[0].TotalRetainedMessages != 0 {
		t.Fatalf("snapshot = %#v", snapshot.Subscriptions)
	}
}

func TestRESTSubscriptionRetentionIsPerSubscription(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                 "devcloud",
		MessageRetentionSeconds: 60,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/short-retention", `{
		"topic":"projects/devcloud/topics/orders",
		"messageRetentionDuration":"5s"
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/long-retention", `{
		"topic":"projects/devcloud/topics/orders",
		"messageRetentionDuration":"30s"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cGVyLXN1Yi1yZXRlbnRpb24="}]}`)

	now = now.Add(6 * time.Second)
	shortPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/short-retention:pull", `{"maxMessages":1}`)
	if shortPull.Code != http.StatusOK {
		t.Fatalf("short pull status = %d, want %d: %s", shortPull.Code, http.StatusOK, shortPull.Body.String())
	}
	if strings.Contains(shortPull.Body.String(), "receivedMessages") {
		t.Fatalf("short-retention subscription should not receive expired message, got %s", shortPull.Body.String())
	}

	longPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/long-retention:pull", `{"maxMessages":1}`)
	if longPull.Code != http.StatusOK {
		t.Fatalf("long pull status = %d, want %d: %s", longPull.Code, http.StatusOK, longPull.Body.String())
	}
	if !strings.Contains(longPull.Body.String(), "cGVyLXN1Yi1yZXRlbnRpb24=") {
		t.Fatalf("long-retention subscription should still receive message, got %s", longPull.Body.String())
	}
}

func TestRESTTopicRetentionAppliesWhenSubscriptionDoesNotOverride(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                 "devcloud",
		MessageRetentionSeconds: 60,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", `{
		"messageRetentionDuration":"5s"
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/topic-retention", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/subscription-retention", `{
		"topic":"projects/devcloud/topics/orders",
		"messageRetentionDuration":"30s"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"dG9waWMtcmV0ZW50aW9u"}]}`)

	now = now.Add(6 * time.Second)
	topicRetentionPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/topic-retention:pull", `{"maxMessages":1}`)
	if topicRetentionPull.Code != http.StatusOK {
		t.Fatalf("topic retention pull status = %d, want %d: %s", topicRetentionPull.Code, http.StatusOK, topicRetentionPull.Body.String())
	}
	if strings.Contains(topicRetentionPull.Body.String(), "receivedMessages") {
		t.Fatalf("topic-retention subscription should not receive expired message, got %s", topicRetentionPull.Body.String())
	}

	subscriptionRetentionPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/subscription-retention:pull", `{"maxMessages":1}`)
	if subscriptionRetentionPull.Code != http.StatusOK {
		t.Fatalf("subscription retention pull status = %d, want %d: %s", subscriptionRetentionPull.Code, http.StatusOK, subscriptionRetentionPull.Body.String())
	}
	if !strings.Contains(subscriptionRetentionPull.Body.String(), "dG9waWMtcmV0ZW50aW9u") {
		t.Fatalf("subscription retention should override topic retention, got %s", subscriptionRetentionPull.Body.String())
	}
}

func TestSnapshotExposesSafeDeliverySummaries(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 10,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"enableExactlyOnceDelivery":true
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"YmFja2xvZw==","attributes":{"secret":"hidden"},"orderingKey":"backlog-key"},
			{"data":"aW4tZmxpZ2h0","attributes":{"secret":"hidden"},"orderingKey":"leased-key"}
		]
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)

	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 {
		t.Fatalf("subscriptions = %#v", snapshot.Subscriptions)
	}
	subscription := snapshot.Subscriptions[0]
	if !subscription.EnableExactlyOnceDelivery {
		t.Fatalf("subscription snapshot did not expose exactly-once metadata: %#v", subscription)
	}
	if subscription.BacklogMessages != 1 || subscription.InFlightMessages != 1 || len(subscription.RecentDeliveries) != 2 {
		t.Fatalf("subscription snapshot = %#v", subscription)
	}
	if subscription.RecentDeliveries[0].State != "in-flight" || subscription.RecentDeliveries[0].LeaseDeadline == "" {
		t.Fatalf("in-flight delivery = %#v", subscription.RecentDeliveries[0])
	}
	if subscription.RecentDeliveries[1].State != "backlog" || subscription.RecentDeliveries[1].LeaseDeadline != "" {
		t.Fatalf("backlog delivery = %#v", subscription.RecentDeliveries[1])
	}
	data, err := json.Marshal(subscription.RecentDeliveries)
	if err != nil {
		t.Fatalf("marshal delivery summaries: %v", err)
	}
	for _, forbidden := range []string{"ackId", "YmFja2xvZw==", "aW4tZmxpZ2h0", "secret"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("delivery summaries leaked %q: %s", forbidden, data)
		}
	}
}

func TestSnapshotExpiresLeasesAndShowsRetryDelay(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 1,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":1,
		"retryPolicy":{"minimumBackoff":"5s"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cmV0cnktZGVsYXk=","attributes":{"secret":"hidden"}}]
	}`)

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK || !strings.Contains(pull.Body.String(), `"deliveryAttempt":1`) {
		t.Fatalf("pull = status %d body %s", pull.Code, pull.Body.String())
	}

	now = now.Add(2 * time.Second)
	snapshot := server.Snapshot()
	if len(snapshot.Subscriptions) != 1 || len(snapshot.Subscriptions[0].RecentDeliveries) != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	subscription := snapshot.Subscriptions[0]
	if subscription.InFlightMessages != 0 || subscription.BacklogMessages != 1 {
		t.Fatalf("subscription counters = %#v", subscription)
	}
	delivery := subscription.RecentDeliveries[0]
	if delivery.State != "delayed" || delivery.LeaseDeadline != "" || delivery.NextDeliveryTime == "" {
		t.Fatalf("delivery summary = %#v", delivery)
	}
	data, err := json.Marshal(delivery)
	if err != nil {
		t.Fatalf("marshal delivery summary: %v", err)
	}
	for _, forbidden := range []string{"ackId", "cmV0cnktZGVsYXk=", "secret"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("delivery summary leaked %q: %s", forbidden, data)
		}
	}
}
