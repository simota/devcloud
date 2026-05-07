package dashboard

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	pubsubsvc "devcloud/internal/services/pubsub"
)

func TestPubSubDashboardAPIListsTopicsAndSubscriptions(t *testing.T) {
	pubsubServer := pubsubsvc.NewServer(pubsubsvc.Config{Project: "devcloud"})
	createTopic := pubsubRequest(t, pubsubServer, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	if createTopic.Code != http.StatusOK {
		t.Fatalf("create topic status = %d, body = %s", createTopic.Code, createTopic.Body.String())
	}
	createSubscription := pubsubRequest(t, pubsubServer, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":10
	}`)
	if createSubscription.Code != http.StatusOK {
		t.Fatalf("create subscription status = %d, body = %s", createSubscription.Code, createSubscription.Body.String())
	}

	server := NewServer(Config{
		PubSubRESTEndpoint: "http://127.0.0.1:8086",
		PubSubProject:      "devcloud",
		PubSubStoragePath:  ".devcloud/test/pubsub",
	}, newDashboardStore(nil, nil))
	server.SetPubSub(pubsubServer)
	routes := server.routes()

	status := performRequest(routes, http.MethodGet, "/api/pubsub/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"running":true`) || !strings.Contains(status.Body.String(), `"topicCount":1`) {
		t.Fatalf("pubsub status = %d body=%s", status.Code, status.Body.String())
	}
	health := performRequest(routes, http.MethodGet, "/api/pubsub/health")
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"running":true`) || !strings.Contains(health.Body.String(), `"service":"pubsub"`) {
		t.Fatalf("pubsub health = %d body=%s", health.Code, health.Body.String())
	}
	projects := performRequest(routes, http.MethodGet, "/api/pubsub/projects")
	if projects.Code != http.StatusOK || !strings.Contains(projects.Body.String(), `"project":"devcloud"`) || !strings.Contains(projects.Body.String(), `"running":true`) {
		t.Fatalf("pubsub projects = %d body=%s", projects.Code, projects.Body.String())
	}
	topics := performRequest(routes, http.MethodGet, "/api/pubsub/topics")
	if topics.Code != http.StatusOK || !strings.Contains(topics.Body.String(), `"name":"projects/devcloud/topics/orders"`) || !strings.Contains(topics.Body.String(), `"subscriptionCount":1`) {
		t.Fatalf("pubsub topics = %d body=%s", topics.Code, topics.Body.String())
	}
	subscriptions := performRequest(routes, http.MethodGet, "/api/pubsub/subscriptions")
	if subscriptions.Code != http.StatusOK || !strings.Contains(subscriptions.Body.String(), `"name":"projects/devcloud/subscriptions/orders-sub"`) {
		t.Fatalf("pubsub subscriptions = %d body=%s", subscriptions.Code, subscriptions.Body.String())
	}
	topicDetail := performRequest(routes, http.MethodGet, "/api/pubsub/topics/orders")
	if topicDetail.Code != http.StatusOK || !strings.Contains(topicDetail.Body.String(), `"topic"`) || !strings.Contains(topicDetail.Body.String(), `"subscriptionCount":1`) {
		t.Fatalf("pubsub topic detail = %d body=%s", topicDetail.Code, topicDetail.Body.String())
	}
	subscriptionDetail := performRequest(routes, http.MethodGet, "/api/pubsub/subscriptions/orders-sub")
	if subscriptionDetail.Code != http.StatusOK || !strings.Contains(subscriptionDetail.Body.String(), `"subscription"`) || !strings.Contains(subscriptionDetail.Body.String(), `"ackDeadlineSeconds":10`) {
		t.Fatalf("pubsub subscription detail = %d body=%s", subscriptionDetail.Code, subscriptionDetail.Body.String())
	}
	missingTopic := performRequest(routes, http.MethodGet, "/api/pubsub/topics/missing")
	if missingTopic.Code != http.StatusNotFound {
		t.Fatalf("missing pubsub topic = %d body=%s", missingTopic.Code, missingTopic.Body.String())
	}
}

func TestPubSubDashboardAPICreatesTopicsAndSubscriptions(t *testing.T) {
	pubsubServer := pubsubsvc.NewServer(pubsubsvc.Config{Project: "devcloud", DefaultAckDeadlineSeconds: 10})
	server := NewServer(Config{PubSubProject: "devcloud"}, newDashboardStore(nil, nil))
	server.SetPubSub(pubsubServer)
	routes := server.routes()

	createTopic := performRequestWithBody(routes, http.MethodPost, "/api/pubsub/topics", `{"topicId":"dashboard-orders"}`)
	if createTopic.Code != http.StatusOK {
		t.Fatalf("create topic status = %d body=%s", createTopic.Code, createTopic.Body.String())
	}
	if !strings.Contains(createTopic.Body.String(), `"name":"projects/devcloud/topics/dashboard-orders"`) {
		t.Fatalf("create topic body = %s", createTopic.Body.String())
	}

	createSubscription := performRequestWithBody(routes, http.MethodPost, "/api/pubsub/subscriptions", `{
		"subscriptionId":"dashboard-orders-sub",
		"topicId":"dashboard-orders",
		"ackDeadlineSeconds":15
	}`)
	if createSubscription.Code != http.StatusOK {
		t.Fatalf("create subscription status = %d body=%s", createSubscription.Code, createSubscription.Body.String())
	}
	if !strings.Contains(createSubscription.Body.String(), `"name":"projects/devcloud/subscriptions/dashboard-orders-sub"`) || !strings.Contains(createSubscription.Body.String(), `"ackDeadlineSeconds":15`) {
		t.Fatalf("create subscription body = %s", createSubscription.Body.String())
	}

	topics := performRequest(routes, http.MethodGet, "/api/pubsub/topics")
	if !strings.Contains(topics.Body.String(), `"subscriptionCount":1`) {
		t.Fatalf("topics after create = %s", topics.Body.String())
	}
	missingTopicID := performRequestWithBody(routes, http.MethodPost, "/api/pubsub/topics", `{}`)
	if missingTopicID.Code != http.StatusBadRequest {
		t.Fatalf("missing topic id status = %d body=%s", missingTopicID.Code, missingTopicID.Body.String())
	}
	wrongMethod := performRequest(routes, http.MethodDelete, "/api/pubsub/topics")
	if wrongMethod.Code != http.StatusMethodNotAllowed || wrongMethod.Header().Get("Allow") != "GET, POST" {
		t.Fatalf("wrong method status = %d allow=%q", wrongMethod.Code, wrongMethod.Header().Get("Allow"))
	}
}

func TestPubSubDashboardAPIPullsAcksAndShowsSafeMessageMetadata(t *testing.T) {
	pubsubServer := pubsubsvc.NewServer(pubsubsvc.Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	pubsubRequest(t, pubsubServer, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	pubsubRequest(t, pubsubServer, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	publish := pubsubRequest(t, pubsubServer, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"c2VjcmV0LXBheWxvYWQ=","attributes":{"token":"hidden"},"orderingKey":"group-1"}]
	}`)
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", publish.Code, publish.Body.String())
	}
	var published struct {
		MessageIDs []string `json:"messageIds"`
	}
	if err := json.NewDecoder(publish.Body).Decode(&published); err != nil {
		t.Fatalf("decode publish: %v", err)
	}
	if len(published.MessageIDs) != 1 {
		t.Fatalf("messageIds = %#v", published.MessageIDs)
	}

	server := NewServer(Config{PubSubProject: "devcloud"}, newDashboardStore(nil, nil))
	server.SetPubSub(pubsubServer)
	routes := server.routes()

	messageDetail := performRequest(routes, http.MethodGet, "/api/pubsub/messages/"+published.MessageIDs[0])
	if messageDetail.Code != http.StatusOK {
		t.Fatalf("message detail status = %d body=%s", messageDetail.Code, messageDetail.Body.String())
	}
	for _, forbidden := range []string{"c2VjcmV0LXBheWxvYWQ=", "hidden", "ackId"} {
		if strings.Contains(messageDetail.Body.String(), forbidden) {
			t.Fatalf("message metadata leaked %q: %s", forbidden, messageDetail.Body.String())
		}
	}
	if !strings.Contains(messageDetail.Body.String(), `"subscription":"projects/devcloud/subscriptions/orders-sub"`) {
		t.Fatalf("message metadata did not include subscription state: %s", messageDetail.Body.String())
	}

	pull := performRequestWithBody(routes, http.MethodPost, "/api/pubsub/subscriptions/orders-sub/pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d body=%s", pull.Code, pull.Body.String())
	}
	var pulled struct {
		ReceivedMessages []struct {
			AckID string `json:"ackId"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(pull.Body).Decode(&pulled); err != nil {
		t.Fatalf("decode pull: %v", err)
	}
	if len(pulled.ReceivedMessages) != 1 || pulled.ReceivedMessages[0].AckID == "" {
		t.Fatalf("receivedMessages = %#v", pulled.ReceivedMessages)
	}

	release := performRequestWithBody(routes, http.MethodPost, "/api/pubsub/subscriptions/orders-sub/modifyAckDeadline", `{"ackIds":["`+pulled.ReceivedMessages[0].AckID+`"],"ackDeadlineSeconds":0}`)
	if release.Code != http.StatusOK {
		t.Fatalf("modifyAckDeadline status = %d body=%s", release.Code, release.Body.String())
	}
	redelivery := performRequestWithBody(routes, http.MethodPost, "/api/pubsub/subscriptions/orders-sub/pull", `{"maxMessages":1}`)
	if redelivery.Code != http.StatusOK {
		t.Fatalf("redelivery pull status = %d body=%s", redelivery.Code, redelivery.Body.String())
	}
	if err := json.NewDecoder(redelivery.Body).Decode(&pulled); err != nil {
		t.Fatalf("decode redelivery pull: %v", err)
	}
	if len(pulled.ReceivedMessages) != 1 || pulled.ReceivedMessages[0].AckID == "" {
		t.Fatalf("redelivered receivedMessages = %#v", pulled.ReceivedMessages)
	}

	ack := performRequestWithBody(routes, http.MethodPost, "/api/pubsub/subscriptions/orders-sub/ack", `{"ackIds":["`+pulled.ReceivedMessages[0].AckID+`"]}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status = %d body=%s", ack.Code, ack.Body.String())
	}
	emptyPull := performRequestWithBody(routes, http.MethodPost, "/api/pubsub/subscriptions/orders-sub/pull", `{"maxMessages":1}`)
	if strings.Contains(emptyPull.Body.String(), "receivedMessages") {
		t.Fatalf("acked message should not be pulled again: %s", emptyPull.Body.String())
	}
}

func TestPubSubDashboardAPIPublishesToTopic(t *testing.T) {
	pubsubServer := pubsubsvc.NewServer(pubsubsvc.Config{Project: "devcloud", DefaultAckDeadlineSeconds: 30})
	pubsubRequest(t, pubsubServer, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	pubsubRequest(t, pubsubServer, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)

	server := NewServer(Config{PubSubProject: "devcloud"}, newDashboardStore(nil, nil))
	server.SetPubSub(pubsubServer)
	routes := server.routes()

	publish := performRequestWithBody(routes, http.MethodPost, "/api/pubsub/topics/orders/publish", `{
		"messages":[{"data":"ZGFzaGJvYXJkLXB1Ymxpc2g=","attributes":{"source":"dashboard"},"orderingKey":"group-1"}]
	}`)
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status = %d body=%s", publish.Code, publish.Body.String())
	}
	if !strings.Contains(publish.Body.String(), `"messageIds"`) {
		t.Fatalf("publish response = %s", publish.Body.String())
	}

	pull := pubsubRequest(t, pubsubServer, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d body=%s", pull.Code, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), "ZGFzaGJvYXJkLXB1Ymxpc2g=") || !strings.Contains(pull.Body.String(), `"orderingKey":"group-1"`) {
		t.Fatalf("published message was not delivered: %s", pull.Body.String())
	}

	wrongMethod := performRequest(routes, http.MethodGet, "/api/pubsub/topics/orders/publish")
	if wrongMethod.Code != http.StatusMethodNotAllowed {
		t.Fatalf("publish wrong method status = %d body=%s", wrongMethod.Code, wrongMethod.Body.String())
	}
}

func TestPubSubDashboardAPIMarksDisabled(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))

	rec := performRequest(server.routes(), http.MethodGet, "/api/pubsub/topics")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	projects := performRequest(server.routes(), http.MethodGet, "/api/pubsub/projects")
	if projects.Code != http.StatusOK || !strings.Contains(projects.Body.String(), `"project":"devcloud"`) || !strings.Contains(projects.Body.String(), `"running":false`) {
		t.Fatalf("disabled pubsub projects = %d body=%s", projects.Code, projects.Body.String())
	}
}
