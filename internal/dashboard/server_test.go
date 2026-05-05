package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestSQSQueuesAPIListsQueueSnapshots(t *testing.T) {
	sqsServer := sqssvc.NewServer(sqssvc.Config{Addr: "127.0.0.1:9324"})
	createRec := sqsJSONRequest(t, sqsServer, "CreateQueue", `{"QueueName":"dashboard-queue"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetSQS(sqsServer)

	rec := performRequest(server.routes(), http.MethodGet, "/api/sqs/queues")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"name":"dashboard-queue"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestSQSQueueDetailAPIsExposeMessagesLeasesAndPurge(t *testing.T) {
	sqsServer := sqssvc.NewServer(sqssvc.Config{
		Addr:                            "127.0.0.1:9324",
		DefaultVisibilityTimeoutSeconds: 30,
	})
	createRec := sqsJSONRequest(t, sqsServer, "CreateQueue", `{"QueueName":"dashboard-detail"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createBody struct {
		QueueURL string `json:"QueueUrl"`
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createBody); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	sendRec := sqsJSONRequest(t, sqsServer, "SendMessage", `{"QueueUrl":"`+createBody.QueueURL+`","MessageBody":"dashboard body"}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}
	receiveRec := sqsJSONRequest(t, sqsServer, "ReceiveMessage", `{"QueueUrl":"`+createBody.QueueURL+`","MaxNumberOfMessages":1}`)
	if receiveRec.Code != http.StatusOK {
		t.Fatalf("receive status = %d, body = %s", receiveRec.Code, receiveRec.Body.String())
	}
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetSQS(sqsServer)
	routes := server.routes()

	detailRec := performRequest(routes, http.MethodGet, "/api/sqs/queues/dashboard-detail")
	if detailRec.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want %d, body = %s", detailRec.Code, http.StatusOK, detailRec.Body.String())
	}
	if !strings.Contains(detailRec.Body.String(), `"name":"dashboard-detail"`) {
		t.Fatalf("detail body = %s", detailRec.Body.String())
	}

	messagesRec := performRequest(routes, http.MethodGet, "/api/sqs/queues/dashboard-detail/messages")
	if messagesRec.Code != http.StatusOK {
		t.Fatalf("messages status = %d, want %d, body = %s", messagesRec.Code, http.StatusOK, messagesRec.Body.String())
	}
	if !strings.Contains(messagesRec.Body.String(), `"body":"dashboard body"`) || !strings.Contains(messagesRec.Body.String(), `"state":"in_flight"`) {
		t.Fatalf("messages body = %s", messagesRec.Body.String())
	}

	leasesRec := performRequest(routes, http.MethodGet, "/api/sqs/queues/dashboard-detail/leases")
	if leasesRec.Code != http.StatusOK {
		t.Fatalf("leases status = %d, want %d, body = %s", leasesRec.Code, http.StatusOK, leasesRec.Body.String())
	}
	if !strings.Contains(leasesRec.Body.String(), `"receiptHandlePresent":true`) || strings.Contains(leasesRec.Body.String(), "rct-") {
		t.Fatalf("leases should expose only redacted receipt state: %s", leasesRec.Body.String())
	}

	dlqRec := performRequest(routes, http.MethodGet, "/api/sqs/queues/dashboard-detail/dlq")
	if dlqRec.Code != http.StatusOK {
		t.Fatalf("dlq status = %d, want %d, body = %s", dlqRec.Code, http.StatusOK, dlqRec.Body.String())
	}

	purgeRec := performRequest(routes, http.MethodPost, "/api/sqs/queues/dashboard-detail/purge")
	if purgeRec.Code != http.StatusNoContent {
		t.Fatalf("purge status = %d, want %d, body = %s", purgeRec.Code, http.StatusNoContent, purgeRec.Body.String())
	}
	afterPurgeRec := performRequest(routes, http.MethodGet, "/api/sqs/queues/dashboard-detail/messages")
	if !strings.Contains(afterPurgeRec.Body.String(), `"messages":[]`) {
		t.Fatalf("messages after purge = %s", afterPurgeRec.Body.String())
	}
}

func TestSQSQueuesAPIMarksDisabled(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))

	rec := performRequest(server.routes(), http.MethodGet, "/api/sqs/queues")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}

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

func TestRedshiftDashboardAPIListsClusters(t *testing.T) {
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		SQLAddr:           "127.0.0.1:15439",
		Region:            "us-east-1",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	server := NewServer(Config{
		RedshiftSQLEndpoint: "127.0.0.1:15439",
		RedshiftAPIEndpoint: "http://127.0.0.1:19099",
		RedshiftRegion:      "us-east-1",
	}, newDashboardStore(nil, nil))
	server.SetRedshift(redshiftServer)

	status := performRequest(server.routes(), http.MethodGet, "/api/redshift/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"running":true`) || !strings.Contains(status.Body.String(), `"clusterCount":1`) {
		t.Fatalf("status = %d body=%s", status.Code, status.Body.String())
	}

	clusters := performRequest(server.routes(), http.MethodGet, "/api/redshift/clusters")
	if clusters.Code != http.StatusOK || !strings.Contains(clusters.Body.String(), `"clusterIdentifier":"devcloud"`) || !strings.Contains(clusters.Body.String(), `"port":15439`) {
		t.Fatalf("clusters = %d body=%s", clusters.Code, clusters.Body.String())
	}
}

func TestRedshiftDashboardAPIExposesBackendMode(t *testing.T) {
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		ClusterIdentifier: "devcloud",
		BackendKind:       "postgres",
		BackendMode:       "external",
	})
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedshift(redshiftServer)

	status := performRequest(server.routes(), http.MethodGet, "/api/redshift/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"backendKind":"postgres"`) || !strings.Contains(status.Body.String(), `"backendMode":"external"`) {
		t.Fatalf("status = %d body=%s", status.Code, status.Body.String())
	}
	if strings.Contains(status.Body.String(), "postgres://") || strings.Contains(status.Body.String(), "secret") {
		t.Fatalf("status leaked backend credentials: %s", status.Body.String())
	}
}

func TestRedshiftDashboardAPIExposesCatalogAndStatementMetadata(t *testing.T) {
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		SQLAddr:           "127.0.0.1:15439",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	for _, sql := range []string{
		"create schema if not exists loop",
		"create table loop.events(id integer encode raw, payload varchar(64)) distkey(id)",
		"insert into loop.events values (1, 'created')",
		"select id from loop.events where id = 1",
	} {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Sql":`+strconv.Quote(sql)+`}`))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "RedshiftData.ExecuteStatement")
		redshiftServer.ServeHTTP(httptest.NewRecorder(), req)
	}
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedshift(redshiftServer)

	catalog := performRequest(server.routes(), http.MethodGet, "/api/redshift/catalog")
	if catalog.Code != http.StatusOK || !strings.Contains(catalog.Body.String(), `"database":"dev"`) || !strings.Contains(catalog.Body.String(), `"name":"events"`) || !strings.Contains(catalog.Body.String(), `"encoding":"raw"`) {
		t.Fatalf("catalog = %d body=%s", catalog.Code, catalog.Body.String())
	}

	statements := performRequest(server.routes(), http.MethodGet, "/api/redshift/statements")
	if statements.Code != http.StatusOK || !strings.Contains(statements.Body.String(), `"status":"FINISHED"`) || !strings.Contains(statements.Body.String(), `"queryPreview":"select id from loop.events where id = 1"`) {
		t.Fatalf("statements = %d body=%s", statements.Code, statements.Body.String())
	}
	if strings.Contains(statements.Body.String(), `"created"`) {
		t.Fatalf("statements response leaked statement result value: %s", statements.Body.String())
	}
}

func TestRedshiftDashboardAPITableDetailAndQueryRunner(t *testing.T) {
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		SQLAddr:           "127.0.0.1:15439",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	for _, sql := range []string{
		"create schema if not exists loop",
		"create table loop.events(id integer encode raw, payload varchar(64)) distkey(id) sortkey(id)",
		"insert into loop.events values (1, 'created')",
		"insert into loop.events values (2, 'queued')",
	} {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Sql":`+strconv.Quote(sql)+`}`))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "RedshiftData.ExecuteStatement")
		redshiftServer.ServeHTTP(httptest.NewRecorder(), req)
	}
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedshift(redshiftServer)

	table := performRequest(server.routes(), http.MethodGet, "/api/redshift/tables/loop/events?limit=1")
	if table.Code != http.StatusOK || !strings.Contains(table.Body.String(), `"rowCount":2`) || !strings.Contains(table.Body.String(), `"rows":[["1","created"]]`) || !strings.Contains(table.Body.String(), `"encoding":"raw"`) {
		t.Fatalf("redshift table detail = %d body=%s", table.Code, table.Body.String())
	}

	query := performRequestWithBody(server.routes(), http.MethodPost, "/api/redshift/query", `{"sql":"select id, payload from loop.events where id = 2","maxRows":5}`)
	if query.Code != http.StatusOK || !strings.Contains(query.Body.String(), `"rowCount":1`) || !strings.Contains(query.Body.String(), `"rows":[["2","queued"]]`) || !strings.Contains(query.Body.String(), `"typeName":"int4"`) {
		t.Fatalf("redshift dashboard query = %d body=%s", query.Code, query.Body.String())
	}

	statements := performRequest(server.routes(), http.MethodGet, "/api/redshift/statements")
	if statements.Code != http.StatusOK || !strings.Contains(statements.Body.String(), `"queryPreview":"select id, payload from loop.events where id = 2"`) {
		t.Fatalf("redshift statements after query = %d body=%s", statements.Code, statements.Body.String())
	}
}

func TestRedshiftDashboardQueryErrorIsSafe(t *testing.T) {
	redshiftServer := redshiftsvc.NewServer(redshiftsvc.Config{
		SQLAddr:           "127.0.0.1:15439",
		ClusterIdentifier: "devcloud",
		Database:          "dev",
		User:              "dev",
	})
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetRedshift(redshiftServer)

	query := performRequestWithBody(server.routes(), http.MethodPost, "/api/redshift/query", `{"sql":"select * from missing where token = 'secret-token'","maxRows":5}`)
	if query.Code != http.StatusBadRequest || !strings.Contains(query.Body.String(), `"error":"redshift query failed"`) {
		t.Fatalf("redshift dashboard query error = %d body=%s", query.Code, query.Body.String())
	}
	if strings.Contains(query.Body.String(), "secret-token") || strings.Contains(query.Body.String(), "queryPreview") || strings.Contains(query.Body.String(), "statement") {
		t.Fatalf("redshift dashboard query error leaked SQL details: %s", query.Body.String())
	}
}

func TestRedshiftDashboardAPIMarksDisabled(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))

	status := performRequest(server.routes(), http.MethodGet, "/api/redshift/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"running":false`) {
		t.Fatalf("disabled status = %d body=%s", status.Code, status.Body.String())
	}
	clusters := performRequest(server.routes(), http.MethodGet, "/api/redshift/clusters")
	if clusters.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled clusters status = %d body=%s", clusters.Code, clusters.Body.String())
	}
	catalog := performRequest(server.routes(), http.MethodGet, "/api/redshift/catalog")
	if catalog.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled catalog status = %d body=%s", catalog.Code, catalog.Body.String())
	}
	statements := performRequest(server.routes(), http.MethodGet, "/api/redshift/statements")
	if statements.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled statements status = %d body=%s", statements.Code, statements.Body.String())
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

func TestDynamoDBDashboardPageAndAPIExposeTables(t *testing.T) {
	dynamo := dynamodbsvc.NewServer(dynamodbsvc.Config{Region: "us-east-1"})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"TableName":"Demo",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"gpk","AttributeType":"S"}
		],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
		"GlobalSecondaryIndexes":[{
			"IndexName":"gsi1",
			"KeySchema":[{"AttributeName":"gpk","KeyType":"HASH"}],
			"Projection":{"ProjectionType":"ALL"}
		}]
	}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	rec := httptest.NewRecorder()
	dynamo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"gpk":{"S":"group#1"},"name":{"S":"Ada"}}
	}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	rec = httptest.NewRecorder()
	dynamo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"TableName":"Demo",
		"TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}
	}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateTimeToLive")
	rec = httptest.NewRecorder()
	dynamo.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("UpdateTimeToLive status = %d, body = %s", rec.Code, rec.Body.String())
	}

	server := NewServer(Config{
		DynamoDBEndpoint:    "http://127.0.0.1:8000",
		DynamoDBRegion:      "us-east-1",
		DynamoDBStoragePath: ".devcloud/test/dynamodb",
	}, newDashboardStore(nil, nil))
	server.SetDynamoDB(dynamo)
	routes := server.routes()

	page := performRequest(routes, http.MethodGet, "/dynamodb")
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "devcloud Dashboard") {
		t.Fatalf("DynamoDB page changed: status=%d body=%s", page.Code, page.Body.String())
	}

	dashboardPage := performRequest(routes, http.MethodGet, "/dashboard/dynamodb")
	if dashboardPage.Code != http.StatusOK || !strings.Contains(dashboardPage.Body.String(), "devcloud Dashboard") {
		t.Fatalf("DynamoDB dashboard route changed: status=%d body=%s", dashboardPage.Code, dashboardPage.Body.String())
	}

	status := performRequest(routes, http.MethodGet, "/api/dynamodb/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"running":true`) {
		t.Fatalf("DynamoDB status = %d body=%s", status.Code, status.Body.String())
	}

	tables := performRequest(routes, http.MethodGet, "/api/dynamodb/tables")
	if tables.Code != http.StatusOK || !strings.Contains(tables.Body.String(), `"tableName":"Demo"`) {
		t.Fatalf("DynamoDB tables = %d body=%s", tables.Code, tables.Body.String())
	}

	items := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Demo/items?limit=1")
	if items.Code != http.StatusOK || !strings.Contains(items.Body.String(), `"tableName":"Demo"`) || !strings.Contains(items.Body.String(), `"Ada"`) {
		t.Fatalf("DynamoDB table items = %d body=%s", items.Code, items.Body.String())
	}

	detail := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Demo")
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), `"tableName":"Demo"`) {
		t.Fatalf("DynamoDB table detail = %d body=%s", detail.Code, detail.Body.String())
	}

	indexes := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Demo/indexes")
	if indexes.Code != http.StatusOK || !strings.Contains(indexes.Body.String(), `"IndexName":"gsi1"`) {
		t.Fatalf("DynamoDB table indexes = %d body=%s", indexes.Code, indexes.Body.String())
	}

	ttl := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Demo/ttl")
	if ttl.Code != http.StatusOK || !strings.Contains(ttl.Body.String(), `"TimeToLiveStatus":"ENABLED"`) {
		t.Fatalf("DynamoDB table ttl = %d body=%s", ttl.Code, ttl.Body.String())
	}

	streams := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Demo/streams")
	if streams.Code != http.StatusOK || !strings.Contains(streams.Body.String(), `"streamEnabled":false`) {
		t.Fatalf("DynamoDB table streams = %d body=%s", streams.Code, streams.Body.String())
	}

	missing := performRequest(routes, http.MethodGet, "/api/dynamodb/tables/Missing/items")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing DynamoDB table items status = %d, want %d", missing.Code, http.StatusNotFound)
	}
}

func TestBigQueryDashboardPageAndAPIExposeCatalog(t *testing.T) {
	bq := bigquerysvc.NewServer(bigquerysvc.Config{
		Project:     "devcloud",
		Location:    "US",
		StoragePath: t.TempDir(),
	})
	performBigQueryRequest(t, bq, http.MethodPost, "/bigquery/v2/projects/devcloud/datasets", `{"datasetReference":{"datasetId":"analytics"}}`)
	performBigQueryRequest(t, bq, http.MethodPost, "/bigquery/v2/projects/devcloud/datasets/analytics/tables", `{
		"tableReference":{"tableId":"people"},
		"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"},{"name":"name","type":"STRING"},{"name":"age","type":"INTEGER"}]}
	}`)
	performBigQueryRequest(t, bq, http.MethodPost, "/bigquery/v2/projects/devcloud/datasets/analytics/tables/people/insertAll", `{
		"rows":[{"insertId":"row-1","json":{"id":"1","name":"Ada","age":37}}]
	}`)
	performBigQueryRequest(t, bq, http.MethodPost, "/bigquery/v2/projects/devcloud/queries", `{
		"query":"SELECT id, name FROM `+"`devcloud.analytics.people`"+` WHERE age >= 30",
		"useLegacySql":false
	}`)

	server := NewServer(Config{
		BigQueryEndpoint:    "http://127.0.0.1:9050",
		BigQueryProject:     "devcloud",
		BigQueryLocation:    "US",
		BigQueryAuthMode:    "bearer-dev",
		BigQueryStoragePath: ".devcloud/test/bigquery",
	}, newDashboardStore(nil, nil))
	server.SetBigQuery(bq)
	routes := server.routes()

	page := performRequest(routes, http.MethodGet, "/dashboard/bigquery")
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "devcloud Dashboard") {
		t.Fatalf("BigQuery dashboard route changed: status=%d body=%s", page.Code, page.Body.String())
	}
	compatPage := performRequest(routes, http.MethodGet, "/bigquery")
	if compatPage.Code != http.StatusOK || !strings.Contains(compatPage.Body.String(), "devcloud Dashboard") {
		t.Fatalf("BigQuery compat route changed: status=%d body=%s", compatPage.Code, compatPage.Body.String())
	}

	status := performRequest(routes, http.MethodGet, "/api/bigquery/status")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"service":"bigquery"`) || !strings.Contains(status.Body.String(), `"running":true`) || !strings.Contains(status.Body.String(), `"authMode":"bearer-dev"`) {
		t.Fatalf("BigQuery status = %d body=%s", status.Code, status.Body.String())
	}
	projects := performRequest(routes, http.MethodGet, "/api/services")
	if projects.Code != http.StatusOK || !strings.Contains(projects.Body.String(), `"id":"bigquery"`) {
		t.Fatalf("service alias = %d body=%s", projects.Code, projects.Body.String())
	}
	projectList := performRequest(routes, http.MethodGet, "/api/bigquery/projects")
	if projectList.Code != http.StatusOK || !strings.Contains(projectList.Body.String(), `"projectId":"devcloud"`) {
		t.Fatalf("BigQuery projects = %d body=%s", projectList.Code, projectList.Body.String())
	}
	datasets := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets")
	if datasets.Code != http.StatusOK || !strings.Contains(datasets.Body.String(), `"datasetId":"analytics"`) {
		t.Fatalf("BigQuery datasets = %d body=%s", datasets.Code, datasets.Body.String())
	}
	tables := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets/analytics/tables")
	if tables.Code != http.StatusOK || !strings.Contains(tables.Body.String(), `"tableId":"people"`) {
		t.Fatalf("BigQuery tables = %d body=%s", tables.Code, tables.Body.String())
	}
	table := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets/analytics/tables/people")
	if table.Code != http.StatusOK || !strings.Contains(table.Body.String(), `"tableId":"people"`) || !strings.Contains(table.Body.String(), `"numRows":"1"`) {
		t.Fatalf("BigQuery table detail = %d body=%s", table.Code, table.Body.String())
	}
	schema := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets/analytics/tables/people/schema")
	if schema.Code != http.StatusOK || !strings.Contains(schema.Body.String(), `"name":"age"`) || !strings.Contains(schema.Body.String(), `"type":"INTEGER"`) {
		t.Fatalf("BigQuery schema = %d body=%s", schema.Code, schema.Body.String())
	}
	rows := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/datasets/analytics/tables/people/rows?limit=1")
	if rows.Code != http.StatusOK || !strings.Contains(rows.Body.String(), `"name":"Ada"`) {
		t.Fatalf("BigQuery rows = %d body=%s", rows.Code, rows.Body.String())
	}
	jobs := performRequest(routes, http.MethodGet, "/api/bigquery/projects/devcloud/jobs")
	if jobs.Code != http.StatusOK || !strings.Contains(jobs.Body.String(), `"state":"DONE"`) {
		t.Fatalf("BigQuery jobs = %d body=%s", jobs.Code, jobs.Body.String())
	}
	query := performRequestWithBody(routes, http.MethodPost, "/api/bigquery/projects/devcloud/queries", `{
		"query":"SELECT id, name FROM `+"`devcloud.analytics.people`"+` WHERE age >= 30",
		"useLegacySql":false
	}`)
	if query.Code != http.StatusOK || !strings.Contains(query.Body.String(), `"kind":"bigquery#queryResponse"`) || !strings.Contains(query.Body.String(), `"totalRows":"1"`) {
		t.Fatalf("BigQuery dashboard query = %d body=%s", query.Code, query.Body.String())
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

func TestGCSDashboardPageAndAPIExposeObjects(t *testing.T) {
	gcsStore := s3svc.NewFileBucketStore(t.TempDir())
	sessionDir := t.TempDir()
	sessionID := "session-test"
	if err := os.MkdirAll(filepath.Join(sessionDir, sessionID), 0o755); err != nil {
		t.Fatalf("create upload session dir: %v", err)
	}
	sessionCreatedAt := time.Date(2026, 5, 1, 10, 30, 0, 0, time.UTC)
	sessionJSON := `{"Bucket":"demo-bucket","Name":"docs/resumable.txt","ContentType":"text/plain","CreatedAt":"` + sessionCreatedAt.Format(time.RFC3339Nano) + `","ReceivedBytes":9}` + "\n"
	if err := os.WriteFile(filepath.Join(sessionDir, sessionID, "session.json"), []byte(sessionJSON), 0o644); err != nil {
		t.Fatalf("write upload session: %v", err)
	}
	if _, created, err := gcsStore.CreateBucket(context.Background(), "demo-bucket"); err != nil || !created {
		t.Fatalf("create bucket created=%t err=%v", created, err)
	}
	if _, err := gcsStore.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:      "demo-bucket",
		Key:         "docs/readme.txt",
		Body:        strings.NewReader("hello from gcs dashboard\n"),
		ContentType: "text/plain",
		Metadata:    map[string]string{"source": "gcs-dashboard-test"},
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}
	if _, found, err := gcsStore.UpdateObjectMetadata(context.Background(), s3svc.UpdateObjectMetadataInput{
		Bucket:      "demo-bucket",
		Key:         "docs/readme.txt",
		ContentType: "text/markdown",
		Metadata:    map[string]string{"source": "gcs-dashboard-test", "owner": "dashboard"},
	}); err != nil || !found {
		t.Fatalf("update object metadata found=%t err=%v", found, err)
	}
	routes := NewServer(Config{
		GCSEndpoint:          "http://127.0.0.1:4443",
		GCSProject:           "devcloud",
		GCSStoragePath:       ".devcloud/data/s3",
		GCSUploadSessionPath: sessionDir,
	}, newDashboardStore(nil, nil), nil, gcsStore).routes()

	page := performRequest(routes, http.MethodGet, "/gcs")
	if page.Code != http.StatusOK {
		t.Fatalf("gcs page status = %d, want %d", page.Code, http.StatusOK)
	}
	if body := page.Body.String(); !strings.Contains(body, "devcloud GCS") || !strings.Contains(body, "/api/gcs/buckets") {
		t.Fatalf("gcs page missing expected shell: %s", body)
	}

	status := performRequest(routes, http.MethodGet, "/api/gcs/status")
	if status.Code != http.StatusOK {
		t.Fatalf("gcs status code = %d, want %d", status.Code, http.StatusOK)
	}
	if !strings.Contains(status.Body.String(), `"running"`) || !strings.Contains(status.Body.String(), `"project":"devcloud"`) {
		t.Fatalf("gcs status missing running project: %s", status.Body.String())
	}

	buckets := performRequest(routes, http.MethodGet, "/api/gcs/buckets")
	if buckets.Code != http.StatusOK {
		t.Fatalf("gcs buckets code = %d, want %d", buckets.Code, http.StatusOK)
	}
	if !strings.Contains(buckets.Body.String(), "demo-bucket") || !strings.Contains(buckets.Body.String(), "gs://demo-bucket") {
		t.Fatalf("gcs buckets missing bucket: %s", buckets.Body.String())
	}

	objects := performRequest(routes, http.MethodGet, "/api/gcs/buckets/demo-bucket/objects?prefix=docs/")
	if objects.Code != http.StatusOK {
		t.Fatalf("gcs objects code = %d, want %d", objects.Code, http.StatusOK)
	}
	body := objects.Body.String()
	for _, want := range []string{"docs/readme.txt", `"contentType":"text/markdown"`, `"source":"gcs-dashboard-test"`, `"owner":"dashboard"`, "gs://demo-bucket/docs/readme.txt", `"generation"`, `"metageneration":"2"`, `"crc32c"`, `"storageClass":"STANDARD"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("gcs objects missing %q: %s", want, body)
		}
	}

	detail := performRequest(routes, http.MethodGet, "/api/gcs/buckets/demo-bucket/objects/docs/readme.txt")
	if detail.Code != http.StatusOK {
		t.Fatalf("gcs object detail code = %d, want %d; body=%s", detail.Code, http.StatusOK, detail.Body.String())
	}
	for _, want := range []string{"docs/readme.txt", `"contentType":"text/markdown"`, `"storageClass":"STANDARD"`, `"metageneration":"2"`} {
		if !strings.Contains(detail.Body.String(), want) {
			t.Fatalf("gcs object detail missing %q: %s", want, detail.Body.String())
		}
	}

	download := performRequest(routes, http.MethodGet, "/api/gcs/buckets/demo-bucket/objects/docs/readme.txt/download")
	if download.Code != http.StatusOK {
		t.Fatalf("gcs download code = %d, want %d", download.Code, http.StatusOK)
	}
	if got := download.Body.String(); got != "hello from gcs dashboard\n" {
		t.Fatalf("gcs download body = %q", got)
	}
	if got := download.Header().Get("x-goog-meta-source"); got != "gcs-dashboard-test" {
		t.Fatalf("gcs download metadata = %q, want gcs-dashboard-test", got)
	}

	deleteObject := performRequest(routes, http.MethodDelete, "/api/gcs/buckets/demo-bucket/objects/docs/readme.txt")
	if deleteObject.Code != http.StatusNoContent {
		t.Fatalf("gcs object delete code = %d, want %d; body=%s", deleteObject.Code, http.StatusNoContent, deleteObject.Body.String())
	}
	deletedObject := performRequest(routes, http.MethodGet, "/api/gcs/buckets/demo-bucket/objects/docs/readme.txt")
	if deletedObject.Code != http.StatusNotFound {
		t.Fatalf("deleted gcs object code = %d, want %d; body=%s", deletedObject.Code, http.StatusNotFound, deletedObject.Body.String())
	}

	createBucket := performRequestWithBody(routes, http.MethodPost, "/api/gcs/buckets", `{"name":"dashboard-created"}`)
	if createBucket.Code != http.StatusCreated {
		t.Fatalf("gcs bucket create code = %d, want %d; body=%s", createBucket.Code, http.StatusCreated, createBucket.Body.String())
	}
	if !strings.Contains(createBucket.Body.String(), `"name":"dashboard-created"`) {
		t.Fatalf("gcs bucket create missing name: %s", createBucket.Body.String())
	}
	deleteBucket := performRequest(routes, http.MethodDelete, "/api/gcs/buckets/dashboard-created")
	if deleteBucket.Code != http.StatusNoContent {
		t.Fatalf("gcs bucket delete code = %d, want %d; body=%s", deleteBucket.Code, http.StatusNoContent, deleteBucket.Body.String())
	}

	sessions := performRequest(routes, http.MethodGet, "/api/gcs/upload-sessions")
	if sessions.Code != http.StatusOK {
		t.Fatalf("gcs upload sessions code = %d, want %d", sessions.Code, http.StatusOK)
	}
	for _, want := range []string{`"id":"session-test"`, `"bucket":"demo-bucket"`, `"name":"docs/resumable.txt"`, `"receivedBytes":9`} {
		if !strings.Contains(sessions.Body.String(), want) {
			t.Fatalf("gcs upload sessions missing %q: %s", want, sessions.Body.String())
		}
	}

	uploads := performRequest(routes, http.MethodGet, "/api/gcs/uploads")
	if uploads.Code != http.StatusOK {
		t.Fatalf("gcs uploads alias code = %d, want %d", uploads.Code, http.StatusOK)
	}
	if !strings.Contains(uploads.Body.String(), `"id":"session-test"`) {
		t.Fatalf("gcs uploads alias missing session: %s", uploads.Body.String())
	}

	deleteSession := performRequest(routes, http.MethodDelete, "/api/gcs/uploads/session-test")
	if deleteSession.Code != http.StatusNoContent {
		t.Fatalf("gcs upload session delete code = %d, want %d", deleteSession.Code, http.StatusNoContent)
	}
	if _, err := os.Stat(filepath.Join(sessionDir, sessionID)); !os.IsNotExist(err) {
		t.Fatalf("upload session dir still exists or stat failed: %v", err)
	}

	rejectTraversal := performRequest(routes, http.MethodDelete, "/api/gcs/uploads/..%2Foutside")
	if rejectTraversal.Code != http.StatusNotFound {
		t.Fatalf("gcs upload session traversal code = %d, want %d", rejectTraversal.Code, http.StatusNotFound)
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
