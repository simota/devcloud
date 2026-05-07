package pubsub

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRESTRejectsPublishWithInvalidMessageData(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	invalidBase64 := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"not base64"}]
	}`)
	if invalidBase64.Code != http.StatusBadRequest {
		t.Fatalf("invalid base64 status = %d, want %d: %s", invalidBase64.Code, http.StatusBadRequest, invalidBase64.Body.String())
	}

	emptyMessage := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{}]
	}`)
	if emptyMessage.Code != http.StatusBadRequest {
		t.Fatalf("empty message status = %d, want %d: %s", emptyMessage.Code, http.StatusBadRequest, emptyMessage.Body.String())
	}

	emptyAttributeKey := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"attributes":{"":"do-not-leak"}}]
	}`)
	if emptyAttributeKey.Code != http.StatusBadRequest {
		t.Fatalf("empty attribute key status = %d, want %d: %s", emptyAttributeKey.Code, http.StatusBadRequest, emptyAttributeKey.Body.String())
	}
	if strings.Contains(emptyAttributeKey.Body.String(), "do-not-leak") {
		t.Fatalf("publish validation leaked attribute value: %s", emptyAttributeKey.Body.String())
	}

	attributeOnly := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"attributes":{"kind":"signal"}}]
	}`)
	if attributeOnly.Code != http.StatusOK {
		t.Fatalf("attribute-only status = %d, want %d: %s", attributeOnly.Code, http.StatusOK, attributeOnly.Body.String())
	}
}

func TestRESTPublishDoesNotRetainMessagesWithoutMatchingSubscriptions(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")

	publish := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"bm8tc3Vic2NyaWJlcnM="}]
	}`)
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want %d: %s", publish.Code, http.StatusOK, publish.Body.String())
	}
	var response struct {
		MessageIDs []string `json:"messageIds"`
	}
	if err := json.NewDecoder(publish.Body).Decode(&response); err != nil {
		t.Fatalf("decode publish: %v", err)
	}
	if len(response.MessageIDs) != 1 {
		t.Fatalf("messageIds = %#v, want one id", response.MessageIDs)
	}
	if _, found := server.MessageSnapshot(response.MessageIDs[0]); found {
		t.Fatalf("unreferenced published message %q was retained", response.MessageIDs[0])
	}
}

func TestRESTPersistsResources(t *testing.T) {
	dir := t.TempDir()
	server := NewServer(Config{Project: "devcloud", StoragePath: filepath.Join(dir, "pubsub")})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)

	reloaded := NewServer(Config{Project: "devcloud", StoragePath: filepath.Join(dir, "pubsub")})
	getTopic := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/topics/orders", "")
	if getTopic.Code != http.StatusOK {
		t.Fatalf("reloaded topic status = %d, want %d", getTopic.Code, http.StatusOK)
	}
	getSubscription := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if getSubscription.Code != http.StatusOK {
		t.Fatalf("reloaded subscription status = %d, want %d", getSubscription.Code, http.StatusOK)
	}
}

func TestRESTPublishAcceptsProtoJSONBase64Variants(t *testing.T) {
	server := NewServer(Config{Project: "devcloud", MaxPullMessages: 10})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)

	publish := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"aGk"},
			{"data":"c3RhbmRhcmQ"}
		]
	}`)
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want %d: %s", publish.Code, http.StatusOK, publish.Body.String())
	}

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":10}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), `"data":"aGk"`) || !strings.Contains(pull.Body.String(), `"data":"c3RhbmRhcmQ"`) {
		t.Fatalf("pull response did not preserve accepted base64 spellings: %s", pull.Body.String())
	}
}

func TestRESTPersistsMessagesDeliveriesAndAckState(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server := NewServer(Config{
		Project:                   "devcloud",
		StoragePath:               storagePath,
		DefaultAckDeadlineSeconds: 2,
	})
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"enableExactlyOnceDelivery":true
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cGVyc2lzdGVk"}]}`)

	reloaded := NewServer(Config{
		Project:                   "devcloud",
		StoragePath:               storagePath,
		DefaultAckDeadlineSeconds: 2,
	})
	reloaded.now = func() time.Time { return now }
	getSubscription := performPubSubRequest(reloaded, http.MethodGet, "/v1/projects/devcloud/subscriptions/orders-sub", "")
	if getSubscription.Code != http.StatusOK {
		t.Fatalf("get reloaded subscription status = %d, want %d: %s", getSubscription.Code, http.StatusOK, getSubscription.Body.String())
	}
	var reloadedSubscription subscriptionResource
	if err := json.NewDecoder(getSubscription.Body).Decode(&reloadedSubscription); err != nil {
		t.Fatalf("decode reloaded subscription: %v", err)
	}
	if !reloadedSubscription.EnableExactlyOnceDelivery {
		t.Fatalf("exactly-once metadata was not persisted: %#v", reloadedSubscription)
	}
	pull := performPubSubRequest(reloaded, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	var pulled struct {
		ReceivedMessages []struct {
			AckID   string `json:"ackId"`
			Message struct {
				Data      string `json:"data"`
				MessageID string `json:"messageId"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(pull.Body).Decode(&pulled); err != nil {
		t.Fatalf("decode pull: %v", err)
	}
	if len(pulled.ReceivedMessages) != 1 || pulled.ReceivedMessages[0].Message.Data != "cGVyc2lzdGVk" {
		t.Fatalf("receivedMessages = %#v", pulled.ReceivedMessages)
	}

	ack := performPubSubRequest(reloaded, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":["`+pulled.ReceivedMessages[0].AckID+`"]}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusOK, ack.Body.String())
	}

	ackedReload := NewServer(Config{
		Project:                   "devcloud",
		StoragePath:               storagePath,
		DefaultAckDeadlineSeconds: 2,
	})
	ackedReload.now = func() time.Time { return now.Add(3 * time.Second) }
	emptyPull := performPubSubRequest(ackedReload, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(emptyPull.Body.String(), "receivedMessages") {
		t.Fatalf("acked message should stay acknowledged after reload, got %s", emptyPull.Body.String())
	}

	publishAgain := performPubSubRequest(ackedReload, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"bmV4dA=="}]}`)
	if publishAgain.Code != http.StatusOK {
		t.Fatalf("second publish status = %d, want %d: %s", publishAgain.Code, http.StatusOK, publishAgain.Body.String())
	}
	var published struct {
		MessageIDs []string `json:"messageIds"`
	}
	if err := json.NewDecoder(publishAgain.Body).Decode(&published); err != nil {
		t.Fatalf("decode second publish: %v", err)
	}
	if len(published.MessageIDs) != 1 || published.MessageIDs[0] == pulled.ReceivedMessages[0].Message.MessageID {
		t.Fatalf("message IDs did not advance after reload: first=%q next=%#v", pulled.ReceivedMessages[0].Message.MessageID, published.MessageIDs)
	}
}

func TestRESTPersistsMessageStateInSeparateMessageStoragePath(t *testing.T) {
	dir := t.TempDir()
	storagePath := filepath.Join(dir, "pubsub")
	messageStoragePath := filepath.Join(dir, "message")
	server := NewServer(Config{
		Project:            "devcloud",
		StoragePath:        storagePath,
		MessageStoragePath: messageStoragePath,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"c2VwYXJhdGUtc3RhdGU=","attributes":{"kind":"local"}}]
	}`)

	resourceData, err := os.ReadFile(filepath.Join(storagePath, "resources.json"))
	if err != nil {
		t.Fatalf("read resource state: %v", err)
	}
	if strings.Contains(string(resourceData), "c2VwYXJhdGUtc3RhdGU=") || strings.Contains(string(resourceData), `"deliveries"`) {
		t.Fatal("resource state should not contain message delivery data")
	}
	messageData, err := os.ReadFile(filepath.Join(messageStoragePath, "pubsub.json"))
	if err != nil {
		t.Fatalf("read message state: %v", err)
	}
	if !strings.Contains(string(messageData), "c2VwYXJhdGUtc3RhdGU=") || !strings.Contains(string(messageData), `"deliveries"`) {
		t.Fatal("message state missing persisted delivery data")
	}

	reloaded := NewServer(Config{
		Project:            "devcloud",
		StoragePath:        storagePath,
		MessageStoragePath: messageStoragePath,
	})
	pull := performPubSubRequest(reloaded, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK || !strings.Contains(pull.Body.String(), "c2VwYXJhdGUtc3RhdGU=") {
		t.Fatalf("pull after separate reload = status %d body %s", pull.Code, pull.Body.String())
	}
}

func TestRESTPublishPullAckAndModifyAckDeadline(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxPullMessages:           10,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2
	}`)

	publish := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"aGVsbG8=","attributes":{"kind":"test"},"orderingKey":"group-1"}]
	}`)
	if publish.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want %d: %s", publish.Code, http.StatusOK, publish.Body.String())
	}
	var publishBody struct {
		MessageIDs []string `json:"messageIds"`
	}
	if err := json.NewDecoder(publish.Body).Decode(&publishBody); err != nil {
		t.Fatalf("decode publish: %v", err)
	}
	if len(publishBody.MessageIDs) != 1 || publishBody.MessageIDs[0] == "" {
		t.Fatalf("messageIds = %#v", publishBody.MessageIDs)
	}

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if firstPull.Code != http.StatusOK {
		t.Fatalf("first pull status = %d, want %d: %s", firstPull.Code, http.StatusOK, firstPull.Body.String())
	}
	var firstPullBody struct {
		ReceivedMessages []struct {
			AckID           string `json:"ackId"`
			DeliveryAttempt int    `json:"deliveryAttempt"`
			Message         struct {
				Data        string            `json:"data"`
				Attributes  map[string]string `json:"attributes"`
				MessageID   string            `json:"messageId"`
				PublishTime string            `json:"publishTime"`
				OrderingKey string            `json:"orderingKey"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(firstPull.Body).Decode(&firstPullBody); err != nil {
		t.Fatalf("decode first pull: %v", err)
	}
	if len(firstPullBody.ReceivedMessages) != 1 {
		t.Fatalf("receivedMessages = %#v", firstPullBody.ReceivedMessages)
	}
	message := firstPullBody.ReceivedMessages[0]
	if message.AckID == "" || message.DeliveryAttempt != 1 {
		t.Fatalf("received message lease = %#v", message)
	}
	if message.Message.Data != "aGVsbG8=" || message.Message.Attributes["kind"] != "test" || message.Message.OrderingKey != "group-1" {
		t.Fatalf("received message payload = %#v", message.Message)
	}

	invisible := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if invisible.Code != http.StatusOK {
		t.Fatalf("invisible pull status = %d, want %d", invisible.Code, http.StatusOK)
	}
	if strings.Contains(invisible.Body.String(), "receivedMessages") {
		t.Fatalf("leased message should be invisible, got %s", invisible.Body.String())
	}

	release := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyAckDeadline", `{
		"ackIds":["`+message.AckID+`"],
		"ackDeadlineSeconds":0
	}`)
	if release.Code != http.StatusOK {
		t.Fatalf("modifyAckDeadline status = %d, want %d: %s", release.Code, http.StatusOK, release.Body.String())
	}
	redelivery := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var redeliveryBody struct {
		ReceivedMessages []struct {
			AckID           string `json:"ackId"`
			DeliveryAttempt int    `json:"deliveryAttempt"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(redelivery.Body).Decode(&redeliveryBody); err != nil {
		t.Fatalf("decode redelivery: %v", err)
	}
	if len(redeliveryBody.ReceivedMessages) != 1 || redeliveryBody.ReceivedMessages[0].DeliveryAttempt != 2 {
		t.Fatalf("redelivery = %#v", redeliveryBody.ReceivedMessages)
	}

	ack := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{
		"ackIds":["`+redeliveryBody.ReceivedMessages[0].AckID+`"]
	}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusOK, ack.Body.String())
	}
	now = now.Add(3 * time.Second)
	afterAck := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(afterAck.Body.String(), "receivedMessages") {
		t.Fatalf("acked message should not be received, got %s", afterAck.Body.String())
	}
}

func TestRESTPullWaitsWhenReturnImmediatelyIsFalse(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		PullWaitTimeout:           500 * time.Millisecond,
	})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)

	published := make(chan struct{})
	go func() {
		defer close(published)
		time.Sleep(20 * time.Millisecond)
		performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
			"messages":[{"data":"bG9uZy1wb2xs"}]
		}`)
	}()

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{
		"maxMessages":1,
		"returnImmediately":false
	}`)
	<-published
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), "bG9uZy1wb2xs") {
		t.Fatalf("waited pull did not receive published message: %s", pull.Body.String())
	}
}

func TestRESTRejectsExpiredAckID(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"ZXhwaXJlZA=="}]}`)

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var firstPullBody struct {
		ReceivedMessages []struct {
			AckID string `json:"ackId"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(firstPull.Body).Decode(&firstPullBody); err != nil {
		t.Fatalf("decode first pull: %v", err)
	}
	if len(firstPullBody.ReceivedMessages) != 1 || firstPullBody.ReceivedMessages[0].AckID == "" {
		t.Fatalf("first pull = %#v", firstPullBody.ReceivedMessages)
	}
	expiredAckID := firstPullBody.ReceivedMessages[0].AckID

	now = now.Add(3 * time.Second)
	staleAck := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":["`+expiredAckID+`"]}`)
	if staleAck.Code != http.StatusOK {
		t.Fatalf("stale ack status = %d, want %d: %s", staleAck.Code, http.StatusOK, staleAck.Body.String())
	}

	redelivery := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var redeliveryBody struct {
		ReceivedMessages []struct {
			AckID           string `json:"ackId"`
			DeliveryAttempt int    `json:"deliveryAttempt"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(redelivery.Body).Decode(&redeliveryBody); err != nil {
		t.Fatalf("decode redelivery: %v", err)
	}
	if len(redeliveryBody.ReceivedMessages) != 1 || redeliveryBody.ReceivedMessages[0].AckID == expiredAckID || redeliveryBody.ReceivedMessages[0].DeliveryAttempt != 2 {
		t.Fatalf("redelivery = %#v, expired ackID = %q", redeliveryBody.ReceivedMessages, expiredAckID)
	}
}

func TestRESTModifyAckDeadlineExtendsLease(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxAckDeadlineSeconds:     10,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"ZXh0ZW5k"}]}`)

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if firstPull.Code != http.StatusOK {
		t.Fatalf("first pull status = %d, want %d: %s", firstPull.Code, http.StatusOK, firstPull.Body.String())
	}
	var first struct {
		ReceivedMessages []struct {
			AckID string `json:"ackId"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(firstPull.Body).Decode(&first); err != nil {
		t.Fatalf("decode first pull: %v", err)
	}
	if len(first.ReceivedMessages) != 1 || first.ReceivedMessages[0].AckID == "" {
		t.Fatalf("first pull receivedMessages = %#v", first.ReceivedMessages)
	}

	modify := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyAckDeadline", `{
		"ackIds":["`+first.ReceivedMessages[0].AckID+`"],
		"ackDeadlineSeconds":5
	}`)
	if modify.Code != http.StatusOK {
		t.Fatalf("modify status = %d, want %d: %s", modify.Code, http.StatusOK, modify.Body.String())
	}

	now = now.Add(3 * time.Second)
	stillLeased := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if stillLeased.Code != http.StatusOK {
		t.Fatalf("still leased pull status = %d, want %d: %s", stillLeased.Code, http.StatusOK, stillLeased.Body.String())
	}
	if strings.Contains(stillLeased.Body.String(), "receivedMessages") {
		t.Fatalf("extended lease should hide message past original deadline, got %s", stillLeased.Body.String())
	}

	now = now.Add(3 * time.Second)
	redelivery := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var second struct {
		ReceivedMessages []struct {
			DeliveryAttempt int `json:"deliveryAttempt"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(redelivery.Body).Decode(&second); err != nil {
		t.Fatalf("decode redelivery: %v", err)
	}
	if len(second.ReceivedMessages) != 1 || second.ReceivedMessages[0].DeliveryAttempt != 2 {
		t.Fatalf("redelivery after extended deadline = %#v", second.ReceivedMessages)
	}
}

func TestRESTRejectsEmptyAckIDsWithoutAckingBacklog(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"c3RpbGwtYmFja2xvZw=="}]
	}`)

	ack := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":[""]}`)
	if ack.Code != http.StatusBadRequest {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusBadRequest, ack.Body.String())
	}
	if !strings.Contains(ack.Body.String(), "INVALID_ARGUMENT") {
		t.Fatalf("ack error body = %s", ack.Body.String())
	}

	modify := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:modifyAckDeadline", `{
		"ackIds":[" "],
		"ackDeadlineSeconds":0
	}`)
	if modify.Code != http.StatusBadRequest {
		t.Fatalf("modify status = %d, want %d: %s", modify.Code, http.StatusBadRequest, modify.Body.String())
	}

	pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if pull.Code != http.StatusOK {
		t.Fatalf("pull status = %d, want %d: %s", pull.Code, http.StatusOK, pull.Body.String())
	}
	if !strings.Contains(pull.Body.String(), "c3RpbGwtYmFja2xvZw==") {
		t.Fatalf("empty ack ID should not remove backlog, got %s", pull.Body.String())
	}
}

func TestRESTRetryPolicyDelaysRedeliveryAfterAckDeadline(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
		MaxPullMessages:           10,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2,
		"retryPolicy":{"minimumBackoff":"5s","maximumBackoff":"10s"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"cmV0cnk="}]}`)

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var first struct {
		ReceivedMessages []struct {
			DeliveryAttempt int `json:"deliveryAttempt"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(firstPull.Body).Decode(&first); err != nil {
		t.Fatalf("decode first pull: %v", err)
	}
	if len(first.ReceivedMessages) != 1 || first.ReceivedMessages[0].DeliveryAttempt != 1 {
		t.Fatalf("first receivedMessages = %#v", first.ReceivedMessages)
	}

	now = now.Add(3 * time.Second)
	beforeBackoff := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(beforeBackoff.Body.String(), "receivedMessages") {
		t.Fatalf("retryPolicy.minimumBackoff should delay redelivery, got %s", beforeBackoff.Body.String())
	}

	now = now.Add(5 * time.Second)
	afterBackoff := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	var second struct {
		ReceivedMessages []struct {
			DeliveryAttempt int `json:"deliveryAttempt"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(afterBackoff.Body).Decode(&second); err != nil {
		t.Fatalf("decode second pull: %v", err)
	}
	if len(second.ReceivedMessages) != 1 || second.ReceivedMessages[0].DeliveryAttempt != 2 {
		t.Fatalf("second receivedMessages = %#v", second.ReceivedMessages)
	}
}

func TestRESTPublishFansOutPerSubscription(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-a", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-b", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"ZmFub3V0"}]}`)

	for _, subscription := range []string{"orders-a", "orders-b"} {
		pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/"+subscription+":pull", `{"maxMessages":1}`)
		if pull.Code != http.StatusOK {
			t.Fatalf("%s pull status = %d, want %d", subscription, pull.Code, http.StatusOK)
		}
		if !strings.Contains(pull.Body.String(), "ZmFub3V0") {
			t.Fatalf("%s did not receive fan-out message: %s", subscription, pull.Body.String())
		}
	}
}

func TestRESTAckDoesNotRemoveMessageForOtherSubscriptions(t *testing.T) {
	server := NewServer(Config{Project: "devcloud"})
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-a", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-b", `{"topic":"projects/devcloud/topics/orders"}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{"messages":[{"data":"c2hhcmVk"}]}`)

	pullA := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-a:pull", `{"maxMessages":1}`)
	var pulledA struct {
		ReceivedMessages []struct {
			AckID string `json:"ackId"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(pullA.Body).Decode(&pulledA); err != nil {
		t.Fatalf("decode pull A: %v", err)
	}
	if len(pulledA.ReceivedMessages) != 1 {
		t.Fatalf("pull A receivedMessages = %#v", pulledA.ReceivedMessages)
	}
	ackA := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-a:acknowledge", `{"ackIds":["`+pulledA.ReceivedMessages[0].AckID+`"]}`)
	if ackA.Code != http.StatusOK {
		t.Fatalf("ack A status = %d, want %d: %s", ackA.Code, http.StatusOK, ackA.Body.String())
	}

	pullB := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-b:pull", `{"maxMessages":1}`)
	if pullB.Code != http.StatusOK {
		t.Fatalf("pull B status = %d, want %d: %s", pullB.Code, http.StatusOK, pullB.Body.String())
	}
	if !strings.Contains(pullB.Body.String(), "c2hhcmVk") {
		t.Fatalf("subscription B lost fan-out message after A ack: %s", pullB.Body.String())
	}
}

func TestRESTPullRespectsOrderingKeyGate(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 10,
		MaxPullMessages:           10,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"enableMessageOrdering":true
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"Zmlyc3Q=","orderingKey":"customer-1"},
			{"data":"c2Vjb25k","orderingKey":"customer-1"},
			{"data":"b3RoZXI=","orderingKey":"customer-2"}
		]
	}`)

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":3}`)
	if firstPull.Code != http.StatusOK {
		t.Fatalf("first pull status = %d, want %d: %s", firstPull.Code, http.StatusOK, firstPull.Body.String())
	}
	var first struct {
		ReceivedMessages []struct {
			AckID   string `json:"ackId"`
			Message struct {
				Data        string `json:"data"`
				OrderingKey string `json:"orderingKey"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(firstPull.Body).Decode(&first); err != nil {
		t.Fatalf("decode first pull: %v", err)
	}
	if len(first.ReceivedMessages) != 2 {
		t.Fatalf("first pull receivedMessages = %#v", first.ReceivedMessages)
	}
	if first.ReceivedMessages[0].Message.Data != "Zmlyc3Q=" || first.ReceivedMessages[1].Message.Data != "b3RoZXI=" {
		t.Fatalf("ordering gate delivered wrong messages: %#v", first.ReceivedMessages)
	}
	if first.ReceivedMessages[0].Message.OrderingKey != "customer-1" || first.ReceivedMessages[1].Message.OrderingKey != "customer-2" {
		t.Fatalf("ordering keys = %#v", first.ReceivedMessages)
	}

	ack := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:acknowledge", `{"ackIds":["`+first.ReceivedMessages[0].AckID+`"]}`)
	if ack.Code != http.StatusOK {
		t.Fatalf("ack status = %d, want %d: %s", ack.Code, http.StatusOK, ack.Body.String())
	}
	secondPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":3}`)
	var second struct {
		ReceivedMessages []struct {
			Message struct {
				Data        string `json:"data"`
				OrderingKey string `json:"orderingKey"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(secondPull.Body).Decode(&second); err != nil {
		t.Fatalf("decode second pull: %v", err)
	}
	if len(second.ReceivedMessages) != 1 || second.ReceivedMessages[0].Message.Data != "c2Vjb25k" || second.ReceivedMessages[0].Message.OrderingKey != "customer-1" {
		t.Fatalf("second pull receivedMessages = %#v", second.ReceivedMessages)
	}
}

func TestRESTOrderingKeyGateBlocksBehindRetryDelay(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 1,
		MaxPullMessages:           10,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":1,
		"enableMessageOrdering":true,
		"retryPolicy":{"minimumBackoff":"5s"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[
			{"data":"Zmlyc3Q=","orderingKey":"customer-1"},
			{"data":"c2Vjb25k","orderingKey":"customer-1"}
		]
	}`)

	firstPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if firstPull.Code != http.StatusOK || !strings.Contains(firstPull.Body.String(), "Zmlyc3Q=") {
		t.Fatalf("first pull = status %d body %s", firstPull.Code, firstPull.Body.String())
	}

	now = now.Add(2 * time.Second)
	blockedPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":2}`)
	if blockedPull.Code != http.StatusOK {
		t.Fatalf("blocked pull status = %d, want %d: %s", blockedPull.Code, http.StatusOK, blockedPull.Body.String())
	}
	if strings.Contains(blockedPull.Body.String(), "receivedMessages") || strings.Contains(blockedPull.Body.String(), "c2Vjb25k") {
		t.Fatalf("later ordering key message bypassed retry-delayed predecessor: %s", blockedPull.Body.String())
	}

	now = now.Add(4 * time.Second)
	redeliveryPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":2}`)
	if redeliveryPull.Code != http.StatusOK || !strings.Contains(redeliveryPull.Body.String(), "Zmlyc3Q=") || strings.Contains(redeliveryPull.Body.String(), "c2Vjb25k") {
		t.Fatalf("redelivery pull = status %d body %s", redeliveryPull.Code, redeliveryPull.Body.String())
	}
}

func TestRESTDeadLetterPolicyTransfersAfterMaxDeliveryAttempts(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 2,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders-dlq", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":2,
		"deadLetterPolicy":{"deadLetterTopic":"projects/devcloud/topics/orders-dlq","maxDeliveryAttempts":5}
	}`)
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-dlq-sub", `{
		"topic":"projects/devcloud/topics/orders-dlq"
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"ZGxx","attributes":{"kind":"retry"},"orderingKey":"group-1"}]
	}`)

	for attempt := 1; attempt <= 5; attempt++ {
		pull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
		if pull.Code != http.StatusOK {
			t.Fatalf("attempt %d pull status = %d, want %d: %s", attempt, pull.Code, http.StatusOK, pull.Body.String())
		}
		var pulled struct {
			ReceivedMessages []struct {
				DeliveryAttempt int `json:"deliveryAttempt"`
			} `json:"receivedMessages"`
		}
		if err := json.NewDecoder(pull.Body).Decode(&pulled); err != nil {
			t.Fatalf("decode attempt %d pull: %v", attempt, err)
		}
		if len(pulled.ReceivedMessages) != 1 || pulled.ReceivedMessages[0].DeliveryAttempt != attempt {
			t.Fatalf("attempt %d receivedMessages = %#v", attempt, pulled.ReceivedMessages)
		}
		now = now.Add(3 * time.Second)
	}

	mainAfterLimit := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(mainAfterLimit.Body.String(), "receivedMessages") {
		t.Fatalf("main subscription should not receive message after dead-letter transfer: %s", mainAfterLimit.Body.String())
	}

	dlqPull := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-dlq-sub:pull", `{"maxMessages":1}`)
	if dlqPull.Code != http.StatusOK {
		t.Fatalf("dlq pull status = %d, want %d: %s", dlqPull.Code, http.StatusOK, dlqPull.Body.String())
	}
	var dlq struct {
		ReceivedMessages []struct {
			DeliveryAttempt int `json:"deliveryAttempt"`
			Message         struct {
				Data        string            `json:"data"`
				Attributes  map[string]string `json:"attributes"`
				OrderingKey string            `json:"orderingKey"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.NewDecoder(dlqPull.Body).Decode(&dlq); err != nil {
		t.Fatalf("decode dlq pull: %v", err)
	}
	if len(dlq.ReceivedMessages) != 1 || dlq.ReceivedMessages[0].DeliveryAttempt != 1 {
		t.Fatalf("dlq receivedMessages = %#v", dlq.ReceivedMessages)
	}
	message := dlq.ReceivedMessages[0].Message
	if message.Data != "ZGxx" || message.Attributes["kind"] != "retry" || message.OrderingKey != "group-1" {
		t.Fatalf("dlq message = %#v", message)
	}
}

func TestRESTRetryPolicyUsesCappedBackoffAfterLeaseExpiration(t *testing.T) {
	server := NewServer(Config{
		Project:                   "devcloud",
		DefaultAckDeadlineSeconds: 1,
	})
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	server.now = func() time.Time { return now }
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/topics/orders", "{}")
	performPubSubRequest(server, http.MethodPut, "/v1/projects/devcloud/subscriptions/orders-sub", `{
		"topic":"projects/devcloud/topics/orders",
		"ackDeadlineSeconds":1,
		"retryPolicy":{"minimumBackoff":"2s","maximumBackoff":"3s"}
	}`)
	performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/topics/orders:publish", `{
		"messages":[{"data":"cmV0cnk="}]
	}`)

	first := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), "cmV0cnk=") {
		t.Fatalf("first pull = status %d body %s", first.Code, first.Body.String())
	}
	now = now.Add(2 * time.Second)
	beforeMinimum := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(beforeMinimum.Body.String(), "receivedMessages") {
		t.Fatalf("message was redelivered before minimum backoff elapsed: %s", beforeMinimum.Body.String())
	}
	now = now.Add(1 * time.Second)
	second := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"deliveryAttempt":2`) {
		t.Fatalf("second pull = status %d body %s", second.Code, second.Body.String())
	}
	now = now.Add(3 * time.Second)
	beforeMaximum := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if strings.Contains(beforeMaximum.Body.String(), "receivedMessages") {
		t.Fatalf("message was redelivered before capped maximum backoff elapsed: %s", beforeMaximum.Body.String())
	}
	now = now.Add(1 * time.Second)
	third := performPubSubRequest(server, http.MethodPost, "/v1/projects/devcloud/subscriptions/orders-sub:pull", `{"maxMessages":1}`)
	if third.Code != http.StatusOK || !strings.Contains(third.Body.String(), `"deliveryAttempt":3`) {
		t.Fatalf("third pull = status %d body %s", third.Code, third.Body.String())
	}
}
