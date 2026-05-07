package dashboard

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	sqssvc "devcloud/internal/services/sqs"
)

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

func TestSQSDashboardManagementAPIsCreateQueueAndSendMessage(t *testing.T) {
	sqsServer := sqssvc.NewServer(sqssvc.Config{Addr: "127.0.0.1:9324"})
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetSQS(sqsServer)
	routes := server.routes()

	createRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues", `{
		"input":{
			"QueueName":"dashboard-managed.fifo",
			"Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true","VisibilityTimeout":"30"},
			"Tags":{"source":"dashboard"}
		}
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	if !strings.Contains(createRec.Body.String(), `"QueueUrl"`) || !strings.Contains(createRec.Body.String(), "dashboard-managed.fifo") {
		t.Fatalf("create body = %s", createRec.Body.String())
	}

	sendRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues/dashboard-managed.fifo/messages", `{
		"input":{
			"MessageBody":"dashboard send",
			"MessageGroupId":"workers",
			"MessageAttributes":{"kind":{"DataType":"String","StringValue":"test"}}
		}
	}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}
	if !strings.Contains(sendRec.Body.String(), `"MessageId"`) || !strings.Contains(sendRec.Body.String(), `"MD5OfMessageAttributes"`) {
		t.Fatalf("send body = %s", sendRec.Body.String())
	}

	messagesRec := performRequest(routes, http.MethodGet, "/api/sqs/queues/dashboard-managed.fifo/messages")
	if messagesRec.Code != http.StatusOK {
		t.Fatalf("messages status = %d, body = %s", messagesRec.Code, messagesRec.Body.String())
	}
	if !strings.Contains(messagesRec.Body.String(), `"body":"dashboard send"`) || !strings.Contains(messagesRec.Body.String(), `"messageGroupId":"workers"`) {
		t.Fatalf("messages body = %s", messagesRec.Body.String())
	}
}

func TestSQSDashboardSendMessageRejectsMismatchedQueueURL(t *testing.T) {
	sqsServer := sqssvc.NewServer(sqssvc.Config{Addr: "127.0.0.1:9324"})
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetSQS(sqsServer)
	routes := server.routes()

	createRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues", `{"input":{"QueueName":"dashboard-safe"}}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}

	sendRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues/dashboard-safe/messages", `{
		"input":{"QueueUrl":"http://127.0.0.1:9324/000000000000/other","MessageBody":"wrong queue"}
	}`)
	if sendRec.Code != http.StatusBadRequest {
		t.Fatalf("send status = %d, want %d, body = %s", sendRec.Code, http.StatusBadRequest, sendRec.Body.String())
	}
	if !strings.Contains(sendRec.Body.String(), "QueueUrl must match") {
		t.Fatalf("send body = %s", sendRec.Body.String())
	}
}

func TestSQSDashboardManagementAPIsReceiveAndDeleteMessage(t *testing.T) {
	sqsServer := sqssvc.NewServer(sqssvc.Config{Addr: "127.0.0.1:9324"})
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetSQS(sqsServer)
	routes := server.routes()

	createRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues", `{"input":{"QueueName":"dashboard-receive"}}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	sendRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues/dashboard-receive/messages", `{
		"input":{"MessageBody":"dashboard receive","MessageAttributes":{"kind":{"DataType":"String","StringValue":"test"}}}
	}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}

	receiveRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues/dashboard-receive/receive", `{
		"input":{"MaxNumberOfMessages":1,"VisibilityTimeout":30,"WaitTimeSeconds":0,"AttributeNames":["All"],"MessageAttributeNames":["All"]}
	}`)
	if receiveRec.Code != http.StatusOK {
		t.Fatalf("receive status = %d, body = %s", receiveRec.Code, receiveRec.Body.String())
	}
	var receiveBody struct {
		Messages []struct {
			MessageID     string `json:"MessageId"`
			ReceiptHandle string `json:"ReceiptHandle"`
			Body          string `json:"Body"`
		} `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 || receiveBody.Messages[0].ReceiptHandle == "" || receiveBody.Messages[0].Body != "dashboard receive" {
		t.Fatalf("receive body = %s", receiveRec.Body.String())
	}

	deleteRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues/dashboard-receive/delete", `{
		"input":{"ReceiptHandle":"`+receiveBody.Messages[0].ReceiptHandle+`"}
	}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}

	afterDeleteRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues/dashboard-receive/receive", `{
		"input":{"MaxNumberOfMessages":1,"WaitTimeSeconds":0}
	}`)
	if afterDeleteRec.Code != http.StatusOK {
		t.Fatalf("after delete status = %d, body = %s", afterDeleteRec.Code, afterDeleteRec.Body.String())
	}
	if strings.Contains(afterDeleteRec.Body.String(), "dashboard receive") {
		t.Fatalf("message was not deleted: %s", afterDeleteRec.Body.String())
	}
}

func TestSQSDashboardManagementAPIsChangeMessageVisibility(t *testing.T) {
	sqsServer := sqssvc.NewServer(sqssvc.Config{Addr: "127.0.0.1:9324"})
	server := NewServer(Config{}, newDashboardStore(nil, nil))
	server.SetSQS(sqsServer)
	routes := server.routes()

	createRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues", `{"input":{"QueueName":"dashboard-visibility"}}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	sendRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues/dashboard-visibility/messages", `{
		"input":{"MessageBody":"dashboard visibility"}
	}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}
	receiveRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues/dashboard-visibility/receive", `{
		"input":{"MaxNumberOfMessages":1,"VisibilityTimeout":30,"WaitTimeSeconds":0}
	}`)
	if receiveRec.Code != http.StatusOK {
		t.Fatalf("receive status = %d, body = %s", receiveRec.Code, receiveRec.Body.String())
	}
	var receiveBody struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 || receiveBody.Messages[0].ReceiptHandle == "" {
		t.Fatalf("receive body = %s", receiveRec.Body.String())
	}

	changeRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues/dashboard-visibility/visibility", `{
		"input":{"ReceiptHandle":"`+receiveBody.Messages[0].ReceiptHandle+`","VisibilityTimeout":0}
	}`)
	if changeRec.Code != http.StatusOK {
		t.Fatalf("change visibility status = %d, body = %s", changeRec.Code, changeRec.Body.String())
	}
	againRec := performRequestWithBody(routes, http.MethodPost, "/api/sqs/queues/dashboard-visibility/receive", `{
		"input":{"MaxNumberOfMessages":1,"WaitTimeSeconds":0}
	}`)
	if againRec.Code != http.StatusOK {
		t.Fatalf("receive again status = %d, body = %s", againRec.Code, againRec.Body.String())
	}
	if !strings.Contains(againRec.Body.String(), "dashboard visibility") {
		t.Fatalf("message was not made visible: %s", againRec.Body.String())
	}
}

func TestSQSQueuesAPIMarksDisabled(t *testing.T) {
	server := NewServer(Config{}, newDashboardStore(nil, nil))

	rec := performRequest(server.routes(), http.MethodGet, "/api/sqs/queues")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}
