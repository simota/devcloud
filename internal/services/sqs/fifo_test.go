package sqs

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestFIFOQueueRequiresFifoAttribute(t *testing.T) {
	server := NewServer(Config{})

	rec := serveJSON(t, server, "CreateQueue", `{"QueueName":"jobs.fifo"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	rec = serveJSON(t, server, "CreateQueue", `{"QueueName":"jobs.fifo","Attributes":{"FifoQueue":"true"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestJSONFIFOSendRequiresGroupAndDeduplication(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"jobs.fifo","Attributes":{"FifoQueue":"true"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	withoutGroup := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"fifo job","MessageDeduplicationId":"dedup-1"}`)
	if withoutGroup.Code != http.StatusBadRequest {
		t.Fatalf("without group status = %d, body = %s", withoutGroup.Code, withoutGroup.Body.String())
	}
	if !strings.Contains(withoutGroup.Body.String(), "MessageGroupId is required") {
		t.Fatalf("without group body = %s", withoutGroup.Body.String())
	}

	withoutDedup := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"fifo job","MessageGroupId":"workers"}`)
	if withoutDedup.Code != http.StatusBadRequest {
		t.Fatalf("without dedup status = %d, body = %s", withoutDedup.Code, withoutDedup.Body.String())
	}
	if !strings.Contains(withoutDedup.Body.String(), "MessageDeduplicationId is required") {
		t.Fatalf("without dedup body = %s", withoutDedup.Body.String())
	}

	sent := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"fifo job","MessageGroupId":"workers","MessageDeduplicationId":"dedup-1"}`)
	if sent.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sent.Code, sent.Body.String())
	}
	if !strings.Contains(sent.Body.String(), `"SequenceNumber":"1"`) {
		t.Fatalf("send body = %s", sent.Body.String())
	}
}

func TestQueryFIFOSendUsesMessageGroupAndDeduplicationID(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"query-jobs.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	rec := serveQuery(t, server, "/", "Action=SendMessage&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&MessageBody=fifo-query&MessageGroupId=workers")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<SequenceNumber>1</SequenceNumber>") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestFIFOSendMessageRejectsPerMessageDelaySeconds(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"delayed.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	jsonRec := serveJSON(t, server, "SendMessage", `{
		"QueueUrl":"`+queueURL+`",
		"MessageBody":"fifo delayed",
		"MessageGroupId":"workers",
		"DelaySeconds":1
	}`)
	if jsonRec.Code != http.StatusBadRequest || !strings.Contains(jsonRec.Body.String(), "InvalidParameterValue") {
		t.Fatalf("json delay response = %d %s", jsonRec.Code, jsonRec.Body.String())
	}

	queryRec := serveQuery(t, server, "/", "Action=SendMessage&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&MessageBody=fifo-delayed&MessageGroupId=workers&DelaySeconds=1")
	if queryRec.Code != http.StatusBadRequest || !strings.Contains(queryRec.Body.String(), "<Code>InvalidParameterValue</Code>") {
		t.Fatalf("query delay response = %d %s", queryRec.Code, queryRec.Body.String())
	}
}

func TestFIFODeduplicationSuppressesDuplicateDelivery(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"dedupe.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	firstRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"same body","MessageGroupId":"group-a"}`)
	secondRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"same body","MessageGroupId":"group-a"}`)
	if firstRec.Code != http.StatusOK || secondRec.Code != http.StatusOK {
		t.Fatalf("send statuses = %d/%d bodies = %s / %s", firstRec.Code, secondRec.Code, firstRec.Body.String(), secondRec.Body.String())
	}
	var firstBody, secondBody map[string]string
	if err := json.Unmarshal(firstRec.Body.Bytes(), &firstBody); err != nil {
		t.Fatalf("decode first body: %v", err)
	}
	if err := json.Unmarshal(secondRec.Body.Bytes(), &secondBody); err != nil {
		t.Fatalf("decode second body: %v", err)
	}
	if firstBody["MessageId"] == "" || firstBody["MessageId"] != secondBody["MessageId"] {
		t.Fatalf("dedupe message ids = %#v / %#v", firstBody, secondBody)
	}
	if firstBody["SequenceNumber"] == "" || firstBody["SequenceNumber"] != secondBody["SequenceNumber"] {
		t.Fatalf("dedupe sequence numbers = %#v / %#v", firstBody, secondBody)
	}

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":10}`)
	var receiveBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}
}

func TestFIFODeduplicationIDSuppressesDifferentBodies(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"explicit-dedupe.fifo","Attributes":{"FifoQueue":"true"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	for _, body := range []string{"first body", "second body"} {
		rec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"`+body+`","MessageGroupId":"group-a","MessageDeduplicationId":"dedupe-key"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("send status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":10}`)
	if strings.Contains(receiveRec.Body.String(), "second body") {
		t.Fatalf("duplicate body was delivered: %s", receiveRec.Body.String())
	}
	if !strings.Contains(receiveRec.Body.String(), "first body") {
		t.Fatalf("first body missing: %s", receiveRec.Body.String())
	}
}

func TestFIFOReceiveBlocksLaterMessagesInSameGroup(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"ordered.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true","VisibilityTimeout":"30"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	for _, body := range []string{"group-a first", "group-a second", "group-b first"} {
		groupID := "group-a"
		if strings.Contains(body, "group-b") {
			groupID = "group-b"
		}
		rec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"`+body+`","MessageGroupId":"`+groupID+`"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("send %q status = %d, body = %s", body, rec.Code, rec.Body.String())
		}
	}

	firstReceive := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":10}`)
	var firstBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(firstReceive.Body.Bytes(), &firstBody); err != nil {
		t.Fatalf("decode first receive body: %v", err)
	}
	if len(firstBody.Messages) != 2 {
		t.Fatalf("first receive messages = %#v", firstBody.Messages)
	}
	if firstBody.Messages[0].Body != "group-a first" || firstBody.Messages[1].Body != "group-b first" {
		t.Fatalf("first receive order = %#v", firstBody.Messages)
	}

	secondReceive := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":10,"WaitTimeSeconds":0}`)
	if strings.Contains(secondReceive.Body.String(), "group-a second") {
		t.Fatalf("same-group message was delivered while prior group message was in flight: %s", secondReceive.Body.String())
	}

	deleteRec := serveJSON(t, server, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+firstBody.Messages[0].ReceiptHandle+`"}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	afterDelete := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":10}`)
	if !strings.Contains(afterDelete.Body.String(), "group-a second") {
		t.Fatalf("same-group message did not deliver after prior message deletion: %s", afterDelete.Body.String())
	}
}
