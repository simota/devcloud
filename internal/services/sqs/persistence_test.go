package sqs

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestSnapshotSummarizesQueuesWithoutMessagePayloads(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324", Region: "ap-northeast-1"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"dashboard"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"do not expose this body","DelaySeconds":60}`)

	snapshot := server.Snapshot()
	if snapshot.Status != "running" || !snapshot.Running || snapshot.Region != "ap-northeast-1" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if len(snapshot.Queues) != 1 {
		t.Fatalf("queues = %#v", snapshot.Queues)
	}
	queue := snapshot.Queues[0]
	if queue.Name != "dashboard" || queue.DelayedMessages != 1 || queue.TotalRetainedMessages != 1 {
		t.Fatalf("queue snapshot = %#v", queue)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if strings.Contains(string(encoded), "do not expose this body") {
		t.Fatalf("snapshot exposed message body: %s", encoded)
	}
}

func TestServerPersistsQueuesMessagesAndDeletesToStoragePath(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Addr: "127.0.0.1:9324", StoragePath: storagePath})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"persisted","Attributes":{"VisibilityTimeout":"30"},"Tags":{"env":"test"}}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	sendRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"persist me","MessageAttributes":{"kind":{"DataType":"String","StringValue":"stored"}}}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}
	if _, err := os.Stat(storagePath + "/state.json"); err != nil {
		t.Fatalf("state file stat: %v", err)
	}

	reloaded := NewServer(Config{Addr: "127.0.0.1:9324", StoragePath: storagePath})
	listRec := serveJSON(t, reloaded, "ListQueues", `{}`)
	if !strings.Contains(listRec.Body.String(), "persisted") {
		t.Fatalf("list body = %s", listRec.Body.String())
	}
	tagRec := serveJSON(t, reloaded, "ListQueueTags", `{"QueueUrl":"`+queueURL+`"}`)
	if !strings.Contains(tagRec.Body.String(), `"env":"test"`) {
		t.Fatalf("tags body = %s", tagRec.Body.String())
	}
	receiveRec := serveJSON(t, reloaded, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MessageAttributeNames":["All"]}`)
	var receiveBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("received messages = %d, want 1", len(receiveBody.Messages))
	}
	if receiveBody.Messages[0].Body != "persist me" || receiveBody.Messages[0].MessageAttributes["kind"].StringValue != "stored" {
		t.Fatal("reloaded message content or attributes did not match")
	}

	deleteRec := serveJSON(t, reloaded, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiveBody.Messages[0].ReceiptHandle+`"}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	afterDelete := NewServer(Config{Addr: "127.0.0.1:9324", StoragePath: storagePath})
	emptyRec := serveJSON(t, afterDelete, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","WaitTimeSeconds":0}`)
	if strings.Contains(emptyRec.Body.String(), "persist me") {
		t.Fatal("deleted message was reloaded")
	}
}

func TestServerPersistsDashboardPurgeToStoragePath(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Addr: "127.0.0.1:9324", StoragePath: storagePath})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"purge-persisted"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	sendRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"purge me"}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}

	if !server.PurgeQueueByName("purge-persisted") {
		t.Fatal("PurgeQueueByName returned false")
	}

	reloaded := NewServer(Config{Addr: "127.0.0.1:9324", StoragePath: storagePath})
	receiveRec := serveJSON(t, reloaded, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","WaitTimeSeconds":0}`)
	if strings.Contains(receiveRec.Body.String(), "purge me") {
		t.Fatalf("purged message was reloaded: %s", receiveRec.Body.String())
	}
}

func TestServerPersistsMessageMoveTasksToStoragePath(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Addr: "127.0.0.1:9324", Region: "us-east-1", AccountID: "123456789012", StoragePath: storagePath})
	serveJSON(t, server, "CreateQueue", `{"QueueName":"persist-move-source"}`)
	serveJSON(t, server, "CreateQueue", `{"QueueName":"persist-move-dlq"}`)
	dlqARN := "arn:aws:sqs:us-east-1:123456789012:persist-move-dlq"
	sourceARN := "arn:aws:sqs:us-east-1:123456789012:persist-move-source"

	startRec := serveJSON(t, server, "StartMessageMoveTask", `{"SourceArn":"`+dlqARN+`","DestinationArn":"`+sourceARN+`"}`)
	if startRec.Code != http.StatusOK {
		t.Fatalf("start status = %d, body = %s", startRec.Code, startRec.Body.String())
	}
	var startBody map[string]string
	if err := json.Unmarshal(startRec.Body.Bytes(), &startBody); err != nil {
		t.Fatalf("decode start body: %v", err)
	}
	if startBody["TaskHandle"] == "" {
		t.Fatalf("missing task handle: %s", startRec.Body.String())
	}

	reloaded := NewServer(Config{Addr: "127.0.0.1:9324", Region: "us-east-1", AccountID: "123456789012", StoragePath: storagePath})
	listRec := serveJSON(t, reloaded, "ListMessageMoveTasks", `{"SourceArn":"`+dlqARN+`"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), startBody["TaskHandle"]) || !strings.Contains(listRec.Body.String(), `"Status":"COMPLETED"`) {
		t.Fatalf("move task was not reloaded: %s", listRec.Body.String())
	}
}
