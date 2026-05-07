package sqs

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

func TestRedrivePolicyMovesMessageToDeadLetterQueue(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324", Region: "us-east-1", AccountID: "123456789012"})
	dlqRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"orders-dlq"}`)
	if dlqRec.Code != http.StatusOK {
		t.Fatalf("dlq create status = %d, body = %s", dlqRec.Code, dlqRec.Body.String())
	}
	sourceRec := serveJSON(t, server, "CreateQueue", `{
		"QueueName":"orders-source",
		"Attributes":{
			"VisibilityTimeout":"0",
			"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:orders-dlq\",\"maxReceiveCount\":\"2\"}"
		}
	}`)
	if sourceRec.Code != http.StatusOK {
		t.Fatalf("source create status = %d, body = %s", sourceRec.Code, sourceRec.Body.String())
	}
	var sourceBody map[string]string
	if err := json.Unmarshal(sourceRec.Body.Bytes(), &sourceBody); err != nil {
		t.Fatalf("decode source body: %v", err)
	}
	sourceURL := sourceBody["QueueUrl"]

	sendRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+sourceURL+`","MessageBody":"move me"}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}
	for i := 0; i < 2; i++ {
		receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+sourceURL+`","MaxNumberOfMessages":1,"WaitTimeSeconds":0}`)
		if !strings.Contains(receiveRec.Body.String(), "move me") {
			t.Fatalf("receive %d body = %s", i+1, receiveRec.Body.String())
		}
	}
	movedRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+sourceURL+`","MaxNumberOfMessages":1,"WaitTimeSeconds":0}`)
	if strings.Contains(movedRec.Body.String(), "move me") {
		t.Fatalf("message should have moved to dlq: %s", movedRec.Body.String())
	}

	dlqURL := "http://127.0.0.1:9324/123456789012/orders-dlq"
	dlqReceiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+dlqURL+`","MaxNumberOfMessages":1,"WaitTimeSeconds":0}`)
	if !strings.Contains(dlqReceiveRec.Body.String(), "move me") {
		t.Fatalf("dlq receive body = %s", dlqReceiveRec.Body.String())
	}

	listRec := serveJSON(t, server, "ListDeadLetterSourceQueues", `{"QueueUrl":"`+dlqURL+`"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list dlq sources status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), "orders-source") {
		t.Fatalf("list dlq sources body = %s", listRec.Body.String())
	}

	snapshot, ok := server.DeadLetterSnapshot("orders-dlq")
	if !ok || len(snapshot.DeadLetterSourceQueues) != 1 || snapshot.DeadLetterSourceQueues[0].Name != "orders-source" {
		t.Fatalf("dlq snapshot = %#v, ok = %v", snapshot, ok)
	}
}

func TestRedriveAllowPolicyRestrictsSourceQueues(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324", Region: "us-east-1", AccountID: "123456789012"})
	denyRec := serveJSON(t, server, "CreateQueue", `{
		"QueueName":"deny-dlq",
		"Attributes":{"RedriveAllowPolicy":"{\"redrivePermission\":\"denyAll\"}"}
	}`)
	if denyRec.Code != http.StatusOK {
		t.Fatalf("deny dlq create status = %d, body = %s", denyRec.Code, denyRec.Body.String())
	}
	deniedSourceRec := serveJSON(t, server, "CreateQueue", `{
		"QueueName":"denied-source",
		"Attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:deny-dlq\",\"maxReceiveCount\":1}"}
	}`)
	if deniedSourceRec.Code != http.StatusBadRequest || !strings.Contains(deniedSourceRec.Body.String(), "RedriveAllowPolicy") {
		t.Fatalf("denied source status = %d, body = %s", deniedSourceRec.Code, deniedSourceRec.Body.String())
	}

	allowedSourceARN := "arn:aws:sqs:us-east-1:123456789012:allowed-source"
	byQueuePolicy := strings.ReplaceAll(`{"redrivePermission":"byQueue","sourceQueueArns":["ARN"]}`, "ARN", allowedSourceARN)
	byQueueRec := serveJSON(t, server, "CreateQueue", `{
		"QueueName":"byqueue-dlq",
		"Attributes":{"RedriveAllowPolicy":`+strconv.Quote(byQueuePolicy)+`}
	}`)
	if byQueueRec.Code != http.StatusOK {
		t.Fatalf("byQueue dlq create status = %d, body = %s", byQueueRec.Code, byQueueRec.Body.String())
	}
	rejectedRec := serveJSON(t, server, "CreateQueue", `{
		"QueueName":"rejected-source",
		"Attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:byqueue-dlq\",\"maxReceiveCount\":1}"}
	}`)
	if rejectedRec.Code != http.StatusBadRequest || !strings.Contains(rejectedRec.Body.String(), "source queue") {
		t.Fatalf("rejected source status = %d, body = %s", rejectedRec.Code, rejectedRec.Body.String())
	}
	allowedRec := serveJSON(t, server, "CreateQueue", `{
		"QueueName":"allowed-source",
		"Attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:byqueue-dlq\",\"maxReceiveCount\":1}"}
	}`)
	if allowedRec.Code != http.StatusOK {
		t.Fatalf("allowed source status = %d, body = %s", allowedRec.Code, allowedRec.Body.String())
	}
}

func TestQueryListDeadLetterSourceQueuesReturnsXML(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	serveJSON(t, server, "CreateQueue", `{"QueueName":"query-dlq"}`)
	createRec := serveJSON(t, server, "CreateQueue", `{
		"QueueName":"query-source",
		"Attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:query-dlq\",\"maxReceiveCount\":1}"}
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("source create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}

	dlqURL := "http://127.0.0.1:9324/000000000000/query-dlq"
	rec := serveQuery(t, server, "/", "Action=ListDeadLetterSourceQueues&Version=2012-11-05&QueueUrl="+urlQueryEscape(dlqURL))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<ListDeadLetterSourceQueuesResponse") || !strings.Contains(rec.Body.String(), "query-source") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestJSONMessageMoveTaskMovesMessagesFromDLQToOriginalSource(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324", Region: "us-east-1", AccountID: "123456789012"})
	serveJSON(t, server, "CreateQueue", `{"QueueName":"move-dlq"}`)
	sourceRec := serveJSON(t, server, "CreateQueue", `{
		"QueueName":"move-source",
		"Attributes":{
			"VisibilityTimeout":"0",
			"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:move-dlq\",\"maxReceiveCount\":1}"
		}
	}`)
	var sourceBody map[string]string
	if err := json.Unmarshal(sourceRec.Body.Bytes(), &sourceBody); err != nil {
		t.Fatalf("decode source body: %v", err)
	}
	sourceURL := sourceBody["QueueUrl"]
	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+sourceURL+`","MessageBody":"return me"}`)
	serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+sourceURL+`","MaxNumberOfMessages":1,"WaitTimeSeconds":0}`)
	serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+sourceURL+`","MaxNumberOfMessages":1,"WaitTimeSeconds":0}`)

	dlqARN := "arn:aws:sqs:us-east-1:123456789012:move-dlq"
	startRec := serveJSON(t, server, "StartMessageMoveTask", `{"SourceArn":"`+dlqARN+`"}`)
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

	sourceReceiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+sourceURL+`","MaxNumberOfMessages":1,"WaitTimeSeconds":0}`)
	if !strings.Contains(sourceReceiveRec.Body.String(), "return me") {
		t.Fatalf("message was not moved back to source: %s", sourceReceiveRec.Body.String())
	}

	listRec := serveJSON(t, server, "ListMessageMoveTasks", `{"SourceArn":"`+dlqARN+`"}`)
	if listRec.Code != http.StatusOK || !strings.Contains(listRec.Body.String(), `"Status":"COMPLETED"`) || !strings.Contains(listRec.Body.String(), `"ApproximateNumberOfMessagesMoved":1`) {
		t.Fatalf("list body = %s", listRec.Body.String())
	}

	cancelRec := serveJSON(t, server, "CancelMessageMoveTask", `{"TaskHandle":"`+startBody["TaskHandle"]+`"}`)
	if cancelRec.Code != http.StatusOK || !strings.Contains(cancelRec.Body.String(), `"ApproximateNumberOfMessagesMoved":1`) {
		t.Fatalf("cancel body = %s", cancelRec.Body.String())
	}
}

func TestQueryMessageMoveTasksReturnXML(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324", Region: "us-east-1", AccountID: "123456789012"})
	serveJSON(t, server, "CreateQueue", `{"QueueName":"query-move-source"}`)
	serveJSON(t, server, "CreateQueue", `{"QueueName":"query-move-dlq"}`)
	dlqARN := "arn:aws:sqs:us-east-1:123456789012:query-move-dlq"
	sourceARN := "arn:aws:sqs:us-east-1:123456789012:query-move-source"

	startRec := serveQuery(t, server, "/", "Action=StartMessageMoveTask&Version=2012-11-05&SourceArn="+url.QueryEscape(dlqARN)+"&DestinationArn="+url.QueryEscape(sourceARN))
	if startRec.Code != http.StatusOK {
		t.Fatalf("start status = %d, body = %s", startRec.Code, startRec.Body.String())
	}
	if !strings.Contains(startRec.Body.String(), "<StartMessageMoveTaskResponse") || !strings.Contains(startRec.Body.String(), "<TaskHandle>") {
		t.Fatalf("start body = %s", startRec.Body.String())
	}

	listRec := serveQuery(t, server, "/", "Action=ListMessageMoveTasks&Version=2012-11-05&SourceArn="+url.QueryEscape(dlqARN))
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), "<ListMessageMoveTasksResponse") || !strings.Contains(listRec.Body.String(), "<Status>COMPLETED</Status>") {
		t.Fatalf("list body = %s", listRec.Body.String())
	}
}
