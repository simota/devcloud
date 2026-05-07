package sqs

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestSendMessageBatchReportsInvalidMessageContentsPerEntry(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"batch-contents"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	rec := serveJSON(t, server, "SendMessageBatch", `{
		"QueueUrl":"`+queueURL+`",
		"Entries":[
			{"Id":"ok","MessageBody":"valid body"},
			{"Id":"bad","MessageBody":"bad\u0001body"}
		]
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("batch status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Successful []sendMessageBatchResultEntry `json:"Successful"`
		Failed     []batchResultErrorEntry       `json:"Failed"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode batch body: %v", err)
	}
	if len(body.Successful) != 1 || len(body.Failed) != 1 || body.Failed[0].Code != "InvalidMessageContents" {
		t.Fatalf("batch body = %#v", body)
	}
}

func TestJSONSendMessageBatchSendsMultipleMessages(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"batch-json"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	batchRec := serveJSON(t, server, "SendMessageBatch", `{
		"QueueUrl":"`+queueURL+`",
		"Entries":[
			{"Id":"first","MessageBody":"batch one"},
			{"Id":"second","MessageBody":"batch two","MessageAttributes":{"kind":{"DataType":"String","StringValue":"batch"}}}
		]
	}`)
	if batchRec.Code != http.StatusOK {
		t.Fatalf("batch status = %d, body = %s", batchRec.Code, batchRec.Body.String())
	}
	var batchBody struct {
		Successful []sendMessageBatchResultEntry `json:"Successful"`
		Failed     []batchResultErrorEntry       `json:"Failed"`
	}
	if err := json.Unmarshal(batchRec.Body.Bytes(), &batchBody); err != nil {
		t.Fatalf("decode batch body: %v", err)
	}
	if len(batchBody.Successful) != 2 || len(batchBody.Failed) != 0 {
		t.Fatalf("batch body = %#v", batchBody)
	}
	if batchBody.Successful[0].ID != "first" || batchBody.Successful[0].MD5OfMessageBody != md5Hex("batch one") {
		t.Fatalf("first batch result = %#v", batchBody.Successful[0])
	}
	if batchBody.Successful[0].MD5OfMessageAttributes != "" {
		t.Fatalf("first batch attribute md5 should be omitted: %#v", batchBody.Successful[0])
	}
	if batchBody.Successful[1].MD5OfMessageAttributes == "" {
		t.Fatalf("second batch attribute md5 missing: %#v", batchBody.Successful[1])
	}

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":2,"MessageAttributeNames":["All"]}`)
	if !strings.Contains(receiveRec.Body.String(), "batch one") || !strings.Contains(receiveRec.Body.String(), "batch two") {
		t.Fatalf("receive body = %s", receiveRec.Body.String())
	}
	if !strings.Contains(receiveRec.Body.String(), `"kind"`) {
		t.Fatalf("message attributes missing: %s", receiveRec.Body.String())
	}
}

func TestJSONSendMessageBatchRejectsDuplicateIDs(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"batch-duplicate"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	rec := serveJSON(t, server, "SendMessageBatch", `{
		"QueueUrl":"`+queueURL+`",
		"Entries":[
			{"Id":"same","MessageBody":"one"},
			{"Id":"same","MessageBody":"two"}
		]
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "batch entry Id must be unique") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestBatchOperationsRejectInvalidEntryIDs(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"batch-invalid-id"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	sendRec := serveJSON(t, server, "SendMessageBatch", `{
		"QueueUrl":"`+queueURL+`",
		"Entries":[{"Id":"bad/id","MessageBody":"one"}]
	}`)
	if sendRec.Code != http.StatusBadRequest || !strings.Contains(sendRec.Body.String(), "InvalidBatchEntryId") {
		t.Fatalf("send invalid id response = %d %s", sendRec.Code, sendRec.Body.String())
	}

	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"lease me"}`)
	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1}`)
	var receiveBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}
	receiptHandle := receiveBody.Messages[0].ReceiptHandle

	deleteRec := serveJSON(t, server, "DeleteMessageBatch", `{
		"QueueUrl":"`+queueURL+`",
		"Entries":[{"Id":"`+strings.Repeat("a", 81)+`","ReceiptHandle":"`+receiptHandle+`"}]
	}`)
	if deleteRec.Code != http.StatusBadRequest || !strings.Contains(deleteRec.Body.String(), "InvalidBatchEntryId") {
		t.Fatalf("delete invalid id response = %d %s", deleteRec.Code, deleteRec.Body.String())
	}

	changeRec := serveQuery(t, server, "/", "Action=ChangeMessageVisibilityBatch&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&ChangeMessageVisibilityBatchRequestEntry.1.Id=bad.id&ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle="+urlQueryEscape(receiptHandle)+"&ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout=0")
	if changeRec.Code != http.StatusBadRequest || !strings.Contains(changeRec.Body.String(), "<Code>InvalidBatchEntryId</Code>") {
		t.Fatalf("change invalid id response = %d %s", changeRec.Code, changeRec.Body.String())
	}
}

func TestJSONSendMessageBatchReturnsPerEntryFailure(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324", MaxMessageBytes: 8})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"batch-failure"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	rec := serveJSON(t, server, "SendMessageBatch", `{
		"QueueUrl":"`+queueURL+`",
		"Entries":[
			{"Id":"ok","MessageBody":"small"},
			{"Id":"bad","MessageBody":"this is too large"}
		]
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Successful []sendMessageBatchResultEntry `json:"Successful"`
		Failed     []batchResultErrorEntry       `json:"Failed"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Successful) != 1 || body.Successful[0].ID != "ok" {
		t.Fatalf("successful = %#v", body.Successful)
	}
	if len(body.Failed) != 1 || body.Failed[0].ID != "bad" || !body.Failed[0].SenderFault {
		t.Fatalf("failed = %#v", body.Failed)
	}
}

func TestQuerySendMessageBatchReturnsXMLResponse(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"batch-query"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	rec := serveQuery(t, server, "/", "Action=SendMessageBatch&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&SendMessageBatchRequestEntry.1.Id=a&SendMessageBatchRequestEntry.1.MessageBody=query-one&SendMessageBatchRequestEntry.2.Id=b&SendMessageBatchRequestEntry.2.MessageBody=query-two")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<SendMessageBatchResponse") || !strings.Contains(rec.Body.String(), "<Id>a</Id>") || !strings.Contains(rec.Body.String(), "<Id>b</Id>") {
		t.Fatalf("body = %s", rec.Body.String())
	}

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":2}`)
	if !strings.Contains(receiveRec.Body.String(), "query-one") || !strings.Contains(receiveRec.Body.String(), "query-two") {
		t.Fatalf("receive body = %s", receiveRec.Body.String())
	}
}

func TestJSONDeleteMessageBatchDeletesMessagesAndReportsFailures(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"delete-batch","Attributes":{"VisibilityTimeout":"30"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"delete one"}`)
	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"delete two"}`)

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":2}`)
	var receiveBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 2 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}

	deleteRec := serveJSON(t, server, "DeleteMessageBatch", `{
		"QueueUrl":"`+queueURL+`",
		"Entries":[
			{"Id":"first","ReceiptHandle":"`+receiveBody.Messages[0].ReceiptHandle+`"},
			{"Id":"bad","ReceiptHandle":"not-a-valid-handle"},
			{"Id":"second","ReceiptHandle":"`+receiveBody.Messages[1].ReceiptHandle+`"}
		]
	}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete batch status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	var deleteBody deleteMessageBatchResult
	if err := json.Unmarshal(deleteRec.Body.Bytes(), &deleteBody); err != nil {
		t.Fatalf("decode delete body: %v", err)
	}
	if len(deleteBody.Successful) != 2 || len(deleteBody.Failed) != 1 || deleteBody.Failed[0].Code != "ReceiptHandleIsInvalid" {
		t.Fatalf("delete body = %#v", deleteBody)
	}

	changeRec := serveJSON(t, server, "ChangeMessageVisibilityBatch", `{
		"QueueUrl":"`+queueURL+`",
		"Entries":[{"Id":"bad","ReceiptHandle":"`+receiveBody.Messages[0].ReceiptHandle+`","VisibilityTimeout":0}]
	}`)
	var changeBody changeMessageVisibilityBatchResult
	if err := json.Unmarshal(changeRec.Body.Bytes(), &changeBody); err != nil {
		t.Fatalf("decode change body: %v", err)
	}
	if len(changeBody.Failed) != 1 || changeBody.Failed[0].Code != "ReceiptHandleIsInvalid" {
		t.Fatalf("change body = %#v", changeBody)
	}
}

func TestJSONChangeMessageVisibilityBatchUpdatesMessages(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"visibility-batch","Attributes":{"VisibilityTimeout":"30"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"visible batch one"}`)
	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"visible batch two"}`)

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":2}`)
	var receiveBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 2 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}

	changeRec := serveJSON(t, server, "ChangeMessageVisibilityBatch", `{
		"QueueUrl":"`+queueURL+`",
		"Entries":[
			{"Id":"first","ReceiptHandle":"`+receiveBody.Messages[0].ReceiptHandle+`","VisibilityTimeout":0},
			{"Id":"second","ReceiptHandle":"`+receiveBody.Messages[1].ReceiptHandle+`","VisibilityTimeout":0}
		]
	}`)
	if changeRec.Code != http.StatusOK {
		t.Fatalf("change batch status = %d, body = %s", changeRec.Code, changeRec.Body.String())
	}
	var changeBody changeMessageVisibilityBatchResult
	if err := json.Unmarshal(changeRec.Body.Bytes(), &changeBody); err != nil {
		t.Fatalf("decode change body: %v", err)
	}
	if len(changeBody.Successful) != 2 || len(changeBody.Failed) != 0 {
		t.Fatalf("change body = %#v", changeBody)
	}

	againRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":2}`)
	if !strings.Contains(againRec.Body.String(), "visible batch one") || !strings.Contains(againRec.Body.String(), "visible batch two") {
		t.Fatalf("messages were not visible again: %s", againRec.Body.String())
	}
}

func TestQueryDeleteAndVisibilityBatchReturnXMLResponses(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"batch-query-delete","Attributes":{"VisibilityTimeout":"30"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"query batch"}`)
	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1}`)
	var receiveBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}
	receiptHandle := urlQueryEscape(receiveBody.Messages[0].ReceiptHandle)

	changeRec := serveQuery(t, server, "/", "Action=ChangeMessageVisibilityBatch&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&ChangeMessageVisibilityBatchRequestEntry.1.Id=a&ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle="+receiptHandle+"&ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout=0")
	if changeRec.Code != http.StatusOK {
		t.Fatalf("change status = %d, body = %s", changeRec.Code, changeRec.Body.String())
	}
	if !strings.Contains(changeRec.Body.String(), "<ChangeMessageVisibilityBatchResponse") || !strings.Contains(changeRec.Body.String(), "<Id>a</Id>") {
		t.Fatalf("change body = %s", changeRec.Body.String())
	}

	receiveRec = serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1}`)
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode second receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("second messages = %#v", receiveBody.Messages)
	}
	receiptHandle = urlQueryEscape(receiveBody.Messages[0].ReceiptHandle)
	deleteRec := serveQuery(t, server, "/", "Action=DeleteMessageBatch&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&DeleteMessageBatchRequestEntry.1.Id=a&DeleteMessageBatchRequestEntry.1.ReceiptHandle="+receiptHandle)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	if !strings.Contains(deleteRec.Body.String(), "<DeleteMessageBatchResponse") || !strings.Contains(deleteRec.Body.String(), "<Id>a</Id>") {
		t.Fatalf("delete body = %s", deleteRec.Body.String())
	}
}
