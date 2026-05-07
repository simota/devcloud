package sqs

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestJSONQueueCoreCreatesListsAndReadsAttributes(t *testing.T) {
	server := NewServer(Config{
		Addr:         "127.0.0.1:9324",
		Region:       "ap-northeast-1",
		AccountID:    "123456789012",
		QueueURLHost: "localhost",
	})

	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"orders","Attributes":{"VisibilityTimeout":"7"}}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	if queueURL != "http://localhost:9324/123456789012/orders" {
		t.Fatalf("QueueUrl = %q", queueURL)
	}

	listRec := serveJSON(t, server, "ListQueues", `{}`)
	if !strings.Contains(listRec.Body.String(), queueURL) {
		t.Fatalf("list body = %s", listRec.Body.String())
	}

	getURLRec := serveJSON(t, server, "GetQueueUrl", `{"QueueName":"orders"}`)
	if !strings.Contains(getURLRec.Body.String(), queueURL) {
		t.Fatalf("get url body = %s", getURLRec.Body.String())
	}

	attrsRec := serveJSON(t, server, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["All"]}`)
	if !strings.Contains(attrsRec.Body.String(), `"VisibilityTimeout":"7"`) {
		t.Fatalf("attributes body = %s", attrsRec.Body.String())
	}
	if !strings.Contains(attrsRec.Body.String(), `"QueueArn":"arn:aws:sqs:ap-northeast-1:123456789012:orders"`) {
		t.Fatalf("attributes body = %s", attrsRec.Body.String())
	}
}

func TestQueryGetQueueAttributesAcceptsUnindexedAttributeName(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"query-attribute-name","Attributes":{"VisibilityTimeout":"7"}}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	attrsRec := serveQuery(t, server, "/", "Action=GetQueueAttributes&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&AttributeName=All")
	if attrsRec.Code != http.StatusOK {
		t.Fatalf("attributes status = %d, body = %s", attrsRec.Code, attrsRec.Body.String())
	}
	if !strings.Contains(attrsRec.Body.String(), "<Name>VisibilityTimeout</Name>") || !strings.Contains(attrsRec.Body.String(), "<Value>7</Value>") {
		t.Fatalf("attributes body = %s", attrsRec.Body.String())
	}
}

func TestJSONQueueAttributesIncludeTimestampsAndUpdateLastModified(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"timestamps"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	createdAt := time.Unix(1700000000, 0).UTC()
	modifiedAt := time.Unix(1700000100, 0).UTC()
	server.mu.Lock()
	server.queues["timestamps"].CreatedAt = createdAt
	server.queues["timestamps"].ModifiedAt = modifiedAt
	server.mu.Unlock()

	attrsRec := serveJSON(t, server, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["CreatedTimestamp","LastModifiedTimestamp"]}`)
	var attrsBody struct {
		Attributes map[string]string `json:"Attributes"`
	}
	if err := json.Unmarshal(attrsRec.Body.Bytes(), &attrsBody); err != nil {
		t.Fatalf("decode attributes body: %v", err)
	}
	if attrsBody.Attributes["CreatedTimestamp"] != "1700000000" || attrsBody.Attributes["LastModifiedTimestamp"] != "1700000100" {
		t.Fatalf("attributes = %#v", attrsBody.Attributes)
	}

	setRec := serveJSON(t, server, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"VisibilityTimeout":"9"}}`)
	if setRec.Code != http.StatusOK {
		t.Fatalf("set status = %d, body = %s", setRec.Code, setRec.Body.String())
	}
	attrsRec = serveJSON(t, server, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["CreatedTimestamp","LastModifiedTimestamp","VisibilityTimeout"]}`)
	if err := json.Unmarshal(attrsRec.Body.Bytes(), &attrsBody); err != nil {
		t.Fatalf("decode updated attributes body: %v", err)
	}
	if attrsBody.Attributes["CreatedTimestamp"] != "1700000000" {
		t.Fatalf("CreatedTimestamp = %q", attrsBody.Attributes["CreatedTimestamp"])
	}
	if attrsBody.Attributes["LastModifiedTimestamp"] == "1700000100" || attrsBody.Attributes["LastModifiedTimestamp"] == "" {
		t.Fatalf("LastModifiedTimestamp was not updated: %#v", attrsBody.Attributes)
	}
	if attrsBody.Attributes["VisibilityTimeout"] != "9" {
		t.Fatalf("VisibilityTimeout = %q", attrsBody.Attributes["VisibilityTimeout"])
	}

	readOnlyRec := serveJSON(t, server, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"CreatedTimestamp":"1"}}`)
	if readOnlyRec.Code != http.StatusBadRequest {
		t.Fatalf("read-only timestamp status = %d, body = %s", readOnlyRec.Code, readOnlyRec.Body.String())
	}
}

func TestJSONCreateQueueUsesConfiguredDefaultsAndLimits(t *testing.T) {
	server := NewServer(Config{
		Addr:                            "127.0.0.1:9324",
		MaxQueues:                       1,
		MaxMessageBytes:                 12,
		DefaultVisibilityTimeoutSeconds: 4,
		DefaultDelaySeconds:             2,
		DefaultMessageRetentionSeconds:  60,
		DefaultReceiveWaitTimeSeconds:   1,
	})

	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"limited"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	attrsRec := serveJSON(t, server, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["All"]}`)
	for _, want := range []string{
		`"DelaySeconds":"2"`,
		`"MaximumMessageSize":"12"`,
		`"MessageRetentionPeriod":"60"`,
		`"ReceiveMessageWaitTimeSeconds":"1"`,
		`"VisibilityTimeout":"4"`,
	} {
		if !strings.Contains(attrsRec.Body.String(), want) {
			t.Fatalf("attributes missing %s: %s", want, attrsRec.Body.String())
		}
	}

	tooManyRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"second"}`)
	if tooManyRec.Code != http.StatusBadRequest {
		t.Fatalf("queue limit status = %d, body = %s", tooManyRec.Code, tooManyRec.Body.String())
	}

	tooLargeRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"this message is too large"}`)
	if tooLargeRec.Code != http.StatusBadRequest {
		t.Fatalf("message limit status = %d, body = %s", tooLargeRec.Code, tooLargeRec.Body.String())
	}
}

func TestCreateQueueIsIdempotentOnlyForMatchingAttributes(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})

	firstRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"idempotent","Attributes":{"VisibilityTimeout":"7"}}`)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first create status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}
	var firstBody map[string]string
	if err := json.Unmarshal(firstRec.Body.Bytes(), &firstBody); err != nil {
		t.Fatalf("decode first create body: %v", err)
	}

	sameRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"idempotent","Attributes":{"VisibilityTimeout":"7"}}`)
	if sameRec.Code != http.StatusOK {
		t.Fatalf("same create status = %d, body = %s", sameRec.Code, sameRec.Body.String())
	}
	if !strings.Contains(sameRec.Body.String(), firstBody["QueueUrl"]) {
		t.Fatalf("same create body = %s", sameRec.Body.String())
	}

	differentRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"idempotent","Attributes":{"VisibilityTimeout":"8"}}`)
	if differentRec.Code != http.StatusBadRequest || !strings.Contains(differentRec.Body.String(), "QueueNameExists") {
		t.Fatalf("different create response = %d %s", differentRec.Code, differentRec.Body.String())
	}
}

func TestJSONGetQueueAttributesFiltersAndIncludesApproximateCounts(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"filtered","Attributes":{"VisibilityTimeout":"5"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	sendRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"count me"}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}

	attrsRec := serveJSON(t, server, "GetQueueAttributes", `{
		"QueueUrl":"`+queueURL+`",
		"AttributeNames":["VisibilityTimeout","ApproximateNumberOfMessages"]
	}`)
	if attrsRec.Code != http.StatusOK {
		t.Fatalf("attributes status = %d, body = %s", attrsRec.Code, attrsRec.Body.String())
	}
	var attrsBody struct {
		Attributes map[string]string `json:"Attributes"`
	}
	if err := json.Unmarshal(attrsRec.Body.Bytes(), &attrsBody); err != nil {
		t.Fatalf("decode attributes body: %v", err)
	}
	if attrsBody.Attributes["VisibilityTimeout"] != "5" || attrsBody.Attributes["ApproximateNumberOfMessages"] != "1" {
		t.Fatalf("attributes = %#v", attrsBody.Attributes)
	}
	if _, ok := attrsBody.Attributes["DelaySeconds"]; ok {
		t.Fatalf("unexpected unrequested attribute: %#v", attrsBody.Attributes)
	}
}

func TestQueueAttributesRejectUnknownAttributeNames(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"attrs"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	getRec := serveJSON(t, server, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["NoSuchAttribute"]}`)
	if getRec.Code != http.StatusBadRequest || !strings.Contains(getRec.Body.String(), "InvalidAttributeName") {
		t.Fatalf("get unknown attribute response = %d %s", getRec.Code, getRec.Body.String())
	}

	setRec := serveJSON(t, server, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"NoSuchAttribute":"x"}}`)
	if setRec.Code != http.StatusBadRequest || !strings.Contains(setRec.Body.String(), "InvalidAttributeName") {
		t.Fatalf("set unknown attribute response = %d %s", setRec.Code, setRec.Body.String())
	}
}

func TestQueueAttributesRejectInvalidAttributeValues(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})

	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"bad-attrs","Attributes":{"VisibilityTimeout":"soon"}}`)
	if createRec.Code != http.StatusBadRequest || !strings.Contains(createRec.Body.String(), "InvalidAttributeValue") {
		t.Fatalf("create invalid attribute response = %d %s", createRec.Code, createRec.Body.String())
	}

	validRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"valid-attrs"}`)
	var createBody map[string]string
	if err := json.Unmarshal(validRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	setRec := serveJSON(t, server, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"FifoQueue":"yes"}}`)
	if setRec.Code != http.StatusBadRequest || !strings.Contains(setRec.Body.String(), "InvalidAttributeValue") {
		t.Fatalf("set invalid attribute response = %d %s", setRec.Code, setRec.Body.String())
	}

	boundsRec := serveJSON(t, server, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"VisibilityTimeout":"43201"}}`)
	if boundsRec.Code != http.StatusBadRequest || !strings.Contains(boundsRec.Body.String(), "InvalidAttributeValue") {
		t.Fatalf("set out-of-range attribute response = %d %s", boundsRec.Code, boundsRec.Body.String())
	}
}

func TestMessageTimingParametersRejectSQSOutOfRangeValues(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"timing-bounds"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	sendRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"too delayed","DelaySeconds":901}`)
	if sendRec.Code != http.StatusBadRequest || !strings.Contains(sendRec.Body.String(), "InvalidParameterValue") {
		t.Fatalf("send out-of-range delay response = %d %s", sendRec.Code, sendRec.Body.String())
	}

	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"receive me"}`)
	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","WaitTimeSeconds":21}`)
	if receiveRec.Code != http.StatusBadRequest || !strings.Contains(receiveRec.Body.String(), "InvalidParameterValue") {
		t.Fatalf("receive out-of-range wait response = %d %s", receiveRec.Code, receiveRec.Body.String())
	}

	validReceiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","VisibilityTimeout":30}`)
	var receiveBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(validReceiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}

	changeRec := serveJSON(t, server, "ChangeMessageVisibility", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiveBody.Messages[0].ReceiptHandle+`","VisibilityTimeout":43201}`)
	if changeRec.Code != http.StatusBadRequest || !strings.Contains(changeRec.Body.String(), "InvalidParameterValue") {
		t.Fatalf("change out-of-range visibility response = %d %s", changeRec.Code, changeRec.Body.String())
	}
}

func TestJSONSetAndDeleteQueue(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"updates"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	setRec := serveJSON(t, server, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"DelaySeconds":"3"}}`)
	if setRec.Code != http.StatusOK {
		t.Fatalf("set status = %d, body = %s", setRec.Code, setRec.Body.String())
	}
	attrsRec := serveJSON(t, server, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["All"]}`)
	if !strings.Contains(attrsRec.Body.String(), `"DelaySeconds":"3"`) {
		t.Fatalf("attributes body = %s", attrsRec.Body.String())
	}

	deleteRec := serveJSON(t, server, "DeleteQueue", `{"QueueUrl":"`+queueURL+`"}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	listRec := serveJSON(t, server, "ListQueues", `{}`)
	if strings.Contains(listRec.Body.String(), "updates") {
		t.Fatalf("list body = %s", listRec.Body.String())
	}
}

func TestQueryCreateQueueReturnsXMLResponse(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=CreateQueue&Version=2012-11-05&QueueName=query-demo"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<CreateQueueResponse") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<QueueUrl>http://127.0.0.1:9324/000000000000/query-demo</QueueUrl>") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}
