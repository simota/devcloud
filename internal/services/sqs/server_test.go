package sqs

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestJSONListQueuesReturnsEmptyQueueURLs(t *testing.T) {
	server := NewServer(Config{})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/x-amz-json-1.0" {
		t.Fatalf("Content-Type = %q", got)
	}
	if !strings.Contains(rec.Body.String(), `"QueueUrls":[]`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestQueryListQueuesReturnsXMLResponse(t *testing.T) {
	server := NewServer(Config{})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListQueues&Version=2012-11-05"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/xml; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	if !strings.Contains(rec.Body.String(), "<ListQueuesResponse") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestStrictAuthRejectsUnsignedRequest(t *testing.T) {
	server := NewServer(Config{AuthMode: "strict"})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "AccessDenied") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestStrictAuthAcceptsSignedJSONRequestAndPreservesBody(t *testing.T) {
	server := NewServer(Config{AuthMode: "strict", Region: "us-east-1", AccessKeyID: "dev", SecretAccessKey: "dev"})
	req := signedSQSJSONRequest(t, "CreateQueue", `{"QueueName":"signed"}`)
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"QueueUrl"`) || !strings.Contains(rec.Body.String(), "/signed") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestStrictAuthRejectsWrongCredentialScope(t *testing.T) {
	server := NewServer(Config{AuthMode: "strict", Region: "us-east-1", AccessKeyID: "dev", SecretAccessKey: "dev"})
	req := signedSQSJSONRequest(t, "ListQueues", `{}`)
	req.Header.Set("Authorization", strings.Replace(req.Header.Get("Authorization"), "Credential=dev/20260501/us-east-1/sqs/aws4_request", "Credential=other/20260501/ap-northeast-1/sqs/aws4_request", 1))
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "InvalidAccessKeyId") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestUnsupportedQueryActionReturnsXMLError(t *testing.T) {
	server := NewServer(Config{})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=UnknownOperation&Version=2012-11-05"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<Code>InvalidAction</Code>") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestQueryProtocolRequiresSupportedVersion(t *testing.T) {
	server := NewServer(Config{})
	for _, tt := range []struct {
		name     string
		body     string
		wantCode string
	}{
		{
			name:     "missing",
			body:     "Action=ListQueues",
			wantCode: "MissingParameter",
		},
		{
			name:     "unsupported",
			body:     "Action=ListQueues&Version=2011-10-01",
			wantCode: "InvalidParameterValue",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()

			server.routes().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != "text/xml; charset=utf-8" {
				t.Fatalf("Content-Type = %q", got)
			}
			if !strings.Contains(rec.Body.String(), "<Code>"+tt.wantCode+"</Code>") {
				t.Fatalf("body = %s", rec.Body.String())
			}
		})
	}
}

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

func TestSendMessageRejectsInvalidMessageContents(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"contents"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	jsonRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"bad\u0001body"}`)
	if jsonRec.Code != http.StatusBadRequest || !strings.Contains(jsonRec.Body.String(), "InvalidMessageContents") {
		t.Fatalf("json invalid contents response = %d %s", jsonRec.Code, jsonRec.Body.String())
	}

	queryRec := serveQuery(t, server, "/", "Action=SendMessage&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&MessageBody=bad%01body")
	if queryRec.Code != http.StatusBadRequest || !strings.Contains(queryRec.Body.String(), "<Code>InvalidMessageContents</Code>") {
		t.Fatalf("query invalid contents response = %d %s", queryRec.Code, queryRec.Body.String())
	}
}

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

func TestJSONReceiveMessageHonorsConfiguredBatchLimit(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324", MaxReceiveBatchSize: 2})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"batch-limit"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	for _, body := range []string{"one", "two", "three"} {
		rec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"`+body+`"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("send status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":10}`)
	var receiveBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 2 {
		t.Fatalf("messages = %d", len(receiveBody.Messages))
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

func TestJSONQueueTagsCanBeCreatedListedUpdatedAndRemoved(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"tagged","Tags":{"env":"test"}}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	listRec := serveJSON(t, server, "ListQueueTags", `{"QueueUrl":"`+queueURL+`"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list tags status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), `"env":"test"`) {
		t.Fatalf("list tags body = %s", listRec.Body.String())
	}

	tagRec := serveJSON(t, server, "TagQueue", `{"QueueUrl":"`+queueURL+`","Tags":{"team":"platform"}}`)
	if tagRec.Code != http.StatusOK {
		t.Fatalf("tag status = %d, body = %s", tagRec.Code, tagRec.Body.String())
	}
	untagRec := serveJSON(t, server, "UntagQueue", `{"QueueUrl":"`+queueURL+`","TagKeys":["env"]}`)
	if untagRec.Code != http.StatusOK {
		t.Fatalf("untag status = %d, body = %s", untagRec.Code, untagRec.Body.String())
	}

	afterRec := serveJSON(t, server, "ListQueueTags", `{"QueueUrl":"`+queueURL+`"}`)
	if strings.Contains(afterRec.Body.String(), `"env"`) || !strings.Contains(afterRec.Body.String(), `"team":"platform"`) {
		t.Fatalf("after tags body = %s", afterRec.Body.String())
	}

	snapshot := server.Snapshot()
	if got := snapshot.Queues[0].Tags["team"]; got != "platform" {
		t.Fatalf("snapshot tags = %#v", snapshot.Queues[0].Tags)
	}

	lowercaseCreateRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"lowercase-tags","tags":{"compat":"yes"}}`)
	if lowercaseCreateRec.Code != http.StatusOK {
		t.Fatalf("lowercase create status = %d, body = %s", lowercaseCreateRec.Code, lowercaseCreateRec.Body.String())
	}
	var lowercaseCreateBody map[string]string
	if err := json.Unmarshal(lowercaseCreateRec.Body.Bytes(), &lowercaseCreateBody); err != nil {
		t.Fatalf("decode lowercase create body: %v", err)
	}
	lowercaseListRec := serveJSON(t, server, "ListQueueTags", `{"QueueUrl":"`+lowercaseCreateBody["QueueUrl"]+`"}`)
	if !strings.Contains(lowercaseListRec.Body.String(), `"compat":"yes"`) {
		t.Fatalf("lowercase list tags body = %s", lowercaseListRec.Body.String())
	}
}

func TestQueryQueueTagsReturnXMLResponse(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveQuery(t, server, "/", "Action=CreateQueue&Version=2012-11-05&QueueName=query-tagged&Tag.1.Key=env&Tag.1.Value=test")
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	queueURL := "http://127.0.0.1:9324/000000000000/query-tagged"

	tagRec := serveQuery(t, server, "/", "Action=TagQueue&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&Tag.1.Key=team&Tag.1.Value=platform")
	if tagRec.Code != http.StatusOK {
		t.Fatalf("tag status = %d, body = %s", tagRec.Code, tagRec.Body.String())
	}
	listRec := serveQuery(t, server, "/", "Action=ListQueueTags&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL))
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), "<ListQueueTagsResponse") || !strings.Contains(listRec.Body.String(), "<Key>env</Key>") || !strings.Contains(listRec.Body.String(), "<Key>team</Key>") {
		t.Fatalf("list body = %s", listRec.Body.String())
	}

	untagRec := serveQuery(t, server, "/", "Action=UntagQueue&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&TagKey.1=env")
	if untagRec.Code != http.StatusOK {
		t.Fatalf("untag status = %d, body = %s", untagRec.Code, untagRec.Body.String())
	}
	afterRec := serveQuery(t, server, "/", "Action=ListQueueTags&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL))
	if strings.Contains(afterRec.Body.String(), "<Key>env</Key>") || !strings.Contains(afterRec.Body.String(), "<Key>team</Key>") {
		t.Fatalf("after body = %s", afterRec.Body.String())
	}
}

func TestJSONAddAndRemovePermissionUpdatesPolicyAttribute(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324", Region: "us-east-1", AccountID: "123456789012"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"permissioned"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	addRec := serveJSON(t, server, "AddPermission", `{
		"QueueUrl":"`+queueURL+`",
		"Label":"sender",
		"AWSAccountIds":["111122223333"],
		"Actions":["SendMessage"]
	}`)
	if addRec.Code != http.StatusOK {
		t.Fatalf("add status = %d, body = %s", addRec.Code, addRec.Body.String())
	}

	attrsRec := serveJSON(t, server, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["Policy"]}`)
	if attrsRec.Code != http.StatusOK {
		t.Fatalf("attributes status = %d, body = %s", attrsRec.Code, attrsRec.Body.String())
	}
	var attrsBody struct {
		Attributes map[string]string `json:"Attributes"`
	}
	if err := json.Unmarshal(attrsRec.Body.Bytes(), &attrsBody); err != nil {
		t.Fatalf("decode attributes body: %v", err)
	}
	var policy queuePolicy
	if err := json.Unmarshal([]byte(attrsBody.Attributes["Policy"]), &policy); err != nil {
		t.Fatalf("decode policy: %v; raw = %s", err, attrsBody.Attributes["Policy"])
	}
	if len(policy.Statement) != 1 || policy.Statement[0].Sid != "sender" || policy.Statement[0].Action[0] != "SQS:SendMessage" || policy.Statement[0].Principal.AWS[0] != "111122223333" {
		t.Fatalf("policy = %#v", policy)
	}

	removeRec := serveJSON(t, server, "RemovePermission", `{"QueueUrl":"`+queueURL+`","Label":"sender"}`)
	if removeRec.Code != http.StatusOK {
		t.Fatalf("remove status = %d, body = %s", removeRec.Code, removeRec.Body.String())
	}
	afterRec := serveJSON(t, server, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["All"]}`)
	if strings.Contains(afterRec.Body.String(), `"Policy"`) {
		t.Fatalf("policy should be removed: %s", afterRec.Body.String())
	}
}

func TestQueryAddAndRemovePermissionReturnXMLResponses(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"query-permissioned"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	addRec := serveQuery(t, server, "/", "Action=AddPermission&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&Label=reader&AWSAccountId.1=111122223333&ActionName.1=ReceiveMessage")
	if addRec.Code != http.StatusOK {
		t.Fatalf("add status = %d, body = %s", addRec.Code, addRec.Body.String())
	}
	if !strings.Contains(addRec.Body.String(), "<AddPermissionResponse") {
		t.Fatalf("add body = %s", addRec.Body.String())
	}
	attrsRec := serveJSON(t, server, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["Policy"]}`)
	if !strings.Contains(attrsRec.Body.String(), "SQS:ReceiveMessage") {
		t.Fatalf("attributes body = %s", attrsRec.Body.String())
	}

	removeRec := serveQuery(t, server, "/", "Action=RemovePermission&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&Label=reader")
	if removeRec.Code != http.StatusOK {
		t.Fatalf("remove status = %d, body = %s", removeRec.Code, removeRec.Body.String())
	}
	if !strings.Contains(removeRec.Body.String(), "<RemovePermissionResponse") {
		t.Fatalf("remove body = %s", removeRec.Body.String())
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

func TestQueryOperationsInferQueueURLFromRequestPath(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"path-demo"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}

	sendRec := serveQuery(t, server, "/000000000000/path-demo", "Action=SendMessage&Version=2012-11-05&MessageBody=from-path")
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}
	if !strings.Contains(sendRec.Body.String(), "<SendMessageResponse") {
		t.Fatalf("send body = %s", sendRec.Body.String())
	}

	receiveRec := serveQuery(t, server, "/000000000000/path-demo", "Action=ReceiveMessage&Version=2012-11-05&MaxNumberOfMessages=1")
	if receiveRec.Code != http.StatusOK {
		t.Fatalf("receive status = %d, body = %s", receiveRec.Code, receiveRec.Body.String())
	}
	if !strings.Contains(receiveRec.Body.String(), "<Body>from-path</Body>") {
		t.Fatalf("receive body = %s", receiveRec.Body.String())
	}

	attrsRec := serveQuery(t, server, "/000000000000/path-demo", "Action=GetQueueAttributes&Version=2012-11-05&AttributeName.1=All")
	if attrsRec.Code != http.StatusOK {
		t.Fatalf("attributes status = %d, body = %s", attrsRec.Code, attrsRec.Body.String())
	}
	if !strings.Contains(attrsRec.Body.String(), "<Name>QueueArn</Name>") {
		t.Fatalf("attributes body = %s", attrsRec.Body.String())
	}
}

func TestQueueURLRequiresAccountPathSegment(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"path-shape"}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createRec.Code, createRec.Body.String())
	}

	sendRec := serveJSON(t, server, "SendMessage", `{
		"QueueUrl":"http://127.0.0.1:9324/path-shape",
		"MessageBody":"must not route by name only"
	}`)
	if sendRec.Code != http.StatusBadRequest || !strings.Contains(sendRec.Body.String(), "QueueDoesNotExist") {
		t.Fatalf("send invalid queue url response = %d %s", sendRec.Code, sendRec.Body.String())
	}

	queryRec := serveQuery(t, server, "/path-shape", "Action=GetQueueAttributes&Version=2012-11-05&AttributeName.1=All")
	if queryRec.Code != http.StatusNotFound || !strings.Contains(queryRec.Body.String(), "<Code>InvalidAddress</Code>") {
		t.Fatalf("query invalid path response = %d %s", queryRec.Code, queryRec.Body.String())
	}
}

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

func TestJSONSendReceiveDeleteMessage(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"messages","Attributes":{"VisibilityTimeout":"1"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	sendRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"hello sqs","MessageAttributes":{"kind":{"DataType":"String","StringValue":"test"}}}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}
	if !strings.Contains(sendRec.Body.String(), `"MD5OfMessageBody":"3b7bef57d06c0021d0aafe8f6d587241"`) {
		t.Fatalf("send body = %s", sendRec.Body.String())
	}

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1,"MessageAttributeNames":["All"],"AttributeNames":["All"]}`)
	if receiveRec.Code != http.StatusOK {
		t.Fatalf("receive status = %d, body = %s", receiveRec.Code, receiveRec.Body.String())
	}
	var receiveBody struct {
		Messages []struct {
			Body              string                           `json:"Body"`
			ReceiptHandle     string                           `json:"ReceiptHandle"`
			Attributes        map[string]string                `json:"Attributes"`
			MessageAttributes map[string]messageAttributeValue `json:"MessageAttributes"`
		} `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}
	message := receiveBody.Messages[0]
	if message.Body != "hello sqs" || message.ReceiptHandle == "" {
		t.Fatalf("message = %#v", message)
	}
	if message.Attributes["ApproximateReceiveCount"] != "1" || message.Attributes["SentTimestamp"] == "" {
		t.Fatalf("attributes = %#v", message.Attributes)
	}
	if message.MessageAttributes["kind"].StringValue != "test" {
		t.Fatalf("message attributes = %#v", message.MessageAttributes)
	}

	emptyRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1,"WaitTimeSeconds":0}`)
	if strings.Contains(emptyRec.Body.String(), "hello sqs") {
		t.Fatalf("message should be invisible: %s", emptyRec.Body.String())
	}

	deleteRec := serveJSON(t, server, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+message.ReceiptHandle+`"}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	time.Sleep(1100 * time.Millisecond)
	afterDeleteRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1,"WaitTimeSeconds":0}`)
	if strings.Contains(afterDeleteRec.Body.String(), "hello sqs") {
		t.Fatalf("deleted message was received: %s", afterDeleteRec.Body.String())
	}
}

func TestLongPollingReceiveWakesWhenMessageArrives(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"long-poll"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	done := make(chan *httptest.ResponseRecorder, 1)
	start := time.Now()
	go func() {
		done <- serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1,"WaitTimeSeconds":2}`)
	}()

	select {
	case rec := <-done:
		t.Fatalf("long poll returned before a message was available: %s", rec.Body.String())
	case <-time.After(150 * time.Millisecond):
	}

	sendRec := serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"wake long poll"}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}

	select {
	case rec := <-done:
		if rec.Code != http.StatusOK {
			t.Fatalf("receive status = %d, body = %s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "wake long poll") {
			t.Fatalf("receive body = %s", rec.Body.String())
		}
		if elapsed := time.Since(start); elapsed >= time.Second {
			t.Fatalf("long poll did not wake promptly; elapsed = %s", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("long poll did not wake after a message was sent")
	}
}

func TestJSONMessageAttributeMD5IsReturnedOnSendAndReceive(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"attribute-md5"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	sendRec := serveJSON(t, server, "SendMessage", `{
		"QueueUrl":"`+queueURL+`",
		"MessageBody":"with attributes",
		"MessageAttributes":{
			"kind":{"DataType":"String","StringValue":"test"},
			"count":{"DataType":"Number","StringValue":"42"}
		}
	}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}
	var sendBody struct {
		MD5OfMessageAttributes string `json:"MD5OfMessageAttributes"`
	}
	if err := json.Unmarshal(sendRec.Body.Bytes(), &sendBody); err != nil {
		t.Fatalf("decode send body: %v", err)
	}
	if sendBody.MD5OfMessageAttributes == "" {
		t.Fatalf("send body missing MD5OfMessageAttributes: %s", sendRec.Body.String())
	}

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MessageAttributeNames":["All"]}`)
	var receiveBody struct {
		Messages []struct {
			MD5OfMessageAttributes string                           `json:"MD5OfMessageAttributes"`
			MessageAttributes      map[string]messageAttributeValue `json:"MessageAttributes"`
		} `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}
	message := receiveBody.Messages[0]
	if message.MD5OfMessageAttributes != sendBody.MD5OfMessageAttributes {
		t.Fatalf("receive MD5OfMessageAttributes = %q, want %q", message.MD5OfMessageAttributes, sendBody.MD5OfMessageAttributes)
	}
	if message.MessageAttributes["kind"].StringValue != "test" || message.MessageAttributes["count"].StringValue != "42" {
		t.Fatalf("message attributes = %#v", message.MessageAttributes)
	}
}

func TestSendMessageRejectsInvalidMessageAttributeValues(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"bad-message-attrs"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	numberRec := serveJSON(t, server, "SendMessage", `{
		"QueueUrl":"`+queueURL+`",
		"MessageBody":"bad number",
		"MessageAttributes":{"count":{"DataType":"Number","StringValue":"not-a-number"}}
	}`)
	if numberRec.Code != http.StatusBadRequest || !strings.Contains(numberRec.Body.String(), "InvalidAttributeValue") {
		t.Fatalf("number attribute response = %d %s", numberRec.Code, numberRec.Body.String())
	}

	systemRec := serveJSON(t, server, "SendMessage", `{
		"QueueUrl":"`+queueURL+`",
		"MessageBody":"bad system",
		"MessageSystemAttributes":{"Unsupported":{"DataType":"String","StringValue":"x"}}
	}`)
	if systemRec.Code != http.StatusBadRequest || !strings.Contains(systemRec.Body.String(), "InvalidAttributeValue") {
		t.Fatalf("system attribute response = %d %s", systemRec.Code, systemRec.Body.String())
	}
}

func TestSendMessageRejectsUnsupportedListMessageAttributeTypes(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"bad-list-message-attrs"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	for _, dataType := range []string{"String.List", "Binary.List"} {
		t.Run(dataType, func(t *testing.T) {
			payload := `{
				"QueueUrl":"` + queueURL + `",
				"MessageBody":"bad list attribute",
				"MessageAttributes":{"items":{"DataType":"` + dataType + `","StringListValues":["one"]}}
			}`
			rec := serveJSON(t, server, "SendMessage", payload)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "InvalidAttributeValue") {
				t.Fatalf("list attribute response = %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestSendMessageRejectsInvalidMessageAttributeNames(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"bad-message-attr-names"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	for _, tt := range []struct {
		name string
		attr string
	}{
		{name: "reserved aws prefix", attr: "AWS.Trace"},
		{name: "reserved amazon prefix", attr: "Amazon.Trace"},
		{name: "consecutive periods", attr: "trace..id"},
		{name: "leading period", attr: ".trace"},
		{name: "unsupported character", attr: "trace/id"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload := `{
				"QueueUrl":"` + queueURL + `",
				"MessageBody":"bad attribute name",
				"MessageAttributes":{` + strconv.Quote(tt.attr) + `:{"DataType":"String","StringValue":"x"}}
			}`
			rec := serveJSON(t, server, "SendMessage", payload)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "InvalidAttributeName") {
				t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestQuerySendMessageRejectsInvalidMessageAttributeName(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"query-bad-message-attr-names"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	rec := serveQuery(t, server, "/", "Action=SendMessage&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&MessageBody=query-attrs&MessageAttribute.1.Name=AWS.Trace&MessageAttribute.1.Value.DataType=String&MessageAttribute.1.Value.StringValue=x")
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "<Code>InvalidAttributeName</Code>") {
		t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
	}
}

func TestJSONReceiveMessageFiltersRequestedMessageAttributes(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"attribute-filter"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	sendRec := serveJSON(t, server, "SendMessage", `{
		"QueueUrl":"`+queueURL+`",
		"MessageBody":"with filtered attributes",
		"MessageAttributes":{
			"kind":{"DataType":"String","StringValue":"test"},
			"meta.region":{"DataType":"String","StringValue":"local"},
			"count":{"DataType":"Number","StringValue":"42"}
		}
	}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MessageAttributeNames":["kind","meta.*"]}`)
	var receiveBody struct {
		Messages []struct {
			MD5OfMessageAttributes string                           `json:"MD5OfMessageAttributes"`
			MessageAttributes      map[string]messageAttributeValue `json:"MessageAttributes"`
		} `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}
	attrs := receiveBody.Messages[0].MessageAttributes
	if attrs["kind"].StringValue != "test" || attrs["meta.region"].StringValue != "local" {
		t.Fatalf("message attributes = %#v", attrs)
	}
	if _, ok := attrs["count"]; ok {
		t.Fatalf("unrequested attribute was returned: %#v", attrs)
	}
	if receiveBody.Messages[0].MD5OfMessageAttributes == "" {
		t.Fatalf("receive body missing MD5OfMessageAttributes: %s", receiveRec.Body.String())
	}
}

func TestJSONMessageSystemAttributesPreserveAWSTraceHeader(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"system-attributes"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	sendRec := serveJSON(t, server, "SendMessage", `{
		"QueueUrl":"`+queueURL+`",
		"MessageBody":"with system attributes",
		"MessageSystemAttributes":{
			"AWSTraceHeader":{"DataType":"String","StringValue":"Root=1-abcdef12-345678912345678912345678"}
		}
	}`)
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}
	var sendBody struct {
		MD5OfMessageSystemAttributes string `json:"MD5OfMessageSystemAttributes"`
	}
	if err := json.Unmarshal(sendRec.Body.Bytes(), &sendBody); err != nil {
		t.Fatalf("decode send body: %v", err)
	}
	if sendBody.MD5OfMessageSystemAttributes == "" {
		t.Fatalf("send body missing MD5OfMessageSystemAttributes: %s", sendRec.Body.String())
	}

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MessageSystemAttributeNames":["AWSTraceHeader"]}`)
	var receiveBody struct {
		Messages []struct {
			Attributes                   map[string]string `json:"Attributes"`
			MD5OfMessageSystemAttributes string            `json:"MD5OfMessageSystemAttributes"`
		} `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}
	message := receiveBody.Messages[0]
	if message.Attributes["AWSTraceHeader"] != "Root=1-abcdef12-345678912345678912345678" {
		t.Fatalf("attributes = %#v", message.Attributes)
	}
	if message.MD5OfMessageSystemAttributes != sendBody.MD5OfMessageSystemAttributes {
		t.Fatalf("receive MD5OfMessageSystemAttributes = %q, want %q", message.MD5OfMessageSystemAttributes, sendBody.MD5OfMessageSystemAttributes)
	}
}

func TestQueryMessageSystemAttributesReturnXMLMD5(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"system-query"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	traceHeader := "Root=1-abcdef12-345678912345678912345678"

	sendRec := serveQuery(t, server, "/", "Action=SendMessage&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&MessageBody=query-system&MessageSystemAttribute.1.Name=AWSTraceHeader&MessageSystemAttribute.1.Value.DataType=String&MessageSystemAttribute.1.Value.StringValue="+urlQueryEscape(traceHeader))
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}
	if !strings.Contains(sendRec.Body.String(), "<MD5OfMessageSystemAttributes>") {
		t.Fatalf("send body = %s", sendRec.Body.String())
	}

	receiveRec := serveQuery(t, server, "/", "Action=ReceiveMessage&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&MessageSystemAttributeName.1=AWSTraceHeader")
	if receiveRec.Code != http.StatusOK {
		t.Fatalf("receive status = %d, body = %s", receiveRec.Code, receiveRec.Body.String())
	}
	if !strings.Contains(receiveRec.Body.String(), "<Name>AWSTraceHeader</Name>") || !strings.Contains(receiveRec.Body.String(), traceHeader) {
		t.Fatalf("receive body = %s", receiveRec.Body.String())
	}
	if !strings.Contains(receiveRec.Body.String(), "<MD5OfMessageSystemAttributes>") {
		t.Fatalf("receive body = %s", receiveRec.Body.String())
	}
}

func TestQueryReceiveMessageReturnsNestedMessageAttributeXML(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"query-message-attributes"}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]

	sendRec := serveQuery(t, server, "/", "Action=SendMessage&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&MessageBody=query-attrs&MessageAttribute.1.Name=kind&MessageAttribute.1.Value.DataType=String&MessageAttribute.1.Value.StringValue=loop")
	if sendRec.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", sendRec.Code, sendRec.Body.String())
	}

	receiveRec := serveQuery(t, server, "/", "Action=ReceiveMessage&Version=2012-11-05&QueueUrl="+urlQueryEscape(queueURL)+"&MessageAttributeName=All")
	if receiveRec.Code != http.StatusOK {
		t.Fatalf("receive status = %d, body = %s", receiveRec.Code, receiveRec.Body.String())
	}
	for _, want := range []string{
		"<MessageAttribute>",
		"<Name>kind</Name>",
		"<Value><DataType>String</DataType><StringValue>loop</StringValue></Value>",
		"<MD5OfMessageAttributes>",
	} {
		if !strings.Contains(receiveRec.Body.String(), want) {
			t.Fatalf("receive body missing %s: %s", want, receiveRec.Body.String())
		}
	}
}

func TestQueryReceiveMessageAcceptsReceiveRequestAttemptID(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ReceiveMessage&Version=2012-11-05&QueueUrl=http%3A%2F%2F127.0.0.1%3A9324%2F000000000000%2Fattempt&ReceiveRequestAttemptId=retry-1"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	input, err := parseReceiveMessageRequest(req, protocolQuery)
	if err != nil {
		t.Fatalf("parse receive request: %v", err)
	}
	if input.ReceiveRequestAttemptID != "retry-1" {
		t.Fatalf("ReceiveRequestAttemptID = %q", input.ReceiveRequestAttemptID)
	}
}

func TestJSONChangeMessageVisibilityMakesMessageVisible(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"visibility","Attributes":{"VisibilityTimeout":"30"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"visible again"}`)
	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1}`)
	var receiveBody struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}

	changeRec := serveJSON(t, server, "ChangeMessageVisibility", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiveBody.Messages[0].ReceiptHandle+`","VisibilityTimeout":0}`)
	if changeRec.Code != http.StatusOK {
		t.Fatalf("change status = %d, body = %s", changeRec.Code, changeRec.Body.String())
	}
	againRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1}`)
	if !strings.Contains(againRec.Body.String(), "visible again") {
		t.Fatalf("message was not visible again: %s", againRec.Body.String())
	}
}

func TestExpiredReceiptHandleCannotDeleteOrChangeVisibility(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:9324"})
	createRec := serveJSON(t, server, "CreateQueue", `{"QueueName":"expired-receipt","Attributes":{"VisibilityTimeout":"1"}}`)
	var createBody map[string]string
	if err := json.Unmarshal(createRec.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create body: %v", err)
	}
	queueURL := createBody["QueueUrl"]
	serveJSON(t, server, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"lease expires"}`)

	receiveRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1}`)
	var receiveBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(receiveRec.Body.Bytes(), &receiveBody); err != nil {
		t.Fatalf("decode receive body: %v", err)
	}
	if len(receiveBody.Messages) != 1 || receiveBody.Messages[0].ReceiptHandle == "" {
		t.Fatalf("messages = %#v", receiveBody.Messages)
	}
	expiredHandle := receiveBody.Messages[0].ReceiptHandle
	time.Sleep(1100 * time.Millisecond)

	deleteRec := serveJSON(t, server, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+expiredHandle+`"}`)
	if deleteRec.Code != http.StatusBadRequest || !strings.Contains(deleteRec.Body.String(), "ReceiptHandleIsInvalid") {
		t.Fatalf("delete expired handle response = %d %s", deleteRec.Code, deleteRec.Body.String())
	}
	changeRec := serveJSON(t, server, "ChangeMessageVisibility", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+expiredHandle+`","VisibilityTimeout":10}`)
	if changeRec.Code != http.StatusBadRequest || !strings.Contains(changeRec.Body.String(), "ReceiptHandleIsInvalid") {
		t.Fatalf("change expired handle response = %d %s", changeRec.Code, changeRec.Body.String())
	}

	againRec := serveJSON(t, server, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":1,"VisibilityTimeout":30}`)
	var againBody struct {
		Messages []receivedMessage `json:"Messages"`
	}
	if err := json.Unmarshal(againRec.Body.Bytes(), &againBody); err != nil {
		t.Fatalf("decode second receive body: %v", err)
	}
	if len(againBody.Messages) != 1 || againBody.Messages[0].ReceiptHandle == "" || againBody.Messages[0].ReceiptHandle == expiredHandle {
		t.Fatalf("second receive messages = %#v", againBody.Messages)
	}
	deleteRec = serveJSON(t, server, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+againBody.Messages[0].ReceiptHandle+`"}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete fresh handle status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
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

func serveJSON(t *testing.T, server *Server, operation string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+operation)
	rec := httptest.NewRecorder()
	server.routes().ServeHTTP(rec, req)
	return rec
}

func serveQuery(t *testing.T, server *Server, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	server.routes().ServeHTTP(rec, req)
	return rec
}

func signedSQSJSONRequest(t *testing.T, operation string, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+operation)
	req.Header.Set("x-amz-date", "20260501T120000Z")
	payloadHash := sha256Hex([]byte(body))
	req.Header.Set("x-amz-content-sha256", payloadHash)

	signedHeaders := "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-target"
	dateStamp := "20260501"
	scope := dateStamp + "/us-east-1/sqs/aws4_request"
	canonicalRequest := strings.Join([]string{
		req.Method,
		"/",
		"",
		canonicalHeaders(req, signedHeaders),
		signedHeaders,
		payloadHash,
	}, "\n")
	stringToSign := strings.Join([]string{
		sigV4Algorithm,
		"20260501T120000Z",
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signature := hmacSHA256(deriveSigningKey("dev", dateStamp, "us-east-1"), stringToSign)
	req.Header.Set("Authorization", sigV4Algorithm+" Credential=dev/"+scope+", SignedHeaders="+signedHeaders+", Signature="+hex.EncodeToString(signature))
	return req
}

func urlQueryEscape(value string) string {
	replacer := strings.NewReplacer(":", "%3A", "/", "%2F")
	return replacer.Replace(value)
}
