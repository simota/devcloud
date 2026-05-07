package sqs

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

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
