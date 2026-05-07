package sqs

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

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
