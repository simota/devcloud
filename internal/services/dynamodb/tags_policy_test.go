package dynamodb

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"
	"testing"
)

func TestTableTagsCanBeListedUpdatedUntaggedAndPersisted(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	createTestTable(t, server)
	tableArn := "arn:aws:dynamodb:us-east-1:000000000000:table/Demo"

	tagRec := dynamodbRequest(t, server, "TagResource", `{
		"ResourceArn":"`+tableArn+`",
		"Tags":[
			{"Key":"env","Value":"dev"},
			{"Key":"owner","Value":"platform"}
		]
	}`)
	if tagRec.Code != http.StatusOK {
		t.Fatalf("TagResource status = %d, body = %s", tagRec.Code, tagRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "TagResource", `{
		"ResourceArn":"`+tableArn+`",
		"Tags":[{"Key":"env","Value":"test"}]
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("second TagResource status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}

	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	listRec := dynamodbRequest(t, reloaded, "ListTagsOfResource", `{"ResourceArn":"`+tableArn+`"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("ListTagsOfResource status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var listResponse struct {
		Tags []tag
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode ListTagsOfResource: %v", err)
	}
	if !reflect.DeepEqual(listResponse.Tags, []tag{{Key: "env", Value: "test"}, {Key: "owner", Value: "platform"}}) {
		t.Fatalf("Tags = %#v, want sorted persisted tags", listResponse.Tags)
	}

	untagRec := dynamodbRequest(t, reloaded, "UntagResource", `{
		"ResourceArn":"`+tableArn+`",
		"TagKeys":["owner","missing"]
	}`)
	if untagRec.Code != http.StatusOK {
		t.Fatalf("UntagResource status = %d, body = %s", untagRec.Code, untagRec.Body.String())
	}
	listRec = dynamodbRequest(t, reloaded, "ListTagsOfResource", `{"ResourceArn":"`+tableArn+`"}`)
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode ListTagsOfResource after untag: %v", err)
	}
	if !reflect.DeepEqual(listResponse.Tags, []tag{{Key: "env", Value: "test"}}) {
		t.Fatalf("Tags after untag = %#v, want env only", listResponse.Tags)
	}
}

func TestTagResourceRejectsMissingTableARN(t *testing.T) {
	server := NewServer(Config{})

	rec := dynamodbRequest(t, server, "TagResource", `{
		"ResourceArn":"arn:aws:dynamodb:us-east-1:000000000000:table/Missing",
		"Tags":[{"Key":"env","Value":"dev"}]
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("TagResource status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ResourceNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ResourceNotFoundException", got)
	}
}

func TestResourcePolicyCanBePutReadDeletedAndPersisted(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	createTestTable(t, server)
	tableArn := "arn:aws:dynamodb:us-east-1:000000000000:table/Demo"
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"dynamodb:GetItem","Resource":"` + tableArn + `"}]}`

	putRec := dynamodbRequest(t, server, "PutResourcePolicy", `{
		"ResourceArn":"`+tableArn+`",
		"Policy":`+strconv.Quote(policy)+`
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutResourcePolicy status = %d, body = %s", putRec.Code, putRec.Body.String())
	}
	var putResponse struct {
		RevisionID string `json:"RevisionId"`
	}
	if err := json.NewDecoder(putRec.Body).Decode(&putResponse); err != nil {
		t.Fatalf("decode PutResourcePolicy: %v", err)
	}
	if putResponse.RevisionID == "" {
		t.Fatalf("RevisionId is empty")
	}

	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	getRec := dynamodbRequest(t, reloaded, "GetResourcePolicy", `{"ResourceArn":"`+tableArn+`"}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetResourcePolicy status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Policy     string `json:"Policy"`
		RevisionID string `json:"RevisionId"`
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetResourcePolicy: %v", err)
	}
	if getResponse.Policy != policy || getResponse.RevisionID != putResponse.RevisionID {
		t.Fatalf("GetResourcePolicy = %#v, want persisted policy and revision", getResponse)
	}

	deleteRec := dynamodbRequest(t, reloaded, "DeleteResourcePolicy", `{"ResourceArn":"`+tableArn+`"}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteResourcePolicy status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	missingRec := dynamodbRequest(t, reloaded, "GetResourcePolicy", `{"ResourceArn":"`+tableArn+`"}`)
	if missingRec.Code != http.StatusBadRequest {
		t.Fatalf("GetResourcePolicy after delete status = %d, want %d, body = %s", missingRec.Code, http.StatusBadRequest, missingRec.Body.String())
	}
	if got := missingRec.Header().Get("X-Amzn-Errortype"); got != "PolicyNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want PolicyNotFoundException", got)
	}
}

func TestPutResourcePolicyRejectsInvalidPolicyJSON(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "PutResourcePolicy", `{
		"ResourceArn":"arn:aws:dynamodb:us-east-1:000000000000:table/Demo",
		"Policy":"not-json"
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PutResourcePolicy status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

