package dynamodb

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestListTablesReturnsEmptyTableNames(t *testing.T) {
	server := NewServer(Config{})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/x-amz-json-1.0" {
		t.Fatalf("Content-Type = %q, want application/x-amz-json-1.0", got)
	}
	var response struct {
		TableNames []string
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.TableNames) != 0 {
		t.Fatalf("TableNames = %#v, want empty", response.TableNames)
	}
}

func TestUnknownOperationReturnsDynamoDBJSONError(t *testing.T) {
	server := NewServer(Config{})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Nope")
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "UnknownOperationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want UnknownOperationException", got)
	}
	if body := rec.Body.String(); !strings.Contains(body, "com.amazonaws.dynamodb.v20120810#UnknownOperationException") {
		t.Fatalf("body missing DynamoDB error type: %s", body)
	}
}

func TestStrictAuthRejectsUnsignedRequests(t *testing.T) {
	server := NewServer(Config{AuthMode: "strict"})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "AccessDeniedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want AccessDeniedException", got)
	}
}

func TestStrictAuthAcceptsSignedRequestsAndPreservesBody(t *testing.T) {
	server := NewServer(Config{
		AuthMode:        "strict",
		Region:          "us-east-1",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	})
	req := signedDynamoDBRequest(t, "CreateTable", `{
		"TableName":"SignedDemo",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]
	}`)
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("signed CreateTable status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode signed CreateTable: %v", err)
	}
	if response.TableDescription.TableName != "SignedDemo" {
		t.Fatalf("TableName = %q, want SignedDemo", response.TableDescription.TableName)
	}
}

func TestStrictAuthRejectsPayloadHashMismatch(t *testing.T) {
	server := NewServer(Config{AuthMode: "strict", Region: "us-east-1", AccessKeyID: "dev", SecretAccessKey: "dev"})
	req := signedDynamoDBRequest(t, "ListTables", `{}`)
	req.Header.Set("x-amz-content-sha256", strings.Repeat("0", 64))
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "InvalidSignatureException" {
		t.Fatalf("X-Amzn-Errortype = %q, want InvalidSignatureException", got)
	}
}

func TestRelaxedAuthAcceptsSignedRequestsWithoutCredentialMatch(t *testing.T) {
	server := NewServer(Config{
		AuthMode:        "relaxed",
		Region:          "us-east-1",
		AccessKeyID:     "configured",
		SecretAccessKey: "configured",
	})
	req := signedDynamoDBRequest(t, "ListTables", `{}`)
	req.Header.Set("Authorization", strings.Replace(req.Header.Get("Authorization"), "Credential=dev/20260501/us-east-1/dynamodb/aws4_request", "Credential=other/20260501/ap-northeast-1/dynamodb/aws4_request", 1))
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestSignedRelaxedAuthRejectsUnsignedRequests(t *testing.T) {
	server := NewServer(Config{AuthMode: "signed-relaxed"})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "AccessDeniedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want AccessDeniedException", got)
	}
}

func TestSignedRelaxedAuthAcceptsSigV4ShapeWithoutCredentialMatch(t *testing.T) {
	server := NewServer(Config{
		AuthMode:        "signed-relaxed",
		Region:          "us-east-1",
		AccessKeyID:     "configured",
		SecretAccessKey: "configured",
	})
	req := signedDynamoDBRequest(t, "ListTables", `{}`)
	req.Header.Set("Authorization", strings.Replace(req.Header.Get("Authorization"), "Credential=dev/20260501/us-east-1/dynamodb/aws4_request", "Credential=other/20260501/ap-northeast-1/dynamodb/aws4_request", 1))
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response struct {
		TableNames []string
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.TableNames) != 0 {
		t.Fatalf("TableNames = %#v, want empty", response.TableNames)
	}
}

func TestSignedRelaxedAuthRejectsMalformedSigV4Shape(t *testing.T) {
	server := NewServer(Config{AuthMode: "signed-relaxed"})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	req.Header.Set("x-amz-date", "20260501T120000Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=dev/20260501/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=not-hex")
	rec := httptest.NewRecorder()

	server.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "IncompleteSignatureException" {
		t.Fatalf("X-Amzn-Errortype = %q, want IncompleteSignatureException", got)
	}
}

func TestTableLifecycle(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})

	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"Demo",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"BillingMode":"PAY_PER_REQUEST"
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateTable: %v", err)
	}
	if createResponse.TableDescription.TableName != "Demo" || createResponse.TableDescription.TableStatus != "ACTIVE" {
		t.Fatalf("TableDescription = %#v", createResponse.TableDescription)
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Demo"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeTable status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if describeResponse.Table.TableName != "Demo" || describeResponse.Table.TableStatus != "ACTIVE" {
		t.Fatalf("Table = %#v", describeResponse.Table)
	}

	listRec := dynamodbRequest(t, server, "ListTables", `{}`)
	var listResponse struct {
		TableNames []string
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode ListTables: %v", err)
	}
	if !reflect.DeepEqual(listResponse.TableNames, []string{"Demo"}) {
		t.Fatalf("TableNames = %#v, want Demo", listResponse.TableNames)
	}

	deleteRec := dynamodbRequest(t, server, "DeleteTable", `{"TableName":"Demo"}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteTable status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	listRec = dynamodbRequest(t, server, "ListTables", `{}`)
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode ListTables after delete: %v", err)
	}
	if len(listResponse.TableNames) != 0 {
		t.Fatalf("TableNames after delete = %#v, want empty", listResponse.TableNames)
	}
}

func TestListTablesPaginatesWithExclusiveStartTableName(t *testing.T) {
	server := NewServer(Config{})
	for _, tableName := range []string{"Bravo", "Alpha", "Charlie"} {
		rec := dynamodbRequest(t, server, "CreateTable", `{
			"TableName":"`+tableName+`",
			"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
			"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]
		}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("CreateTable(%s) status = %d, body = %s", tableName, rec.Code, rec.Body.String())
		}
	}

	firstRec := dynamodbRequest(t, server, "ListTables", `{"Limit":2}`)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first ListTables status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}
	var firstResponse struct {
		TableNames             []string
		LastEvaluatedTableName string
	}
	if err := json.NewDecoder(firstRec.Body).Decode(&firstResponse); err != nil {
		t.Fatalf("decode first ListTables: %v", err)
	}
	if !reflect.DeepEqual(firstResponse.TableNames, []string{"Alpha", "Bravo"}) {
		t.Fatalf("first TableNames = %#v, want Alpha, Bravo", firstResponse.TableNames)
	}
	if firstResponse.LastEvaluatedTableName != "Bravo" {
		t.Fatalf("LastEvaluatedTableName = %q, want Bravo", firstResponse.LastEvaluatedTableName)
	}

	secondRec := dynamodbRequest(t, server, "ListTables", `{"ExclusiveStartTableName":"Bravo","Limit":2}`)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second ListTables status = %d, body = %s", secondRec.Code, secondRec.Body.String())
	}
	var secondResponse struct {
		TableNames             []string
		LastEvaluatedTableName string
	}
	if err := json.NewDecoder(secondRec.Body).Decode(&secondResponse); err != nil {
		t.Fatalf("decode second ListTables: %v", err)
	}
	if !reflect.DeepEqual(secondResponse.TableNames, []string{"Charlie"}) {
		t.Fatalf("second TableNames = %#v, want Charlie", secondResponse.TableNames)
	}
	if secondResponse.LastEvaluatedTableName != "" {
		t.Fatalf("second LastEvaluatedTableName = %q, want empty", secondResponse.LastEvaluatedTableName)
	}
}

func TestCreateTableRejectsDuplicate(t *testing.T) {
	server := NewServer(Config{})
	payload := `{
		"TableName":"Demo",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]
	}`
	first := dynamodbRequest(t, server, "CreateTable", payload)
	if first.Code != http.StatusOK {
		t.Fatalf("first CreateTable status = %d, body = %s", first.Code, first.Body.String())
	}
	second := dynamodbRequest(t, server, "CreateTable", payload)
	if second.Code != http.StatusBadRequest {
		t.Fatalf("second CreateTable status = %d, want %d", second.Code, http.StatusBadRequest)
	}
	if got := second.Header().Get("X-Amzn-Errortype"); got != "ResourceInUseException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ResourceInUseException", got)
	}
}

func TestCreateTableRejectsWhenMaxTablesExceeded(t *testing.T) {
	server := NewServer(Config{MaxTables: 1})
	first := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"First",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]
	}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first CreateTable status = %d, body = %s", first.Code, first.Body.String())
	}

	second := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"Second",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]
	}`)
	if second.Code != http.StatusBadRequest {
		t.Fatalf("second CreateTable status = %d, want %d, body = %s", second.Code, http.StatusBadRequest, second.Body.String())
	}
	if got := second.Header().Get("X-Amzn-Errortype"); got != "LimitExceededException" {
		t.Fatalf("X-Amzn-Errortype = %q, want LimitExceededException", got)
	}
}

func TestUpdateTableUpdatesBillingModeSummary(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	updateRec := dynamodbRequest(t, server, "UpdateTable", `{
		"TableName":"Demo",
		"BillingMode":"PROVISIONED"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateTable status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateTable: %v", err)
	}
	if updateResponse.TableDescription.TableName != "Demo" {
		t.Fatalf("TableName = %q, want Demo", updateResponse.TableDescription.TableName)
	}
	if updateResponse.TableDescription.BillingModeSummary == nil || updateResponse.TableDescription.BillingModeSummary.BillingMode != "PROVISIONED" {
		t.Fatalf("BillingModeSummary = %#v, want PROVISIONED", updateResponse.TableDescription.BillingModeSummary)
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Demo"}`)
	var describeResponse struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if describeResponse.Table.BillingModeSummary == nil || describeResponse.Table.BillingModeSummary.BillingMode != "PROVISIONED" {
		t.Fatalf("described BillingModeSummary = %#v, want PROVISIONED", describeResponse.Table.BillingModeSummary)
	}
}

func TestUpdateTableCanCreateAndDeleteGlobalSecondaryIndex(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"gpk":{"S":"group#1"},
			"gsk":{"N":"10"},
			"name":{"S":"Ada"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	createIndexRec := dynamodbRequest(t, server, "UpdateTable", `{
		"TableName":"Demo",
		"AttributeDefinitions":[
			{"AttributeName":"gpk","AttributeType":"S"},
			{"AttributeName":"gsk","AttributeType":"N"}
		],
		"GlobalSecondaryIndexUpdates":[{
			"Create":{
				"IndexName":"gsi1",
				"KeySchema":[
					{"AttributeName":"gpk","KeyType":"HASH"},
					{"AttributeName":"gsk","KeyType":"RANGE"}
				],
				"Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":["name"]}
			}
		}]
	}`)
	if createIndexRec.Code != http.StatusOK {
		t.Fatalf("UpdateTable create GSI status = %d, body = %s", createIndexRec.Code, createIndexRec.Body.String())
	}
	var createResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(createIndexRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode create GSI UpdateTable: %v", err)
	}
	if len(createResponse.TableDescription.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("GlobalSecondaryIndexes = %#v, want one index", createResponse.TableDescription.GlobalSecondaryIndexes)
	}
	if createResponse.TableDescription.GlobalSecondaryIndexes[0].ItemCount != 1 {
		t.Fatalf("GSI ItemCount = %d, want 1", createResponse.TableDescription.GlobalSecondaryIndexes[0].ItemCount)
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Demo",
		"IndexName":"gsi1",
		"KeyConditionExpression":"gpk = :gpk",
		"ExpressionAttributeValues":{":gpk":{"S":"group#1"}}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("GSI Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var queryResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&queryResponse); err != nil {
		t.Fatalf("decode GSI Query: %v", err)
	}
	if queryResponse.Count != 1 || queryResponse.Items[0]["name"]["S"] != "Ada" {
		t.Fatalf("GSI Query response = %#v, want projected item", queryResponse)
	}

	deleteIndexRec := dynamodbRequest(t, server, "UpdateTable", `{
		"TableName":"Demo",
		"GlobalSecondaryIndexUpdates":[{"Delete":{"IndexName":"gsi1"}}]
	}`)
	if deleteIndexRec.Code != http.StatusOK {
		t.Fatalf("UpdateTable delete GSI status = %d, body = %s", deleteIndexRec.Code, deleteIndexRec.Body.String())
	}
	var deleteResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(deleteIndexRec.Body).Decode(&deleteResponse); err != nil {
		t.Fatalf("decode delete GSI UpdateTable: %v", err)
	}
	if len(deleteResponse.TableDescription.GlobalSecondaryIndexes) != 0 {
		t.Fatalf("GlobalSecondaryIndexes after delete = %#v, want none", deleteResponse.TableDescription.GlobalSecondaryIndexes)
	}

	missingQueryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Demo",
		"IndexName":"gsi1",
		"KeyConditionExpression":"gpk = :gpk",
		"ExpressionAttributeValues":{":gpk":{"S":"group#1"}}
	}`)
	if missingQueryRec.Code != http.StatusBadRequest {
		t.Fatalf("Query deleted GSI status = %d, want %d, body = %s", missingQueryRec.Code, http.StatusBadRequest, missingQueryRec.Body.String())
	}
}

func TestUpdateTableRejectsInvalidGlobalSecondaryIndexDefinition(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "UpdateTable", `{
		"TableName":"Demo",
		"AttributeDefinitions":[{"AttributeName":"gpk","AttributeType":"BOOL"}],
		"GlobalSecondaryIndexUpdates":[{
			"Create":{
				"IndexName":"gsi1",
				"KeySchema":[{"AttributeName":"gpk","KeyType":"HASH"}],
				"Projection":{"ProjectionType":"ALL"}
			}
		}]
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("UpdateTable invalid GSI status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Demo"}`)
	var response struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if len(response.Table.GlobalSecondaryIndexes) != 0 {
		t.Fatalf("rejected GSI update mutated table: %#v", response.Table.GlobalSecondaryIndexes)
	}
}

func TestStreamsMetadataCanBeCreatedListedDescribedUpdatedAndPersisted(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})

	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"StreamDemo",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
		"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateTable: %v", err)
	}
	if createResponse.TableDescription.LatestStreamArn == "" || createResponse.TableDescription.LatestStreamLabel == "" {
		t.Fatalf("stream identifiers were not set: %#v", createResponse.TableDescription)
	}
	if createResponse.TableDescription.StreamSpecification == nil || createResponse.TableDescription.StreamSpecification.StreamViewType != "NEW_AND_OLD_IMAGES" {
		t.Fatalf("StreamSpecification = %#v, want NEW_AND_OLD_IMAGES", createResponse.TableDescription.StreamSpecification)
	}

	listRec := dynamodbRequest(t, server, "ListStreams", `{"TableName":"StreamDemo"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("ListStreams status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var listResponse struct {
		Streams []streamSummary
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode ListStreams: %v", err)
	}
	if len(listResponse.Streams) != 1 || listResponse.Streams[0].StreamArn != createResponse.TableDescription.LatestStreamArn {
		t.Fatalf("ListStreams = %#v, want created stream", listResponse.Streams)
	}

	describeRec := dynamodbRequest(t, server, "DescribeStream", `{"StreamArn":"`+createResponse.TableDescription.LatestStreamArn+`"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeStream status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		StreamDescription streamDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeStream: %v", err)
	}
	if describeResponse.StreamDescription.TableName != "StreamDemo" || describeResponse.StreamDescription.StreamStatus != "ENABLED" {
		t.Fatalf("StreamDescription = %#v", describeResponse.StreamDescription)
	}
	if len(describeResponse.StreamDescription.Shards) != 1 {
		t.Fatalf("Shards = %#v, want one local shard", describeResponse.StreamDescription.Shards)
	}
	shardID := describeResponse.StreamDescription.Shards[0].ShardID
	iteratorRec := dynamodbRequest(t, server, "GetShardIterator", `{
		"StreamArn":"`+createResponse.TableDescription.LatestStreamArn+`",
		"ShardId":"`+shardID+`",
		"ShardIteratorType":"TRIM_HORIZON"
	}`)
	if iteratorRec.Code != http.StatusOK {
		t.Fatalf("GetShardIterator status = %d, body = %s", iteratorRec.Code, iteratorRec.Body.String())
	}
	var iteratorResponse struct {
		ShardIterator string
	}
	if err := json.NewDecoder(iteratorRec.Body).Decode(&iteratorResponse); err != nil {
		t.Fatalf("decode GetShardIterator: %v", err)
	}
	if iteratorResponse.ShardIterator == "" {
		t.Fatal("ShardIterator is empty")
	}

	recordsRec := dynamodbRequest(t, server, "GetRecords", `{"ShardIterator":"`+iteratorResponse.ShardIterator+`","Limit":10}`)
	if recordsRec.Code != http.StatusOK {
		t.Fatalf("GetRecords status = %d, body = %s", recordsRec.Code, recordsRec.Body.String())
	}
	var recordsResponse struct {
		NextShardIterator string
		Records           []map[string]any
	}
	if err := json.NewDecoder(recordsRec.Body).Decode(&recordsResponse); err != nil {
		t.Fatalf("decode GetRecords: %v", err)
	}
	if recordsResponse.NextShardIterator == "" {
		t.Fatal("NextShardIterator is empty")
	}
	if len(recordsResponse.Records) != 0 {
		t.Fatalf("Records = %#v, want empty stream records before mutations", recordsResponse.Records)
	}

	updateRec := dynamodbRequest(t, server, "UpdateTable", `{
		"TableName":"StreamDemo",
		"StreamSpecification":{"StreamEnabled":false}
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateTable stream disable status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	listRec = dynamodbRequest(t, reloaded, "ListStreams", `{"TableName":"StreamDemo"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("reloaded ListStreams status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	listResponse.Streams = nil
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode reloaded ListStreams: %v", err)
	}
	if len(listResponse.Streams) != 0 {
		t.Fatalf("ListStreams after disable = %#v, want empty", listResponse.Streams)
	}
}

func TestStreamRecordsCaptureItemMutationsAndPersist(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})

	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"StreamDemo",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
		"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateTable: %v", err)
	}

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"StreamDemo",
		"Item":{"pk":{"S":"user#1"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}
	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"StreamDemo",
		"Key":{"pk":{"S":"user#1"}},
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Grace"}}
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	deleteRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"StreamDemo",
		"Key":{"pk":{"S":"user#1"}}
	}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteItem status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}

	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	describeRec := dynamodbRequest(t, reloaded, "DescribeStream", `{"StreamArn":"`+createResponse.TableDescription.LatestStreamArn+`"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeStream status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		StreamDescription streamDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeStream: %v", err)
	}
	iteratorRec := dynamodbRequest(t, reloaded, "GetShardIterator", `{
		"StreamArn":"`+createResponse.TableDescription.LatestStreamArn+`",
		"ShardId":"`+describeResponse.StreamDescription.Shards[0].ShardID+`",
		"ShardIteratorType":"TRIM_HORIZON"
	}`)
	if iteratorRec.Code != http.StatusOK {
		t.Fatalf("GetShardIterator status = %d, body = %s", iteratorRec.Code, iteratorRec.Body.String())
	}
	var iteratorResponse struct {
		ShardIterator string
	}
	if err := json.NewDecoder(iteratorRec.Body).Decode(&iteratorResponse); err != nil {
		t.Fatalf("decode GetShardIterator: %v", err)
	}
	recordsRec := dynamodbRequest(t, reloaded, "GetRecords", `{"ShardIterator":"`+iteratorResponse.ShardIterator+`","Limit":2}`)
	if recordsRec.Code != http.StatusOK {
		t.Fatalf("GetRecords status = %d, body = %s", recordsRec.Code, recordsRec.Body.String())
	}
	var recordsResponse struct {
		NextShardIterator string
		Records           []streamRecord
	}
	if err := json.NewDecoder(recordsRec.Body).Decode(&recordsResponse); err != nil {
		t.Fatalf("decode GetRecords: %v", err)
	}
	if len(recordsResponse.Records) != 2 {
		t.Fatalf("first Records len = %d, want 2: %#v", len(recordsResponse.Records), recordsResponse.Records)
	}
	if recordsResponse.Records[0].EventName != "INSERT" || recordsResponse.Records[0].DynamoDB.NewImage["name"]["S"] != "Ada" {
		t.Fatalf("insert stream record = %#v", recordsResponse.Records[0])
	}
	if recordsResponse.Records[1].EventName != "MODIFY" || recordsResponse.Records[1].DynamoDB.OldImage["name"]["S"] != "Ada" || recordsResponse.Records[1].DynamoDB.NewImage["name"]["S"] != "Grace" {
		t.Fatalf("modify stream record = %#v", recordsResponse.Records[1])
	}

	nextRec := dynamodbRequest(t, reloaded, "GetRecords", `{"ShardIterator":"`+recordsResponse.NextShardIterator+`","Limit":2}`)
	if nextRec.Code != http.StatusOK {
		t.Fatalf("next GetRecords status = %d, body = %s", nextRec.Code, nextRec.Body.String())
	}
	var nextResponse struct {
		Records []streamRecord
	}
	if err := json.NewDecoder(nextRec.Body).Decode(&nextResponse); err != nil {
		t.Fatalf("decode next GetRecords: %v", err)
	}
	if len(nextResponse.Records) != 1 || nextResponse.Records[0].EventName != "REMOVE" || nextResponse.Records[0].DynamoDB.OldImage["name"]["S"] != "Grace" {
		t.Fatalf("remove stream record = %#v", nextResponse.Records)
	}
}

func TestCreateTableRejectsInvalidStreamSpecification(t *testing.T) {
	server := NewServer(Config{})
	rec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"BadStream",
		"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
		"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
		"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"ALL_IMAGES"}
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("CreateTable status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

func TestDescribeLimitsReturnsLocalCapacityEnvelope(t *testing.T) {
	server := NewServer(Config{})
	rec := dynamodbRequest(t, server, "DescribeLimits", `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("DescribeLimits status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		AccountMaxReadCapacityUnits  int
		AccountMaxWriteCapacityUnits int
		TableMaxReadCapacityUnits    int
		TableMaxWriteCapacityUnits   int
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode DescribeLimits: %v", err)
	}
	if response.AccountMaxReadCapacityUnits <= 0 || response.AccountMaxWriteCapacityUnits <= 0 || response.TableMaxReadCapacityUnits <= 0 || response.TableMaxWriteCapacityUnits <= 0 {
		t.Fatalf("DescribeLimits response has non-positive capacity: %#v", response)
	}
}

func TestDescribeEndpointsReturnsLocalEndpointDiscoveryMetadata(t *testing.T) {
	server := NewServer(Config{Addr: "127.0.0.1:8010"})
	rec := dynamodbRequest(t, server, "DescribeEndpoints", `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("DescribeEndpoints status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Endpoints []struct {
			Address              string
			CachePeriodInMinutes int64
		}
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode DescribeEndpoints: %v", err)
	}
	if len(response.Endpoints) != 1 {
		t.Fatalf("Endpoints = %#v, want one local endpoint", response.Endpoints)
	}
	if response.Endpoints[0].Address != "127.0.0.1:8010" || response.Endpoints[0].CachePeriodInMinutes <= 0 {
		t.Fatalf("endpoint metadata = %#v, want configured address and positive cache period", response.Endpoints[0])
	}
}

func TestDescribeContinuousBackupsReturnsLocalMetadata(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "DescribeContinuousBackups", `{"TableName":"Demo"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("DescribeContinuousBackups status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		ContinuousBackupsDescription continuousBackupsDescription
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode DescribeContinuousBackups: %v", err)
	}
	if response.ContinuousBackupsDescription.ContinuousBackupsStatus != "ENABLED" {
		t.Fatalf("ContinuousBackupsStatus = %q, want ENABLED", response.ContinuousBackupsDescription.ContinuousBackupsStatus)
	}
	if response.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus != "DISABLED" {
		t.Fatalf("PointInTimeRecoveryStatus = %q, want DISABLED", response.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus)
	}
}

func TestUpdateContinuousBackupsPersistsPointInTimeRecoveryMetadata(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{StoragePath: storagePath})
	createTestTable(t, server)

	updateRec := dynamodbRequest(t, server, "UpdateContinuousBackups", `{
		"TableName":"Demo",
		"PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true}
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateContinuousBackups status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}

	reloaded := NewServer(Config{StoragePath: storagePath})
	describeRec := dynamodbRequest(t, reloaded, "DescribeContinuousBackups", `{"TableName":"Demo"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeContinuousBackups status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var response struct {
		ContinuousBackupsDescription continuousBackupsDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode DescribeContinuousBackups: %v", err)
	}
	if response.ContinuousBackupsDescription.ContinuousBackupsStatus != "ENABLED" {
		t.Fatalf("ContinuousBackupsStatus = %q, want ENABLED", response.ContinuousBackupsDescription.ContinuousBackupsStatus)
	}
	if response.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus != "ENABLED" {
		t.Fatalf("PointInTimeRecoveryStatus = %q, want ENABLED", response.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus)
	}
}

func TestDescribeContinuousBackupsRequiresExistingTable(t *testing.T) {
	server := NewServer(Config{})

	rec := dynamodbRequest(t, server, "DescribeContinuousBackups", `{"TableName":"Missing"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("DescribeContinuousBackups status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ResourceNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ResourceNotFoundException", got)
	}
}

func TestBackupMetadataCanBeCreatedListedDescribedDeletedAndPersisted(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	createTestTable(t, server)

	createRec := dynamodbRequest(t, server, "CreateBackup", `{
		"TableName":"Demo",
		"BackupName":"snapshot-1"
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateBackup status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		BackupDetails backupDetails
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateBackup: %v", err)
	}
	if createResponse.BackupDetails.BackupName != "snapshot-1" || createResponse.BackupDetails.BackupStatus != "AVAILABLE" {
		t.Fatalf("BackupDetails = %#v, want available snapshot-1", createResponse.BackupDetails)
	}
	if createResponse.BackupDetails.BackupArn == "" {
		t.Fatalf("BackupArn is empty")
	}

	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	listRec := dynamodbRequest(t, reloaded, "ListBackups", `{"TableName":"Demo"}`)
	if listRec.Code != http.StatusOK {
		t.Fatalf("ListBackups status = %d, body = %s", listRec.Code, listRec.Body.String())
	}
	var listResponse struct {
		BackupSummaries []backupSummary
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResponse); err != nil {
		t.Fatalf("decode ListBackups: %v", err)
	}
	if len(listResponse.BackupSummaries) != 1 || listResponse.BackupSummaries[0].BackupArn != createResponse.BackupDetails.BackupArn {
		t.Fatalf("BackupSummaries = %#v, want created backup", listResponse.BackupSummaries)
	}

	describeRec := dynamodbRequest(t, reloaded, "DescribeBackup", fmt.Sprintf(`{"BackupArn":%q}`, createResponse.BackupDetails.BackupArn))
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeBackup status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		BackupDescription backupDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeBackup: %v", err)
	}
	if describeResponse.BackupDescription.SourceTableDetails.TableName != "Demo" {
		t.Fatalf("SourceTableDetails = %#v, want Demo table", describeResponse.BackupDescription.SourceTableDetails)
	}

	deleteRec := dynamodbRequest(t, reloaded, "DeleteBackup", fmt.Sprintf(`{"BackupArn":%q}`, createResponse.BackupDetails.BackupArn))
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteBackup status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	missingRec := dynamodbRequest(t, reloaded, "DescribeBackup", fmt.Sprintf(`{"BackupArn":%q}`, createResponse.BackupDetails.BackupArn))
	if missingRec.Code != http.StatusBadRequest {
		t.Fatalf("DescribeBackup after delete status = %d, want %d, body = %s", missingRec.Code, http.StatusBadRequest, missingRec.Body.String())
	}
	if got := missingRec.Header().Get("X-Amzn-Errortype"); got != "BackupNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want BackupNotFoundException", got)
	}
}

func TestCreateBackupRequiresExistingTable(t *testing.T) {
	server := NewServer(Config{})

	rec := dynamodbRequest(t, server, "CreateBackup", `{"TableName":"Missing","BackupName":"snapshot-1"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("CreateBackup status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ResourceNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ResourceNotFoundException", got)
	}
}

func TestRestoreTableFromBackupRestoresSchemaAndItems(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	createTestTable(t, server)

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}
	createRec := dynamodbRequest(t, server, "CreateBackup", `{
		"TableName":"Demo",
		"BackupName":"snapshot-restore"
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateBackup status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		BackupDetails backupDetails
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateBackup: %v", err)
	}

	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	restoreRec := dynamodbRequest(t, reloaded, "RestoreTableFromBackup", fmt.Sprintf(`{
		"BackupArn":%q,
		"TargetTableName":"DemoRestored"
	}`, createResponse.BackupDetails.BackupArn))
	if restoreRec.Code != http.StatusOK {
		t.Fatalf("RestoreTableFromBackup status = %d, body = %s", restoreRec.Code, restoreRec.Body.String())
	}
	var restoreResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(restoreRec.Body).Decode(&restoreResponse); err != nil {
		t.Fatalf("decode RestoreTableFromBackup: %v", err)
	}
	if restoreResponse.TableDescription.TableName != "DemoRestored" || restoreResponse.TableDescription.TableStatus != "ACTIVE" {
		t.Fatalf("restored TableDescription = %#v, want active DemoRestored", restoreResponse.TableDescription)
	}
	if len(restoreResponse.TableDescription.KeySchema) != 2 || restoreResponse.TableDescription.ItemCount != 1 {
		t.Fatalf("restored schema/count = %#v, want key schema and one item", restoreResponse.TableDescription)
	}

	afterRestore := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	getRec := dynamodbRequest(t, afterRestore, "GetItem", `{
		"TableName":"DemoRestored",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem restored status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode restored GetItem: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("restored item name = %#v, want Ada", got)
	}
}

func TestRestoreTableFromBackupRejectsMissingBackup(t *testing.T) {
	server := NewServer(Config{})
	rec := dynamodbRequest(t, server, "RestoreTableFromBackup", `{
		"BackupArn":"arn:aws:dynamodb:us-east-1:000000000000:table/Missing/backup/1-snapshot",
		"TargetTableName":"Restored"
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("RestoreTableFromBackup status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "BackupNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want BackupNotFoundException", got)
	}
}

func TestTimeToLiveMetadataCanBeDescribedUpdatedAndPersisted(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{StoragePath: storagePath})
	createTestTable(t, server)

	describeRec := dynamodbRequest(t, server, "DescribeTimeToLive", `{"TableName":"Demo"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeTimeToLive status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var disabledResponse struct {
		TimeToLiveDescription timeToLiveDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&disabledResponse); err != nil {
		t.Fatalf("decode disabled DescribeTimeToLive: %v", err)
	}
	if disabledResponse.TimeToLiveDescription.TimeToLiveStatus != "DISABLED" {
		t.Fatalf("initial TTL status = %q, want DISABLED", disabledResponse.TimeToLiveDescription.TimeToLiveStatus)
	}

	updateRec := dynamodbRequest(t, server, "UpdateTimeToLive", `{
		"TableName":"Demo",
		"TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateTimeToLive status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		TimeToLiveSpecification timeToLiveSpecification
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateTimeToLive: %v", err)
	}
	if !updateResponse.TimeToLiveSpecification.Enabled || updateResponse.TimeToLiveSpecification.AttributeName != "expiresAt" {
		t.Fatalf("TimeToLiveSpecification = %#v, want enabled expiresAt", updateResponse.TimeToLiveSpecification)
	}

	reloaded := NewServer(Config{StoragePath: storagePath})
	describeRec = dynamodbRequest(t, reloaded, "DescribeTimeToLive", `{"TableName":"Demo"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("reloaded DescribeTimeToLive status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var enabledResponse struct {
		TimeToLiveDescription timeToLiveDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&enabledResponse); err != nil {
		t.Fatalf("decode enabled DescribeTimeToLive: %v", err)
	}
	if enabledResponse.TimeToLiveDescription.TimeToLiveStatus != "ENABLED" || enabledResponse.TimeToLiveDescription.AttributeName != "expiresAt" {
		t.Fatalf("persisted TTL description = %#v, want enabled expiresAt", enabledResponse.TimeToLiveDescription)
	}
}

func TestUpdateTimeToLiveRequiresAttributeNameWhenEnabled(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "UpdateTimeToLive", `{
		"TableName":"Demo",
		"TimeToLiveSpecification":{"Enabled":true}
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("UpdateTimeToLive status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

func TestTimeToLiveExpiresItemsAndPersistsCleanup(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{StoragePath: storagePath})
	createTestTable(t, server)

	updateRec := dynamodbRequest(t, server, "UpdateTimeToLive", `{
		"TableName":"Demo",
		"TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expiresAt"}
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateTimeToLive status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}

	now := time.Now().Unix()
	putExpiredRec := dynamodbRequest(t, server, "PutItem", fmt.Sprintf(`{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"expired"},"expiresAt":{"N":"%d"}}
	}`, now-1))
	if putExpiredRec.Code != http.StatusOK {
		t.Fatalf("expired PutItem status = %d, body = %s", putExpiredRec.Code, putExpiredRec.Body.String())
	}
	putLiveRec := dynamodbRequest(t, server, "PutItem", fmt.Sprintf(`{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"live"},"expiresAt":{"N":"%d"}}
	}`, now+3600))
	if putLiveRec.Code != http.StatusOK {
		t.Fatalf("live PutItem status = %d, body = %s", putLiveRec.Code, putLiveRec.Body.String())
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{"TableName":"Demo"}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if len(scanResponse.Items) != 1 || scanResponse.Items[0]["sk"]["S"] != "live" {
		t.Fatalf("Scan Items = %#v, want only live item", scanResponse.Items)
	}

	reloaded := NewServer(Config{StoragePath: storagePath})
	getExpiredRec := dynamodbRequest(t, reloaded, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"expired"}}
	}`)
	var getExpiredResponse struct {
		Item item
	}
	if err := json.NewDecoder(getExpiredRec.Body).Decode(&getExpiredResponse); err != nil {
		t.Fatalf("decode expired GetItem: %v", err)
	}
	if len(getExpiredResponse.Item) != 0 {
		t.Fatalf("expired item persisted after TTL cleanup: %#v", getExpiredResponse.Item)
	}
}

func TestDescribeTableMissingReturnsResourceNotFound(t *testing.T) {
	server := NewServer(Config{})
	rec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Missing"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ResourceNotFoundException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ResourceNotFoundException", got)
	}
}

func TestItemAndReadOperationsRequireTableName(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	cases := []struct {
		name      string
		operation string
		body      string
	}{
		{
			name:      "GetItem",
			operation: "GetItem",
			body:      `{"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}}`,
		},
		{
			name:      "DeleteItem",
			operation: "DeleteItem",
			body:      `{"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}}`,
		},
		{
			name:      "UpdateItem",
			operation: "UpdateItem",
			body: `{
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"UpdateExpression":"SET #n = :name",
				"ExpressionAttributeNames":{"#n":"name"},
				"ExpressionAttributeValues":{":name":{"S":"Ada"}}
			}`,
		},
		{
			name:      "Query",
			operation: "Query",
			body: `{
				"KeyConditionExpression":"pk = :pk",
				"ExpressionAttributeValues":{":pk":{"S":"user#1"}}
			}`,
		},
		{
			name:      "Scan",
			operation: "Scan",
			body:      `{}`,
		},
	}

	for _, tc := range cases {
		rec := dynamodbRequest(t, server, tc.operation, tc.body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d, body = %s", tc.name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Fatalf("%s X-Amzn-Errortype = %q, want ValidationException", tc.name, got)
		}
		if !strings.Contains(rec.Body.String(), "table name is required") {
			t.Fatalf("%s body missing table-name validation message: %s", tc.name, rec.Body.String())
		}
	}
}

func TestItemCRUD(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"name":{"S":"Ada"},
			"age":{"N":"37"},
			"active":{"BOOL":true},
			"tags":{"SS":["engineer","admin"]},
			"meta":{"M":{"team":{"S":"core"}}},
			"scores":{"L":[{"N":"1"},{"N":"2"}]}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("name = %#v, want Ada", got)
	}
	if got := getResponse.Item["active"]["BOOL"]; got != true {
		t.Fatalf("active = %#v, want true", got)
	}

	deleteRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteItem status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	getRec = dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var emptyGetResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&emptyGetResponse); err != nil {
		t.Fatalf("decode empty GetItem: %v", err)
	}
	if len(emptyGetResponse.Item) != 0 {
		t.Fatalf("Item after delete = %#v, want empty", emptyGetResponse.Item)
	}
}

func TestPutAndDeleteItemRejectInvalidReturnValuesWithoutMutation(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	invalidPutRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Grace"}},
		"ReturnValues":"ALL_NEW"
	}`)
	if invalidPutRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid PutItem status = %d, want %d, body = %s", invalidPutRec.Code, http.StatusBadRequest, invalidPutRec.Body.String())
	}
	if got := invalidPutRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("invalid PutItem X-Amzn-Errortype = %q, want ValidationException", got)
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem after rejected PutItem: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("name after rejected PutItem = %#v, want Ada", got)
	}

	invalidDeleteRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ReturnValues":"UPDATED_OLD"
	}`)
	if invalidDeleteRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid DeleteItem status = %d, want %d, body = %s", invalidDeleteRec.Code, http.StatusBadRequest, invalidDeleteRec.Body.String())
	}
	if got := invalidDeleteRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("invalid DeleteItem X-Amzn-Errortype = %q, want ValidationException", got)
	}

	getRec = dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	getResponse.Item = nil
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem after rejected DeleteItem: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("name after rejected DeleteItem = %#v, want Ada", got)
	}
}

func TestItemWritesRejectOversizedItems(t *testing.T) {
	server := NewServer(Config{MaxItemBytes: 120})
	createTestTable(t, server)

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"payload":{"S":"this value intentionally exceeds the configured local item byte limit"}
		}
	}`)
	if putRec.Code != http.StatusBadRequest {
		t.Fatalf("oversized PutItem status = %d, want %d, body = %s", putRec.Code, http.StatusBadRequest, putRec.Body.String())
	}
	if got := putRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}

	smallPut := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if smallPut.Code != http.StatusOK {
		t.Fatalf("small PutItem status = %d, body = %s", smallPut.Code, smallPut.Body.String())
	}
	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET payload = :payload",
		"ExpressionAttributeValues":{
			":payload":{"S":"this update also exceeds the configured local item byte limit"}
		}
	}`)
	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("oversized UpdateItem status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
	}
	if got := updateRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("UpdateItem X-Amzn-Errortype = %q, want ValidationException", got)
	}
	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem after rejected update: %v", err)
	}
	if _, ok := getResponse.Item["payload"]; ok {
		t.Fatalf("rejected oversized update mutated item: %#v", getResponse.Item)
	}
}

func TestItemWritesRejectInvalidAttributeValueShapes(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	for name, body := range map[string]string{
		"multiple types": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"broken":{"S":"x","N":"1"}}
		}`,
		"invalid number": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"broken":{"N":"not-a-number"}}
		}`,
		"empty set": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#3"},"sk":{"S":"profile"},"broken":{"SS":[]}}
		}`,
		"duplicate string set value": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#4"},"sk":{"S":"profile"},"broken":{"SS":["red","red"]}}
		}`,
		"duplicate number set value": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#5"},"sk":{"S":"profile"},"broken":{"NS":["1","1"]}}
		}`,
		"invalid binary value": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#6"},"sk":{"S":"profile"},"broken":{"B":"not-base64"}}
		}`,
		"invalid binary set value": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#7"},"sk":{"S":"profile"},"broken":{"BS":["YQ==","not-base64"]}}
		}`,
		"nested invalid value": `{
			"TableName":"Demo",
			"Item":{"pk":{"S":"user#8"},"sk":{"S":"profile"},"meta":{"M":{"broken":{"NULL":false}}}}
		}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s PutItem status = %d, want %d, body = %s", name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Fatalf("%s X-Amzn-Errortype = %q, want ValidationException", name, got)
		}
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Demo"}`)
	var describeResponse struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if describeResponse.Table.ItemCount != 0 {
		t.Fatalf("ItemCount after rejected invalid writes = %d, want 0", describeResponse.Table.ItemCount)
	}
}

func TestKeyOperationsRejectInvalidAttributeValueShapes(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	cases := []struct {
		name      string
		operation string
		body      string
	}{
		{
			name:      "GetItem",
			operation: "GetItem",
			body: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1","N":"1"},"sk":{"S":"profile"}}
			}`,
		},
		{
			name:      "DeleteItem",
			operation: "DeleteItem",
			body: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"NULL":false}}
			}`,
		},
		{
			name:      "UpdateItem",
			operation: "UpdateItem",
			body: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"N":"not-a-number"}},
				"UpdateExpression":"SET #n = :name",
				"ExpressionAttributeNames":{"#n":"name"},
				"ExpressionAttributeValues":{":name":{"S":"Grace"}}
			}`,
		},
		{
			name:      "BatchGetItem",
			operation: "BatchGetItem",
			body: `{
				"RequestItems":{
					"Demo":{"Keys":[{"pk":{"S":"user#1"},"sk":{"SS":[]}}]}
				}
			}`,
		},
		{
			name:      "BatchWriteItem",
			operation: "BatchWriteItem",
			body: `{
				"RequestItems":{
					"Demo":[{"DeleteRequest":{"Key":{"pk":{"S":"user#1"},"sk":{"M":{"bad":{"NULL":false}}}}}}]
				}
			}`,
		},
	}

	for _, tc := range cases {
		rec := dynamodbRequest(t, server, tc.operation, tc.body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d, body = %s", tc.name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Fatalf("%s X-Amzn-Errortype = %q, want ValidationException", tc.name, got)
		}
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem after rejected key operations status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem after rejected key operations: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("name after rejected key operations = %#v, want Ada", got)
	}
}

func TestGetItemSupportsProjectionExpression(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"name":{"S":"Ada"},
			"active":{"BOOL":true}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ProjectionExpression":"pk, #n",
		"ExpressionAttributeNames":{"#n":"name"},
		"ConsistentRead":true
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var response struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if _, ok := response.Item["pk"]; !ok {
		t.Fatalf("projected item missing pk: %#v", response.Item)
	}
	if got := response.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("projected name = %#v, want Ada", got)
	}
	if _, ok := response.Item["active"]; ok {
		t.Fatalf("projected item included unrequested active attribute: %#v", response.Item)
	}
}

func TestConditionalPutRejectsExistingItem(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	payload := `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"attribute_not_exists(pk)"
	}`
	first := dynamodbRequest(t, server, "PutItem", payload)
	if first.Code != http.StatusOK {
		t.Fatalf("first PutItem status = %d, body = %s", first.Code, first.Body.String())
	}
	second := dynamodbRequest(t, server, "PutItem", payload)
	if second.Code != http.StatusBadRequest {
		t.Fatalf("second PutItem status = %d, want %d", second.Code, http.StatusBadRequest)
	}
	if got := second.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
}

func TestConditionalPutSupportsAttributeValueComparison(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"version":{"N":"1"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	conditionalRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"version":{"N":"2"}},
		"ConditionExpression":"#v = :expected",
		"ExpressionAttributeNames":{"#v":"version"},
		"ExpressionAttributeValues":{":expected":{"N":"1"}}
	}`)
	if conditionalRec.Code != http.StatusOK {
		t.Fatalf("conditional PutItem status = %d, body = %s", conditionalRec.Code, conditionalRec.Body.String())
	}

	rejectRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"version":{"N":"3"}},
		"ConditionExpression":"#v = :expected",
		"ExpressionAttributeNames":{"#v":"version"},
		"ExpressionAttributeValues":{":expected":{"N":"1"}}
	}`)
	if rejectRec.Code != http.StatusBadRequest {
		t.Fatalf("rejected PutItem status = %d, want %d, body = %s", rejectRec.Code, http.StatusBadRequest, rejectRec.Body.String())
	}
	if got := rejectRec.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
}

func TestItemWritesReturnOldItemOnConditionCheckFailure(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"version":{"N":"1"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"version = :expected",
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{
			":expected":{"N":"2"},
			":name":{"S":"Ada Lovelace"}
		},
		"ReturnValuesOnConditionCheckFailure":"ALL_OLD"
	}`)
	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("UpdateItem status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
	}
	if got := updateRec.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
	var updateResponse struct {
		Item item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem conditional failure: %v", err)
	}
	if got := updateResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("conditional failure Item name = %#v, want Ada", got)
	}

	deleteRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"attribute_not_exists(pk)",
		"ReturnValuesOnConditionCheckFailure":"ALL_OLD"
	}`)
	if deleteRec.Code != http.StatusBadRequest {
		t.Fatalf("DeleteItem status = %d, want %d, body = %s", deleteRec.Code, http.StatusBadRequest, deleteRec.Body.String())
	}
	var deleteResponse struct {
		Item item
	}
	if err := json.NewDecoder(deleteRec.Body).Decode(&deleteResponse); err != nil {
		t.Fatalf("decode DeleteItem conditional failure: %v", err)
	}
	if got := deleteResponse.Item["version"]["N"]; got != "1" {
		t.Fatalf("conditional failure Item version = %#v, want 1", got)
	}
}

func TestUpdateItemSetReturnsAllNew(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name, visits = :one",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{
			":name":{"S":"Ada Lovelace"},
			":one":{"N":"1"}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["name"]["S"]; got != "Ada Lovelace" {
		t.Fatalf("updated name = %#v, want Ada Lovelace", got)
	}
	if got := updateResponse.Attributes["visits"]["N"]; got != "1" {
		t.Fatalf("visits = %#v, want 1", got)
	}
}

func TestUpdateItemSupportsReturnValuesVariants(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"visits":{"N":"1"},"obsolete":{"S":"old"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updatedNewRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name, visits = visits + :one REMOVE obsolete",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{
			":name":{"S":"Ada Lovelace"},
			":one":{"N":"1"}
		},
		"ReturnValues":"UPDATED_NEW"
	}`)
	if updatedNewRec.Code != http.StatusOK {
		t.Fatalf("UPDATED_NEW status = %d, body = %s", updatedNewRec.Code, updatedNewRec.Body.String())
	}
	var updatedNewResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updatedNewRec.Body).Decode(&updatedNewResponse); err != nil {
		t.Fatalf("decode UPDATED_NEW response: %v", err)
	}
	if got := updatedNewResponse.Attributes["name"]["S"]; got != "Ada Lovelace" {
		t.Fatalf("UPDATED_NEW name = %#v, want Ada Lovelace", got)
	}
	if got := updatedNewResponse.Attributes["visits"]["N"]; got != "2" {
		t.Fatalf("UPDATED_NEW visits = %#v, want 2", got)
	}
	if _, ok := updatedNewResponse.Attributes["obsolete"]; ok {
		t.Fatalf("UPDATED_NEW included removed attribute: %#v", updatedNewResponse.Attributes)
	}
	if _, ok := updatedNewResponse.Attributes["pk"]; ok {
		t.Fatalf("UPDATED_NEW included unchanged key: %#v", updatedNewResponse.Attributes)
	}

	updatedOldRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name REMOVE visits",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Ada Byron"}},
		"ReturnValues":"UPDATED_OLD"
	}`)
	if updatedOldRec.Code != http.StatusOK {
		t.Fatalf("UPDATED_OLD status = %d, body = %s", updatedOldRec.Code, updatedOldRec.Body.String())
	}
	var updatedOldResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updatedOldRec.Body).Decode(&updatedOldResponse); err != nil {
		t.Fatalf("decode UPDATED_OLD response: %v", err)
	}
	if got := updatedOldResponse.Attributes["name"]["S"]; got != "Ada Lovelace" {
		t.Fatalf("UPDATED_OLD name = %#v, want Ada Lovelace", got)
	}
	if got := updatedOldResponse.Attributes["visits"]["N"]; got != "2" {
		t.Fatalf("UPDATED_OLD visits = %#v, want 2", got)
	}
	if _, ok := updatedOldResponse.Attributes["pk"]; ok {
		t.Fatalf("UPDATED_OLD included unchanged key: %#v", updatedOldResponse.Attributes)
	}

	allOldRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Countess Lovelace"}},
		"ReturnValues":"ALL_OLD"
	}`)
	if allOldRec.Code != http.StatusOK {
		t.Fatalf("ALL_OLD status = %d, body = %s", allOldRec.Code, allOldRec.Body.String())
	}
	var allOldResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(allOldRec.Body).Decode(&allOldResponse); err != nil {
		t.Fatalf("decode ALL_OLD response: %v", err)
	}
	if got := allOldResponse.Attributes["name"]["S"]; got != "Ada Byron" {
		t.Fatalf("ALL_OLD name = %#v, want Ada Byron", got)
	}

	noneRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Ada"}},
		"ReturnValues":"NONE"
	}`)
	if noneRec.Code != http.StatusOK {
		t.Fatalf("NONE status = %d, body = %s", noneRec.Code, noneRec.Body.String())
	}
	var noneResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(noneRec.Body).Decode(&noneResponse); err != nil {
		t.Fatalf("decode NONE response: %v", err)
	}
	if len(noneResponse.Attributes) != 0 {
		t.Fatalf("NONE Attributes = %#v, want empty", noneResponse.Attributes)
	}
}

func TestUpdateItemRejectsInvalidReturnValuesWithoutMutation(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{":name":{"S":"Grace"}},
		"ReturnValues":"BROKEN"
	}`)
	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("UpdateItem status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
	}
	if got := updateRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("name after rejected update = %#v, want Ada", got)
	}
}

func TestUpdateItemSupportsSetAndRemove(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"name":{"S":"Ada"},
			"obsolete":{"S":"remove-me"},
			"ttl":{"N":"123"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET #n = :name REMOVE obsolete, #ttl",
		"ExpressionAttributeNames":{"#n":"name","#ttl":"ttl"},
		"ExpressionAttributeValues":{":name":{"S":"Ada Lovelace"}},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["name"]["S"]; got != "Ada Lovelace" {
		t.Fatalf("updated name = %#v, want Ada Lovelace", got)
	}
	if _, ok := updateResponse.Attributes["obsolete"]; ok {
		t.Fatalf("obsolete attribute was not removed: %#v", updateResponse.Attributes)
	}
	if _, ok := updateResponse.Attributes["ttl"]; ok {
		t.Fatalf("ttl attribute was not removed: %#v", updateResponse.Attributes)
	}
}

func TestUpdateItemSupportsSetArithmeticAndIfNotExists(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"visits":{"N":"2"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET visits = visits + :one, remaining = :ten - :three, created = if_not_exists(created, :created)",
		"ExpressionAttributeValues":{
			":one":{"N":"1"},
			":ten":{"N":"10"},
			":three":{"N":"3"},
			":created":{"S":"initial"}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["visits"]["N"]; got != "3" {
		t.Fatalf("visits = %#v, want 3", got)
	}
	if got := updateResponse.Attributes["remaining"]["N"]; got != "7" {
		t.Fatalf("remaining = %#v, want 7", got)
	}
	if got := updateResponse.Attributes["created"]["S"]; got != "initial" {
		t.Fatalf("created = %#v, want initial", got)
	}

	secondRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET created = if_not_exists(created, :replacement)",
		"ExpressionAttributeValues":{":replacement":{"S":"replacement"}},
		"ReturnValues":"ALL_NEW"
	}`)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second UpdateItem status = %d, body = %s", secondRec.Code, secondRec.Body.String())
	}
	var secondResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(secondRec.Body).Decode(&secondResponse); err != nil {
		t.Fatalf("decode second UpdateItem: %v", err)
	}
	if got := secondResponse.Attributes["created"]["S"]; got != "initial" {
		t.Fatalf("created after if_not_exists = %#v, want initial", got)
	}
}

func TestUpdateItemSupportsListAppend(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"events":{"L":[{"S":"created"}]}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"SET events = list_append(events, :tail), prefixed = list_append(:head, events)",
		"ExpressionAttributeValues":{
			":tail":{"L":[{"S":"updated"},{"N":"2"}]},
			":head":{"L":[{"S":"first"}]}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	events := attributeValueList(updateResponse.Attributes["events"]["L"])
	if len(events) != 3 || events[0]["S"] != "created" || events[1]["S"] != "updated" || events[2]["N"] != "2" {
		t.Fatalf("events = %#v, want appended list", updateResponse.Attributes["events"])
	}
	prefixed := attributeValueList(updateResponse.Attributes["prefixed"]["L"])
	if len(prefixed) != 4 || prefixed[0]["S"] != "first" || prefixed[1]["S"] != "created" || prefixed[2]["S"] != "updated" || prefixed[3]["N"] != "2" {
		t.Fatalf("prefixed = %#v, want prepended current events", updateResponse.Attributes["prefixed"])
	}
}

func TestUpdateItemSupportsAddNumberAndSetUnion(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"visits":{"N":"2"},
			"tags":{"SS":["engineer"]}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"ADD visits :one, tags :tags, scores :score",
		"ExpressionAttributeValues":{
			":one":{"N":"1"},
			":tags":{"SS":["admin","engineer"]},
			":score":{"NS":["10"]}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["visits"]["N"]; got != "3" {
		t.Fatalf("visits = %#v, want 3", got)
	}
	if got := updateResponse.Attributes["tags"]["SS"]; !reflect.DeepEqual(got, []any{"engineer", "admin"}) {
		t.Fatalf("tags = %#v, want engineer/admin union", got)
	}
	if got := updateResponse.Attributes["scores"]["NS"]; !reflect.DeepEqual(got, []any{"10"}) {
		t.Fatalf("scores = %#v, want new number set", got)
	}
}

func TestUpdateItemSupportsDeleteSetValues(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"tags":{"SS":["engineer","admin","owner"]},
			"scores":{"NS":["10"]},
			"bins":{"BS":["YQ==","Yg=="]}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"UpdateExpression":"DELETE tags :tags, scores :scores, bins :bins, missing :missing",
		"ExpressionAttributeValues":{
			":tags":{"SS":["admin"]},
			":scores":{"NS":["10"]},
			":bins":{"BS":["YQ=="]},
			":missing":{"SS":["unused"]}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["tags"]["SS"]; !reflect.DeepEqual(got, []any{"engineer", "owner"}) {
		t.Fatalf("tags = %#v, want admin removed", got)
	}
	if _, ok := updateResponse.Attributes["scores"]; ok {
		t.Fatalf("scores should be removed after deleting all members: %#v", updateResponse.Attributes)
	}
	if got := updateResponse.Attributes["bins"]["BS"]; !reflect.DeepEqual(got, []any{"Yg=="}) {
		t.Fatalf("bins = %#v, want remaining binary set member", got)
	}
	if _, ok := updateResponse.Attributes["missing"]; ok {
		t.Fatalf("missing attribute should remain absent: %#v", updateResponse.Attributes)
	}
}

func TestUpdateAndDeleteItemHonorConditionExpression(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"version":{"N":"1"},"name":{"S":"Ada"}}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	rejectedUpdate := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"#v = :expected",
		"UpdateExpression":"SET #n = :name",
		"ExpressionAttributeNames":{"#v":"version","#n":"name"},
		"ExpressionAttributeValues":{
			":expected":{"N":"2"},
			":name":{"S":"Grace"}
		}
	}`)
	if rejectedUpdate.Code != http.StatusBadRequest {
		t.Fatalf("rejected UpdateItem status = %d, want %d, body = %s", rejectedUpdate.Code, http.StatusBadRequest, rejectedUpdate.Body.String())
	}
	if got := rejectedUpdate.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}

	deleteRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"#v = :expected",
		"ExpressionAttributeNames":{"#v":"version"},
		"ExpressionAttributeValues":{":expected":{"N":"1"}}
	}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("DeleteItem status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}
	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if len(getResponse.Item) != 0 {
		t.Fatalf("Item after conditional delete = %#v, want empty", getResponse.Item)
	}
}

func TestQuerySupportsKeyConditionLimitAndLastEvaluatedKey(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"event#001"},"kind":{"S":"event"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"event#002"},"kind":{"S":"event"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"event#001"},"kind":{"S":"event"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Demo",
		"KeyConditionExpression":"pk = :pk AND begins_with(sk, :prefix)",
		"ExpressionAttributeValues":{
			":pk":{"S":"user#1"},
			":prefix":{"S":"event#"}
		},
		"Limit":1
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var response struct {
		Items            []item
		Count            int
		LastEvaluatedKey item
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode Query: %v", err)
	}
	if response.Count != 1 || len(response.Items) != 1 {
		t.Fatalf("Query count/items = %d/%d, want 1/1", response.Count, len(response.Items))
	}
	if got := response.Items[0]["pk"]["S"]; got != "user#1" {
		t.Fatalf("Query pk = %#v, want user#1", got)
	}
	if len(response.LastEvaluatedKey) == 0 {
		t.Fatalf("LastEvaluatedKey missing from limited Query response")
	}
}

func TestQueryOrdersNumericSortKeysByDynamoDBTypeSemantics(t *testing.T) {
	server := NewServer(Config{})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"NumericSort",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"score","AttributeType":"N"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"score","KeyType":"RANGE"}
		]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	for _, payload := range []string{
		`{"TableName":"NumericSort","Item":{"pk":{"S":"group#1"},"score":{"N":"10"},"label":{"S":"ten"}}}`,
		`{"TableName":"NumericSort","Item":{"pk":{"S":"group#1"},"score":{"N":"2"},"label":{"S":"two"}}}`,
		`{"TableName":"NumericSort","Item":{"pk":{"S":"group#1"},"score":{"N":"1.5"},"label":{"S":"one-point-five"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"NumericSort",
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"group#1"}}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var response struct {
		Items []item
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode Query: %v", err)
	}
	got := []any{response.Items[0]["score"]["N"], response.Items[1]["score"]["N"], response.Items[2]["score"]["N"]}
	if !reflect.DeepEqual(got, []any{"1.5", "2", "10"}) {
		t.Fatalf("ascending scores = %#v, want numeric order 1.5, 2, 10", got)
	}

	descendingRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"NumericSort",
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"group#1"}},
		"ScanIndexForward":false
	}`)
	if descendingRec.Code != http.StatusOK {
		t.Fatalf("descending Query status = %d, body = %s", descendingRec.Code, descendingRec.Body.String())
	}
	var descendingResponse struct {
		Items []item
	}
	if err := json.NewDecoder(descendingRec.Body).Decode(&descendingResponse); err != nil {
		t.Fatalf("decode descending Query: %v", err)
	}
	got = []any{descendingResponse.Items[0]["score"]["N"], descendingResponse.Items[1]["score"]["N"], descendingResponse.Items[2]["score"]["N"]}
	if !reflect.DeepEqual(got, []any{"10", "2", "1.5"}) {
		t.Fatalf("descending scores = %#v, want numeric order 10, 2, 1.5", got)
	}
}

func TestQuerySupportsSortKeyComparisonAndBetween(t *testing.T) {
	server := NewServer(Config{})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"Scores",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"score","AttributeType":"N"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"score","KeyType":"RANGE"}
		]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	for _, payload := range []string{
		`{"TableName":"Scores","Item":{"pk":{"S":"group#1"},"score":{"N":"1"},"label":{"S":"one"}}}`,
		`{"TableName":"Scores","Item":{"pk":{"S":"group#1"},"score":{"N":"2"},"label":{"S":"two"}}}`,
		`{"TableName":"Scores","Item":{"pk":{"S":"group#1"},"score":{"N":"3"},"label":{"S":"three"}}}`,
		`{"TableName":"Scores","Item":{"pk":{"S":"group#2"},"score":{"N":"2"},"label":{"S":"other"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Scores",
		"KeyConditionExpression":"pk = :pk AND score >= :min",
		"ExpressionAttributeValues":{
			":pk":{"S":"group#1"},
			":min":{"N":"2"}
		}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("comparison Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var comparisonResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&comparisonResponse); err != nil {
		t.Fatalf("decode comparison Query: %v", err)
	}
	if comparisonResponse.Count != 2 {
		t.Fatalf("comparison Query count = %d, want 2; items=%#v", comparisonResponse.Count, comparisonResponse.Items)
	}

	betweenRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Scores",
		"KeyConditionExpression":"pk = :pk AND score BETWEEN :low AND :high",
		"ExpressionAttributeValues":{
			":pk":{"S":"group#1"},
			":low":{"N":"2"},
			":high":{"N":"2"}
		}
	}`)
	if betweenRec.Code != http.StatusOK {
		t.Fatalf("BETWEEN Query status = %d, body = %s", betweenRec.Code, betweenRec.Body.String())
	}
	var betweenResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(betweenRec.Body).Decode(&betweenResponse); err != nil {
		t.Fatalf("decode BETWEEN Query: %v", err)
	}
	if betweenResponse.Count != 1 || betweenResponse.Items[0]["label"]["S"] != "two" {
		t.Fatalf("BETWEEN Query response = %#v, want score 2 item", betweenResponse)
	}
}

func TestScanSupportsFilterAndProjection(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"active":{"BOOL":true},"name":{"S":"Ada"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"active":{"BOOL":false},"name":{"S":"Grace"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"active = :active",
		"ExpressionAttributeValues":{":active":{"BOOL":true}},
		"ProjectionExpression":"pk, sk, active"
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var response struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if response.Count != 1 || len(response.Items) != 1 {
		t.Fatalf("Scan count/items = %d/%d, want 1/1", response.Count, len(response.Items))
	}
	if _, ok := response.Items[0]["name"]; ok {
		t.Fatalf("Scan projection included unrequested name attribute: %#v", response.Items[0])
	}
	if got := response.Items[0]["active"]["BOOL"]; got != true {
		t.Fatalf("Scan active = %#v, want true", got)
	}
}

func TestScanLimitAppliesBeforeFilterExpression(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"active":{"BOOL":false}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"active":{"BOOL":true}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	firstRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"active = :active",
		"ExpressionAttributeValues":{":active":{"BOOL":true}},
		"Limit":1
	}`)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first Scan status = %d, body = %s", firstRec.Code, firstRec.Body.String())
	}
	var firstResponse struct {
		Items            []item
		Count            int
		ScannedCount     int
		LastEvaluatedKey item
	}
	if err := json.NewDecoder(firstRec.Body).Decode(&firstResponse); err != nil {
		t.Fatalf("decode first Scan: %v", err)
	}
	if firstResponse.Count != 0 || len(firstResponse.Items) != 0 || firstResponse.ScannedCount != 1 {
		t.Fatalf("first Scan response = %#v, want no returned items after evaluating one filtered item", firstResponse)
	}
	if len(firstResponse.LastEvaluatedKey) == 0 {
		t.Fatalf("first Scan LastEvaluatedKey missing")
	}

	secondPayload, err := json.Marshal(map[string]any{
		"TableName":                 "Demo",
		"FilterExpression":          "active = :active",
		"ExpressionAttributeValues": map[string]attributeValue{":active": {"BOOL": true}},
		"ExclusiveStartKey":         firstResponse.LastEvaluatedKey,
		"Limit":                     1,
	})
	if err != nil {
		t.Fatalf("marshal second Scan payload: %v", err)
	}
	secondRec := dynamodbRequest(t, server, "Scan", string(secondPayload))
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second Scan status = %d, body = %s", secondRec.Code, secondRec.Body.String())
	}
	var secondResponse struct {
		Items        []item
		Count        int
		ScannedCount int
	}
	if err := json.NewDecoder(secondRec.Body).Decode(&secondResponse); err != nil {
		t.Fatalf("decode second Scan: %v", err)
	}
	if secondResponse.Count != 1 || len(secondResponse.Items) != 1 || secondResponse.ScannedCount != 1 {
		t.Fatalf("second Scan response = %#v, want one matching item after resume", secondResponse)
	}
	if got := secondResponse.Items[0]["pk"]["S"]; got != "user#2" {
		t.Fatalf("second Scan pk = %#v, want user#2", got)
	}
}

func TestQueryAndScanSupportSelectCount(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"active":{"BOOL":true}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"event#001"},"active":{"BOOL":true}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"active":{"BOOL":false}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Demo",
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"user#1"}},
		"Select":"COUNT"
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query COUNT status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var queryResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&queryResponse); err != nil {
		t.Fatalf("decode Query COUNT: %v", err)
	}
	if queryResponse.Count != 2 || queryResponse.Items != nil {
		t.Fatalf("Query COUNT response = %#v, want count only", queryResponse)
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"active = :active",
		"ExpressionAttributeValues":{":active":{"BOOL":true}},
		"Select":"COUNT"
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan COUNT status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan COUNT: %v", err)
	}
	if scanResponse.Count != 2 || scanResponse.Items != nil {
		t.Fatalf("Scan COUNT response = %#v, want count only", scanResponse)
	}
}

func TestSelectCountRejectsProjectionExpression(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"Select":"COUNT",
		"ProjectionExpression":"pk"
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("Scan COUNT with projection status = %d, want %d; body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

func TestQueryScanAndItemOperationsReturnConsumedCapacity(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"active":{"BOOL":true}},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}
	assertConsumedCapacity(t, putRec, "Demo")

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	assertConsumedCapacity(t, getRec, "Demo")

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"Demo",
		"KeyConditionExpression":"pk = :pk",
		"ExpressionAttributeValues":{":pk":{"S":"user#1"}},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	assertConsumedCapacity(t, queryRec, "Demo")

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"active = :active",
		"ExpressionAttributeValues":{":active":{"BOOL":true}},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	assertConsumedCapacity(t, scanRec, "Demo")
}

func TestItemQueryAndScanRejectInvalidReturnConsumedCapacity(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	seedRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}
	}`)
	if seedRec.Code != http.StatusOK {
		t.Fatalf("seed PutItem status = %d, body = %s", seedRec.Code, seedRec.Body.String())
	}

	for name, tc := range map[string]struct {
		operation string
		payload   string
	}{
		"PutItem": {
			operation: "PutItem",
			payload: `{
				"TableName":"Demo",
				"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"}},
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
		"GetItem": {
			operation: "GetItem",
			payload: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
		"UpdateItem": {
			operation: "UpdateItem",
			payload: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"UpdateExpression":"SET #n = :name",
				"ExpressionAttributeNames":{"#n":"name"},
				"ExpressionAttributeValues":{":name":{"S":"Grace"}},
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
		"DeleteItem": {
			operation: "DeleteItem",
			payload: `{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
		"Query": {
			operation: "Query",
			payload: `{
				"TableName":"Demo",
				"KeyConditionExpression":"pk = :pk",
				"ExpressionAttributeValues":{":pk":{"S":"user#1"}},
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
		"Scan": {
			operation: "Scan",
			payload: `{
				"TableName":"Demo",
				"ReturnConsumedCapacity":"BROKEN"
			}`,
		},
	} {
		rec := dynamodbRequest(t, server, tc.operation, tc.payload)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d, body = %s", name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Fatalf("%s X-Amzn-Errortype = %q, want ValidationException", name, got)
		}
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem after rejected requests status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem after rejected requests: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("item after rejected requests = %#v, want Ada", getResponse.Item)
	}
}

func TestScanAndConditionSupportComparisonOperators(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"age":{"N":"37"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"age":{"N":"42"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"age > :age",
		"ExpressionAttributeValues":{":age":{"N":"40"}}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if scanResponse.Count != 1 || scanResponse.Items[0]["pk"]["S"] != "user#2" {
		t.Fatalf("Scan response = %#v, want user#2", scanResponse)
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
		"ConditionExpression":"age <> :age",
		"UpdateExpression":"SET ok = :ok",
		"ExpressionAttributeValues":{
			":age":{"N":"37"},
			":ok":{"BOOL":true}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
}

func TestConditionAndFilterExpressionsSupportInPredicate(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"status":{"S":"open"},
			"priority":{"N":"2"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	conditionalRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"#s IN (:open, :pending) AND priority IN (:one, :two)",
		"UpdateExpression":"SET matched = :matched",
		"ExpressionAttributeNames":{"#s":"status"},
		"ExpressionAttributeValues":{
			":open":{"S":"open"},
			":pending":{"S":"pending"},
			":one":{"N":"1"},
			":two":{"N":"2"},
			":matched":{"BOOL":true}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if conditionalRec.Code != http.StatusOK {
		t.Fatalf("conditional UpdateItem status = %d, body = %s", conditionalRec.Code, conditionalRec.Body.String())
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"#s IN (:closed, :open)",
		"ExpressionAttributeNames":{"#s":"status"},
		"ExpressionAttributeValues":{
			":closed":{"S":"closed"},
			":open":{"S":"open"}
		}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var response struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if response.Count != 1 || len(response.Items) != 1 {
		t.Fatalf("Scan response = %#v, want one matching item", response)
	}
}

func TestConditionAndFilterExpressionsSupportAndPredicates(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"age":{"N":"37"},"active":{"BOOL":true},"version":{"N":"1"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"age":{"N":"42"},"active":{"BOOL":true},"version":{"N":"1"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#3"},"sk":{"S":"profile"},"age":{"N":"45"},"active":{"BOOL":false},"version":{"N":"1"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"attribute_exists(age) AND active = :active AND age > :age",
		"ExpressionAttributeValues":{
			":active":{"BOOL":true},
			":age":{"N":"40"}
		}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if scanResponse.Count != 1 || scanResponse.Items[0]["pk"]["S"] != "user#2" {
		t.Fatalf("Scan response = %#v, want user#2 only", scanResponse)
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
		"ConditionExpression":"attribute_exists(version) AND #v = :expected AND active = :active",
		"UpdateExpression":"SET #v = :next",
		"ExpressionAttributeNames":{"#v":"version"},
		"ExpressionAttributeValues":{
			":expected":{"N":"1"},
			":active":{"BOOL":true},
			":next":{"N":"2"}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["version"]["N"]; got != "2" {
		t.Fatalf("version = %#v, want 2", got)
	}

	rejectRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
		"ConditionExpression":"attribute_exists(version) AND #v = :expected",
		"ExpressionAttributeNames":{"#v":"version"},
		"ExpressionAttributeValues":{":expected":{"N":"1"}}
	}`)
	if rejectRec.Code != http.StatusBadRequest {
		t.Fatalf("DeleteItem status = %d, want %d, body = %s", rejectRec.Code, http.StatusBadRequest, rejectRec.Body.String())
	}
	if got := rejectRec.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
}

func TestConditionAndFilterExpressionsSupportOrAndNotPredicates(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"status":{"S":"open"},"active":{"BOOL":true}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"status":{"S":"pending"},"active":{"BOOL":false},"deletedAt":{"N":"1"}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#3"},"sk":{"S":"profile"},"status":{"S":"closed"},"active":{"BOOL":false}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"active = :active OR #s = :pending",
		"ExpressionAttributeNames":{"#s":"status"},
		"ExpressionAttributeValues":{
			":active":{"BOOL":true},
			":pending":{"S":"pending"}
		}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if scanResponse.Count != 2 {
		t.Fatalf("OR Scan count = %d, want 2; items=%#v", scanResponse.Count, scanResponse.Items)
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"NOT attribute_exists(deletedAt) OR #s = :closed",
		"UpdateExpression":"SET matched = :matched",
		"ExpressionAttributeNames":{"#s":"status"},
		"ExpressionAttributeValues":{
			":closed":{"S":"closed"},
			":matched":{"BOOL":true}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["matched"]["BOOL"]; got != true {
		t.Fatalf("matched = %#v, want true", got)
	}

	rejectRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
		"ConditionExpression":"NOT attribute_exists(deletedAt) OR active = :active",
		"ExpressionAttributeValues":{":active":{"BOOL":true}}
	}`)
	if rejectRec.Code != http.StatusBadRequest {
		t.Fatalf("DeleteItem status = %d, want %d, body = %s", rejectRec.Code, http.StatusBadRequest, rejectRec.Body.String())
	}
	if got := rejectRec.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
}

func TestConditionAndFilterExpressionsSupportContains(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	for _, payload := range []string{
		`{"TableName":"Demo","Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"bio":{"S":"Ada writes systems"},"tags":{"SS":["engineer","admin"]},"scores":{"L":[{"N":"1"},{"N":"2"}]}}}`,
		`{"TableName":"Demo","Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"bio":{"S":"Grace documents systems"},"tags":{"SS":["engineer"]},"scores":{"L":[{"N":"3"}]}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"contains(#bio, :needle) AND contains(tags, :tag) AND contains(scores, :score)",
		"ExpressionAttributeNames":{"#bio":"bio"},
		"ExpressionAttributeValues":{
			":needle":{"S":"Ada"},
			":tag":{"S":"admin"},
			":score":{"N":"2"}
		}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if scanResponse.Count != 1 || scanResponse.Items[0]["pk"]["S"] != "user#1" {
		t.Fatalf("Scan response = %#v, want user#1", scanResponse)
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"contains(tags, :tag)",
		"UpdateExpression":"SET matched = :matched",
		"ExpressionAttributeValues":{
			":tag":{"S":"admin"},
			":matched":{"BOOL":true}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["matched"]["BOOL"]; got != true {
		t.Fatalf("matched = %#v, want true", got)
	}

	rejectRec := dynamodbRequest(t, server, "DeleteItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
		"ConditionExpression":"contains(tags, :tag)",
		"ExpressionAttributeValues":{":tag":{"S":"admin"}}
	}`)
	if rejectRec.Code != http.StatusBadRequest {
		t.Fatalf("DeleteItem status = %d, want %d, body = %s", rejectRec.Code, http.StatusBadRequest, rejectRec.Body.String())
	}
	if got := rejectRec.Header().Get("X-Amzn-Errortype"); got != "ConditionalCheckFailedException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ConditionalCheckFailedException", got)
	}
}

func TestConditionAndFilterExpressionsSupportAttributeTypeAndSize(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"name":{"S":"Ada"},
			"active":{"BOOL":true},
			"tags":{"SS":["engineer","admin"]},
			"meta":{"M":{"team":{"S":"core"}}}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"Demo",
		"FilterExpression":"attribute_type(meta, :mapType) AND size(tags) >= :tagCount AND size(#n) = :nameLength",
		"ExpressionAttributeNames":{"#n":"name"},
		"ExpressionAttributeValues":{
			":mapType":{"S":"M"},
			":tagCount":{"N":"2"},
			":nameLength":{"N":"3"}
		}
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var scanResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&scanResponse); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if scanResponse.Count != 1 || scanResponse.Items[0]["pk"]["S"] != "user#1" {
		t.Fatalf("Scan response = %#v, want user#1", scanResponse)
	}

	updateRec := dynamodbRequest(t, server, "UpdateItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
		"ConditionExpression":"attribute_type(active, :boolType) AND size(tags) <> :wrongCount",
		"UpdateExpression":"SET matched = :matched",
		"ExpressionAttributeValues":{
			":boolType":{"S":"BOOL"},
			":wrongCount":{"N":"1"},
			":matched":{"BOOL":true}
		},
		"ReturnValues":"ALL_NEW"
	}`)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("UpdateItem status = %d, body = %s", updateRec.Code, updateRec.Body.String())
	}
	var updateResponse struct {
		Attributes item
	}
	if err := json.NewDecoder(updateRec.Body).Decode(&updateResponse); err != nil {
		t.Fatalf("decode UpdateItem: %v", err)
	}
	if got := updateResponse.Attributes["matched"]["BOOL"]; got != true {
		t.Fatalf("matched = %#v, want true", got)
	}
}

func TestCreateTableStoresGSIAndQueryAcceptsIndexName(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"DemoIndex",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"},
			{"AttributeName":"gpk","AttributeType":"S"},
			{"AttributeName":"gsk","AttributeType":"N"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"GlobalSecondaryIndexes":[{
			"IndexName":"gsi1",
			"KeySchema":[
				{"AttributeName":"gpk","KeyType":"HASH"},
				{"AttributeName":"gsk","KeyType":"RANGE"}
			],
			"Projection":{"ProjectionType":"ALL"}
		}]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateTable: %v", err)
	}
	if len(createResponse.TableDescription.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("GlobalSecondaryIndexes = %#v, want one index", createResponse.TableDescription.GlobalSecondaryIndexes)
	}
	if got := createResponse.TableDescription.GlobalSecondaryIndexes[0].IndexStatus; got != "ACTIVE" {
		t.Fatalf("IndexStatus = %q, want ACTIVE", got)
	}

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"DemoIndex",
		"Item":{
			"pk":{"S":"item#1"},
			"sk":{"S":"v1"},
			"gpk":{"S":"group#1"},
			"gsk":{"N":"10"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"DemoIndex",
		"IndexName":"gsi1",
		"KeyConditionExpression":"gpk = :gpk",
		"ExpressionAttributeValues":{":gpk":{"S":"group#1"}}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var queryResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&queryResponse); err != nil {
		t.Fatalf("decode Query: %v", err)
	}
	if queryResponse.Count != 1 || queryResponse.Items[0]["pk"]["S"] != "item#1" {
		t.Fatalf("Query response = %#v, want indexed item", queryResponse)
	}
}

func TestGSIQueryHonorsKeysOnlyProjection(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"DemoKeysOnlyIndex",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"},
			{"AttributeName":"gpk","AttributeType":"S"},
			{"AttributeName":"gsk","AttributeType":"N"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"GlobalSecondaryIndexes":[{
			"IndexName":"gsi1",
			"KeySchema":[
				{"AttributeName":"gpk","KeyType":"HASH"},
				{"AttributeName":"gsk","KeyType":"RANGE"}
			],
			"Projection":{"ProjectionType":"KEYS_ONLY"}
		}]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"DemoKeysOnlyIndex",
		"Item":{
			"pk":{"S":"item#1"},
			"sk":{"S":"v1"},
			"gpk":{"S":"group#1"},
			"gsk":{"N":"10"},
			"name":{"S":"Ada"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"DemoKeysOnlyIndex",
		"IndexName":"gsi1",
		"KeyConditionExpression":"gpk = :gpk",
		"ExpressionAttributeValues":{":gpk":{"S":"group#1"}}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var response struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode Query: %v", err)
	}
	if response.Count != 1 {
		t.Fatalf("Count = %d, want 1", response.Count)
	}
	for _, name := range []string{"pk", "sk", "gpk", "gsk"} {
		if _, ok := response.Items[0][name]; !ok {
			t.Fatalf("KEYS_ONLY projection missing %s: %#v", name, response.Items[0])
		}
	}
	if _, ok := response.Items[0]["name"]; ok {
		t.Fatalf("KEYS_ONLY projection included non-key attribute: %#v", response.Items[0])
	}
}

func TestGSIScanHonorsIncludeProjection(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"DemoIncludeIndex",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"},
			{"AttributeName":"gpk","AttributeType":"S"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"GlobalSecondaryIndexes":[{
			"IndexName":"gsi1",
			"KeySchema":[{"AttributeName":"gpk","KeyType":"HASH"}],
			"Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":["name"]}
		}]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"DemoIncludeIndex",
		"Item":{
			"pk":{"S":"item#1"},
			"sk":{"S":"v1"},
			"gpk":{"S":"group#1"},
			"name":{"S":"Ada"},
			"secret":{"S":"hidden"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"DemoIncludeIndex",
		"IndexName":"gsi1"
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var response struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode Scan: %v", err)
	}
	if response.Count != 1 || response.Items[0]["name"]["S"] != "Ada" {
		t.Fatalf("INCLUDE projection response = %#v, want projected name", response)
	}
	if _, ok := response.Items[0]["secret"]; ok {
		t.Fatalf("INCLUDE projection leaked non-projected attribute: %#v", response.Items[0])
	}
}

func TestCreateTableStoresLSIAndQueryAcceptsIndexName(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"DemoLocalIndex",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"},
			{"AttributeName":"status","AttributeType":"S"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"LocalSecondaryIndexes":[{
			"IndexName":"lsi1",
			"KeySchema":[
				{"AttributeName":"pk","KeyType":"HASH"},
				{"AttributeName":"status","KeyType":"RANGE"}
			],
			"Projection":{"ProjectionType":"ALL"}
		}]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	var createResponse struct {
		TableDescription tableDescription
	}
	if err := json.NewDecoder(createRec.Body).Decode(&createResponse); err != nil {
		t.Fatalf("decode CreateTable: %v", err)
	}
	if len(createResponse.TableDescription.LocalSecondaryIndexes) != 1 {
		t.Fatalf("LocalSecondaryIndexes = %#v, want one index", createResponse.TableDescription.LocalSecondaryIndexes)
	}
	if got := createResponse.TableDescription.LocalSecondaryIndexes[0].IndexName; got != "lsi1" {
		t.Fatalf("IndexName = %q, want lsi1", got)
	}

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"DemoLocalIndex",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"status":{"S":"active"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	queryRec := dynamodbRequest(t, server, "Query", `{
		"TableName":"DemoLocalIndex",
		"IndexName":"lsi1",
		"KeyConditionExpression":"pk = :pk AND status = :status",
		"ExpressionAttributeValues":{
			":pk":{"S":"user#1"},
			":status":{"S":"active"}
		}
	}`)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("Query status = %d, body = %s", queryRec.Code, queryRec.Body.String())
	}
	var queryResponse struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&queryResponse); err != nil {
		t.Fatalf("decode Query: %v", err)
	}
	if queryResponse.Count != 1 || queryResponse.Items[0]["sk"]["S"] != "profile" {
		t.Fatalf("Query response = %#v, want local-indexed item", queryResponse)
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"DemoLocalIndex"}`)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("DescribeTable status = %d, body = %s", describeRec.Code, describeRec.Body.String())
	}
	var describeResponse struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if describeResponse.Table.LocalSecondaryIndexes[0].ItemCount != 1 {
		t.Fatalf("LSI ItemCount = %d, want 1", describeResponse.Table.LocalSecondaryIndexes[0].ItemCount)
	}
}

func TestScanWithIndexNameReturnsOnlyIndexedItems(t *testing.T) {
	server := NewServer(Config{Region: "us-east-1"})
	createRec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"DemoIndexScan",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"},
			{"AttributeName":"gpk","AttributeType":"S"},
			{"AttributeName":"gsk","AttributeType":"N"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		],
		"GlobalSecondaryIndexes":[{
			"IndexName":"gsi1",
			"KeySchema":[
				{"AttributeName":"gpk","KeyType":"HASH"},
				{"AttributeName":"gsk","KeyType":"RANGE"}
			],
			"Projection":{"ProjectionType":"ALL"}
		}]
	}`)
	if createRec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", createRec.Code, createRec.Body.String())
	}
	for _, payload := range []string{
		`{"TableName":"DemoIndexScan","Item":{"pk":{"S":"item#1"},"sk":{"S":"v1"},"gpk":{"S":"group#1"},"gsk":{"N":"2"},"name":{"S":"indexed-two"}}}`,
		`{"TableName":"DemoIndexScan","Item":{"pk":{"S":"item#2"},"sk":{"S":"v1"},"gpk":{"S":"group#1"},"gsk":{"N":"1"},"name":{"S":"indexed-one"}}}`,
		`{"TableName":"DemoIndexScan","Item":{"pk":{"S":"item#3"},"sk":{"S":"v1"},"name":{"S":"not-indexed"}}}`,
	} {
		rec := dynamodbRequest(t, server, "PutItem", payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("PutItem status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	scanRec := dynamodbRequest(t, server, "Scan", `{
		"TableName":"DemoIndexScan",
		"IndexName":"gsi1",
		"ProjectionExpression":"pk, gsk"
	}`)
	if scanRec.Code != http.StatusOK {
		t.Fatalf("Scan status = %d, body = %s", scanRec.Code, scanRec.Body.String())
	}
	var response struct {
		Items []item
		Count int
	}
	if err := json.NewDecoder(scanRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode indexed Scan: %v", err)
	}
	if response.Count != 2 || len(response.Items) != 2 {
		t.Fatalf("indexed Scan count/items = %d/%d, want 2/2: %#v", response.Count, len(response.Items), response.Items)
	}
	got := []any{response.Items[0]["gsk"]["N"], response.Items[1]["gsk"]["N"]}
	if !reflect.DeepEqual(got, []any{"1", "2"}) {
		t.Fatalf("indexed Scan order = %#v, want numeric index order 1, 2", got)
	}
	for _, item := range response.Items {
		if _, ok := item["name"]; ok {
			t.Fatalf("indexed Scan projection included unrequested name: %#v", item)
		}
	}
}

func TestBatchWriteAndBatchGetItems(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"active":{"BOOL":true}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"},"active":{"BOOL":false}}}}
			]
		},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}
	var writeResponse struct {
		UnprocessedItems map[string][]writeRequest
		ConsumedCapacity []map[string]any
	}
	if err := json.NewDecoder(writeRec.Body).Decode(&writeResponse); err != nil {
		t.Fatalf("decode BatchWriteItem: %v", err)
	}
	if len(writeResponse.UnprocessedItems) != 0 {
		t.Fatalf("UnprocessedItems = %#v, want empty", writeResponse.UnprocessedItems)
	}
	if len(writeResponse.ConsumedCapacity) != 1 || writeResponse.ConsumedCapacity[0]["TableName"] != "Demo" {
		t.Fatalf("BatchWriteItem ConsumedCapacity = %#v, want Demo table capacity", writeResponse.ConsumedCapacity)
	}

	getRec := dynamodbRequest(t, server, "BatchGetItem", `{
		"RequestItems":{
			"Demo":{
				"Keys":[
					{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
					{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
					{"pk":{"S":"missing"},"sk":{"S":"profile"}}
				],
				"ProjectionExpression":"pk, name"
			}
		},
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("BatchGetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Responses        map[string][]item
		UnprocessedKeys  map[string]batchGetTableRequest
		ConsumedCapacity []map[string]any
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode BatchGetItem: %v", err)
	}
	if len(getResponse.UnprocessedKeys) != 0 {
		t.Fatalf("UnprocessedKeys = %#v, want empty", getResponse.UnprocessedKeys)
	}
	if len(getResponse.ConsumedCapacity) != 1 || getResponse.ConsumedCapacity[0]["TableName"] != "Demo" {
		t.Fatalf("BatchGetItem ConsumedCapacity = %#v, want Demo table capacity", getResponse.ConsumedCapacity)
	}
	if len(getResponse.Responses["Demo"]) != 2 {
		t.Fatalf("Responses[Demo] = %#v, want two items", getResponse.Responses["Demo"])
	}
	for _, got := range getResponse.Responses["Demo"] {
		if _, ok := got["active"]; ok {
			t.Fatalf("BatchGetItem projection included active: %#v", got)
		}
		if _, ok := got["pk"]; !ok {
			t.Fatalf("BatchGetItem projection missing pk: %#v", got)
		}
		if _, ok := got["name"]; !ok {
			t.Fatalf("BatchGetItem projection missing name: %#v", got)
		}
	}

	deleteRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"DeleteRequest":{"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}}}
			]
		}
	}`)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem delete status = %d, body = %s", deleteRec.Code, deleteRec.Body.String())
	}

	describeRec := dynamodbRequest(t, server, "DescribeTable", `{"TableName":"Demo"}`)
	var describeResponse struct {
		Table tableDescription
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResponse); err != nil {
		t.Fatalf("decode DescribeTable: %v", err)
	}
	if describeResponse.Table.ItemCount != 1 {
		t.Fatalf("ItemCount = %d, want 1", describeResponse.Table.ItemCount)
	}
}

func TestBatchWriteValidationFailureDoesNotPartiallyApply(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"name":{"S":"Grace"}}}}
			]
		}
	}`)
	if writeRec.Code != http.StatusBadRequest {
		t.Fatalf("BatchWriteItem status = %d, want %d, body = %s", writeRec.Code, http.StatusBadRequest, writeRec.Body.String())
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if len(getResponse.Item) != 0 {
		t.Fatalf("item was partially applied after failed BatchWriteItem: %#v", getResponse.Item)
	}
}

func TestBatchOperationsRejectInvalidReturnConsumedCapacity(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"}}}}
			]
		},
		"ReturnConsumedCapacity":"BROKEN"
	}`)
	if writeRec.Code != http.StatusBadRequest {
		t.Fatalf("BatchWriteItem status = %d, want %d, body = %s", writeRec.Code, http.StatusBadRequest, writeRec.Body.String())
	}
	if got := writeRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("BatchWriteItem X-Amzn-Errortype = %q, want ValidationException", got)
	}

	getAfterRejectedWrite := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	var getAfterRejectedWriteResponse struct {
		Item item
	}
	if err := json.NewDecoder(getAfterRejectedWrite.Body).Decode(&getAfterRejectedWriteResponse); err != nil {
		t.Fatalf("decode GetItem after rejected BatchWriteItem: %v", err)
	}
	if len(getAfterRejectedWriteResponse.Item) != 0 {
		t.Fatalf("rejected BatchWriteItem mutated item: %#v", getAfterRejectedWriteResponse.Item)
	}

	getRec := dynamodbRequest(t, server, "BatchGetItem", `{
		"RequestItems":{
			"Demo":{"Keys":[{"pk":{"S":"user#1"},"sk":{"S":"profile"}}]}
		},
		"ReturnConsumedCapacity":"BROKEN"
	}`)
	if getRec.Code != http.StatusBadRequest {
		t.Fatalf("BatchGetItem status = %d, want %d, body = %s", getRec.Code, http.StatusBadRequest, getRec.Body.String())
	}
	if got := getRec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("BatchGetItem X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

func TestTransactGetItemsReturnsProjectedItemsInRequestOrder(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"active":{"BOOL":true}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"},"active":{"BOOL":false}}}}
			]
		}
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}

	getRec := dynamodbRequest(t, server, "TransactGetItems", `{
		"TransactItems":[
			{"Get":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}},
				"ProjectionExpression":"pk, #n",
				"ExpressionAttributeNames":{"#n":"name"}
			}},
			{"Get":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"missing"},"sk":{"S":"profile"}}
			}},
			{"Get":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"ProjectionExpression":"name"
			}}
		]
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("TransactGetItems status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Responses []transactGetItemResponse
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode TransactGetItems: %v", err)
	}
	if len(getResponse.Responses) != 3 {
		t.Fatalf("Responses = %#v, want three entries", getResponse.Responses)
	}
	if got := getResponse.Responses[0].Item["name"]["S"]; got != "Grace" {
		t.Fatalf("first response name = %#v, want Grace", got)
	}
	if _, ok := getResponse.Responses[0].Item["active"]; ok {
		t.Fatalf("first response projection included active: %#v", getResponse.Responses[0].Item)
	}
	if len(getResponse.Responses[1].Item) != 0 {
		t.Fatalf("missing item response = %#v, want empty item response", getResponse.Responses[1].Item)
	}
	if got := getResponse.Responses[2].Item["name"]["S"]; got != "Ada" {
		t.Fatalf("third response name = %#v, want Ada", got)
	}
}

func TestTransactWriteItemsAppliesPutUpdateDeleteAtomically(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	seedRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"visits":{"N":"1"},"status":{"S":"active"}}
	}`)
	if seedRec.Code != http.StatusOK {
		t.Fatalf("seed PutItem status = %d, body = %s", seedRec.Code, seedRec.Body.String())
	}

	writeRec := dynamodbRequest(t, server, "TransactWriteItems", `{
		"TransactItems":[
			{"ConditionCheck":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"ConditionExpression":"#s = :active",
				"ExpressionAttributeNames":{"#s":"status"},
				"ExpressionAttributeValues":{":active":{"S":"active"}}
			}},
			{"Update":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"UpdateExpression":"SET visits = visits + :one, #s = :updated",
				"ExpressionAttributeNames":{"#s":"status"},
				"ExpressionAttributeValues":{":one":{"N":"1"},":updated":{"S":"updated"}}
			}},
			{"Put":{
				"TableName":"Demo",
				"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"}},
				"ConditionExpression":"attribute_not_exists(pk)"
			}},
			{"Delete":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"missing"},"sk":{"S":"profile"}}
			}}
		]
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("TransactWriteItems status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}

	getUpdatedRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getUpdatedRec.Code != http.StatusOK {
		t.Fatalf("GetItem updated status = %d, body = %s", getUpdatedRec.Code, getUpdatedRec.Body.String())
	}
	var updatedResponse struct {
		Item item
	}
	if err := json.NewDecoder(getUpdatedRec.Body).Decode(&updatedResponse); err != nil {
		t.Fatalf("decode updated item: %v", err)
	}
	if got := updatedResponse.Item["visits"]["N"]; got != "2" {
		t.Fatalf("visits = %#v, want 2", got)
	}
	if got := updatedResponse.Item["status"]["S"]; got != "updated" {
		t.Fatalf("status = %#v, want updated", got)
	}

	getPutRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}}
	}`)
	if getPutRec.Code != http.StatusOK {
		t.Fatalf("GetItem put status = %d, body = %s", getPutRec.Code, getPutRec.Body.String())
	}
	var putResponse struct {
		Item item
	}
	if err := json.NewDecoder(getPutRec.Body).Decode(&putResponse); err != nil {
		t.Fatalf("decode put item: %v", err)
	}
	if got := putResponse.Item["name"]["S"]; got != "Grace" {
		t.Fatalf("put item name = %#v, want Grace", got)
	}
}

func TestTransactWriteItemsConditionFailureDoesNotPartiallyApply(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	seedRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"status":{"S":"active"}}
	}`)
	if seedRec.Code != http.StatusOK {
		t.Fatalf("seed PutItem status = %d, body = %s", seedRec.Code, seedRec.Body.String())
	}

	writeRec := dynamodbRequest(t, server, "TransactWriteItems", `{
		"TransactItems":[
			{"Put":{
				"TableName":"Demo",
				"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"}}
			}},
			{"ConditionCheck":{
				"TableName":"Demo",
				"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}},
				"ConditionExpression":"#s = :inactive",
				"ExpressionAttributeNames":{"#s":"status"},
				"ExpressionAttributeValues":{":inactive":{"S":"inactive"}}
			}}
		]
	}`)
	if writeRec.Code != http.StatusBadRequest {
		t.Fatalf("TransactWriteItems status = %d, want %d, body = %s", writeRec.Code, http.StatusBadRequest, writeRec.Body.String())
	}
	if got := writeRec.Header().Get("X-Amzn-Errortype"); got != "TransactionCanceledException" {
		t.Fatalf("X-Amzn-Errortype = %q, want TransactionCanceledException", got)
	}

	getRec := dynamodbRequest(t, server, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#2"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if len(getResponse.Item) != 0 {
		t.Fatalf("item was partially applied after failed TransactWriteItems: %#v", getResponse.Item)
	}
}

func TestExecuteStatementSelectsItemsWithParametersAndProjection(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"active":{"BOOL":true}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"settings"},"name":{"S":"Ada Settings"},"active":{"BOOL":false}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"},"active":{"BOOL":true}}}}
			]
		}
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}

	selectRec := dynamodbRequest(t, server, "ExecuteStatement", `{
		"Statement":"SELECT pk, name FROM Demo WHERE pk = ? AND sk = ?",
		"Parameters":[{"S":"user#1"},{"S":"profile"}],
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if selectRec.Code != http.StatusOK {
		t.Fatalf("ExecuteStatement status = %d, body = %s", selectRec.Code, selectRec.Body.String())
	}
	var response struct {
		Items            []item
		ConsumedCapacity map[string]any
	}
	if err := json.NewDecoder(selectRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode ExecuteStatement: %v", err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("Items = %#v, want one result", response.Items)
	}
	if got := response.Items[0]["name"]["S"]; got != "Ada" {
		t.Fatalf("name = %#v, want Ada", got)
	}
	if _, ok := response.Items[0]["active"]; ok {
		t.Fatalf("projection included active: %#v", response.Items[0])
	}
	if response.ConsumedCapacity["TableName"] != "Demo" {
		t.Fatalf("ConsumedCapacity = %#v, want Demo table", response.ConsumedCapacity)
	}
}

func TestExecuteStatementRejectsUnsupportedPartiQL(t *testing.T) {
	server := NewServer(Config{})

	rec := dynamodbRequest(t, server, "ExecuteStatement", `{
		"Statement":"UPDATE Demo SET name = ? WHERE pk = ?",
		"Parameters":[{"S":"Ada"},{"S":"user#1"}]
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ExecuteStatement status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

func TestBatchExecuteStatementSelectsSingleItemsInRequestOrder(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"active":{"BOOL":true}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"},"active":{"BOOL":false}}}}
			]
		}
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}

	batchRec := dynamodbRequest(t, server, "BatchExecuteStatement", `{
		"Statements":[
			{
				"Statement":"SELECT pk, name FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#2"},{"S":"profile"}]
			},
			{
				"Statement":"SELECT name FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"missing"},{"S":"profile"}]
			},
			{
				"Statement":"SELECT * FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#1"},{"S":"profile"}]
			}
		],
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if batchRec.Code != http.StatusOK {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", batchRec.Code, batchRec.Body.String())
	}
	var response struct {
		Responses        []batchStatementResponse
		ConsumedCapacity []map[string]any
	}
	if err := json.NewDecoder(batchRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode BatchExecuteStatement: %v", err)
	}
	if len(response.Responses) != 3 {
		t.Fatalf("Responses = %#v, want three entries", response.Responses)
	}
	if got := response.Responses[0].Item["name"]["S"]; got != "Grace" {
		t.Fatalf("first response name = %#v, want Grace", got)
	}
	if _, ok := response.Responses[0].Item["active"]; ok {
		t.Fatalf("first response projection included active: %#v", response.Responses[0].Item)
	}
	if len(response.Responses[1].Item) != 0 {
		t.Fatalf("missing item response = %#v, want no item", response.Responses[1].Item)
	}
	if got := response.Responses[2].Item["name"]["S"]; got != "Ada" {
		t.Fatalf("third response name = %#v, want Ada", got)
	}
	if len(response.ConsumedCapacity) != 3 {
		t.Fatalf("ConsumedCapacity = %#v, want one entry per statement", response.ConsumedCapacity)
	}
}

func TestBatchExecuteStatementReturnsStatementErrorsWithoutFailingWholeBatch(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	batchRec := dynamodbRequest(t, server, "BatchExecuteStatement", `{
		"Statements":[
			{
				"Statement":"SELECT * FROM Demo WHERE pk = ?",
				"Parameters":[{"S":"user#1"}]
			},
			{
				"Statement":"UPDATE Demo SET name = ? WHERE pk = ?",
				"Parameters":[{"S":"Ada"},{"S":"user#1"}]
			},
			{
				"Statement":"SELECT * FROM Missing WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#1"},{"S":"profile"}]
			}
		]
	}`)
	if batchRec.Code != http.StatusOK {
		t.Fatalf("BatchExecuteStatement status = %d, body = %s", batchRec.Code, batchRec.Body.String())
	}
	var response struct {
		Responses []batchStatementResponse
	}
	if err := json.NewDecoder(batchRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode BatchExecuteStatement: %v", err)
	}
	if len(response.Responses) != 3 {
		t.Fatalf("Responses = %#v, want three entries", response.Responses)
	}
	if response.Responses[0].Error == nil || response.Responses[0].Error.Code != "ValidationError" {
		t.Fatalf("first error = %#v, want ValidationError", response.Responses[0].Error)
	}
	if response.Responses[1].Error == nil || response.Responses[1].Error.Code != "ValidationError" {
		t.Fatalf("second error = %#v, want ValidationError", response.Responses[1].Error)
	}
	if response.Responses[2].Error == nil || response.Responses[2].Error.Code != "ResourceNotFound" {
		t.Fatalf("third error = %#v, want ResourceNotFound", response.Responses[2].Error)
	}
}

func TestExecuteTransactionSelectsSingleItemsInRequestOrder(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	writeRec := dynamodbRequest(t, server, "BatchWriteItem", `{
		"RequestItems":{
			"Demo":[
				{"PutRequest":{"Item":{"pk":{"S":"user#1"},"sk":{"S":"profile"},"name":{"S":"Ada"},"active":{"BOOL":true}}}},
				{"PutRequest":{"Item":{"pk":{"S":"user#2"},"sk":{"S":"profile"},"name":{"S":"Grace"},"active":{"BOOL":false}}}}
			]
		}
	}`)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("BatchWriteItem status = %d, body = %s", writeRec.Code, writeRec.Body.String())
	}

	transactionRec := dynamodbRequest(t, server, "ExecuteTransaction", `{
		"TransactStatements":[
			{
				"Statement":"SELECT pk, name FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#2"},{"S":"profile"}]
			},
			{
				"Statement":"SELECT * FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"missing"},{"S":"profile"}]
			},
			{
				"Statement":"SELECT name FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#1"},{"S":"profile"}]
			}
		],
		"ReturnConsumedCapacity":"TOTAL"
	}`)
	if transactionRec.Code != http.StatusOK {
		t.Fatalf("ExecuteTransaction status = %d, body = %s", transactionRec.Code, transactionRec.Body.String())
	}
	var response struct {
		Responses        []batchStatementResponse
		ConsumedCapacity []map[string]any
	}
	if err := json.NewDecoder(transactionRec.Body).Decode(&response); err != nil {
		t.Fatalf("decode ExecuteTransaction: %v", err)
	}
	if len(response.Responses) != 3 {
		t.Fatalf("Responses = %#v, want three entries", response.Responses)
	}
	if got := response.Responses[0].Item["name"]["S"]; got != "Grace" {
		t.Fatalf("first response name = %#v, want Grace", got)
	}
	if _, ok := response.Responses[0].Item["active"]; ok {
		t.Fatalf("first response projection included active: %#v", response.Responses[0].Item)
	}
	if len(response.Responses[1].Item) != 0 {
		t.Fatalf("missing item response = %#v, want no item", response.Responses[1].Item)
	}
	if got := response.Responses[2].Item["name"]["S"]; got != "Ada" {
		t.Fatalf("third response name = %#v, want Ada", got)
	}
	if len(response.ConsumedCapacity) != 3 {
		t.Fatalf("ConsumedCapacity = %#v, want one entry per statement", response.ConsumedCapacity)
	}
}

func TestExecuteTransactionRejectsUnsupportedPartiQLAtomically(t *testing.T) {
	server := NewServer(Config{})
	createTestTable(t, server)

	rec := dynamodbRequest(t, server, "ExecuteTransaction", `{
		"TransactStatements":[
			{
				"Statement":"SELECT * FROM Demo WHERE pk = ? AND sk = ?",
				"Parameters":[{"S":"user#1"},{"S":"profile"}]
			},
			{
				"Statement":"UPDATE Demo SET name = ? WHERE pk = ?",
				"Parameters":[{"S":"Ada"},{"S":"user#1"}]
			}
		]
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ExecuteTransaction status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
		t.Fatalf("X-Amzn-Errortype = %q, want ValidationException", got)
	}
}

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

func TestServerPersistsTablesAndItemsToStoragePath(t *testing.T) {
	storagePath := t.TempDir()
	server := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	createTestTable(t, server)

	putRec := dynamodbRequest(t, server, "PutItem", `{
		"TableName":"Demo",
		"Item":{
			"pk":{"S":"user#1"},
			"sk":{"S":"profile"},
			"name":{"S":"Ada"}
		}
	}`)
	if putRec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, body = %s", putRec.Code, putRec.Body.String())
	}

	reloaded := NewServer(Config{Region: "us-east-1", StoragePath: storagePath})
	getRec := dynamodbRequest(t, reloaded, "GetItem", `{
		"TableName":"Demo",
		"Key":{"pk":{"S":"user#1"},"sk":{"S":"profile"}}
	}`)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	var getResponse struct {
		Item item
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResponse); err != nil {
		t.Fatalf("decode GetItem: %v", err)
	}
	if got := getResponse.Item["name"]["S"]; got != "Ada" {
		t.Fatalf("persisted item name = %#v, want Ada", got)
	}
}

func createTestTable(t *testing.T, server *Server) {
	t.Helper()
	rec := dynamodbRequest(t, server, "CreateTable", `{
		"TableName":"Demo",
		"AttributeDefinitions":[
			{"AttributeName":"pk","AttributeType":"S"},
			{"AttributeName":"sk","AttributeType":"S"}
		],
		"KeySchema":[
			{"AttributeName":"pk","KeyType":"HASH"},
			{"AttributeName":"sk","KeyType":"RANGE"}
		]
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("CreateTable status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func dynamodbRequest(t *testing.T, server *Server, operation string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+operation)
	rec := httptest.NewRecorder()
	server.routes().ServeHTTP(rec, req)
	return rec
}

func assertConsumedCapacity(t *testing.T, rec *httptest.ResponseRecorder, tableName string) {
	t.Helper()
	var response struct {
		ConsumedCapacity struct {
			TableName          string
			CapacityUnits      float64
			ReadCapacityUnits  float64
			WriteCapacityUnits float64
		}
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode consumed capacity response: %v", err)
	}
	if response.ConsumedCapacity.TableName != tableName {
		t.Fatalf("ConsumedCapacity.TableName = %q, want %q", response.ConsumedCapacity.TableName, tableName)
	}
	if response.ConsumedCapacity.CapacityUnits <= 0 {
		t.Fatalf("ConsumedCapacity.CapacityUnits = %v, want positive", response.ConsumedCapacity.CapacityUnits)
	}
}

func signedDynamoDBRequest(t *testing.T, operation string, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+operation)
	req.Header.Set("x-amz-date", "20260501T120000Z")
	payloadHash := testSHA256Hex([]byte(body))
	req.Header.Set("x-amz-content-sha256", payloadHash)

	signedHeaders := "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-target"
	dateStamp := "20260501"
	scope := dateStamp + "/us-east-1/dynamodb/aws4_request"
	canonicalRequest := strings.Join([]string{
		req.Method,
		"/",
		"",
		canonicalHeaders(req, signedHeaders),
		signedHeaders,
		payloadHash,
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		"20260501T120000Z",
		scope,
		testSHA256Hex([]byte(canonicalRequest)),
	}, "\n")
	signature := hmac.New(sha256.New, testSigningKey("dev", dateStamp, "us-east-1"))
	_, _ = signature.Write([]byte(stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=dev/"+scope+", SignedHeaders="+signedHeaders+", Signature="+hex.EncodeToString(signature.Sum(nil)))
	return req
}

func testSigningKey(secret string, dateStamp string, region string) []byte {
	dateKey := testHMACSHA256([]byte("AWS4"+secret), dateStamp)
	regionKey := testHMACSHA256(dateKey, region)
	serviceKey := testHMACSHA256(regionKey, "dynamodb")
	return testHMACSHA256(serviceKey, "aws4_request")
}

func testHMACSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}

func testSHA256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
