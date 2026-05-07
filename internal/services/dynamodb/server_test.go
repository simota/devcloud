package dynamodb

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
