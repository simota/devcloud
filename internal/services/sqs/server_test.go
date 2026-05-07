package sqs

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
