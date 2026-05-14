package s3

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPresignedURLValidatesSignature(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "strict",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()
	fixedNow := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	previousNow := nowUTC
	nowUTC = func() time.Time { return fixedNow }
	defer func() { nowUTC = previousNow }()

	create := signedRequest(t, http.MethodPut, "/demo-bucket", "", fixedNow)
	createRec := httptest.NewRecorder()
	routes.ServeHTTP(createRec, create)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", createRec.Code, createRec.Body.String())
	}
	put := signedRequest(t, http.MethodPut, "/demo-bucket/docs/readme.txt", "hello from devcloud s3\n", fixedNow)
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put status = %d; body=%s", putRec.Code, putRec.Body.String())
	}

	target := presignedTarget(t, "GET", "example.com", "/demo-bucket/docs/readme.txt", fixedNow)
	get := httptest.NewRequest(http.MethodGet, target, nil)
	get.Host = "example.com"
	getRec := httptest.NewRecorder()
	routes.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("presigned get status = %d, want %d; body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != "hello from devcloud s3\n" {
		t.Fatalf("presigned get body = %q", got)
	}

	bad := httptest.NewRequest(http.MethodGet, target[:len(target)-1]+"0", nil)
	bad.Host = "example.com"
	badRec := httptest.NewRecorder()
	routes.ServeHTTP(badRec, bad)
	if badRec.Code != http.StatusForbidden {
		t.Fatalf("bad signature status = %d, want %d", badRec.Code, http.StatusForbidden)
	}
	var parsed errorResponse
	if err := xml.NewDecoder(badRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode bad signature error: %v", err)
	}
	if parsed.Code != "SignatureDoesNotMatch" {
		t.Fatalf("bad signature code = %q", parsed.Code)
	}
}

func TestPresignedURLRejectsInvalidArguments(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "strict",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()
	fixedNow := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	previousNow := nowUTC
	nowUTC = func() time.Time { return fixedNow }
	defer func() { nowUTC = previousNow }()

	validTarget := presignedTarget(t, "GET", "example.com", "/demo-bucket/docs/readme.txt", fixedNow)
	tests := []struct {
		name       string
		mutate     func(url.Values)
		wantStatus int
		wantCode   string
	}{
		{
			name: "unsupported algorithm",
			mutate: func(values url.Values) {
				values.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA1")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidArgument",
		},
		{
			name: "malformed credential",
			mutate: func(values url.Values) {
				values.Set("X-Amz-Credential", "dev/us-east-1/s3")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "AuthorizationHeaderMalformed",
		},
		{
			name: "wrong access key",
			mutate: func(values url.Values) {
				values.Set("X-Amz-Credential", strings.Replace(values.Get("X-Amz-Credential"), "dev/", "other/", 1))
			},
			wantStatus: http.StatusForbidden,
			wantCode:   "InvalidAccessKeyId",
		},
		{
			name: "bad date",
			mutate: func(values url.Values) {
				values.Set("X-Amz-Date", "not-a-date")
			},
			wantStatus: http.StatusForbidden,
			wantCode:   "AccessDenied",
		},
		{
			name: "expires too large",
			mutate: func(values url.Values) {
				values.Set("X-Amz-Expires", "604801")
			},
			wantStatus: http.StatusForbidden,
			wantCode:   "AccessDenied",
		},
		{
			name: "missing signed headers",
			mutate: func(values url.Values) {
				values.Del("X-Amz-SignedHeaders")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "AuthorizationHeaderMalformed",
		},
	}

	parsedTarget, err := url.Parse(validTarget)
	if err != nil {
		t.Fatalf("parse presigned target: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := parsedTarget.Query()
			tt.mutate(values)
			req := httptest.NewRequest(http.MethodGet, parsedTarget.Path+"?"+values.Encode(), nil)
			req.Host = "example.com"
			rec := httptest.NewRecorder()
			routes.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			var parsed errorResponse
			if err := xml.NewDecoder(rec.Body).Decode(&parsed); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if parsed.Code != tt.wantCode {
				t.Fatalf("error code = %q, want %q", parsed.Code, tt.wantCode)
			}
		})
	}
}

func TestAuthorizationHeaderValidatesPayloadHashAndPreservesBody(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "strict",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()
	fixedNow := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	create := signedRequest(t, http.MethodPut, "/demo-bucket", "", fixedNow)
	createRec := httptest.NewRecorder()
	routes.ServeHTTP(createRec, create)
	if createRec.Code != http.StatusOK {
		t.Fatalf("signed create status = %d, want %d; body=%s", createRec.Code, http.StatusOK, createRec.Body.String())
	}

	put := signedRequest(t, http.MethodPut, "/demo-bucket/docs/readme.txt", "signed body\n", fixedNow)
	put.Header.Set("Content-Type", "text/plain")
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusOK {
		t.Fatalf("signed put status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}

	get := signedRequest(t, http.MethodGet, "/demo-bucket/docs/readme.txt", "", fixedNow)
	getRec := httptest.NewRecorder()
	routes.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("signed get status = %d, want %d; body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != "signed body\n" {
		t.Fatalf("signed get body = %q", got)
	}
}

func TestAuthorizationHeaderRejectsPayloadHashMismatch(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "strict",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()
	fixedNow := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	create := signedRequest(t, http.MethodPut, "/demo-bucket", "", fixedNow)
	createRec := httptest.NewRecorder()
	routes.ServeHTTP(createRec, create)
	if createRec.Code != http.StatusOK {
		t.Fatalf("signed create status = %d, want %d; body=%s", createRec.Code, http.StatusOK, createRec.Body.String())
	}

	put := signedRequest(t, http.MethodPut, "/demo-bucket/docs/readme.txt", "original body", fixedNow)
	put.Body = io.NopCloser(strings.NewReader("tampered body"))
	put.ContentLength = int64(len("tampered body"))
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusBadRequest {
		t.Fatalf("tampered put status = %d, want %d; body=%s", putRec.Code, http.StatusBadRequest, putRec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(putRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode tampered put error: %v", err)
	}
	if parsed.Code != "XAmzContentSHA256Mismatch" {
		t.Fatalf("tampered put code = %q, want XAmzContentSHA256Mismatch", parsed.Code)
	}
}

