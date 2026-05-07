package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestServerSideEncryptionMetadataRoundTripsOnObjectRequests(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	putReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/kms.txt", strings.NewReader("kms metadata"))
	putReq.Header.Set("x-amz-server-side-encryption", "aws:kms")
	putReq.Header.Set("x-amz-server-side-encryption-aws-kms-key-id", "arn:aws:kms:us-east-1:000000000000:key/local")
	putReq.Header.Set("x-amz-server-side-encryption-bucket-key-enabled", "true")
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}
	if got := putRec.Header().Get("x-amz-server-side-encryption"); got != "aws:kms" {
		t.Fatalf("put sse = %q, want aws:kms", got)
	}
	if got := putRec.Header().Get("x-amz-server-side-encryption-aws-kms-key-id"); got != "arn:aws:kms:us-east-1:000000000000:key/local" {
		t.Fatalf("put kms key id = %q", got)
	}
	if got := putRec.Header().Get("x-amz-server-side-encryption-bucket-key-enabled"); got != "true" {
		t.Fatalf("put bucket key = %q, want true", got)
	}

	head := performRequest(routes, http.MethodHead, "/demo-bucket/docs/kms.txt", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("head status = %d, want %d; body=%s", head.Code, http.StatusOK, head.Body.String())
	}
	if got := head.Header().Get("x-amz-server-side-encryption"); got != "aws:kms" {
		t.Fatalf("head sse = %q, want aws:kms", got)
	}
	if got := head.Header().Get("x-amz-server-side-encryption-aws-kms-key-id"); got == "" {
		t.Fatal("head missing kms key id")
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/docs/kms.txt", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	if got := get.Header().Get("x-amz-server-side-encryption-bucket-key-enabled"); got != "true" {
		t.Fatalf("get bucket key = %q, want true", got)
	}

	reloaded := NewServer(Config{}, NewFileBucketStore(store.root)).routes()
	persistedHead := performRequest(reloaded, http.MethodHead, "/demo-bucket/docs/kms.txt", nil)
	if got := persistedHead.Header().Get("x-amz-server-side-encryption"); got != "aws:kms" {
		t.Fatalf("persisted head sse = %q, want aws:kms", got)
	}
}

func TestServerSideEncryptionMetadataOnCopyAndMultipart(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}
	sourceReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/source.txt", strings.NewReader("source body"))
	sourceReq.Header.Set("x-amz-server-side-encryption", "AES256")
	sourceRec := httptest.NewRecorder()
	routes.ServeHTTP(sourceRec, sourceReq)
	if sourceRec.Code != http.StatusOK {
		t.Fatalf("put source status = %d; body=%s", sourceRec.Code, sourceRec.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/copy.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/demo-bucket/source.txt")
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy status = %d; body=%s", copyRec.Code, copyRec.Body.String())
	}
	if got := copyRec.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("copy inherited sse = %q, want AES256", got)
	}

	replaceReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/copy-kms.txt", nil)
	replaceReq.Header.Set("x-amz-copy-source", "/demo-bucket/source.txt")
	replaceReq.Header.Set("x-amz-server-side-encryption", "aws:kms")
	replaceReq.Header.Set("x-amz-server-side-encryption-aws-kms-key-id", "local-key")
	replaceRec := httptest.NewRecorder()
	routes.ServeHTTP(replaceRec, replaceReq)
	if replaceRec.Code != http.StatusOK {
		t.Fatalf("copy replace status = %d; body=%s", replaceRec.Code, replaceRec.Body.String())
	}
	if got := replaceRec.Header().Get("x-amz-server-side-encryption"); got != "aws:kms" {
		t.Fatalf("copy replaced sse = %q, want aws:kms", got)
	}
	if got := replaceRec.Header().Get("x-amz-server-side-encryption-aws-kms-key-id"); got != "local-key" {
		t.Fatalf("copy replaced kms key = %q, want local-key", got)
	}

	initReq := httptest.NewRequest(http.MethodPost, "/demo-bucket/multipart.txt?uploads", nil)
	initReq.Header.Set("x-amz-server-side-encryption", "AES256")
	initRec := httptest.NewRecorder()
	routes.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("init multipart status = %d; body=%s", initRec.Code, initRec.Body.String())
	}
	if got := initRec.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("init multipart sse = %q, want AES256", got)
	}
	var initResult initiateMultipartUploadResult
	if err := xml.NewDecoder(initRec.Body).Decode(&initResult); err != nil {
		t.Fatalf("decode init multipart: %v", err)
	}
	uploadPart := performRequest(routes, http.MethodPut, "/demo-bucket/multipart.txt?partNumber=1&uploadId="+url.QueryEscape(initResult.UploadID), strings.NewReader("multipart body"))
	if uploadPart.Code != http.StatusOK {
		t.Fatalf("upload part status = %d; body=%s", uploadPart.Code, uploadPart.Body.String())
	}
	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + uploadPart.Header().Get("ETag") + `</ETag></Part></CompleteMultipartUpload>`
	complete := performRequest(routes, http.MethodPost, "/demo-bucket/multipart.txt?uploadId="+url.QueryEscape(initResult.UploadID), strings.NewReader(completeBody))
	if complete.Code != http.StatusOK {
		t.Fatalf("complete status = %d; body=%s", complete.Code, complete.Body.String())
	}
	if got := complete.Header().Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("complete multipart sse = %q, want AES256", got)
	}
}

func TestServerSideEncryptionRejectsUnsupportedOrInvalidHeaders(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	customerKeyReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/ssec.txt", strings.NewReader("secret"))
	customerKeyReq.Header.Set("x-amz-server-side-encryption-customer-algorithm", "AES256")
	customerKeyRec := httptest.NewRecorder()
	routes.ServeHTTP(customerKeyRec, customerKeyReq)
	if customerKeyRec.Code != http.StatusNotImplemented {
		t.Fatalf("sse-c status = %d, want %d; body=%s", customerKeyRec.Code, http.StatusNotImplemented, customerKeyRec.Body.String())
	}

	invalidKMSReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/invalid.txt", strings.NewReader("invalid"))
	invalidKMSReq.Header.Set("x-amz-server-side-encryption-aws-kms-key-id", "local-key")
	invalidKMSRec := httptest.NewRecorder()
	routes.ServeHTTP(invalidKMSRec, invalidKMSReq)
	if invalidKMSRec.Code != http.StatusBadRequest {
		t.Fatalf("kms without algorithm status = %d, want %d; body=%s", invalidKMSRec.Code, http.StatusBadRequest, invalidKMSRec.Body.String())
	}

	unsupportedReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/unsupported.txt", strings.NewReader("unsupported"))
	unsupportedReq.Header.Set("x-amz-server-side-encryption", "aws:kms:dsse")
	unsupportedRec := httptest.NewRecorder()
	routes.ServeHTTP(unsupportedRec, unsupportedReq)
	if unsupportedRec.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported sse status = %d, want %d; body=%s", unsupportedRec.Code, http.StatusNotImplemented, unsupportedRec.Body.String())
	}
}

