package s3

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBucketLifecycleAndListBuckets(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	create := performRequest(routes, http.MethodPut, "/demo-bucket", nil)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	head := performRequest(routes, http.MethodHead, "/demo-bucket", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("head status = %d, want %d", head.Code, http.StatusOK)
	}

	list := performRequest(routes, http.MethodGet, "/", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", list.Code, http.StatusOK)
	}
	if !strings.Contains(list.Body.String(), "<Name>demo-bucket</Name>") {
		t.Fatalf("list body missing bucket: %s", list.Body.String())
	}

	deleteBucket := performRequest(routes, http.MethodDelete, "/demo-bucket", nil)
	if deleteBucket.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d; body=%s", deleteBucket.Code, http.StatusNoContent, deleteBucket.Body.String())
	}

	missingHead := performRequest(routes, http.MethodHead, "/demo-bucket", nil)
	if missingHead.Code != http.StatusNotFound {
		t.Fatalf("missing head status = %d, want %d", missingHead.Code, http.StatusNotFound)
	}
}

func TestGetBucketLocationReturnsConfiguredRegion(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{Region: "ap-northeast-1"}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	location := performRequest(routes, http.MethodGet, "/demo-bucket?location", nil)
	if location.Code != http.StatusOK {
		t.Fatalf("location status = %d, want %d; body=%s", location.Code, http.StatusOK, location.Body.String())
	}
	var parsed locationConstraint
	if err := xml.NewDecoder(location.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode location response: %v", err)
	}
	if parsed.Value != "ap-northeast-1" {
		t.Fatalf("location constraint = %q, want ap-northeast-1", parsed.Value)
	}

	missing := performRequest(routes, http.MethodGet, "/missing-bucket?location", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing bucket location status = %d, want %d", missing.Code, http.StatusNotFound)
	}
}

func TestGetBucketLocationReturnsEmptyConstraintForUSEast1(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{Region: "us-east-1"}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	location := performRequest(routes, http.MethodGet, "/demo-bucket?location", nil)
	if location.Code != http.StatusOK {
		t.Fatalf("location status = %d, want %d; body=%s", location.Code, http.StatusOK, location.Body.String())
	}
	var parsed locationConstraint
	if err := xml.NewDecoder(location.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode location response: %v", err)
	}
	if parsed.Value != "" {
		t.Fatalf("location constraint = %q, want empty us-east-1 constraint", parsed.Value)
	}
}

func TestObjectCRUDListRangeAndCopy(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	create := performRequest(routes, http.MethodPut, "/demo-bucket", nil)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}

	putReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("hello from devcloud s3\n"))
	putReq.Header.Set("Content-Type", "text/plain")
	putReq.Header.Set("x-amz-meta-source", "unit-test")
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}
	if got := putRec.Header().Get("ETag"); got == "" {
		t.Fatal("put response missing ETag")
	}

	head := performRequest(routes, http.MethodHead, "/demo-bucket/docs/readme.txt", nil)
	if head.Code != http.StatusOK {
		t.Fatalf("head status = %d, want %d; body=%s", head.Code, http.StatusOK, head.Body.String())
	}
	if got := head.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("head Content-Type = %q, want text/plain", got)
	}
	if got := head.Header().Get("x-amz-meta-source"); got != "unit-test" {
		t.Fatalf("head metadata = %q, want unit-test", got)
	}
	if got := head.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("head Accept-Ranges = %q, want bytes", got)
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/docs/readme.txt", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	if got := get.Body.String(); got != "hello from devcloud s3\n" {
		t.Fatalf("get body = %q", got)
	}

	rangeReq := httptest.NewRequest(http.MethodGet, "/demo-bucket/docs/readme.txt", nil)
	rangeReq.Header.Set("Range", "bytes=0-4")
	rangeRec := httptest.NewRecorder()
	routes.ServeHTTP(rangeRec, rangeReq)
	if rangeRec.Code != http.StatusPartialContent {
		t.Fatalf("range status = %d, want %d; body=%s", rangeRec.Code, http.StatusPartialContent, rangeRec.Body.String())
	}
	if got := rangeRec.Body.String(); got != "hello" {
		t.Fatalf("range body = %q, want hello", got)
	}

	list := performRequest(routes, http.MethodGet, "/demo-bucket?list-type=2&prefix=docs/", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list objects status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	if !strings.Contains(list.Body.String(), "<Key>docs/readme.txt</Key>") {
		t.Fatalf("list objects body missing key: %s", list.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/copy.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/demo-bucket/docs/readme.txt")
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want %d; body=%s", copyRec.Code, http.StatusOK, copyRec.Body.String())
	}
	copyGet := performRequest(routes, http.MethodGet, "/demo-bucket/docs/copy.txt", nil)
	if copyGet.Body.String() != "hello from devcloud s3\n" {
		t.Fatalf("copy body = %q", copyGet.Body.String())
	}

	deleteCopy := performRequest(routes, http.MethodDelete, "/demo-bucket/docs/copy.txt", nil)
	if deleteCopy.Code != http.StatusNoContent {
		t.Fatalf("delete object status = %d, want %d", deleteCopy.Code, http.StatusNoContent)
	}
	missingCopy := performRequest(routes, http.MethodGet, "/demo-bucket/docs/copy.txt", nil)
	if missingCopy.Code != http.StatusNotFound {
		t.Fatalf("missing object status = %d, want %d", missingCopy.Code, http.StatusNotFound)
	}
}

func TestPutObjectValidatesContentMD5(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	body := "checksum body"
	putReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/checksum.txt", strings.NewReader(body))
	putReq.Header.Set("Content-MD5", contentMD5(body))
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put with valid md5 status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}

	mismatchReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/bad-checksum.txt", strings.NewReader(body))
	mismatchReq.Header.Set("Content-MD5", contentMD5("different body"))
	mismatchRec := httptest.NewRecorder()
	routes.ServeHTTP(mismatchRec, mismatchReq)
	if mismatchRec.Code != http.StatusBadRequest {
		t.Fatalf("put with bad md5 status = %d, want %d; body=%s", mismatchRec.Code, http.StatusBadRequest, mismatchRec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(mismatchRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode md5 mismatch error: %v", err)
	}
	if parsed.Code != "BadDigest" {
		t.Fatalf("md5 mismatch code = %q, want BadDigest", parsed.Code)
	}

	invalidReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/invalid-checksum.txt", strings.NewReader(body))
	invalidReq.Header.Set("Content-MD5", "not-base64")
	invalidRec := httptest.NewRecorder()
	routes.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("put with invalid md5 status = %d, want %d; body=%s", invalidRec.Code, http.StatusBadRequest, invalidRec.Body.String())
	}
	if err := xml.NewDecoder(invalidRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode invalid md5 error: %v", err)
	}
	if parsed.Code != "InvalidDigest" {
		t.Fatalf("invalid md5 code = %q, want InvalidDigest", parsed.Code)
	}
}

func TestCopyObjectAcceptsEscapedSourceWithQuery(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	put := performRequest(routes, http.MethodPut, "/demo-bucket/docs/source%20file.txt", strings.NewReader("copy me"))
	if put.Code != http.StatusOK {
		t.Fatalf("put source status = %d; body=%s", put.Code, put.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/copied.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/demo-bucket/docs/source%20file.txt?versionId=ignored")
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want %d; body=%s", copyRec.Code, http.StatusOK, copyRec.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/docs/copied.txt", nil)
	if got := get.Body.String(); got != "copy me" {
		t.Fatalf("copied body = %q, want copy me", got)
	}
}

func TestRangeOnEmptyObjectReturnsInvalidRange(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	put := performRequest(routes, http.MethodPut, "/demo-bucket/empty.txt", strings.NewReader(""))
	if put.Code != http.StatusOK {
		t.Fatalf("put empty object status = %d; body=%s", put.Code, put.Body.String())
	}

	rangeReq := httptest.NewRequest(http.MethodGet, "/demo-bucket/empty.txt", nil)
	rangeReq.Header.Set("Range", "bytes=0-0")
	rangeRec := httptest.NewRecorder()
	routes.ServeHTTP(rangeRec, rangeReq)
	if rangeRec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("empty range status = %d, want %d; body=%s", rangeRec.Code, http.StatusRequestedRangeNotSatisfiable, rangeRec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(rangeRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode range error: %v", err)
	}
	if parsed.Code != "InvalidRange" {
		t.Fatalf("range error code = %q, want InvalidRange", parsed.Code)
	}
}

func TestListObjectsV2SupportsDelimiterAndContinuation(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	for _, key := range []string{
		"docs/a.txt",
		"docs/archive/2026.txt",
		"docs/b.txt",
		"logs/app.log",
	} {
		put := performRequest(routes, http.MethodPut, "/demo-bucket/"+key, strings.NewReader(key))
		if put.Code != http.StatusOK {
			t.Fatalf("put %s status = %d; body=%s", key, put.Code, put.Body.String())
		}
	}

	firstPage := performRequest(routes, http.MethodGet, "/demo-bucket?list-type=2&prefix=docs/&delimiter=/&max-keys=2", nil)
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d; body=%s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first listBucketResult
	if err := xml.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if !first.IsTruncated {
		t.Fatal("first page IsTruncated = false, want true")
	}
	if first.KeyCount != 2 {
		t.Fatalf("first page KeyCount = %d, want 2", first.KeyCount)
	}
	if len(first.Contents) != 1 || first.Contents[0].Key != "docs/a.txt" {
		t.Fatalf("first page contents = %#v", first.Contents)
	}
	if len(first.CommonPrefixes) != 1 || first.CommonPrefixes[0].Prefix != "docs/archive/" {
		t.Fatalf("first page common prefixes = %#v", first.CommonPrefixes)
	}
	if first.NextContinuationToken == "" {
		t.Fatal("first page missing NextContinuationToken")
	}

	secondPage := performRequest(routes, http.MethodGet, "/demo-bucket?list-type=2&prefix=docs/&delimiter=/&continuation-token="+url.QueryEscape(first.NextContinuationToken), nil)
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, want %d; body=%s", secondPage.Code, http.StatusOK, secondPage.Body.String())
	}
	var second listBucketResult
	if err := xml.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if second.IsTruncated {
		t.Fatal("second page IsTruncated = true, want false")
	}
	if len(second.Contents) != 1 || second.Contents[0].Key != "docs/b.txt" {
		t.Fatalf("second page contents = %#v", second.Contents)
	}
	if len(second.CommonPrefixes) != 0 {
		t.Fatalf("second page common prefixes = %#v, want none", second.CommonPrefixes)
	}
}

func TestListObjectsSupportsV1MarkerAndURLEncoding(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	for _, key := range []string{"docs/a file.txt", "docs/b file.txt"} {
		put := performRequest(routes, http.MethodPut, "/demo-bucket/"+url.PathEscape(key), strings.NewReader(key))
		if put.Code != http.StatusOK {
			t.Fatalf("put %s status = %d; body=%s", key, put.Code, put.Body.String())
		}
	}

	list := performRequest(routes, http.MethodGet, "/demo-bucket?prefix=docs/&marker=docs/a%20file.txt&encoding-type=url", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var parsed listBucketResult
	if err := xml.NewDecoder(list.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(parsed.Contents) != 1 || parsed.Contents[0].Key != "docs%2Fb%20file.txt" {
		t.Fatalf("encoded contents = %#v", parsed.Contents)
	}
	if parsed.Marker != "docs%2Fa%20file.txt" {
		t.Fatalf("encoded marker = %q", parsed.Marker)
	}
}

func TestBucketRoutesReturnS3XMLErrors(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	invalid := performRequest(routes, http.MethodPut, "/../bad", nil)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d, want %d", invalid.Code, http.StatusBadRequest)
	}
	var parsed errorResponse
	if err := xml.NewDecoder(invalid.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if parsed.Code != "InvalidBucketName" {
		t.Fatalf("error code = %q, want InvalidBucketName", parsed.Code)
	}

	objectRoute := performRequest(routes, http.MethodGet, "/demo-bucket/key.txt", nil)
	if objectRoute.Code != http.StatusNotFound {
		t.Fatalf("object route status = %d, want %d", objectRoute.Code, http.StatusNotFound)
	}
}

func TestCreateBucketRejectsS3InvalidBucketNames(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	for _, name := range []string{
		".starts-with-dot",
		"ends-with-dot.",
		"-starts-with-hyphen",
		"ends-with-hyphen-",
		"has.-adjacent",
		"has-.adjacent",
		"192.168.0.1",
	} {
		t.Run(name, func(t *testing.T) {
			rec := performRequest(routes, http.MethodPut, "/"+name, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("create status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			var parsed errorResponse
			if err := xml.NewDecoder(rec.Body).Decode(&parsed); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if parsed.Code != "InvalidBucketName" {
				t.Fatalf("error code = %q, want InvalidBucketName", parsed.Code)
			}
		})
	}
}

func TestPresignedURLValidatesSignature(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "relaxed",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()
	fixedNow := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	previousNow := nowUTC
	nowUTC = func() time.Time { return fixedNow }
	defer func() { nowUTC = previousNow }()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d", create.Code)
	}
	put := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/readme.txt", strings.NewReader("hello from devcloud s3\n"))
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put status = %d", putRec.Code)
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
		AuthMode:        "relaxed",
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

func TestFileBucketStoreUpdateObjectMetadataPreservesBody(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	ctx := context.Background()
	if _, created, err := store.CreateBucket(ctx, "demo-bucket"); err != nil || !created {
		t.Fatalf("create bucket created=%t err=%v", created, err)
	}
	if _, err := store.PutObject(ctx, PutObjectInput{
		Bucket:             "demo-bucket",
		Key:                "docs/readme.txt",
		Body:               strings.NewReader("metadata body"),
		ContentType:        "text/plain",
		ContentDisposition: `inline; filename="readme.txt"`,
		Metadata:           map[string]string{"source": "original"},
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}

	updated, found, err := store.UpdateObjectMetadata(ctx, UpdateObjectMetadataInput{
		Bucket:          "demo-bucket",
		Key:             "docs/readme.txt",
		ContentType:     "text/markdown",
		ContentEncoding: "gzip",
		CacheControl:    "max-age=60",
		Metadata:        map[string]string{"source": "updated", "empty": ""},
	})
	if err != nil || !found {
		t.Fatalf("update metadata found=%t err=%v", found, err)
	}
	if updated.ContentType != "text/markdown" || updated.ContentEncoding != "gzip" || updated.CacheControl != "max-age=60" {
		t.Fatalf("updated headers = contentType:%q contentEncoding:%q cacheControl:%q", updated.ContentType, updated.ContentEncoding, updated.CacheControl)
	}
	if got := updated.ContentDisposition; got != `inline; filename="readme.txt"` {
		t.Fatalf("content disposition = %q, want preserved inline disposition", got)
	}
	if got := updated.Metadata["source"]; got != "updated" {
		t.Fatalf("metadata source = %q, want updated", got)
	}
	if got := updated.Metadata["empty"]; got != "" {
		t.Fatalf("empty metadata = %q, want empty value preserved", got)
	}
	if updated.Metageneration != 2 {
		t.Fatalf("metageneration = %d, want 2", updated.Metageneration)
	}

	gotObject, body, found, err := store.GetObject(ctx, "demo-bucket", "docs/readme.txt")
	if err != nil || !found {
		t.Fatalf("get updated object found=%t err=%v", found, err)
	}
	if string(body) != "metadata body" {
		t.Fatalf("body changed after metadata update: %q", string(body))
	}
	if gotObject.ETag != updated.ETag {
		t.Fatalf("etag changed on metadata update: got %q want %q", gotObject.ETag, updated.ETag)
	}

	if _, found, err := store.UpdateObjectMetadata(ctx, UpdateObjectMetadataInput{
		Bucket:      "demo-bucket",
		Key:         "missing.txt",
		ContentType: "text/plain",
	}); err != nil || found {
		t.Fatalf("update missing object found=%t err=%v", found, err)
	}
}

func TestMultipartUploadFlow(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}

	initiateReq := httptest.NewRequest(http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	initiateReq.Header.Set("Content-Type", "application/octet-stream")
	initiateReq.Header.Set("Cache-Control", "max-age=60")
	initiateReq.Header.Set("x-amz-meta-source", "multipart-test")
	initiate := httptest.NewRecorder()
	routes.ServeHTTP(initiate, initiateReq)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}
	if initiated.UploadID == "" {
		t.Fatal("initiate response missing UploadId")
	}

	partOne := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber=1&uploadId="+initiated.UploadID, strings.NewReader("part-one-"))
	if partOne.Code != http.StatusOK {
		t.Fatalf("part one status = %d, want %d; body=%s", partOne.Code, http.StatusOK, partOne.Body.String())
	}
	if got := partOne.Header().Get("ETag"); got == "" {
		t.Fatal("part one missing ETag")
	}
	partTwo := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber=2&uploadId="+initiated.UploadID, strings.NewReader("part-two"))
	if partTwo.Code != http.StatusOK {
		t.Fatalf("part two status = %d, want %d; body=%s", partTwo.Code, http.StatusOK, partTwo.Body.String())
	}

	listUploads := performRequest(routes, http.MethodGet, "/demo-bucket?uploads", nil)
	if listUploads.Code != http.StatusOK {
		t.Fatalf("list uploads status = %d, want %d; body=%s", listUploads.Code, http.StatusOK, listUploads.Body.String())
	}
	if !strings.Contains(listUploads.Body.String(), "<Key>large.bin</Key>") || !strings.Contains(listUploads.Body.String(), "<UploadId>"+initiated.UploadID+"</UploadId>") {
		t.Fatalf("list uploads missing initiated upload: %s", listUploads.Body.String())
	}

	listParts := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin?uploadId="+initiated.UploadID, nil)
	if listParts.Code != http.StatusOK {
		t.Fatalf("list parts status = %d, want %d; body=%s", listParts.Code, http.StatusOK, listParts.Body.String())
	}
	if !strings.Contains(listParts.Body.String(), "<PartNumber>1</PartNumber>") {
		t.Fatalf("list parts missing first part: %s", listParts.Body.String())
	}

	completeBody := strings.NewReader("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>ignored</ETag></Part><Part><PartNumber>2</PartNumber><ETag>ignored</ETag></Part></CompleteMultipartUpload>")
	complete := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploadId="+initiated.UploadID, completeBody)
	if complete.Code != http.StatusOK {
		t.Fatalf("complete status = %d, want %d; body=%s", complete.Code, http.StatusOK, complete.Body.String())
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("get completed object status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	if got := get.Body.String(); got != "part-one-part-two" {
		t.Fatalf("completed object body = %q", got)
	}
	if got := get.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("completed object Content-Type = %q, want application/octet-stream", got)
	}
	if got := get.Header().Get("Cache-Control"); got != "max-age=60" {
		t.Fatalf("completed object Cache-Control = %q, want max-age=60", got)
	}
	if got := get.Header().Get("x-amz-meta-source"); got != "multipart-test" {
		t.Fatalf("completed object metadata = %q, want multipart-test", got)
	}
	if got := get.Header().Get("ETag"); !strings.HasSuffix(got, `-2"`) {
		t.Fatalf("completed object ETag = %q, want multipart ETag with part count", got)
	}

	abortInit := performRequest(routes, http.MethodPost, "/demo-bucket/aborted.bin?uploads", nil)
	var abortUpload initiateMultipartUploadResult
	if err := xml.NewDecoder(abortInit.Body).Decode(&abortUpload); err != nil {
		t.Fatalf("decode abort initiate response: %v", err)
	}
	abort := performRequest(routes, http.MethodDelete, "/demo-bucket/aborted.bin?uploadId="+abortUpload.UploadID, nil)
	if abort.Code != http.StatusNoContent {
		t.Fatalf("abort status = %d, want %d; body=%s", abort.Code, http.StatusNoContent, abort.Body.String())
	}
	listAborted := performRequest(routes, http.MethodGet, "/demo-bucket/aborted.bin?uploadId="+abortUpload.UploadID, nil)
	if listAborted.Code != http.StatusNotFound {
		t.Fatalf("list aborted status = %d, want %d", listAborted.Code, http.StatusNotFound)
	}
}

func TestUploadPartValidatesContentMD5(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	partReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/large.bin?partNumber=1&uploadId="+initiated.UploadID, strings.NewReader("part-body"))
	partReq.Header.Set("Content-MD5", contentMD5("different-body"))
	partRec := httptest.NewRecorder()
	routes.ServeHTTP(partRec, partReq)
	if partRec.Code != http.StatusBadRequest {
		t.Fatalf("part with bad md5 status = %d, want %d; body=%s", partRec.Code, http.StatusBadRequest, partRec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(partRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode part md5 mismatch error: %v", err)
	}
	if parsed.Code != "BadDigest" {
		t.Fatalf("part md5 mismatch code = %q, want BadDigest", parsed.Code)
	}
}

func TestUploadPartRejectsOutOfRangePartNumber(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	part := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber=10001&uploadId="+initiated.UploadID, strings.NewReader("part-body"))
	if part.Code != http.StatusBadRequest {
		t.Fatalf("part status = %d, want %d; body=%s", part.Code, http.StatusBadRequest, part.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(part.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode part number error: %v", err)
	}
	if parsed.Code != "InvalidArgument" {
		t.Fatalf("part number error code = %q, want InvalidArgument", parsed.Code)
	}
}

func TestCompleteMultipartUploadRejectsEmptyPartList(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	complete := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploadId="+initiated.UploadID, strings.NewReader("<CompleteMultipartUpload></CompleteMultipartUpload>"))
	if complete.Code != http.StatusBadRequest {
		t.Fatalf("complete status = %d, want %d; body=%s", complete.Code, http.StatusBadRequest, complete.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(complete.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode empty complete error: %v", err)
	}
	if parsed.Code != "MalformedXML" {
		t.Fatalf("empty complete error code = %q, want MalformedXML", parsed.Code)
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin", nil)
	if get.Code != http.StatusNotFound {
		t.Fatalf("empty complete created object; get status = %d, want %d", get.Code, http.StatusNotFound)
	}
}

func TestListPartsSupportsPagination(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}
	for _, part := range []struct {
		number int
		body   string
	}{
		{number: 1, body: "one"},
		{number: 2, body: "two"},
		{number: 3, body: "three"},
	} {
		rec := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber="+strconv.Itoa(part.number)+"&uploadId="+initiated.UploadID, strings.NewReader(part.body))
		if rec.Code != http.StatusOK {
			t.Fatalf("upload part %d status = %d, want %d; body=%s", part.number, rec.Code, http.StatusOK, rec.Body.String())
		}
	}

	firstPage := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin?uploadId="+initiated.UploadID+"&max-parts=2", nil)
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d; body=%s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first listPartsResult
	if err := xml.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if !first.IsTruncated {
		t.Fatal("first page IsTruncated = false, want true")
	}
	if first.MaxParts != 2 || first.NextPartNumberMarker != 2 {
		t.Fatalf("first page markers MaxParts=%d NextPartNumberMarker=%d", first.MaxParts, first.NextPartNumberMarker)
	}
	if len(first.Parts) != 2 || first.Parts[0].PartNumber != 1 || first.Parts[1].PartNumber != 2 {
		t.Fatalf("first page parts = %#v", first.Parts)
	}

	secondPage := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin?uploadId="+initiated.UploadID+"&part-number-marker=2&max-parts=2", nil)
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, want %d; body=%s", secondPage.Code, http.StatusOK, secondPage.Body.String())
	}
	var second listPartsResult
	if err := xml.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if second.IsTruncated {
		t.Fatal("second page IsTruncated = true, want false")
	}
	if second.PartNumberMarker != 2 || second.NextPartNumberMarker != 0 {
		t.Fatalf("second page markers PartNumberMarker=%d NextPartNumberMarker=%d", second.PartNumberMarker, second.NextPartNumberMarker)
	}
	if len(second.Parts) != 1 || second.Parts[0].PartNumber != 3 {
		t.Fatalf("second page parts = %#v", second.Parts)
	}
}

func TestCompleteMultipartUploadRejectsOutOfOrderParts(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}

	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	for _, part := range []struct {
		number int
		body   string
	}{
		{number: 1, body: "part-one-"},
		{number: 2, body: "part-two"},
	} {
		rec := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber="+strconv.Itoa(part.number)+"&uploadId="+initiated.UploadID, strings.NewReader(part.body))
		if rec.Code != http.StatusOK {
			t.Fatalf("upload part %d status = %d, want %d; body=%s", part.number, rec.Code, http.StatusOK, rec.Body.String())
		}
	}

	completeBody := strings.NewReader("<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>ignored</ETag></Part><Part><PartNumber>1</PartNumber><ETag>ignored</ETag></Part></CompleteMultipartUpload>")
	complete := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploadId="+initiated.UploadID, completeBody)
	if complete.Code != http.StatusBadRequest {
		t.Fatalf("complete status = %d, want %d; body=%s", complete.Code, http.StatusBadRequest, complete.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(complete.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode complete error: %v", err)
	}
	if parsed.Code != "InvalidPartOrder" {
		t.Fatalf("complete error code = %q, want InvalidPartOrder", parsed.Code)
	}
}

func TestCompleteMultipartUploadRejectsCombinedObjectOverMaxBytes(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{MaxObjectBytes: 10}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}

	initiate := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploads", nil)
	if initiate.Code != http.StatusOK {
		t.Fatalf("initiate status = %d, want %d; body=%s", initiate.Code, http.StatusOK, initiate.Body.String())
	}
	var initiated initiateMultipartUploadResult
	if err := xml.NewDecoder(initiate.Body).Decode(&initiated); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}

	for _, part := range []struct {
		number int
		body   string
	}{
		{number: 1, body: "123456"},
		{number: 2, body: "78901"},
	} {
		rec := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber="+strconv.Itoa(part.number)+"&uploadId="+initiated.UploadID, strings.NewReader(part.body))
		if rec.Code != http.StatusOK {
			t.Fatalf("upload part %d status = %d, want %d; body=%s", part.number, rec.Code, http.StatusOK, rec.Body.String())
		}
	}

	completeBody := strings.NewReader("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>ignored</ETag></Part><Part><PartNumber>2</PartNumber><ETag>ignored</ETag></Part></CompleteMultipartUpload>")
	complete := performRequest(routes, http.MethodPost, "/demo-bucket/large.bin?uploadId="+initiated.UploadID, completeBody)
	if complete.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("complete status = %d, want %d; body=%s", complete.Code, http.StatusRequestEntityTooLarge, complete.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(complete.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode complete error: %v", err)
	}
	if parsed.Code != "EntityTooLarge" {
		t.Fatalf("complete error code = %q, want EntityTooLarge", parsed.Code)
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/large.bin", nil)
	if get.Code != http.StatusNotFound {
		t.Fatalf("oversized multipart object should not be stored; get status = %d, want %d", get.Code, http.StatusNotFound)
	}
}

func TestMultipartUploadRejectsInvalidUploadID(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}

	rec := performRequest(routes, http.MethodPut, "/demo-bucket/large.bin?partNumber=1&uploadId=../escape", strings.NewReader("part"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid upload id status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	var parsed errorResponse
	if err := xml.NewDecoder(rec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode invalid upload id error: %v", err)
	}
	if parsed.Code != "InvalidArgument" {
		t.Fatalf("invalid upload id code = %q, want InvalidArgument", parsed.Code)
	}
}

func TestStrictAuthRejectsUnsignedRequests(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "strict",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()

	rec := performRequest(routes, http.MethodGet, "/", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("strict unsigned status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func performRequest(handler http.Handler, method string, target string, body *strings.Reader) *httptest.ResponseRecorder {
	var reader *strings.Reader
	if body == nil {
		reader = strings.NewReader("")
	} else {
		reader = body
	}
	req := httptest.NewRequest(method, target, reader)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func presignedTarget(t *testing.T, method string, host string, path string, now time.Time) string {
	t.Helper()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	scope := dateStamp + "/us-east-1/s3/aws4_request"
	values := url.Values{}
	values.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	values.Set("X-Amz-Credential", "dev/"+scope)
	values.Set("X-Amz-Date", amzDate)
	values.Set("X-Amz-Expires", "300")
	values.Set("X-Amz-SignedHeaders", "host")
	canonicalQuery := testCanonicalQuery(values)
	canonicalRequest := strings.Join([]string{
		method,
		path,
		canonicalQuery,
		"host:" + host + "\n",
		"host",
		"UNSIGNED-PAYLOAD",
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		testSHA256Hex(canonicalRequest),
	}, "\n")
	signingKey := testSigningKey("dev", dateStamp, "us-east-1")
	signature := hmac.New(sha256.New, signingKey)
	signature.Write([]byte(stringToSign))
	values.Set("X-Amz-Signature", hex.EncodeToString(signature.Sum(nil)))
	return path + "?" + testCanonicalQuery(values)
}

func testCanonicalQuery(values url.Values) string {
	type pair struct {
		key   string
		value string
	}
	var pairs []pair
	for key, vals := range values {
		for _, val := range vals {
			pairs = append(pairs, pair{key: key, value: val})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})
	out := make([]string, 0, len(pairs))
	for _, item := range pairs {
		out = append(out, awsPercentEncode(item.key, "~-_")+"="+awsPercentEncode(item.value, "~-_"))
	}
	return strings.Join(out, "&")
}

func testSigningKey(secret string, dateStamp string, region string) []byte {
	sign := func(key []byte, value string) []byte {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(value))
		return mac.Sum(nil)
	}
	dateKey := sign([]byte("AWS4"+secret), dateStamp)
	regionKey := sign(dateKey, region)
	serviceKey := sign(regionKey, "s3")
	return sign(serviceKey, "aws4_request")
}

func testSHA256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func contentMD5(value string) string {
	sum := md5.Sum([]byte(value))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func signedRequest(t *testing.T, method string, path string, body string, now time.Time) *http.Request {
	t.Helper()
	bodyHash := testSHA256Hex(body)
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Host = "example.com"
	amzDate := now.Format("20060102T150405Z")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", bodyHash)
	req.Header.Set("Authorization", authorizationHeader(t, method, path, "example.com", bodyHash, amzDate))
	return req
}

func authorizationHeader(t *testing.T, method string, path string, host string, bodyHash string, amzDate string) string {
	t.Helper()
	dateStamp := amzDate[:8]
	scope := dateStamp + "/us-east-1/s3/aws4_request"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{
		method,
		path,
		"",
		"host:" + host + "\n" +
			"x-amz-content-sha256:" + bodyHash + "\n" +
			"x-amz-date:" + amzDate + "\n",
		signedHeaders,
		bodyHash,
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		testSHA256Hex(canonicalRequest),
	}, "\n")
	signature := hmac.New(sha256.New, testSigningKey("dev", dateStamp, "us-east-1"))
	signature.Write([]byte(stringToSign))
	return "AWS4-HMAC-SHA256 Credential=dev/" + scope + ", SignedHeaders=" + signedHeaders + ", Signature=" + hex.EncodeToString(signature.Sum(nil))
}
