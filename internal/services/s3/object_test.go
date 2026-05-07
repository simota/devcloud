package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

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

func TestVirtualHostStyleRoutesUseHostBucket(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	create := httptest.NewRequest(http.MethodPut, "/", nil)
	create.Host = "demo-bucket.localhost"
	createRec := httptest.NewRecorder()
	routes.ServeHTTP(createRec, create)
	if createRec.Code != http.StatusOK {
		t.Fatalf("virtual create status = %d, want %d; body=%s", createRec.Code, http.StatusOK, createRec.Body.String())
	}

	put := httptest.NewRequest(http.MethodPut, "/docs/readme.txt", strings.NewReader("hello virtual host\n"))
	put.Host = "demo-bucket.localhost"
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusOK {
		t.Fatalf("virtual put status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/docs/readme.txt", nil)
	get.Host = "demo-bucket.localhost:4566"
	getRec := httptest.NewRecorder()
	routes.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("virtual get status = %d, want %d; body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != "hello virtual host\n" {
		t.Fatalf("virtual get body = %q", got)
	}

	list := httptest.NewRequest(http.MethodGet, "/?list-type=2&prefix=docs/", nil)
	list.Host = "demo-bucket.localhost"
	listRec := httptest.NewRecorder()
	routes.ServeHTTP(listRec, list)
	if listRec.Code != http.StatusOK {
		t.Fatalf("virtual list status = %d, want %d; body=%s", listRec.Code, http.StatusOK, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), "<Key>docs/readme.txt</Key>") {
		t.Fatalf("virtual list body missing key: %s", listRec.Body.String())
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
	copyReq.Header.Set("x-amz-copy-source", "/demo-bucket/docs/source%20file.txt?response-content-type=text/plain")
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

func TestCopyObjectUsesSourceVersionID(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPut, "/demo-bucket", nil); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if versioning := performRequest(routes, http.MethodPut, "/demo-bucket?versioning", strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)); versioning.Code != http.StatusOK {
		t.Fatalf("enable versioning status = %d; body=%s", versioning.Code, versioning.Body.String())
	}

	first := performRequest(routes, http.MethodPut, "/demo-bucket/docs/source.txt", strings.NewReader("first"))
	if first.Code != http.StatusOK {
		t.Fatalf("put first status = %d; body=%s", first.Code, first.Body.String())
	}
	firstVersionID := first.Header().Get("x-amz-version-id")
	if firstVersionID == "" {
		t.Fatal("first put missing version id")
	}
	second := performRequest(routes, http.MethodPut, "/demo-bucket/docs/source.txt", strings.NewReader("second"))
	if second.Code != http.StatusOK {
		t.Fatalf("put second status = %d; body=%s", second.Code, second.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/demo-bucket/docs/copied.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/demo-bucket/docs/source.txt?versionId="+url.QueryEscape(firstVersionID))
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy version status = %d, want %d; body=%s", copyRec.Code, http.StatusOK, copyRec.Body.String())
	}
	if got := copyRec.Header().Get("x-amz-version-id"); got == "" {
		t.Fatal("copy response missing destination version id")
	}

	get := performRequest(routes, http.MethodGet, "/demo-bucket/docs/copied.txt", nil)
	if got := get.Body.String(); got != "first" {
		t.Fatalf("copied version body = %q, want first", got)
	}
	latest := performRequest(routes, http.MethodGet, "/demo-bucket/docs/source.txt", nil)
	if got := latest.Body.String(); got != "second" {
		t.Fatalf("source latest body = %q, want second", got)
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

