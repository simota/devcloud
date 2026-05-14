package s3

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
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

func TestRelaxedAuthAcceptsArbitraryCredentials(t *testing.T) {
	store := NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		Region:          "us-east-1",
		AuthMode:        "relaxed",
		AccessKeyID:     "dev",
		SecretAccessKey: "dev",
	}, store).routes()

	// awslocal / LocalStack default credentials are "test"/"test", which do
	// not match the configured "dev"/"dev". In relaxed mode the server must
	// still accept the request rather than returning InvalidAccessKeyId.
	create := httptest.NewRequest(http.MethodPut, "/awslocal-bucket", nil)
	create.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260514/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef")
	create.Header.Set("x-amz-date", "20260514T120000Z")
	createRec := httptest.NewRecorder()
	routes.ServeHTTP(createRec, create)
	if createRec.Code != http.StatusOK {
		t.Fatalf("relaxed create-bucket with foreign creds status = %d, want %d; body=%s", createRec.Code, http.StatusOK, createRec.Body.String())
	}

	list := performRequest(routes, http.MethodGet, "/", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("relaxed list status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	if !strings.Contains(list.Body.String(), "<Name>awslocal-bucket</Name>") {
		t.Fatalf("relaxed list body missing bucket: %s", list.Body.String())
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

func eventStreamRecords(t *testing.T, data []byte) []byte {
	t.Helper()
	var records []byte
	for len(data) > 0 {
		if len(data) < 16 {
			t.Fatalf("eventstream message too short: %d bytes", len(data))
		}
		totalLength := int(binary.BigEndian.Uint32(data[0:4]))
		headersLength := int(binary.BigEndian.Uint32(data[4:8]))
		if totalLength < 16 || totalLength > len(data) {
			t.Fatalf("eventstream total length = %d for %d bytes", totalLength, len(data))
		}
		if got, want := crc32.ChecksumIEEE(data[0:8]), binary.BigEndian.Uint32(data[8:12]); got != want {
			t.Fatalf("eventstream prelude crc = %08x, want %08x", got, want)
		}
		if got, want := crc32.ChecksumIEEE(data[:totalLength-4]), binary.BigEndian.Uint32(data[totalLength-4:totalLength]); got != want {
			t.Fatalf("eventstream message crc = %08x, want %08x", got, want)
		}
		payloadStart := 12 + headersLength
		if payloadStart > totalLength-4 {
			t.Fatalf("eventstream headers length = %d exceeds message length %d", headersLength, totalLength)
		}
		records = append(records, data[payloadStart:totalLength-4]...)
		data = data[totalLength:]
	}
	return records
}

func containsString(values []string, target string) bool {
	return indexOfString(values, target) >= 0
}

func indexOfString(values []string, target string) int {
	for i, value := range values {
		if value == target {
			return i
		}
	}
	return -1
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
