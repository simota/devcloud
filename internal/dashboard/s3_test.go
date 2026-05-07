package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

func TestS3DashboardPageAndAPIExposeObjects(t *testing.T) {
	s3Store := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := s3Store.CreateBucket(context.Background(), "demo-bucket"); err != nil || !created {
		t.Fatalf("create bucket created=%t err=%v", created, err)
	}
	if _, err := s3Store.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:      "demo-bucket",
		Key:         "docs/readme.txt",
		Body:        strings.NewReader("hello from dashboard\n"),
		ContentType: "text/plain",
		Metadata:    map[string]string{"source": "dashboard-test"},
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}
	if _, err := s3Store.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:      "demo-bucket",
		Key:         "docs/read%2Fme.txt",
		Body:        strings.NewReader("literal percent key\n"),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("put object with escaped-looking key: %v", err)
	}
	routes := NewServer(Config{
		S3Endpoint:    "http://127.0.0.1:4566",
		S3Region:      "us-east-1",
		S3AuthMode:    "relaxed",
		S3StoragePath: ".devcloud/data/s3",
	}, newDashboardStore(nil, nil), s3Store).routes()

	page := performRequest(routes, http.MethodGet, "/s3")
	if page.Code != http.StatusOK {
		t.Fatalf("s3 page status = %d, want %d", page.Code, http.StatusOK)
	}
	if body := page.Body.String(); !strings.Contains(body, "devcloud S3") || !strings.Contains(body, "/api/s3/buckets") {
		t.Fatalf("s3 page missing expected shell: %s", body)
	}

	status := performRequest(routes, http.MethodGet, "/api/s3/status")
	if status.Code != http.StatusOK {
		t.Fatalf("s3 status code = %d, want %d", status.Code, http.StatusOK)
	}
	if !strings.Contains(status.Body.String(), `"running"`) {
		t.Fatalf("s3 status missing running state: %s", status.Body.String())
	}

	buckets := performRequest(routes, http.MethodGet, "/api/s3/buckets")
	if buckets.Code != http.StatusOK {
		t.Fatalf("s3 buckets code = %d, want %d", buckets.Code, http.StatusOK)
	}
	if !strings.Contains(buckets.Body.String(), "demo-bucket") {
		t.Fatalf("s3 buckets missing bucket: %s", buckets.Body.String())
	}

	objects := performRequest(routes, http.MethodGet, "/api/s3/buckets/demo-bucket/objects?prefix=docs/")
	if objects.Code != http.StatusOK {
		t.Fatalf("s3 objects code = %d, want %d", objects.Code, http.StatusOK)
	}
	body := objects.Body.String()
	for _, want := range []string{"docs/readme.txt", `"contentType":"text/plain"`, `"source":"dashboard-test"`, "s3://demo-bucket/docs/readme.txt"} {
		if !strings.Contains(body, want) {
			t.Fatalf("s3 objects missing %q: %s", want, body)
		}
	}
	var objectList struct {
		Objects []struct {
			Key         string `json:"key"`
			DownloadURL string `json:"downloadUrl"`
		} `json:"objects"`
	}
	if err := json.Unmarshal(objects.Body.Bytes(), &objectList); err != nil {
		t.Fatalf("decode object list: %v", err)
	}
	var escapedLookingDownloadURL string
	for _, object := range objectList.Objects {
		if object.Key == "docs/read%2Fme.txt" {
			escapedLookingDownloadURL = object.DownloadURL
		}
	}
	if escapedLookingDownloadURL == "" {
		t.Fatalf("object list missing escaped-looking key: %s", objects.Body.String())
	}

	download := performRequest(routes, http.MethodGet, "/api/s3/buckets/demo-bucket/objects/docs/readme.txt/download")
	if download.Code != http.StatusOK {
		t.Fatalf("s3 download code = %d, want %d", download.Code, http.StatusOK)
	}
	if got := download.Body.String(); got != "hello from dashboard\n" {
		t.Fatalf("s3 download body = %q", got)
	}
	if got := download.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("s3 download Content-Type = %q, want text/plain", got)
	}
	if got := download.Header().Get("x-amz-meta-source"); got != "dashboard-test" {
		t.Fatalf("s3 download metadata = %q, want dashboard-test", got)
	}
	if got := download.Header().Get("Content-Disposition"); got != `attachment; filename="readme.txt"` {
		t.Fatalf("s3 download Content-Disposition = %q", got)
	}

	escapedLookingDownload := performRequest(routes, http.MethodGet, escapedLookingDownloadURL)
	if escapedLookingDownload.Code != http.StatusOK {
		t.Fatalf("escaped-looking key download code = %d, want %d; body=%s", escapedLookingDownload.Code, http.StatusOK, escapedLookingDownload.Body.String())
	}
	if got := escapedLookingDownload.Body.String(); got != "literal percent key\n" {
		t.Fatalf("escaped-looking key download body = %q", got)
	}
}

func TestS3DashboardBucketObjectManagement(t *testing.T) {
	s3Store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{
		S3Endpoint:    "http://127.0.0.1:4566",
		S3Region:      "us-east-1",
		S3AuthMode:    "relaxed",
		S3StoragePath: ".devcloud/data/s3",
	}, newDashboardStore(nil, nil), s3Store).routes()

	createdBucket := performRequestWithBody(routes, http.MethodPost, "/api/s3/buckets", `{"name":"managed-bucket"}`)
	if createdBucket.Code != http.StatusCreated {
		t.Fatalf("create s3 bucket code = %d, want %d; body=%s", createdBucket.Code, http.StatusCreated, createdBucket.Body.String())
	}
	if !strings.Contains(createdBucket.Body.String(), `"name":"managed-bucket"`) {
		t.Fatalf("create s3 bucket response missing name: %s", createdBucket.Body.String())
	}

	if _, err := s3Store.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:      "managed-bucket",
		Key:         "docs/delete-me.txt",
		Body:        strings.NewReader("temporary object"),
		ContentType: "text/plain",
		Metadata:    map[string]string{"source": "dashboard-management-test"},
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}

	deleteNonEmptyBucket := performRequest(routes, http.MethodDelete, "/api/s3/buckets/managed-bucket")
	if deleteNonEmptyBucket.Code != http.StatusConflict {
		t.Fatalf("delete non-empty bucket code = %d, want %d", deleteNonEmptyBucket.Code, http.StatusConflict)
	}

	deleteObject := performRequest(routes, http.MethodDelete, "/api/s3/buckets/managed-bucket/objects/docs/delete-me.txt")
	if deleteObject.Code != http.StatusNoContent {
		t.Fatalf("delete s3 object code = %d, want %d; body=%s", deleteObject.Code, http.StatusNoContent, deleteObject.Body.String())
	}
	if _, _, found, err := s3Store.GetObject(context.Background(), "managed-bucket", "docs/delete-me.txt"); err != nil || found {
		t.Fatalf("deleted object found=%t err=%v", found, err)
	}

	deleteBucket := performRequest(routes, http.MethodDelete, "/api/s3/buckets/managed-bucket")
	if deleteBucket.Code != http.StatusNoContent {
		t.Fatalf("delete empty bucket code = %d, want %d; body=%s", deleteBucket.Code, http.StatusNoContent, deleteBucket.Body.String())
	}
}

func TestS3DashboardUploadCopyAndMetadata(t *testing.T) {
	s3Store := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := s3Store.CreateBucket(context.Background(), "managed-bucket"); err != nil || !created {
		t.Fatalf("create bucket created=%t err=%v", created, err)
	}
	routes := NewServer(Config{
		S3Endpoint:    "http://127.0.0.1:4566",
		S3Region:      "us-east-1",
		S3AuthMode:    "relaxed",
		S3StoragePath: ".devcloud/data/s3",
	}, newDashboardStore(nil, nil), s3Store).routes()

	putReq := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/managed-bucket/objects/docs/upload.txt", strings.NewReader("dashboard upload"))
	putReq.Header.Set("Content-Type", "text/plain")
	putReq.Header.Set("x-amz-meta-source", "dashboard-upload")
	putRec := httptest.NewRecorder()
	routes.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("upload s3 object code = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}
	for _, want := range []string{`"key":"docs/upload.txt"`, `"contentType":"text/plain"`, `"source":"dashboard-upload"`, `"downloadUrl":`} {
		if !strings.Contains(putRec.Body.String(), want) {
			t.Fatalf("upload response missing %q: %s", want, putRec.Body.String())
		}
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/managed-bucket/objects/docs/copied.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/managed-bucket/docs%2Fupload.txt")
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy s3 object code = %d, want %d; body=%s", copyRec.Code, http.StatusOK, copyRec.Body.String())
	}
	if !strings.Contains(copyRec.Body.String(), `"key":"docs/copied.txt"`) || !strings.Contains(copyRec.Body.String(), `"source":"dashboard-upload"`) {
		t.Fatalf("copy response missing copied object metadata: %s", copyRec.Body.String())
	}

	_, body, found, err := s3Store.GetObject(context.Background(), "managed-bucket", "docs/copied.txt")
	if err != nil || !found {
		t.Fatalf("copied object found=%t err=%v", found, err)
	}
	if got := string(body); got != "dashboard upload" {
		t.Fatalf("copied object body = %q, want dashboard upload", got)
	}
}

func TestS3DashboardMultipartUploadsCanBeListedAndAborted(t *testing.T) {
	s3Store := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := s3Store.CreateBucket(context.Background(), "managed-bucket"); err != nil || !created {
		t.Fatalf("create bucket created=%t err=%v", created, err)
	}
	upload, err := s3Store.CreateMultipartUpload(context.Background(), s3svc.CreateMultipartUploadInput{
		Bucket:      "managed-bucket",
		Key:         "large.bin",
		ContentType: "application/octet-stream",
		Metadata:    map[string]string{"source": "dashboard-multipart"},
	})
	if err != nil {
		t.Fatalf("create multipart upload: %v", err)
	}
	routes := NewServer(Config{
		S3Endpoint:    "http://127.0.0.1:4566",
		S3Region:      "us-east-1",
		S3AuthMode:    "relaxed",
		S3StoragePath: ".devcloud/data/s3",
	}, newDashboardStore(nil, nil), s3Store).routes()

	list := performRequest(routes, http.MethodGet, "/api/s3/buckets/managed-bucket/multipart")
	if list.Code != http.StatusOK {
		t.Fatalf("list multipart uploads code = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	for _, want := range []string{`"key":"large.bin"`, `"uploadId":"` + upload.UploadID + `"`, `"source":"dashboard-multipart"`} {
		if !strings.Contains(list.Body.String(), want) {
			t.Fatalf("multipart uploads response missing %q: %s", want, list.Body.String())
		}
	}

	abort := performRequest(routes, http.MethodDelete, "/api/s3/buckets/managed-bucket/multipart/"+url.PathEscape(upload.UploadID))
	if abort.Code != http.StatusNoContent {
		t.Fatalf("abort multipart upload code = %d, want %d; body=%s", abort.Code, http.StatusNoContent, abort.Body.String())
	}
	uploads, ok, err := s3Store.ListMultipartUploads(context.Background(), "managed-bucket")
	if err != nil || !ok || len(uploads) != 0 {
		t.Fatalf("multipart uploads after abort len=%d ok=%t err=%v", len(uploads), ok, err)
	}
}

func TestS3DashboardAPIReturnsDisabledState(t *testing.T) {
	routes := NewServer(Config{
		S3Endpoint:    "http://127.0.0.1:4566",
		S3Region:      "us-east-1",
		S3AuthMode:    "relaxed",
		S3StoragePath: ".devcloud/data/s3",
	}, newDashboardStore(nil, nil)).routes()

	status := performRequest(routes, http.MethodGet, "/api/s3/status")
	if status.Code != http.StatusOK {
		t.Fatalf("s3 status code = %d, want %d", status.Code, http.StatusOK)
	}
	for _, want := range []string{`"status":"disabled"`, `"running":false`, `"endpoint":"http://127.0.0.1:4566"`} {
		if !strings.Contains(status.Body.String(), want) {
			t.Fatalf("disabled status missing %q: %s", want, status.Body.String())
		}
	}

	for _, target := range []string{
		"/api/s3/buckets",
		"/api/s3/buckets/demo-bucket",
		"/api/s3/buckets/demo-bucket/objects",
		"/api/s3/buckets/demo-bucket/multipart",
	} {
		rec := performRequest(routes, http.MethodGet, target)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s code = %d, want %d", target, rec.Code, http.StatusServiceUnavailable)
		}
		if !strings.Contains(rec.Body.String(), "s3 service is disabled") {
			t.Fatalf("%s response missing disabled message: %s", target, rec.Body.String())
		}
	}
}

func TestS3DashboardAPIValidationAndMissingResources(t *testing.T) {
	s3Store := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := s3Store.CreateBucket(context.Background(), "managed-bucket"); err != nil || !created {
		t.Fatalf("create bucket created=%t err=%v", created, err)
	}
	server := NewServer(Config{
		S3Endpoint:    "http://127.0.0.1:4566",
		S3Region:      "us-east-1",
		S3AuthMode:    "relaxed",
		S3StoragePath: ".devcloud/data/s3",
	}, newDashboardStore(nil, nil), s3Store)
	routes := server.routes()

	invalidJSON := performRequestWithBody(routes, http.MethodPost, "/api/s3/buckets", `{"name":`)
	if invalidJSON.Code != http.StatusBadRequest {
		t.Fatalf("invalid bucket json code = %d, want %d", invalidJSON.Code, http.StatusBadRequest)
	}
	invalidBucket := performRequestWithBody(routes, http.MethodPost, "/api/s3/buckets", `{"name":"Bad_Bucket"}`)
	if invalidBucket.Code != http.StatusBadRequest {
		t.Fatalf("invalid bucket name code = %d, want %d", invalidBucket.Code, http.StatusBadRequest)
	}
	missingBucketObjects := performRequest(routes, http.MethodGet, "/api/s3/buckets/missing-bucket/objects")
	if missingBucketObjects.Code != http.StatusNotFound {
		t.Fatalf("missing bucket objects code = %d, want %d", missingBucketObjects.Code, http.StatusNotFound)
	}
	invalidObjectPathReq := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/managed-bucket/objects/bad", strings.NewReader("body"))
	invalidObjectPath := httptest.NewRecorder()
	server.handleS3ObjectPut(invalidObjectPath, invalidObjectPathReq, "managed-bucket", "%zz")
	if invalidObjectPath.Code != http.StatusBadRequest {
		t.Fatalf("invalid object path code = %d, want %d", invalidObjectPath.Code, http.StatusBadRequest)
	}
	invalidCopy := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/managed-bucket/objects/docs/copied.txt", nil)
	invalidCopy.Header.Set("x-amz-copy-source", "missing-key-only")
	invalidCopyRec := httptest.NewRecorder()
	routes.ServeHTTP(invalidCopyRec, invalidCopy)
	if invalidCopyRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid copy source code = %d, want %d", invalidCopyRec.Code, http.StatusBadRequest)
	}
	missingCopySource := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/managed-bucket/objects/docs/copied.txt", nil)
	missingCopySource.Header.Set("x-amz-copy-source", "/managed-bucket/docs%2Fmissing.txt")
	missingCopyRec := httptest.NewRecorder()
	routes.ServeHTTP(missingCopyRec, missingCopySource)
	if missingCopyRec.Code != http.StatusNotFound {
		t.Fatalf("missing copy source code = %d, want %d", missingCopyRec.Code, http.StatusNotFound)
	}
	if _, _, found, err := s3Store.GetObject(context.Background(), "managed-bucket", "docs/copied.txt"); err != nil || found {
		t.Fatalf("copy target should not be created after validation failure; found=%t err=%v", found, err)
	}
}

func TestS3DashboardCopyReplaceMetadataDoesNotMutateSource(t *testing.T) {
	s3Store := s3svc.NewFileBucketStore(t.TempDir())
	if _, created, err := s3Store.CreateBucket(context.Background(), "managed-bucket"); err != nil || !created {
		t.Fatalf("create bucket created=%t err=%v", created, err)
	}
	if _, err := s3Store.PutObject(context.Background(), s3svc.PutObjectInput{
		Bucket:             "managed-bucket",
		Key:                "docs/source.txt",
		Body:               strings.NewReader("copy body"),
		ContentType:        "text/plain",
		CacheControl:       "max-age=60",
		ContentDisposition: `inline; filename="source.txt"`,
		Metadata:           map[string]string{"source": "original", "owner": "dashboard"},
	}); err != nil {
		t.Fatalf("put source object: %v", err)
	}
	routes := NewServer(Config{
		S3Endpoint:    "http://127.0.0.1:4566",
		S3Region:      "us-east-1",
		S3AuthMode:    "relaxed",
		S3StoragePath: ".devcloud/data/s3",
	}, newDashboardStore(nil, nil), s3Store).routes()

	copyReq := httptest.NewRequest(http.MethodPut, "/api/s3/buckets/managed-bucket/objects/docs/replaced.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/managed-bucket/docs%2Fsource.txt")
	copyReq.Header.Set("x-amz-metadata-directive", "REPLACE")
	copyReq.Header.Set("Content-Type", "application/json")
	copyReq.Header.Set("Cache-Control", "no-store")
	copyReq.Header.Set("Content-Disposition", `attachment; filename="replaced.json"`)
	copyReq.Header.Set("x-amz-meta-source", "replacement")
	copyRec := httptest.NewRecorder()
	routes.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy replace code = %d, want %d; body=%s", copyRec.Code, http.StatusOK, copyRec.Body.String())
	}
	for _, want := range []string{`"key":"docs/replaced.txt"`, `"contentType":"application/json"`, `"source":"replacement"`} {
		if !strings.Contains(copyRec.Body.String(), want) {
			t.Fatalf("copy replace response missing %q: %s", want, copyRec.Body.String())
		}
	}
	if strings.Contains(copyRec.Body.String(), `"owner"`) {
		t.Fatalf("copy replace response retained old metadata: %s", copyRec.Body.String())
	}

	source, sourceBody, found, err := s3Store.GetObject(context.Background(), "managed-bucket", "docs/source.txt")
	if err != nil || !found {
		t.Fatalf("get source object found=%t err=%v", found, err)
	}
	if string(sourceBody) != "copy body" || source.ContentType != "text/plain" || source.Metadata["source"] != "original" || source.Metadata["owner"] != "dashboard" {
		t.Fatalf("source object mutated after copy: object=%#v body=%q", source, string(sourceBody))
	}

	copied, copiedBody, found, err := s3Store.GetObject(context.Background(), "managed-bucket", "docs/replaced.txt")
	if err != nil || !found {
		t.Fatalf("get copied object found=%t err=%v", found, err)
	}
	if string(copiedBody) != "copy body" {
		t.Fatalf("copied body = %q, want copy body", string(copiedBody))
	}
	if copied.ContentType != "application/json" || copied.CacheControl != "no-store" || copied.ContentDisposition != `attachment; filename="replaced.json"` {
		t.Fatalf("copied headers = contentType:%q cacheControl:%q contentDisposition:%q", copied.ContentType, copied.CacheControl, copied.ContentDisposition)
	}
	if got := copied.Metadata["source"]; got != "replacement" {
		t.Fatalf("copied metadata source = %q, want replacement", got)
	}
	if _, ok := copied.Metadata["owner"]; ok {
		t.Fatalf("copied metadata should not keep owner: %#v", copied.Metadata)
	}
}

func TestS3DashboardEscapesDynamicObjectValues(t *testing.T) {
	for _, want := range []string{
		"function escapeHTML(value)",
		"escapeHTML(object.key)",
		"escapeHTML(object.s3Uri)",
		"escapeHTML(metadata[key])",
		"data-index",
	} {
		if !strings.Contains(s3IndexHTML, want) {
			t.Fatalf("s3 dashboard HTML missing %q", want)
		}
	}
	for _, forbidden := range []string{
		`data-bucket="' + bucket.name + '"`,
		`<td class="key">' + object.key + '</td>`,
		`<dt>Key</dt><dd>' + object.key + '</dd>`,
		`metadata[key] + '</dd>`,
	} {
		if strings.Contains(s3IndexHTML, forbidden) {
			t.Fatalf("s3 dashboard HTML still contains unsafe interpolation %q", forbidden)
		}
	}
}
