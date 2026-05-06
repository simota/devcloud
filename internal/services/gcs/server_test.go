package gcs

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

func TestBucketLifecycleAndListBuckets(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{Project: "devcloud", Location: "US"}, store).routes()

	create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket","location":"US","storageClass":"STANDARD"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, want %d; body=%s", create.Code, http.StatusOK, create.Body.String())
	}
	var created bucketResource
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.Name != "demo-bucket" || created.Kind != "storage#bucket" || created.StorageClass != "STANDARD" {
		t.Fatalf("created bucket = %#v", created)
	}

	get := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d; body=%s", get.Code, http.StatusOK, get.Body.String())
	}
	var got bucketResource
	if err := json.NewDecoder(get.Body).Decode(&got); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if got.Name != "demo-bucket" {
		t.Fatalf("get bucket name = %q", got.Name)
	}

	list := performRequest(routes, http.MethodGet, "/storage/v1/b?project=devcloud", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed bucketsListResponse
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listed.Items) != 1 || listed.Items[0].Name != "demo-bucket" {
		t.Fatalf("listed buckets = %#v", listed.Items)
	}

	deleteBucket := performRequest(routes, http.MethodDelete, "/storage/v1/b/demo-bucket", "")
	if deleteBucket.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d; body=%s", deleteBucket.Code, http.StatusNoContent, deleteBucket.Body.String())
	}

	missing := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing get status = %d, want %d", missing.Code, http.StatusNotFound)
	}
}

func TestBucketInsertRejectsDuplicateAndInvalidJSON(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	duplicate := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate status = %d, want %d; body=%s", duplicate.Code, http.StatusConflict, duplicate.Body.String())
	}

	invalid := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid json status = %d, want %d; body=%s", invalid.Code, http.StatusBadRequest, invalid.Body.String())
	}
}

func TestAuthModes(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())

	relaxed := NewServer(Config{AuthMode: "relaxed"}, store).routes()
	if rec := performRequest(relaxed, http.MethodGet, "/storage/v1/b?project=devcloud", ""); rec.Code != http.StatusOK {
		t.Fatalf("relaxed status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	oauthRelaxed := NewServer(Config{AuthMode: "oauth-relaxed"}, store).routes()
	if rec := performRequest(oauthRelaxed, http.MethodGet, "/storage/v1/b?project=devcloud", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("oauth-relaxed missing bearer status = %d, want %d; body=%s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
	if rec := performRequestWithHeaders(oauthRelaxed, http.MethodGet, "/storage/v1/b?project=devcloud", "", map[string]string{"Authorization": "Bearer local-token"}); rec.Code != http.StatusOK {
		t.Fatalf("oauth-relaxed bearer status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	bearerDev := NewServer(Config{AuthMode: "bearer-dev", BearerToken: "expected-token"}, store).routes()
	if rec := performRequestWithHeaders(bearerDev, http.MethodGet, "/storage/v1/b?project=devcloud", "", map[string]string{"Authorization": "Bearer wrong-token"}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bearer-dev wrong token status = %d, want %d; body=%s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
	if rec := performRequestWithHeaders(bearerDev, http.MethodGet, "/storage/v1/b?project=devcloud", "", map[string]string{"Authorization": "Bearer expected-token"}); rec.Code != http.StatusOK {
		t.Fatalf("bearer-dev matching token status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestBucketsListSupportsPagination(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	for _, name := range []string{"alpha-bucket", "beta-bucket", "gamma-bucket"} {
		create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"`+name+`"}`)
		if create.Code != http.StatusOK {
			t.Fatalf("create %q status = %d; body=%s", name, create.Code, create.Body.String())
		}
	}

	firstPage := performRequest(routes, http.MethodGet, "/storage/v1/b?project=devcloud&maxResults=2", "")
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d; body=%s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first bucketsListResponse
	if err := json.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(first.Items) != 2 || first.Items[0].Name != "alpha-bucket" || first.Items[1].Name != "beta-bucket" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}

	secondPage := performRequest(routes, http.MethodGet, "/storage/v1/b?project=devcloud&pageToken="+url.QueryEscape(first.NextPageToken), "")
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, want %d; body=%s", secondPage.Code, http.StatusOK, secondPage.Body.String())
	}
	var second bucketsListResponse
	if err := json.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(second.Items) != 1 || second.Items[0].Name != "gamma-bucket" || second.NextPageToken != "" {
		t.Fatalf("second page = %#v", second)
	}

	invalid := performRequest(routes, http.MethodGet, "/storage/v1/b?project=devcloud&pageToken=bad", "")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid page token status = %d, want %d; body=%s", invalid.Code, http.StatusBadRequest, invalid.Body.String())
	}
}

func TestBucketsListSupportsPrefixFilter(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	for _, name := range []string{"app-assets", "app-logs", "backup-archive"} {
		create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"`+name+`"}`)
		if create.Code != http.StatusOK {
			t.Fatalf("create %q status = %d; body=%s", name, create.Code, create.Body.String())
		}
	}

	list := performRequest(routes, http.MethodGet, "/storage/v1/b?project=devcloud&prefix=app-", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed bucketsListResponse
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listed.Items) != 2 || listed.Items[0].Name != "app-assets" || listed.Items[1].Name != "app-logs" {
		t.Fatalf("listed buckets = %#v", listed.Items)
	}
}

func TestBucketDeleteRequiresEmptyBucket(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}
	if _, err := store.PutObject(httptest.NewRequest(http.MethodGet, "/", nil).Context(), s3svc.PutObjectInput{
		Bucket: "demo-bucket",
		Key:    "object.txt",
		Body:   strings.NewReader("body"),
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}

	deleteBucket := performRequest(routes, http.MethodDelete, "/storage/v1/b/demo-bucket", "")
	if deleteBucket.Code != http.StatusConflict {
		t.Fatalf("delete non-empty status = %d, want %d; body=%s", deleteBucket.Code, http.StatusConflict, deleteBucket.Body.String())
	}
}

func TestObjectMediaLifecycleListRangeAndCopy(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/readme.txt", "hello from devcloud gcs\n", map[string]string{
		"Content-Type":       "text/plain",
		"x-goog-meta-source": "unit-test",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}
	if uploaded.Name != "docs/readme.txt" || uploaded.Generation == "" || uploaded.Metageneration != "1" || uploaded.Metadata["source"] != "unit-test" {
		t.Fatalf("uploaded object = %#v", uploaded)
	}

	metadata := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o/docs%2Freadme.txt", "")
	if metadata.Code != http.StatusOK {
		t.Fatalf("metadata status = %d, want %d; body=%s", metadata.Code, http.StatusOK, metadata.Body.String())
	}
	var got objectResource
	if err := json.NewDecoder(metadata.Body).Decode(&got); err != nil {
		t.Fatalf("decode metadata response: %v", err)
	}
	if got.Name != "docs/readme.txt" || got.MD5Hash == "" || got.CRC32C == "" {
		t.Fatalf("metadata object = %#v", got)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Freadme.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "hello from devcloud gcs\n" {
		t.Fatalf("download body = %q", download.Body.String())
	}

	jsonAltDownload := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o/docs%2Freadme.txt?alt=media", "")
	if jsonAltDownload.Code != http.StatusOK {
		t.Fatalf("json alt media status = %d, want %d; body=%s", jsonAltDownload.Code, http.StatusOK, jsonAltDownload.Body.String())
	}
	if jsonAltDownload.Body.String() != "hello from devcloud gcs\n" {
		t.Fatalf("json alt media body = %q", jsonAltDownload.Body.String())
	}
	if got := jsonAltDownload.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("json alt media Content-Type = %q, want text/plain", got)
	}

	rangeDownload := performRequestWithHeaders(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Freadme.txt?alt=media", "", map[string]string{"Range": "bytes=0-4"})
	if rangeDownload.Code != http.StatusPartialContent {
		t.Fatalf("range status = %d, want %d; body=%s", rangeDownload.Code, http.StatusPartialContent, rangeDownload.Body.String())
	}
	if rangeDownload.Body.String() != "hello" {
		t.Fatalf("range body = %q", rangeDownload.Body.String())
	}
	unsatisfiableRange := performRequestWithHeaders(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Freadme.txt?alt=media", "", map[string]string{"Range": "bytes=999-1000"})
	if unsatisfiableRange.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("unsatisfiable range status = %d, want %d; body=%s", unsatisfiableRange.Code, http.StatusRequestedRangeNotSatisfiable, unsatisfiableRange.Body.String())
	}
	if got := unsatisfiableRange.Header().Get("Content-Range"); got != "bytes */24" {
		t.Fatalf("unsatisfiable range Content-Range = %q, want bytes */24", got)
	}

	list := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o?prefix=docs/", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed objectsListResponse
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listed.Items) != 1 || listed.Items[0].Name != "docs/readme.txt" {
		t.Fatalf("listed objects = %#v", listed.Items)
	}

	copyObject := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/docs%2Freadme.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt", `{}`)
	if copyObject.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want %d; body=%s", copyObject.Code, http.StatusOK, copyObject.Body.String())
	}
	var copied objectResource
	if err := json.NewDecoder(copyObject.Body).Decode(&copied); err != nil {
		t.Fatalf("decode copy response: %v", err)
	}
	if copied.Name != "docs/copy.txt" {
		t.Fatalf("copied object = %#v", copied)
	}

	deleteCopy := performRequest(routes, http.MethodDelete, "/storage/v1/b/demo-bucket/o/docs%2Fcopy.txt", "")
	if deleteCopy.Code != http.StatusNoContent {
		t.Fatalf("delete copy status = %d, want %d; body=%s", deleteCopy.Code, http.StatusNoContent, deleteCopy.Body.String())
	}
}

func TestObjectHeadReturnsGCSMetadataHeadersWithoutBody(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/head.txt", "hello metadata", map[string]string{
		"Content-Type":        "text/plain",
		"Cache-Control":       "no-cache",
		"Content-Disposition": "inline",
		"x-goog-meta-source":  "head-test",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}

	metadata := performRequest(routes, http.MethodHead, "/storage/v1/b/demo-bucket/o/docs%2Fhead.txt", "")
	if metadata.Code != http.StatusOK {
		t.Fatalf("metadata HEAD status = %d, want %d; body=%s", metadata.Code, http.StatusOK, metadata.Body.String())
	}
	if metadata.Body.Len() != 0 {
		t.Fatalf("metadata HEAD body len = %d, want 0", metadata.Body.Len())
	}
	for key, want := range map[string]string{
		"X-Goog-Metageneration":        "1",
		"X-Goog-Stored-Content-Length": "14",
		"X-Goog-Meta-Source":           "head-test",
		"Cache-Control":                "no-cache",
		"Content-Disposition":          "inline",
	} {
		if got := metadata.Header().Get(key); got != want {
			t.Fatalf("metadata HEAD %s = %q, want %q", key, got, want)
		}
	}
	if got := metadata.Header().Get("X-Goog-Generation"); got == "" {
		t.Fatalf("metadata HEAD X-Goog-Generation is empty")
	}
	if got := metadata.Header().Get("ETag"); got == "" {
		t.Fatalf("metadata HEAD ETag is empty")
	}
	if got := metadata.Header().Get("X-Goog-Hash"); !strings.Contains(got, "crc32c=") || !strings.Contains(got, "md5=") {
		t.Fatalf("metadata HEAD X-Goog-Hash = %q, want crc32c and md5", got)
	}

	download := performRequest(routes, http.MethodHead, "/download/storage/v1/b/demo-bucket/o/docs%2Fhead.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download HEAD status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.Len() != 0 {
		t.Fatalf("download HEAD body len = %d, want 0", download.Body.Len())
	}
	if got := download.Header().Get("Content-Length"); got != "14" {
		t.Fatalf("download HEAD Content-Length = %q, want 14", got)
	}
	if got := download.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("download HEAD Content-Type = %q, want text/plain", got)
	}

	rangeDownload := performRequestWithHeaders(routes, http.MethodHead, "/download/storage/v1/b/demo-bucket/o/docs%2Fhead.txt?alt=media", "", map[string]string{"Range": "bytes=0-4"})
	if rangeDownload.Code != http.StatusPartialContent {
		t.Fatalf("range HEAD status = %d, want %d; body=%s", rangeDownload.Code, http.StatusPartialContent, rangeDownload.Body.String())
	}
	if got := rangeDownload.Header().Get("Content-Range"); got != "bytes 0-4/14" {
		t.Fatalf("range HEAD Content-Range = %q, want bytes 0-4/14", got)
	}
	if got := rangeDownload.Header().Get("Content-Length"); got != "5" {
		t.Fatalf("range HEAD Content-Length = %q, want 5", got)
	}
}

func TestObjectsListSupportsDelimiterPrefixes(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	for name, body := range map[string]string{
		"docs/readme.txt":       "readme",
		"docs/guides/setup.txt": "setup",
		"docs/guides/run.txt":   "run",
		"docs/archive.zip":      "archive",
	} {
		target := "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=" + url.QueryEscape(name)
		if upload := performRequestWithHeaders(routes, http.MethodPost, target, body, map[string]string{"Content-Type": "text/plain"}); upload.Code != http.StatusOK {
			t.Fatalf("upload %q status = %d; body=%s", name, upload.Code, upload.Body.String())
		}
	}

	list := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed objectsListResponse
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listed.Items) != 2 {
		t.Fatalf("listed items length = %d, want 2; items=%#v", len(listed.Items), listed.Items)
	}
	gotItems := map[string]bool{}
	for _, item := range listed.Items {
		gotItems[item.Name] = true
	}
	if !gotItems["docs/archive.zip"] || !gotItems["docs/readme.txt"] {
		t.Fatalf("listed item names = %#v", gotItems)
	}
	if len(listed.Prefixes) != 1 || listed.Prefixes[0] != "docs/guides/" {
		t.Fatalf("listed prefixes = %#v, want [docs/guides/]", listed.Prefixes)
	}
}

func TestObjectsListSupportsIncludeTrailingDelimiter(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	for name, body := range map[string]string{
		"docs/folder/":          "",
		"docs/folder/readme.md": "readme",
		"docs/readme.txt":       "readme",
	} {
		target := "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=" + url.QueryEscape(name)
		if upload := performRequestWithHeaders(routes, http.MethodPost, target, body, map[string]string{"Content-Type": "text/plain"}); upload.Code != http.StatusOK {
			t.Fatalf("upload %q status = %d; body=%s", name, upload.Code, upload.Body.String())
		}
	}

	withoutMarker := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/", "")
	if withoutMarker.Code != http.StatusOK {
		t.Fatalf("list without marker status = %d, want %d; body=%s", withoutMarker.Code, http.StatusOK, withoutMarker.Body.String())
	}
	var defaultList objectsListResponse
	if err := json.NewDecoder(withoutMarker.Body).Decode(&defaultList); err != nil {
		t.Fatalf("decode default list response: %v", err)
	}
	if len(defaultList.Items) != 1 || defaultList.Items[0].Name != "docs/readme.txt" {
		t.Fatalf("default listed items = %#v", defaultList.Items)
	}
	if len(defaultList.Prefixes) != 1 || defaultList.Prefixes[0] != "docs/folder/" {
		t.Fatalf("default listed prefixes = %#v, want [docs/folder/]", defaultList.Prefixes)
	}

	withMarker := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/&includeTrailingDelimiter=true", "")
	if withMarker.Code != http.StatusOK {
		t.Fatalf("list with marker status = %d, want %d; body=%s", withMarker.Code, http.StatusOK, withMarker.Body.String())
	}
	var listed objectsListResponse
	if err := json.NewDecoder(withMarker.Body).Decode(&listed); err != nil {
		t.Fatalf("decode marker list response: %v", err)
	}
	if len(listed.Items) != 2 || listed.Items[0].Name != "docs/folder/" || listed.Items[1].Name != "docs/readme.txt" {
		t.Fatalf("listed marker items = %#v", listed.Items)
	}
	if len(listed.Prefixes) != 1 || listed.Prefixes[0] != "docs/folder/" {
		t.Fatalf("listed marker prefixes = %#v, want [docs/folder/]", listed.Prefixes)
	}
}

func TestObjectsListSupportsPaginationAcrossItemsAndPrefixes(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	for name, body := range map[string]string{
		"docs/a.txt":      "a",
		"docs/b/file.txt": "b",
		"docs/c/file.txt": "c",
		"docs/d.txt":      "d",
	} {
		target := "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=" + url.QueryEscape(name)
		if upload := performRequestWithHeaders(routes, http.MethodPost, target, body, map[string]string{"Content-Type": "text/plain"}); upload.Code != http.StatusOK {
			t.Fatalf("upload %q status = %d; body=%s", name, upload.Code, upload.Body.String())
		}
	}

	firstPage := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/&maxResults=2", "")
	if firstPage.Code != http.StatusOK {
		t.Fatalf("first page status = %d, want %d; body=%s", firstPage.Code, http.StatusOK, firstPage.Body.String())
	}
	var first objectsListResponse
	if err := json.NewDecoder(firstPage.Body).Decode(&first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(first.Items) != 1 || first.Items[0].Name != "docs/a.txt" || len(first.Prefixes) != 1 || first.Prefixes[0] != "docs/b/" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}

	secondPage := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/&pageToken="+url.QueryEscape(first.NextPageToken), "")
	if secondPage.Code != http.StatusOK {
		t.Fatalf("second page status = %d, want %d; body=%s", secondPage.Code, http.StatusOK, secondPage.Body.String())
	}
	var second objectsListResponse
	if err := json.NewDecoder(secondPage.Body).Decode(&second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(second.Items) != 1 || second.Items[0].Name != "docs/d.txt" || len(second.Prefixes) != 1 || second.Prefixes[0] != "docs/c/" || second.NextPageToken != "" {
		t.Fatalf("second page = %#v", second)
	}
}

func TestObjectsListSupportsStartAndEndOffset(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	for _, name := range []string{
		"docs/a.txt",
		"docs/b.txt",
		"docs/c/file.txt",
		"docs/d.txt",
	} {
		target := "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=" + url.QueryEscape(name)
		if upload := performRequestWithHeaders(routes, http.MethodPost, target, "body", map[string]string{"Content-Type": "text/plain"}); upload.Code != http.StatusOK {
			t.Fatalf("upload %q status = %d; body=%s", name, upload.Code, upload.Body.String())
		}
	}

	list := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o?prefix=docs/&startOffset=docs/b.txt&endOffset=docs/d.txt", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d; body=%s", list.Code, http.StatusOK, list.Body.String())
	}
	var listed objectsListResponse
	if err := json.NewDecoder(list.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listed.Items) != 2 || listed.Items[0].Name != "docs/b.txt" || listed.Items[1].Name != "docs/c/file.txt" {
		t.Fatalf("listed offset items = %#v", listed.Items)
	}

	delimited := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o?prefix=docs/&delimiter=/&startOffset=docs/b.txt&endOffset=docs/d.txt", "")
	if delimited.Code != http.StatusOK {
		t.Fatalf("delimited list status = %d, want %d; body=%s", delimited.Code, http.StatusOK, delimited.Body.String())
	}
	var delimitedList objectsListResponse
	if err := json.NewDecoder(delimited.Body).Decode(&delimitedList); err != nil {
		t.Fatalf("decode delimited list response: %v", err)
	}
	if len(delimitedList.Items) != 1 || delimitedList.Items[0].Name != "docs/b.txt" {
		t.Fatalf("delimited items = %#v", delimitedList.Items)
	}
	if len(delimitedList.Prefixes) != 1 || delimitedList.Prefixes[0] != "docs/c/" {
		t.Fatalf("delimited prefixes = %#v, want [docs/c/]", delimitedList.Prefixes)
	}
}

func TestObjectMultipartUploadUsesMetadataAndMediaParts(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	body := strings.Join([]string{
		"--devcloud-boundary",
		"Content-Type: application/json; charset=UTF-8",
		"",
		`{"name":"docs/multipart.txt","contentType":"text/plain","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"multipart-test"}}`,
		"--devcloud-boundary",
		"Content-Type: text/plain",
		"",
		"hello multipart",
		"--devcloud-boundary--",
		"",
	}, "\r\n")
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=multipart", body, map[string]string{
		"Content-Type": `multipart/related; boundary="devcloud-boundary"`,
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("multipart upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode multipart response: %v", err)
	}
	if uploaded.Name != "docs/multipart.txt" || uploaded.ContentType != "text/plain" || uploaded.Metadata["source"] != "multipart-test" {
		t.Fatalf("uploaded object = %#v", uploaded)
	}
	if uploaded.CacheControl != "no-cache" || uploaded.ContentDisposition != "inline" {
		t.Fatalf("uploaded cache/disposition = %#v", uploaded)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Fmultipart.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "hello multipart" {
		t.Fatalf("download body = %q", download.Body.String())
	}
}

func TestObjectInsertRejectsGenerationPreconditionMismatch(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	mismatch := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/precondition.txt&ifGenerationMatch=999999", "mismatch\n", map[string]string{
		"Content-Type": "text/plain",
	})
	if mismatch.Code != http.StatusPreconditionFailed {
		t.Fatalf("mismatch status = %d, want %d; body=%s", mismatch.Code, http.StatusPreconditionFailed, mismatch.Body.String())
	}

	createOnly := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/precondition.txt&ifGenerationMatch=0", "created\n", map[string]string{
		"Content-Type": "text/plain",
	})
	if createOnly.Code != http.StatusOK {
		t.Fatalf("create-only status = %d, want %d; body=%s", createOnly.Code, http.StatusOK, createOnly.Body.String())
	}
}

func TestObjectReadDownloadAndDeleteRejectGenerationPreconditionMismatch(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/precondition-read.txt", "body", map[string]string{
		"Content-Type": "text/plain",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}

	for _, tc := range []struct {
		name   string
		method string
		target string
	}{
		{
			name:   "metadata",
			method: http.MethodGet,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch=999999",
		},
		{
			name:   "download",
			method: http.MethodGet,
			target: "/download/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?alt=media&ifGenerationMatch=999999",
		},
		{
			name:   "delete",
			method: http.MethodDelete,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch=999999",
		},
	} {
		rec := performRequest(routes, tc.method, tc.target, "")
		if rec.Code != http.StatusPreconditionFailed {
			t.Fatalf("%s status = %d, want %d; body=%s", tc.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
		}
	}

	metadata := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch="+uploaded.Generation, "")
	if metadata.Code != http.StatusOK {
		t.Fatalf("matching metadata status = %d, want %d; body=%s", metadata.Code, http.StatusOK, metadata.Body.String())
	}
	deleteObject := performRequest(routes, http.MethodDelete, "/storage/v1/b/demo-bucket/o/docs%2Fprecondition-read.txt?ifGenerationMatch="+uploaded.Generation, "")
	if deleteObject.Code != http.StatusNoContent {
		t.Fatalf("matching delete status = %d, want %d; body=%s", deleteObject.Code, http.StatusNoContent, deleteObject.Body.String())
	}
}

func TestObjectOperationsRejectInvalidPreconditionValues(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	if upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/source.txt", "body", map[string]string{"Content-Type": "text/plain"}); upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d; body=%s", upload.Code, upload.Body.String())
	}

	for _, tc := range []struct {
		name   string
		method string
		target string
		body   string
	}{
		{
			name:   "media upload invalid generation match",
			method: http.MethodPost,
			target: "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/new.txt&ifGenerationMatch=bad",
			body:   "new body",
		},
		{
			name:   "metadata invalid metageneration match",
			method: http.MethodGet,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?ifMetagenerationMatch=bad",
		},
		{
			name:   "download invalid generation not match",
			method: http.MethodGet,
			target: "/download/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?alt=media&ifGenerationNotMatch=bad",
		},
		{
			name:   "patch invalid metageneration not match",
			method: http.MethodPatch,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?ifMetagenerationNotMatch=bad",
			body:   `{"metadata":{"source":"invalid"}}`,
		},
		{
			name:   "delete invalid generation match",
			method: http.MethodDelete,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt?ifGenerationMatch=bad",
		},
		{
			name:   "copy invalid source metageneration",
			method: http.MethodPost,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt?ifSourceMetagenerationMatch=bad",
			body:   `{}`,
		},
		{
			name:   "compose invalid destination generation",
			method: http.MethodPost,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fjoined.txt/compose?ifGenerationMatch=bad",
			body:   `{"sourceObjects":[{"name":"docs/source.txt"}]}`,
		},
	} {
		rec := performRequest(routes, tc.method, tc.target, tc.body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d; body=%s", tc.name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
	}
}

func TestObjectOperationsHonorGenerationQuery(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/versioned.txt", "body", map[string]string{
		"Content-Type": "text/plain",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}

	for _, tc := range []struct {
		name   string
		method string
		target string
	}{
		{
			name:   "metadata",
			method: http.MethodGet,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=" + uploaded.Generation,
		},
		{
			name:   "download",
			method: http.MethodGet,
			target: "/download/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?alt=media&generation=" + uploaded.Generation,
		},
		{
			name:   "patch",
			method: http.MethodPatch,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=" + uploaded.Generation,
		},
	} {
		body := ""
		if tc.method == http.MethodPatch {
			body = `{"metadata":{"source":"generation-query"}}`
		}
		rec := performRequest(routes, tc.method, tc.target, body)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want %d; body=%s", tc.name, rec.Code, http.StatusOK, rec.Body.String())
		}
	}

	for _, tc := range []struct {
		name   string
		method string
		target string
	}{
		{
			name:   "metadata",
			method: http.MethodGet,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=1",
		},
		{
			name:   "download",
			method: http.MethodGet,
			target: "/download/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?alt=media&generation=1",
		},
		{
			name:   "patch",
			method: http.MethodPatch,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=1",
		},
		{
			name:   "delete",
			method: http.MethodDelete,
			target: "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=1",
		},
	} {
		rec := performRequest(routes, tc.method, tc.target, `{"metadata":{"source":"stale"}}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s stale generation status = %d, want %d; body=%s", tc.name, rec.Code, http.StatusNotFound, rec.Body.String())
		}
	}

	invalid := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation=bad", "")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid generation status = %d, want %d; body=%s", invalid.Code, http.StatusBadRequest, invalid.Body.String())
	}

	deleteObject := performRequest(routes, http.MethodDelete, "/storage/v1/b/demo-bucket/o/docs%2Fversioned.txt?generation="+uploaded.Generation, "")
	if deleteObject.Code != http.StatusNoContent {
		t.Fatalf("matching delete status = %d, want %d; body=%s", deleteObject.Code, http.StatusNoContent, deleteObject.Body.String())
	}
}

func TestObjectPatchUpdatesMetadataAndMetageneration(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/metadata.txt", "body", map[string]string{
		"Content-Type":       "text/plain",
		"x-goog-meta-source": "initial",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}

	patch := performRequest(routes, http.MethodPatch, "/storage/v1/b/demo-bucket/o/docs%2Fmetadata.txt?ifMetagenerationMatch=1", `{"contentType":"text/markdown","cacheControl":"no-cache","metadata":{"source":"patched","owner":"gcs"}}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want %d; body=%s", patch.Code, http.StatusOK, patch.Body.String())
	}
	var patched objectResource
	if err := json.NewDecoder(patch.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patch response: %v", err)
	}
	if patched.Generation != uploaded.Generation || patched.Metageneration != "2" {
		t.Fatalf("patched generation/metageneration = %s/%s, uploaded generation = %s", patched.Generation, patched.Metageneration, uploaded.Generation)
	}
	if patched.ContentType != "text/markdown" || patched.CacheControl != "no-cache" || patched.Metadata["source"] != "patched" || patched.Metadata["owner"] != "gcs" {
		t.Fatalf("patched metadata = %#v", patched)
	}

	mismatch := performRequest(routes, http.MethodPatch, "/storage/v1/b/demo-bucket/o/docs%2Fmetadata.txt?ifMetagenerationMatch=1", `{"metadata":{"source":"stale"}}`)
	if mismatch.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale metageneration status = %d, want %d; body=%s", mismatch.Code, http.StatusPreconditionFailed, mismatch.Body.String())
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Fmetadata.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "body" {
		t.Fatalf("download body = %q", download.Body.String())
	}
	if got := download.Header().Get("Content-Type"); got != "text/markdown" {
		t.Fatalf("download Content-Type = %q, want text/markdown", got)
	}
}

func TestObjectCopyEnforcesSourcePreconditions(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/source.txt", "copy source", map[string]string{
		"Content-Type": "text/plain",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}

	mismatch := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt?ifSourceGenerationMatch=999999", `{}`)
	if mismatch.Code != http.StatusPreconditionFailed {
		t.Fatalf("mismatch status = %d, want %d; body=%s", mismatch.Code, http.StatusPreconditionFailed, mismatch.Body.String())
	}

	copyObject := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt?ifSourceGenerationMatch="+uploaded.Generation+"&ifSourceMetagenerationMatch=1", `{}`)
	if copyObject.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want %d; body=%s", copyObject.Code, http.StatusOK, copyObject.Body.String())
	}
	var copied objectResource
	if err := json.NewDecoder(copyObject.Body).Decode(&copied); err != nil {
		t.Fatalf("decode copy response: %v", err)
	}
	if copied.Name != "docs/copy.txt" {
		t.Fatalf("copied object = %#v", copied)
	}
}

func TestObjectCopyUsesDestinationMetadataWhenProvided(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/source.txt", "copy source", map[string]string{
		"Content-Type":       "text/plain",
		"x-goog-meta-source": "original",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}

	copyObject := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/copyTo/b/demo-bucket/o/docs%2Fcopy.txt", `{"contentType":"text/markdown","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"copy-request","owner":"gcs"}}`)
	if copyObject.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want %d; body=%s", copyObject.Code, http.StatusOK, copyObject.Body.String())
	}
	var copied objectResource
	if err := json.NewDecoder(copyObject.Body).Decode(&copied); err != nil {
		t.Fatalf("decode copy response: %v", err)
	}
	if copied.ContentType != "text/markdown" || copied.CacheControl != "no-cache" || copied.ContentDisposition != "inline" {
		t.Fatalf("copied metadata fields = %#v", copied)
	}
	if copied.Metadata["source"] != "copy-request" || copied.Metadata["owner"] != "gcs" {
		t.Fatalf("copied user metadata = %#v", copied.Metadata)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Fcopy.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "copy source" {
		t.Fatalf("download body = %q", download.Body.String())
	}
	if got := download.Header().Get("Content-Type"); got != "text/markdown" {
		t.Fatalf("download Content-Type = %q, want text/markdown", got)
	}
}

func TestObjectRewriteCopiesExistingObject(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/source.txt", "rewrite source", map[string]string{
		"Content-Type":       "text/plain",
		"x-goog-meta-source": "rewrite-test",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}
	var uploaded objectResource
	if err := json.NewDecoder(upload.Body).Decode(&uploaded); err != nil {
		t.Fatalf("decode upload response: %v", err)
	}

	rewrite := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/docs%2Fsource.txt/rewriteTo/b/demo-bucket/o/docs%2Frewrite.txt?ifSourceGenerationMatch="+uploaded.Generation, `{}`)
	if rewrite.Code != http.StatusOK {
		t.Fatalf("rewrite status = %d, want %d; body=%s", rewrite.Code, http.StatusOK, rewrite.Body.String())
	}
	var response rewriteResponse
	if err := json.NewDecoder(rewrite.Body).Decode(&response); err != nil {
		t.Fatalf("decode rewrite response: %v", err)
	}
	if response.Kind != "storage#rewriteResponse" || !response.Done || response.Resource.Name != "docs/rewrite.txt" {
		t.Fatalf("rewrite response = %#v", response)
	}
	if response.TotalBytesRewritten != "14" || response.ObjectSize != "14" {
		t.Fatalf("rewrite size fields = %#v", response)
	}
	if response.Resource.ContentType != "text/plain" || response.Resource.Metadata["source"] != "rewrite-test" {
		t.Fatalf("rewritten resource = %#v", response.Resource)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Frewrite.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "rewrite source" {
		t.Fatalf("download body = %q", download.Body.String())
	}
}

func TestObjectComposeConcatenatesSources(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	firstUpload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=parts/one.txt", "hello ", map[string]string{
		"Content-Type": "text/plain",
	})
	if firstUpload.Code != http.StatusOK {
		t.Fatalf("first upload status = %d, want %d; body=%s", firstUpload.Code, http.StatusOK, firstUpload.Body.String())
	}
	var first objectResource
	if err := json.NewDecoder(firstUpload.Body).Decode(&first); err != nil {
		t.Fatalf("decode first upload response: %v", err)
	}
	secondUpload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=parts/two.txt", "compose", map[string]string{
		"Content-Type": "text/plain",
	})
	if secondUpload.Code != http.StatusOK {
		t.Fatalf("second upload status = %d, want %d; body=%s", secondUpload.Code, http.StatusOK, secondUpload.Body.String())
	}

	compose := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt/compose", `{"sourceObjects":[{"name":"parts/one.txt","generation":"`+first.Generation+`"},{"name":"parts/two.txt"}],"destination":{"contentType":"text/plain","metadata":{"source":"compose-test"}}}`)
	if compose.Code != http.StatusOK {
		t.Fatalf("compose status = %d, want %d; body=%s", compose.Code, http.StatusOK, compose.Body.String())
	}
	var composed objectResource
	if err := json.NewDecoder(compose.Body).Decode(&composed); err != nil {
		t.Fatalf("decode compose response: %v", err)
	}
	if composed.Name != "parts/joined.txt" || composed.ContentType != "text/plain" || composed.Metadata["source"] != "compose-test" {
		t.Fatalf("composed object = %#v", composed)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "hello compose" {
		t.Fatalf("download body = %q", download.Body.String())
	}
}

func TestObjectComposeRejectsSourceGenerationMismatch(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	upload := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=parts/source.txt", "body", map[string]string{
		"Content-Type": "text/plain",
	})
	if upload.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body=%s", upload.Code, http.StatusOK, upload.Body.String())
	}

	compose := performRequest(routes, http.MethodPost, "/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt/compose", `{"sourceObjects":[{"name":"parts/source.txt","objectPreconditions":{"ifGenerationMatch":"999999"}}]}`)
	if compose.Code != http.StatusPreconditionFailed {
		t.Fatalf("compose status = %d, want %d; body=%s", compose.Code, http.StatusPreconditionFailed, compose.Body.String())
	}
	missing := performRequest(routes, http.MethodGet, "/storage/v1/b/demo-bucket/o/parts%2Fjoined.txt", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("composed object status = %d, want %d; body=%s", missing.Code, http.StatusNotFound, missing.Body.String())
	}
}

func TestResumableUploadCreatesSessionAndCommitsObject(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/resumable.txt", `{"name":"docs/resumable.txt","contentType":"text/plain"}`, map[string]string{
		"Content-Type":          "application/json",
		"X-Upload-Content-Type": "text/plain",
		"x-goog-meta-source":    "resumable-test",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	location := init.Header().Get("Location")
	if location == "" {
		t.Fatalf("init Location is empty")
	}
	uploadID := init.Header().Get("X-GUploader-UploadID")
	if uploadID == "" {
		t.Fatalf("init X-GUploader-UploadID is empty")
	}
	sessionURL, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}
	if got := sessionURL.Query().Get("upload_id"); got != uploadID {
		t.Fatalf("session upload_id = %q, want X-GUploader-UploadID %q", got, uploadID)
	}

	commit := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "resumable body", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 0-13/14",
	})
	if commit.Code != http.StatusOK {
		t.Fatalf("commit status = %d, want %d; body=%s", commit.Code, http.StatusOK, commit.Body.String())
	}
	var committed objectResource
	if err := json.NewDecoder(commit.Body).Decode(&committed); err != nil {
		t.Fatalf("decode commit response: %v", err)
	}
	if committed.Name != "docs/resumable.txt" || committed.Metadata["source"] != "resumable-test" {
		t.Fatalf("committed object = %#v", committed)
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Fresumable.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "resumable body" {
		t.Fatalf("download body = %q", download.Body.String())
	}
}

func TestResumableUploadUsesJSONMetadataFromInitiationRequest(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable", `{"name":"docs/resumable-metadata.txt","contentType":"text/plain","contentEncoding":"gzip","cacheControl":"no-cache","contentDisposition":"inline","metadata":{"source":"json-init","override":"json"}}`, map[string]string{
		"Content-Type":         "application/json",
		"x-goog-meta-override": "header",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	sessionURL, err := url.Parse(init.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}

	commit := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "metadata body", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 0-12/13",
	})
	if commit.Code != http.StatusOK {
		t.Fatalf("commit status = %d, want %d; body=%s", commit.Code, http.StatusOK, commit.Body.String())
	}
	var committed objectResource
	if err := json.NewDecoder(commit.Body).Decode(&committed); err != nil {
		t.Fatalf("decode commit response: %v", err)
	}
	if committed.Name != "docs/resumable-metadata.txt" || committed.ContentType != "text/plain" {
		t.Fatalf("committed object = %#v", committed)
	}
	if committed.ContentEncoding != "gzip" || committed.CacheControl != "no-cache" || committed.ContentDisposition != "inline" {
		t.Fatalf("committed cache/disposition/encoding = %#v", committed)
	}
	if committed.Metadata["source"] != "json-init" || committed.Metadata["override"] != "header" {
		t.Fatalf("committed metadata = %#v", committed.Metadata)
	}
}

func TestResumableUploadReadsJSONMetadataWithQueryNameAndUploadContentType(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/query-name.txt", `{"metadata":{"source":"json-init"},"cacheControl":"no-cache","contentDisposition":"inline"}`, map[string]string{
		"Content-Type":          "application/json",
		"X-Upload-Content-Type": "text/plain",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	sessionURL, err := url.Parse(init.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}

	commit := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "query metadata", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 0-13/14",
	})
	if commit.Code != http.StatusOK {
		t.Fatalf("commit status = %d, want %d; body=%s", commit.Code, http.StatusOK, commit.Body.String())
	}
	var committed objectResource
	if err := json.NewDecoder(commit.Body).Decode(&committed); err != nil {
		t.Fatalf("decode commit response: %v", err)
	}
	if committed.Name != "docs/query-name.txt" || committed.ContentType != "text/plain" {
		t.Fatalf("committed object = %#v", committed)
	}
	if committed.Metadata["source"] != "json-init" || committed.CacheControl != "no-cache" || committed.ContentDisposition != "inline" {
		t.Fatalf("committed metadata/cache/disposition = %#v", committed)
	}
}

func TestResumableUploadRechecksStoredPreconditionsAtCommit(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/race.txt&ifGenerationMatch=0", `{"name":"docs/race.txt","contentType":"text/plain"}`, map[string]string{
		"Content-Type":          "application/json",
		"X-Upload-Content-Type": "text/plain",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	sessionURL, err := url.Parse(init.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}

	racingWrite := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=media&name=docs/race.txt", "existing body", map[string]string{
		"Content-Type": "text/plain",
	})
	if racingWrite.Code != http.StatusOK {
		t.Fatalf("racing write status = %d, want %d; body=%s", racingWrite.Code, http.StatusOK, racingWrite.Body.String())
	}

	commit := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "new body", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 0-7/8",
	})
	if commit.Code != http.StatusPreconditionFailed {
		t.Fatalf("commit status = %d, want %d; body=%s", commit.Code, http.StatusPreconditionFailed, commit.Body.String())
	}

	download := performRequest(routes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Frace.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if download.Body.String() != "existing body" {
		t.Fatalf("download body = %q, want existing body", download.Body.String())
	}
}

func TestResumableUploadPersistsChunkStatusAcrossServerRestart(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	sessionDir := t.TempDir()
	routes := NewServer(Config{UploadSessionPath: sessionDir}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}
	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/chunked.txt", `{"name":"docs/chunked.txt","contentType":"text/plain"}`, map[string]string{
		"Content-Type":          "application/json",
		"X-Upload-Content-Type": "text/plain",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	sessionURL, err := url.Parse(init.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}

	initialStatus := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "", map[string]string{
		"Content-Range": "bytes */14",
	})
	if initialStatus.Code != 308 {
		t.Fatalf("initial status query code = %d, want 308; body=%s", initialStatus.Code, initialStatus.Body.String())
	}
	if got := initialStatus.Header().Get("Range"); got != "" {
		t.Fatalf("initial status query Range = %q, want empty", got)
	}

	firstChunk := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), "resumable ", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 0-9/14",
	})
	if firstChunk.Code != 308 {
		t.Fatalf("first chunk status = %d, want 308; body=%s", firstChunk.Code, firstChunk.Body.String())
	}
	if got := firstChunk.Header().Get("Range"); got != "bytes=0-9" {
		t.Fatalf("first chunk Range = %q, want bytes=0-9", got)
	}

	restartedRoutes := NewServer(Config{UploadSessionPath: sessionDir}, store).routes()
	status := performRequestWithHeaders(restartedRoutes, http.MethodPut, sessionURL.RequestURI(), "", map[string]string{
		"Content-Range": "bytes */14",
	})
	if status.Code != 308 {
		t.Fatalf("status query code = %d, want 308; body=%s", status.Code, status.Body.String())
	}
	if got := status.Header().Get("Range"); got != "bytes=0-9" {
		t.Fatalf("status Range = %q, want bytes=0-9", got)
	}

	commit := performRequestWithHeaders(restartedRoutes, http.MethodPut, sessionURL.RequestURI(), "body", map[string]string{
		"Content-Type":  "text/plain",
		"Content-Range": "bytes 10-13/14",
	})
	if commit.Code != http.StatusOK {
		t.Fatalf("commit status = %d, want %d; body=%s", commit.Code, http.StatusOK, commit.Body.String())
	}

	download := performRequest(restartedRoutes, http.MethodGet, "/download/storage/v1/b/demo-bucket/o/docs%2Fchunked.txt?alt=media", "")
	if download.Code != http.StatusOK {
		t.Fatalf("download status = %d, want %d; body=%s", download.Code, http.StatusOK, download.Body.String())
	}
	if got := download.Body.String(); got != "resumable body" {
		t.Fatalf("download body = %q", got)
	}
}

func TestResumableUploadRejectsMalformedCommitRequests(t *testing.T) {
	store := s3svc.NewFileBucketStore(t.TempDir())
	routes := NewServer(Config{}, store).routes()

	if create := performRequest(routes, http.MethodPost, "/storage/v1/b?project=devcloud", `{"name":"demo-bucket"}`); create.Code != http.StatusOK {
		t.Fatalf("create bucket status = %d; body=%s", create.Code, create.Body.String())
	}

	missingID := performRequestWithHeaders(routes, http.MethodPut, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable", "body", map[string]string{
		"Content-Range": "bytes 0-3/4",
	})
	if missingID.Code != http.StatusBadRequest {
		t.Fatalf("missing upload id status = %d, want %d; body=%s", missingID.Code, http.StatusBadRequest, missingID.Body.String())
	}

	unknownID := performRequestWithHeaders(routes, http.MethodPut, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&upload_id=missing", "body", map[string]string{
		"Content-Range": "bytes 0-3/4",
	})
	if unknownID.Code != http.StatusNotFound {
		t.Fatalf("unknown upload id status = %d, want %d; body=%s", unknownID.Code, http.StatusNotFound, unknownID.Body.String())
	}

	init := performRequestWithHeaders(routes, http.MethodPost, "/upload/storage/v1/b/demo-bucket/o?uploadType=resumable&name=docs/resumable.txt", `{"contentType":"text/plain"}`, map[string]string{
		"Content-Type": "application/json",
	})
	if init.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d; body=%s", init.Code, http.StatusOK, init.Body.String())
	}
	sessionURL, err := url.Parse(init.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse session location: %v", err)
	}

	for _, tc := range []struct {
		name         string
		contentRange string
		body         string
	}{
		{name: "wrong unit", contentRange: "items 0-3/4", body: "body"},
		{name: "payload size mismatch", contentRange: "bytes 0-9/10", body: "body"},
		{name: "wrong committed offset", contentRange: "bytes 1-4/5", body: "body"},
	} {
		rec := performRequestWithHeaders(routes, http.MethodPut, sessionURL.RequestURI(), tc.body, map[string]string{
			"Content-Type":  "text/plain",
			"Content-Range": tc.contentRange,
		})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d; body=%s", tc.name, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
	}
}

func performRequest(handler http.Handler, method string, target string, body string) *httptest.ResponseRecorder {
	return performRequestWithHeaders(handler, method, target, body, nil)
}

func performRequestWithHeaders(handler http.Handler, method string, target string, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}
