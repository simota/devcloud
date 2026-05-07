package gcs

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	s3svc "devcloud/internal/services/s3"
)

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
